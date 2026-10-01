//! A dedicated OpenCode profile: host login state is never read or mutated implicitly.
use super::{models::OpenCodeCatalog, process::ServerProcess, stop::MarkerInstance};
use crate::error::{CalmError, Result};
use serde::Deserialize;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

pub const CONFIG_FLAG: &str = "--opencode-planner-config";
pub const PINNED_VERSION: &str = "1.18.34";

#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OpenCodePlannerConfig {
    pub opencode_binary: PathBuf,
    pub opencode_version: String,
    /// Private profile root: HOME and all XDG directories are children of it. The owner
    /// configures provider credentials here explicitly, independently of their normal profile.
    pub config_dir: PathBuf,
}

impl OpenCodePlannerConfig {
    pub fn read(path: &Path) -> Result<Self> {
        let config: Self = serde_json::from_slice(&std::fs::read(path)?)?;
        config.validate()?;
        Ok(config)
    }
    fn validate(&self) -> Result<()> {
        if self.opencode_version != PINNED_VERSION
            || !self.opencode_binary.is_absolute()
            || !self.config_dir.is_absolute()
        {
            return Err(CalmError::BadRequest(format!(
                "OpenCode Planner requires an absolute binary/profile and version {PINNED_VERSION}"
            )));
        }
        Ok(())
    }
    pub(crate) async fn verify_version(&self, env: &[(String, OsString)]) -> Result<()> {
        self.validate()?;
        let (status, bytes) = crate::claude_planner::readiness_command::run(
            &self.opencode_binary,
            &["--version"],
            env,
            Duration::from_secs(10),
        )
        .await
        .map_err(|e| CalmError::Conflict(format!("OpenCode version check: {e}")))?;
        if !status.success() || String::from_utf8_lossy(&bytes).trim() != self.opencode_version {
            return Err(CalmError::Conflict(format!(
                "OpenCode binary does not report pinned version {}",
                self.opencode_version
            )));
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct OpenCodePlannerHost {
    config: Option<OpenCodePlannerConfig>,
    pub instance: MarkerInstance,
    pub instructions_dir: PathBuf,
    pub mcp_shim: PathBuf,
    pub mcp_socket: PathBuf,
    pub(crate) scope_id: String,
    catalog_serial: tokio::sync::Mutex<()>,
    _scratch: Option<tempfile::TempDir>,
}

impl OpenCodePlannerHost {
    pub fn new(
        config: Option<OpenCodePlannerConfig>,
        data_dir: &Path,
        mcp_shim: PathBuf,
        mcp_socket: PathBuf,
    ) -> Result<Self> {
        if let Some(config) = &config {
            config.validate()?;
        }
        let instructions_dir = data_dir.join("opencode-planner");
        std::fs::create_dir_all(&instructions_dir)?;
        private_dir(&instructions_dir)?;
        let instance = MarkerInstance::for_data_dir(data_dir)?;
        use sha2::{Digest, Sha256};
        let profile = config
            .as_ref()
            .map(|c| {
                format!(
                    "{}:{}:{}",
                    c.config_dir.display(),
                    c.opencode_binary.display(),
                    c.opencode_version
                )
            })
            .unwrap_or_else(|| "unconfigured".into());
        let scope_id = format!(
            "{}:{}",
            instance.marker("opencode"),
            hex::encode(Sha256::digest(profile.as_bytes()))
        );
        Ok(Self {
            config,
            instance,
            instructions_dir,
            mcp_shim,
            mcp_socket,
            scope_id,
            catalog_serial: tokio::sync::Mutex::new(()),
            _scratch: None,
        })
    }
    pub fn unconfigured_scratch() -> Result<Self> {
        let scratch = tempfile::Builder::new()
            .prefix("calm-opencode-planner-")
            .tempdir()?;
        let mut host = Self::new(
            None,
            scratch.path(),
            "neige-mcp-stdio-shim".into(),
            scratch.path().join("mcp.sock"),
        )?;
        host._scratch = Some(scratch);
        Ok(host)
    }
    pub fn configured(&self) -> Result<&OpenCodePlannerConfig> {
        self.config.as_ref().ok_or_else(|| {
            CalmError::Conflict(format!(
                "OpenCode Planner unavailable: start calm-server with {CONFIG_FLAG}"
            ))
        })
    }
    pub(crate) fn environment(
        &self,
        id: &str,
        proxy: &[(String, String, String)],
    ) -> Result<Vec<(String, OsString)>> {
        let config = self.configured()?;
        let mut env = vec![
            (
                "PATH".into(),
                crate::kernel_bin_path::kernel_led_path()?.path,
            ),
            ("HOME".into(), config.config_dir.clone().into_os_string()),
        ];
        for (key, child) in [
            ("XDG_CONFIG_HOME", "config"),
            ("XDG_DATA_HOME", "data"),
            ("XDG_CACHE_HOME", "cache"),
            ("XDG_STATE_HOME", "state"),
        ] {
            let path = config.config_dir.join(child);
            std::fs::create_dir_all(&path)?;
            private_dir(&path)?;
            env.push((key.into(), path.into_os_string()));
        }
        env.push((
            crate::claude_planner::stop::MARKER_KEY.into(),
            self.instance.marker(&super::stop::identity(id)).into(),
        ));
        for key in [
            "OPENCODE_DISABLE_PROJECT_CONFIG",
            "OPENCODE_DISABLE_AUTOUPDATE",
            "OPENCODE_DISABLE_MODELS_FETCH",
        ] {
            env.push((key.into(), "1".into()));
        }
        for (upper, lower, value) in proxy {
            env.push((upper.clone(), value.clone().into()));
            env.push((lower.clone(), value.clone().into()));
        }
        Ok(env)
    }
    pub async fn check_ready(&self) -> Result<()> {
        self.catalog().await.map(|_| ())
    }
    pub async fn catalog(&self) -> Result<Arc<OpenCodeCatalog>> {
        let _check = self.catalog_serial.lock().await;
        let id = "readiness";
        let mut process =
            ServerProcess::start(self, id, &self.configured()?.config_dir, &[], None).await?;
        let result = async {
            let providers = process.client.get("/provider").await?;
            let config = process.client.get("/config").await?;
            OpenCodeCatalog::from_native(&providers, &config).map(Arc::new)
        }
        .await;
        let stopped = process.shutdown(self, id).await;
        stopped?;
        result
    }
}

pub(crate) fn private_dir(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}
