//! A single scoped serve process with private configuration and authenticated loopback transport.
use super::{client::Client, config::OpenCodePlannerHost, stop};
use crate::error::{CalmError, Result};
use serde_json::{Value, json};
use std::io::Write;
use std::path::Path;
use std::process::Stdio;
use std::time::Duration;
use tokio::io::AsyncReadExt;
use tokio::process::{Child, Command};

pub(crate) struct ServerProcess {
    pub(crate) client: Client,
    child: Child,
    _config: tempfile::NamedTempFile,
    instance: stop::MarkerInstance,
    id: String,
    stopped: bool,
}

impl ServerProcess {
    pub(crate) async fn start(
        host: &OpenCodePlannerHost,
        id: &str,
        cwd: &Path,
        proxy: &[(String, String, String)],
        token: Option<&str>,
    ) -> Result<Self> {
        let config = host.configured()?;
        let env = host.environment(id, proxy)?;
        config.verify_version(&env).await?;
        stop::stop(&host.instance, id).await?;
        let password = uuid::Uuid::new_v4().simple().to_string();
        // Automatic compaction creates synthetic user identities. This first adapter keeps
        // exact parent correlation and reports native context overflow instead of inventing lineage.
        let mut settings = json!({"autoupdate": false, "share":"disabled", "permission":{"question":"deny"},
            "compaction":{"auto":false,"prune":false}});
        if let Some(token) = token {
            let kernel_path = crate::kernel_bin_path::kernel_led_path()?;
            settings["mcp"] = json!({crate::mcp_server::wiring::MCP_SERVER_KEY: {"type":"local", "command":[host.mcp_shim], "environment": {
                "NEIGE_MCP_SOCKET":host.mcp_socket, "NEIGE_MCP_TOKEN":token,
                "PATH":kernel_path.path_utf8()?,
                (crate::claude_planner::stop::MARKER_KEY):host.instance.marker(&stop::identity(id)),
            }, "enabled":true}});
        }
        let mut file = tempfile::Builder::new()
            .prefix("serve-")
            .suffix(".json")
            .tempfile_in(&host.instructions_dir)?;
        serde_json::to_writer(file.as_file_mut(), &settings)?;
        file.as_file_mut().flush()?;
        // The owned child binds its listener before announcing its actual port. Never send
        // credentials to a port reserved then released by the kernel: another process could bind it.
        let mut command = Command::new(&config.opencode_binary);
        command
            .args([
                "serve",
                "--hostname",
                "127.0.0.1",
                "--port",
                "0",
                "--mdns=false",
            ])
            .env_clear()
            .envs(env)
            .env("OPENCODE_SERVER_PASSWORD", &password)
            .env("OPENCODE_CONFIG", file.path())
            .current_dir(cwd)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        // The same freshly minted Planner capability backs both MCP and the neige CLI
        // promised by Planner instructions. Ambient socket/tokens remain excluded.
        if let Some(token) = token {
            command
                .env("NEIGE_MCP_SOCKET", &host.mcp_socket)
                .env("NEIGE_MCP_TOKEN", token);
        }
        let mut child = command.spawn()?;
        let mut stdout = child
            .stdout
            .take()
            .ok_or_else(|| CalmError::Internal("OpenCode serve has no stdout pipe".into()))?;
        let announced =
            tokio::time::timeout(Duration::from_secs(20), listener_port(&mut stdout)).await;
        let port = match announced {
            Ok(Ok(port)) => port,
            failure => {
                let _ = stop::stop(&host.instance, id).await;
                let _ = child.start_kill();
                let _ = tokio::time::timeout(Duration::from_secs(3), child.wait()).await;
                return Err(CalmError::Conflict(format!(
                    "OpenCode did not announce its owned loopback listener: {failure:?}"
                )));
            }
        };
        for output in [
            Some(Box::pin(stdout) as std::pin::Pin<Box<dyn tokio::io::AsyncRead + Send>>),
            child
                .stderr
                .take()
                .map(|s| Box::pin(s) as std::pin::Pin<Box<dyn tokio::io::AsyncRead + Send>>),
        ]
        .into_iter()
        .flatten()
        {
            // Drain without retaining provider output, tokens or unbounded diagnostic logs.
            tokio::spawn(async move {
                let mut output = output;
                let mut buf = [0; 4096];
                while matches!(output.read(&mut buf).await, Ok(n) if n > 0) {}
            });
        }
        let client = Client::new(port, &password, cwd);
        let mut process = Self {
            client,
            child,
            _config: file,
            instance: host.instance.clone(),
            id: id.into(),
            stopped: false,
        };
        let ready = async {
            let until = tokio::time::Instant::now() + Duration::from_secs(20);
            loop {
                if let Some(status) = process.child.try_wait()? {
                    return Err(CalmError::Conflict(format!(
                        "OpenCode serve exited before readiness: {status}"
                    )));
                }
                if let Ok(health) = process.client.get("/global/health").await {
                    if health.get("healthy") == Some(&Value::Bool(true))
                        && health.get("version").and_then(Value::as_str)
                            == Some(config.opencode_version.as_str())
                    {
                        return Ok(());
                    }
                    return Err(CalmError::Conflict(
                        "OpenCode serve health/version did not match".into(),
                    ));
                }
                if tokio::time::Instant::now() >= until {
                    return Err(CalmError::Conflict(
                        "OpenCode serve did not become ready".into(),
                    ));
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
        .await;
        if let Err(error) = ready {
            process.shutdown(host, id).await?;
            return Err(error);
        }
        Ok(process)
    }

    pub(crate) async fn shutdown(&mut self, host: &OpenCodePlannerHost, id: &str) -> Result<()> {
        let sweep = stop::stop(&host.instance, id).await;
        let _ = self.child.start_kill();
        let reap = tokio::time::timeout(Duration::from_secs(3), self.child.wait())
            .await
            .map_err(|_| CalmError::Conflict("OpenCode direct child did not reap".into()));
        sweep?;
        reap??;
        self.stopped = true;
        Ok(())
    }
}

impl Drop for ServerProcess {
    fn drop(&mut self) {
        if !self.stopped
            && let Ok(runtime) = tokio::runtime::Handle::try_current()
        {
            let instance = self.instance.clone();
            let id = self.id.clone();
            runtime.spawn(async move {
                if let Err(error) = stop::stop(&instance, &id).await {
                    tracing::warn!(%error, "OpenCode process drop cleanup did not confirm");
                }
            });
        }
    }
}

async fn listener_port(stdout: &mut tokio::process::ChildStdout) -> Result<u16> {
    let mut line = Vec::new();
    for _ in 0..64 * 1024 {
        let byte = stdout.read_u8().await?;
        if byte != b'\n' {
            line.push(byte);
            continue;
        }
        let text = std::str::from_utf8(&line).map_err(|_| {
            CalmError::Conflict("OpenCode listener announcement is not UTF-8".into())
        })?;
        if let Some(address) = text
            .trim_end()
            .strip_prefix("opencode server listening on ")
        {
            let url = url::Url::parse(address).map_err(|_| {
                CalmError::Conflict("OpenCode listener announcement is malformed".into())
            })?;
            if url.scheme() != "http"
                || url.host_str() != Some("127.0.0.1")
                || url.path() != "/"
                || !url.username().is_empty()
                || url.password().is_some()
                || url.query().is_some()
                || url.fragment().is_some()
            {
                return Err(CalmError::Conflict(
                    "OpenCode did not announce the requested loopback listener".into(),
                ));
            }
            return url
                .port()
                .filter(|p| *p != 0)
                .ok_or_else(|| CalmError::Conflict("OpenCode announced no bound port".into()));
        }
        line.clear();
    }
    Err(CalmError::Conflict(
        "OpenCode listener announcement exceeded its byte budget".into(),
    ))
}
