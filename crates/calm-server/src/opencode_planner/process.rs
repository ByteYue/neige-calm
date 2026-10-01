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
        let socket = std::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))?;
        let port = socket.local_addr()?.port();
        let password = uuid::Uuid::new_v4().simple().to_string();
        let mut settings =
            json!({"autoupdate": false, "share":"disabled", "permission":{"question":"deny"}});
        if let Some(token) = token {
            settings["mcp"] = json!({"calm": {"type":"local", "command":[host.mcp_shim], "environment": {
                "NEIGE_MCP_SOCKET":host.mcp_socket, "NEIGE_MCP_TOKEN":token,
                "PATH":crate::kernel_bin_path::kernel_led_path()?.path,
                (crate::claude_planner::stop::MARKER_KEY):host.instance.marker(&stop::identity(id)),
            }, "enabled":true}});
        }
        let mut file = tempfile::Builder::new()
            .prefix("serve-")
            .suffix(".json")
            .tempfile_in(&host.instructions_dir)?;
        serde_json::to_writer(file.as_file_mut(), &settings)?;
        file.as_file_mut().flush()?;
        // The released port is authenticated by a per-spawn unpredictable password. A process
        // that wins the bind race cannot satisfy our subsequent authenticated version check.
        drop(socket);
        let mut child = Command::new(&config.opencode_binary)
            .args([
                "serve",
                "--hostname",
                "127.0.0.1",
                "--port",
                &port.to_string(),
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
            .kill_on_drop(true)
            .spawn()?;
        for output in [
            child
                .stdout
                .take()
                .map(|s| Box::pin(s) as std::pin::Pin<Box<dyn tokio::io::AsyncRead + Send>>),
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
        stop::stop(&host.instance, id).await?;
        let _ = self.child.start_kill();
        tokio::time::timeout(Duration::from_secs(3), self.child.wait())
            .await
            .map_err(|_| CalmError::Conflict("OpenCode direct child did not reap".into()))??;
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
