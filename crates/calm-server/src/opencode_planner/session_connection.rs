//! Transport and native identity ownership shared by admission and observations.
use super::{
    client::Client,
    process::ServerProcess,
    session::{Shared, State},
};
use crate::{
    codex_appserver::Notification,
    error::{CalmError, Result},
    session_projection_repo::{AgentProvider, ThreadAttribution},
};
use calm_types::worker::WorkerSessionId;
use serde_json::json;
use std::time::Duration;
impl Shared {
    pub(crate) fn state(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().expect("OpenCode Planner state")
    }
    pub(crate) fn pool(&self) -> Result<sqlx::SqlitePool> {
        self.params.repo.sqlite_pool().ok_or_else(|| {
            CalmError::Internal("OpenCode Planner requires durable SQLite admission".into())
        })
    }
    pub(crate) fn scope_id(&self) -> &str {
        self.attachment
            .as_ref()
            .map(|(_, connection)| connection.scope_id.as_str())
            .unwrap_or(&self.params.host.scope_id)
    }
    pub(crate) async fn ensure_server(&self) -> Result<Client> {
        if let Some((_, connection)) = &self.attachment {
            if self.state().shutting_down {
                return Err(CalmError::Conflict(
                    "OpenCode attachment is disconnected".into(),
                ));
            }
            connection.validate_server().await?;
            return Ok(connection.client());
        }
        let mut slot = self.process.lock().await;
        if self.state().shutting_down {
            return Err(CalmError::Conflict(
                "OpenCode Planner is closed; recovery cannot spawn a provider".into(),
            ));
        }
        if let Some(server) = slot.as_ref() {
            return Ok(server.client.clone());
        }
        self.params.host.configured()?;
        let card = self.params.card_id.clone();
        let worker = self.params.worker_session_id.clone();
        let token = crate::db::write_in_tx_typed(self.params.repo.as_ref(), move |tx| {
            Box::pin(async move {
                crate::mcp_server::wiring::mint_and_persist_planner_token(tx, &card, &worker).await
            })
        })
        .await?;
        let server = ServerProcess::start(
            &self.params.host,
            &self.params.worker_session_id,
            &self.params.cwd,
            &self.params.proxy,
            Some(&token),
        )
        .await?;
        let client = server.client.clone();
        *slot = Some(server);
        Ok(client)
    }
    pub(crate) async fn native_session(&self, client: &Client, thread: &str) -> Result<String> {
        let bound = self.state().native_session.clone();
        if let Some(id) = bound {
            super::client::native_id(&id, "ses")?;
            let native = client.get(&format!("/session/{id}")).await?;
            super::session_input::validate_native_session(&native, &id, &self.params.cwd)?;
            self.bind_native(&id, thread).await?;
            return Ok(id);
        }
        if self.attachment.is_some() {
            return Err(CalmError::Conflict(
                "Existing OpenCode session binding is missing; no new session was created".into(),
            ));
        }
        let native = client
            .request(
                "POST",
                "/session",
                Some(&json!({"title":"Neige Planner"})),
                Duration::from_secs(10),
            )
            .await?;
        let id = native["id"]
            .as_str()
            .ok_or_else(|| {
                CalmError::Conflict("OpenCode created no native session identity".into())
            })?
            .to_owned();
        super::client::native_id(&id, "ses")?;
        super::session_input::validate_native_session(&native, &id, &self.params.cwd)?;
        self.bind_native(&id, thread).await?;
        self.state().native_session = Some(id.clone());
        Ok(id)
    }
    pub(crate) async fn bind_native(&self, id: &str, thread: &str) -> Result<()> {
        let worker = self.params.worker_session_id.clone();
        let persisted = id.to_owned();
        let thread = thread.to_owned();
        crate::db::write_in_tx_typed(self.params.repo.as_ref(), move |tx| {
            Box::pin(async move {
                let row = crate::db::sqlite::session_get_tx(tx, &WorkerSessionId(worker.clone()))
                    .await?
                    .ok_or_else(|| CalmError::NotFound(format!("worker session {worker}")))?;
                crate::db::sqlite::session_bind_attribution_tx(
                    tx,
                    &worker,
                    ThreadAttribution {
                        worker_session_id: worker.clone(),
                        provider: AgentProvider::OpenCode,
                        thread_id: Some(thread),
                        session_id: Some(persisted),
                        active_turn_id: row.active_turn_id,
                    },
                )
                .await?;
                Ok(())
            })
        })
        .await?;
        Ok(())
    }
    pub(crate) fn send(&self, notification: Notification) {
        let _ = self.notifications.send(notification);
    }
    pub(crate) fn unknown(&self, thread: &str, turn: &str, reason: &str) {
        self.send(Notification::Other {
            method: "opencode/submission/unknown".into(),
            params: json!({"threadId":thread,"turnId":turn,"message":reason}),
        });
    }
    pub(crate) async fn stop_process(&self) -> Result<()> {
        if self.attachment.is_some() {
            return Ok(());
        }
        let mut process = self.process.lock().await;
        if let Some(mut process) = process.take() {
            process
                .shutdown(&self.params.host, &self.params.worker_session_id)
                .await
        } else {
            super::stop::stop(&self.params.host.instance, &self.params.worker_session_id).await
        }
    }
}
