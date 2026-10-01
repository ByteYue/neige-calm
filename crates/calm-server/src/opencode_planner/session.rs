//! Installed Planner lifecycle and durable admission. Once a dispatch claim is persisted,
//! no transport failure is returned as a retryable turn-start rejection.
use super::{client::Client, config::OpenCodePlannerHost, process::ServerProcess};
use crate::codex_appserver::{InputItem, Notification};
use crate::db::Repo;
use crate::error::{CalmError, Result};
use crate::planner_model::TurnModelSelection;
use crate::planner_submission::TurnAdmission;
use crate::session_projection_repo::{AgentProvider, ThreadAttribution};
use crate::shared_codex_appserver::SharedCodexAppServer;
use calm_truth::opencode_submission::{
    OpenCodeSubmission, OpenCodeSubmissionIntent, OpenCodeSubmissionState,
};
use calm_types::worker::WorkerSessionId;
use serde_json::{Value, json};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{broadcast, watch};

pub struct OpenCodePlannerSessionParams {
    pub host: Arc<OpenCodePlannerHost>,
    pub worker_session_id: String,
    pub card_id: String,
    pub track_id: String,
    pub cwd: PathBuf,
    pub instructions: String,
    pub proxy: Vec<(String, String, String)>,
    pub prior_total_tokens: i64,
    pub repo: Arc<dyn Repo>,
    pub seals: Arc<SharedCodexAppServer>,
}
pub(crate) struct Active {
    pub(crate) submission: OpenCodeSubmission,
    pub(crate) cancelled: watch::Sender<bool>,
}
pub(crate) struct State {
    pub(crate) active: Option<Active>,
    pub(crate) native_session: Option<String>,
    pub(crate) shutting_down: bool,
    pub(crate) total_tokens: i64,
}
pub(crate) struct Shared {
    pub(crate) params: OpenCodePlannerSessionParams,
    pub(crate) state: Mutex<State>,
    pub(crate) process: tokio::sync::Mutex<Option<ServerProcess>>,
    pub(crate) issue: tokio::sync::Mutex<()>,
    pub(crate) notifications: broadcast::Sender<Notification>,
    installed: AtomicBool,
}
impl Shared {
    pub(crate) fn state(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().expect("OpenCode Planner state")
    }
    pub(crate) fn pool(&self) -> Result<sqlx::SqlitePool> {
        self.params.repo.sqlite_pool().ok_or_else(|| {
            CalmError::Internal("OpenCode Planner requires durable SQLite admission".into())
        })
    }
    pub(crate) async fn ensure_server(&self) -> Result<Client> {
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
            validate_native_session(&native, &id, &self.params.cwd)?;
            return Ok(id);
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
        validate_native_session(&native, &id, &self.params.cwd)?;
        let worker = self.params.worker_session_id.clone();
        let persisted = id.clone();
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
        self.state().native_session = Some(id.clone());
        Ok(id)
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

pub struct OpenCodePlannerSession {
    pub(crate) shared: Arc<Shared>,
}
impl OpenCodePlannerSession {
    pub async fn open(params: OpenCodePlannerSessionParams) -> Result<Self> {
        let row = params
            .repo
            .session_get(&WorkerSessionId(params.worker_session_id.clone()))
            .await?
            .ok_or_else(|| {
                CalmError::NotFound(format!(
                    "worker session {} for OpenCode Planner",
                    params.worker_session_id
                ))
            })?;
        let pool = params.repo.sqlite_pool().ok_or_else(|| {
            CalmError::Internal("OpenCode Planner requires durable SQLite admission".into())
        })?;
        let unresolved =
            crate::db::sqlite::opencode_submission_get_unresolved_by_card(&pool, &params.card_id)
                .await?;
        let native_session = unresolved
            .as_ref()
            .map(|s| s.native_session_id.clone())
            .or(row.agent_session_id);
        let active = unresolved.map(|submission| Active {
            submission,
            cancelled: watch::Sender::new(false),
        });
        let (notifications, _) = broadcast::channel(1024);
        let total_tokens = params.prior_total_tokens;
        Ok(Self {
            shared: Arc::new(Shared {
                params,
                state: Mutex::new(State {
                    active,
                    native_session,
                    shutting_down: false,
                    total_tokens,
                }),
                process: tokio::sync::Mutex::new(None),
                issue: tokio::sync::Mutex::new(()),
                notifications,
                installed: AtomicBool::new(false),
            }),
        })
    }
    pub fn host(&self) -> &Arc<OpenCodePlannerHost> {
        &self.shared.params.host
    }
    pub fn codex(&self) -> &Arc<SharedCodexAppServer> {
        &self.shared.params.seals
    }
    pub fn subscribe_notifications(&self) -> broadcast::Receiver<Notification> {
        self.shared.notifications.subscribe()
    }
    pub fn mark_installed(&self) {
        if self.shared.installed.swap(true, Ordering::SeqCst) {
            return;
        }
        let active = self
            .shared
            .state()
            .active
            .as_ref()
            .map(|a| (a.submission.clone(), a.cancelled.subscribe()));
        if let Some((submission, cancelled)) = active {
            let shared = Arc::clone(&self.shared);
            tokio::spawn(async move {
                super::driver::drive(shared, submission, cancelled, None).await;
            });
        }
    }
    pub fn active_turn_id_for_thread(&self, thread: &str) -> Option<String> {
        self.shared
            .state()
            .active
            .as_ref()
            .filter(|a| a.submission.thread_id == thread)
            .map(|a| a.submission.id.clone())
    }
    /// Recovery and watchdogs consult the durable admission fence rather than provider idle.
    pub async fn has_unresolved_submission(&self) -> Result<bool> {
        if self.shared.state().active.is_some() {
            return Ok(true);
        }
        Ok(
            crate::db::sqlite::opencode_submission_get_unresolved_by_card(
                &self.shared.pool()?,
                &self.shared.params.card_id,
            )
            .await?
            .is_some(),
        )
    }
    pub async fn turn_start(
        &self,
        thread: &str,
        items: Vec<InputItem>,
        selection: &TurnModelSelection,
        client_id: &str,
    ) -> Result<TurnAdmission> {
        let shared = &self.shared;
        let _issue = shared.issue.lock().await;
        if !shared.installed.load(Ordering::SeqCst) || shared.state().shutting_down {
            return Err(CalmError::Conflict(
                "OpenCode Planner is not installed or is shutting down".into(),
            ));
        }
        if shared.params.seals.turn_thread_is_sealed(thread) {
            return Err(CalmError::Conflict(
                "OpenCode Planner thread is sealed".into(),
            ));
        }
        if let Some(active) = shared.state().active.as_ref() {
            if active.submission.client_id == client_id && active.submission.thread_id == thread {
                let explicit_model = selection
                    .model
                    .as_deref()
                    .map(super::models::split_model)
                    .transpose()?;
                let mut expected = json!({"messageID":active.submission.native_message_id,"parts":parts(&items)?,"system":shared.params.instructions});
                if let Some((provider, model)) = explicit_model {
                    expected["model"] = json!({"providerID":provider,"modelID":model});
                } else {
                    // Preserve the already admitted effective default on exact receipt replay.
                    expected["model"] = active.submission.input_json["model"].clone();
                }
                if let Some(effort) = &selection.effort {
                    expected["variant"] = json!(effort);
                }
                if active.submission.input_json != expected {
                    return Err(CalmError::Conflict(
                        "OpenCode client receipt belongs to different input".into(),
                    ));
                }
                return Ok(TurnAdmission::Unknown {
                    turn_id: active.submission.id.clone(),
                    reason: "the existing native submission is unresolved; it was not sent again"
                        .into(),
                });
            }
            return Err(CalmError::Conflict(
                "OpenCode has an unresolved native submission; it cannot accept another prompt"
                    .into(),
            ));
        }
        let parts = parts(&items)?;
        let effective_model = match &selection.model {
            Some(model) => model.clone(),
            None => shared
                .params
                .host
                .catalog()
                .await?
                .default_model
                .clone()
                .ok_or_else(|| {
                    CalmError::Conflict(
                        "OpenCode requires an explicit model because its profile has no default"
                            .into(),
                    )
                })?,
        };
        let (provider, model) = super::models::split_model(&effective_model)?;
        let client = shared.ensure_server().await?;
        let native = shared.native_session(&client, thread).await?;
        if shared.params.seals.turn_thread_is_sealed(thread) {
            return Err(CalmError::Conflict(
                "OpenCode Planner thread was sealed before admission".into(),
            ));
        }
        let native_message_id = format!("msg_{}", uuid::Uuid::new_v4().simple());
        let mut input = json!({"messageID":native_message_id,"parts":parts,"system":shared.params.instructions});
        input["model"] = json!({"providerID":provider,"modelID":model});
        if let Some(effort) = &selection.effort {
            input["variant"] = json!(effort);
        }
        let intent = OpenCodeSubmissionIntent {
            id: format!("opencode-turn-{}", uuid::Uuid::new_v4()),
            worker_session_id: shared.params.worker_session_id.clone(),
            card_id: shared.params.card_id.clone(),
            scope_id: shared.params.host.scope_id.clone(),
            generation: 0,
            thread_id: thread.into(),
            native_session_id: native.clone(),
            client_id: client_id.into(),
            native_message_id: native_message_id.clone(),
            input_json: input,
            created_at_ms: chrono::Utc::now().timestamp_millis(),
        };
        let pool = shared.pool()?;
        let submission = if let Some(old) = crate::db::sqlite::opencode_submission_get_by_client(
            &pool,
            &intent.scope_id,
            &native,
            client_id,
        )
        .await?
        {
            let mut old_input = old.input_json.clone();
            old_input["messageID"] = intent.input_json["messageID"].clone();
            if old_input != intent.input_json {
                return Err(CalmError::Conflict(
                    "OpenCode client receipt belongs to different input/model/instructions".into(),
                ));
            }
            old
        } else {
            crate::db::sqlite::opencode_submission_prepare(&pool, &intent).await?
        };
        if matches!(
            submission.state,
            OpenCodeSubmissionState::Completed
                | OpenCodeSubmissionState::Failed
                | OpenCodeSubmissionState::Interrupted
        ) {
            return Ok(TurnAdmission::Rejected {
                reason: "this OpenCode client receipt already settled; it cannot be resent".into(),
            });
        }
        let claimed = crate::db::sqlite::opencode_submission_claim_prepared(
            &pool,
            &submission.id,
            chrono::Utc::now().timestamp_millis(),
        )
        .await?;
        let (cancelled, rx) = watch::channel(false);
        shared.state().active = Some(Active {
            submission: submission.clone(),
            cancelled,
        });
        shared.send(Notification::TurnStarted {
            thread_id: thread.into(),
            turn: json!({"id":submission.id,"status":"inProgress","error":null}),
        });
        let acknowledgement_client = client.clone();
        let native_message_id = submission.native_message_id.clone();
        let payload = if claimed {
            Some((client, submission.input_json.clone()))
        } else {
            None
        };
        let id = submission.id.clone();
        let shared = Arc::clone(shared);
        let reconcile = Arc::clone(&shared);
        tokio::spawn(async move {
            super::driver::drive(shared, submission, rx, payload).await;
        });
        // OpenCode's synchronous POST answers after completion, so use the exact durable
        // user message as its admission evidence. A short acknowledgement budget must not
        // turn an attempted request into a rejection or a retransmission.
        let until = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            if reconcile.state().active.is_none() {
                return Ok(TurnAdmission::Accepted { turn_id: id });
            }
            if let Ok(message) = acknowledgement_client
                .get(&format!("/session/{native}/message/{native_message_id}"))
                .await
            {
                if message["info"]["id"].as_str() == Some(&native_message_id)
                    && message["info"]["sessionID"].as_str() == Some(&native)
                    && message["info"]["role"].as_str() == Some("user")
                {
                    return Ok(TurnAdmission::Accepted { turn_id: id });
                }
            }
            if tokio::time::Instant::now() >= until {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        if let Err(error) = crate::db::sqlite::opencode_submission_mark_unknown(
            &pool,
            &id,
            chrono::Utc::now().timestamp_millis(),
        )
        .await
        {
            tracing::warn!(%error, "OpenCode attempted submission retains its durable dispatch fence after an unknown-state write failed");
        }
        let reason = "OpenCode admission is unknown; the original submission is retained and will not be resent";
        reconcile.unknown(thread, &id, reason);
        Ok(TurnAdmission::Unknown {
            turn_id: id,
            reason: reason.into(),
        })
    }
    pub async fn turn_interrupt(&self, thread: &str, turn: &str) -> Result<()> {
        let slot = self
            .shared
            .state()
            .active
            .as_ref()
            .filter(|a| a.submission.thread_id == thread && a.submission.id == turn)
            .map(|a| a.cancelled.clone());
        if let Some(slot) = slot {
            if slot.send(true).is_err() {
                // A failed observer already stopped its process; repeat cleanup rather than
                // treating a send to a closed watch channel as successful interruption.
                self.shared.stop_process().await?;
            }
        }
        Ok(())
    }
    pub async fn interrupt_active_turn(&self, thread: &str) -> Result<()> {
        if let Some(turn) = self.active_turn_id_for_thread(thread) {
            self.turn_interrupt(thread, &turn).await?;
        }
        Ok(())
    }
    pub async fn shutdown(&self) -> Result<()> {
        self.shared.state().shutting_down = true;
        let _issue = self.shared.issue.lock().await;
        if !self.shared.installed.load(Ordering::SeqCst) {
            return Ok(());
        }
        if let Some(active) = self.shared.state().active.as_ref() {
            let _ = active.cancelled.send(true);
        }
        // Serialize revocation with ensure_server's mint/spawn critical section. A recovery
        // task waiting for this lock observes closed before it can mint or start a child.
        let mut process = self.shared.process.lock().await;
        let revoke = super::lifecycle::revoke_session(
            self.shared.params.repo.as_ref(),
            &self.shared.params.worker_session_id,
        )
        .await;
        let active = self
            .shared
            .state()
            .active
            .as_ref()
            .map(|a| (a.submission.id.clone(), a.submission.state));
        let journal = if let Some((id, state)) = active {
            if state == OpenCodeSubmissionState::Prepared {
                Ok(()) // Recovery abandons a proven unsent intent with visible local evidence.
            } else {
                crate::db::sqlite::opencode_submission_mark_unknown(
                    &self.shared.pool()?,
                    &id,
                    chrono::Utc::now().timestamp_millis(),
                )
                .await
            }
        } else {
            Ok(())
        };
        let stop = if let Some(mut owned) = process.take() {
            owned
                .shutdown(
                    &self.shared.params.host,
                    &self.shared.params.worker_session_id,
                )
                .await
        } else {
            super::stop::stop(
                &self.shared.params.host.instance,
                &self.shared.params.worker_session_id,
            )
            .await
        };
        revoke?;
        journal?;
        stop
    }
}
fn parts(items: &[InputItem]) -> Result<Vec<Value>> {
    items
        .iter()
        .map(|item| match item {
            InputItem::Text { text } => Ok(json!({"type":"text","text":text})),
            InputItem::LocalImage { .. } => Err(CalmError::BadRequest(
                "OpenCode Planner currently accepts text input only".into(),
            )),
        })
        .collect()
}
fn validate_native_session(native: &Value, id: &str, cwd: &std::path::Path) -> Result<()> {
    let actual = native["directory"]
        .as_str()
        .ok_or_else(|| CalmError::Conflict("OpenCode session has no directory".into()))?;
    if native["id"].as_str() != Some(id)
        || std::fs::canonicalize(actual)? != std::fs::canonicalize(cwd)?
    {
        return Err(CalmError::Conflict(
            "OpenCode native session identity/directory does not match this Planner".into(),
        ));
    }
    Ok(())
}
