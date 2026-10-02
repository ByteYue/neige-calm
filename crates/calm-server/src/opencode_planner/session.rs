//! Installed Planner lifecycle and durable admission. Once a dispatch claim is persisted,
//! no transport failure is returned as a retryable turn-start rejection.
use super::session_input::parts;
use super::{config::OpenCodePlannerHost, process::ServerProcess};
use crate::codex_appserver::{InputItem, Notification};
use crate::db::Repo;
use crate::error::{CalmError, Result};
use crate::planner_model::TurnModelSelection;
use crate::planner_submission::TurnAdmission;

use crate::shared_codex_appserver::SharedCodexAppServer;
use calm_truth::opencode_submission::{
    OpenCodeSubmission, OpenCodeSubmissionIntent, OpenCodeSubmissionState,
};
use calm_types::worker::WorkerSessionId;
use serde_json::json;
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
    pub events: crate::event::EventBus,
    pub write: crate::state::WriteContext,
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
    pub(crate) attachment: Option<(
        super::attachment::Binding,
        Arc<super::attachment::Connection>,
    )>,
    pub(crate) attached_metadata: Mutex<Option<super::attachment::AttachedSession>>,
    installed: AtomicBool,
}

pub struct OpenCodePlannerSession {
    pub(crate) shared: Arc<Shared>,
}
impl OpenCodePlannerSession {
    pub async fn open(params: OpenCodePlannerSessionParams) -> Result<Self> {
        let card = params
            .repo
            .card_get(&params.card_id)
            .await?
            .ok_or_else(|| CalmError::NotFound("OpenCode card".into()))?;
        let attachment = super::attachment::Binding::from_payload(&card.payload)?
            .map(|binding| {
                params
                    .host
                    .resolve_binding(&binding)
                    .map(|connection| (binding, connection))
            })
            .transpose()?;
        let metadata = attachment.as_ref().map(|(binding, connection)| {
            super::attachment::AttachedSession::initial(connection, binding)
        });
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
        if attachment.is_some() {
            params
                .host
                .validate_external_owned_directories(&pool)
                .await?;
        }
        let unresolved =
            crate::db::sqlite::opencode_submission_get_unresolved_by_card(&pool, &params.card_id)
                .await?;
        if let Some(submission) = &unresolved {
            validate_receipt(&params, submission, Some(&submission.native_session_id)).await?;
        }
        let native_session = unresolved
            .as_ref()
            .map(|s| s.native_session_id.clone())
            .or(row.agent_session_id);
        let native_session = match &attachment {
            Some((binding, _)) => {
                if native_session
                    .as_deref()
                    .is_some_and(|native| native != binding.session_id)
                {
                    return Err(CalmError::Conflict(
                        "OpenCode native binding differs from its persisted owner".into(),
                    ));
                }
                Some(binding.session_id.clone())
            }
            None => native_session,
        };
        let active = unresolved.clone().map(|submission| Active {
            submission,
            cancelled: watch::Sender::new(false),
        });
        let (notifications, _) = broadcast::channel(1024);
        let recovered = unresolved.clone();
        let total_tokens = params.prior_total_tokens;
        let session = Self {
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
                attachment,
                attached_metadata: Mutex::new(metadata),
                installed: AtomicBool::new(false),
            }),
        };
        if let Some(receipt) = recovered {
            // Correlation belongs to the persisted submission, including across a new worker
            // incarnation. Persist before recovery notifications; no provider is started here.
            session
                .shared
                .bind_native(&receipt.native_session_id, &receipt.thread_id)
                .await?;
        }
        Ok(session)
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
        if self.shared.attachment.is_some() {
            let shared = Arc::clone(&self.shared);
            tokio::spawn(async move {
                super::history::observe(shared).await;
            });
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
    /// Read-only owner recovery lookup. A completed receipt may win the observer/startup race;
    /// use the persisted client key to find it without a new admission or a native query.
    pub async fn recovery_submission(
        &self,
        client_id: Option<&str>,
    ) -> Result<Option<OpenCodeSubmission>> {
        let native = self.shared.state().native_session.clone();
        let pool = self.shared.pool()?;
        let receipt = if let (Some(client), Some(native)) = (client_id, native.as_deref()) {
            crate::db::sqlite::opencode_submission_get_by_client(
                &pool,
                self.shared.scope_id(),
                native,
                client,
            )
            .await?
        } else if client_id.is_none() {
            crate::db::sqlite::opencode_submission_get_unresolved_by_card(
                &pool,
                &self.shared.params.card_id,
            )
            .await?
        } else {
            None
        };
        if let Some(receipt) = &receipt {
            validate_receipt(&self.shared.params, receipt, native.as_deref()).await?;
        }
        Ok(receipt)
    }

    /// The Harness owns when this receipt replaces its correlation. Revalidate the exact
    /// durable record and current authority; persist identity without starting or submitting.
    pub async fn adopt_recovery_binding(&self, receipt: &OpenCodeSubmission) -> Result<()> {
        let recorded = self
            .recovery_submission(Some(&receipt.client_id))
            .await?
            .ok_or_else(|| {
                CalmError::Conflict("OpenCode recovery receipt no longer exists".into())
            })?;
        if &recorded != receipt {
            return Err(CalmError::Conflict(
                "OpenCode recovery receipt changed before adoption".into(),
            ));
        }
        self.shared
            .bind_native(&receipt.native_session_id, &receipt.thread_id)
            .await
    }

    /// Exact durable receipt for the owning Harness to restore projection correlation before
    /// mark_installed starts a GET-only recovery observer. This never admits another prompt.
    pub fn recovered_submission(&self) -> Option<OpenCodeSubmission> {
        self.shared
            .state()
            .active
            .as_ref()
            .map(|a| a.submission.clone())
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
                let mut expected = if shared.attachment.is_some() {
                    active.submission.input_json.clone()
                } else {
                    json!({"messageID":active.submission.native_message_id,"parts":parts(&items)?,"system":shared.params.instructions})
                };
                expected["parts"] = json!(parts(&items)?);
                if shared.attachment.is_none()
                    && let Some((provider, model)) = explicit_model
                {
                    expected["model"] = json!({"providerID":provider,"modelID":model});
                } else {
                    // Preserve the already admitted effective default on exact receipt replay.
                    expected["model"] = active.submission.input_json["model"].clone();
                }
                if shared.attachment.is_none()
                    && let Some(effort) = &selection.effort
                {
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
        self.check_can_submit().await?;
        let parts = parts(&items)?;
        let client = shared.ensure_server().await?;
        let native = shared.native_session(&client, thread).await?;
        let native_messages = if shared.attachment.is_some() {
            Some(super::history::messages(&client, &native).await?)
        } else {
            None
        };
        let native_info = if shared.attachment.is_some() {
            Some(client.get(&format!("/session/{native}")).await?)
        } else {
            None
        };
        let attached_model = native_info
            .as_ref()
            .and_then(super::attachment::session_model)
            .or_else(|| {
                native_messages
                    .as_ref()
                    .and_then(|messages| super::attachment::latest_model(messages))
            });
        let effective_model = match attached_model.as_ref().or(selection.model.as_ref()) {
            Some(model) => model.clone(),
            None if shared.attachment.is_some() => shared
                .attachment
                .as_ref()
                .expect("attachment")
                .1
                .catalog()
                .await?
                .default_model
                .clone()
                .ok_or_else(|| {
                    CalmError::Conflict("Existing OpenCode session has no model".into())
                })?,
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
        if shared.params.seals.turn_thread_is_sealed(thread) {
            return Err(CalmError::Conflict(
                "OpenCode Planner thread was sealed before admission".into(),
            ));
        }
        let native_message_id = format!("msg_{}", uuid::Uuid::new_v4().simple());
        let mut input = json!({"messageID":native_message_id,"parts":parts});
        if shared.attachment.is_none() {
            input["system"] = json!(shared.params.instructions);
        }
        input["model"] = json!({"providerID":provider,"modelID":model});
        if let Some(messages) = native_messages {
            if let Some(info) = messages
                .iter()
                .rev()
                .find(|message| message["info"]["role"].as_str() == Some("user"))
                .map(|message| &message["info"])
            {
                let session = native_info.as_ref().expect("attached native info");
                if let Some(agent) = session["agent"].as_str().or(info["agent"].as_str()) {
                    input["agent"] = json!(agent);
                }
                if let Some(variant) = session["model"]["variant"]
                    .as_str()
                    .or(info["variant"].as_str())
                    .or(info["model"]["variant"].as_str())
                {
                    input["variant"] = json!(variant);
                }
            }
        } else if let Some(effort) = &selection.effort {
            input["variant"] = json!(effort);
        }
        let intent = OpenCodeSubmissionIntent {
            id: format!("opencode-turn-{}", uuid::Uuid::new_v4()),
            worker_session_id: shared.params.worker_session_id.clone(),
            card_id: shared.params.card_id.clone(),
            scope_id: shared.scope_id().to_owned(),
            generation: shared
                .attachment
                .as_ref()
                .map_or(0, |(binding, _)| binding.generation as i64),
            thread_id: thread.into(),
            native_session_id: native.clone(),
            client_id: client_id.into(),
            native_message_id: native_message_id.clone(),
            input_json: input,
            created_at_ms: chrono::Utc::now().timestamp_millis(),
        };
        let pool = shared.pool()?;
        let mut submission = if let Some(old) =
            crate::db::sqlite::opencode_submission_get_by_client(
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
        let now = chrono::Utc::now().timestamp_millis();
        let claim =
            crate::db::sqlite::opencode_submission_claim_prepared(&pool, &submission.id, now).await;
        let mut unknown_reason = None;
        let claimed = match claim {
            Ok(true) => {
                submission.state = OpenCodeSubmissionState::Sending;
                submission.updated_at_ms = now;
                true
            }
            result => {
                let reason = match result {
                    Ok(false) => "OpenCode dispatch was not claimed; reconciling the durable original receipt".to_owned(),
                    Err(error) => format!("OpenCode dispatch claim is unknown after its intent was persisted: {error}"),
                    Ok(true) => unreachable!(),
                };
                // The pre-CAS clone is not proof of Prepared: a connection error can follow
                // the state transition. Re-read before deciding whether the original is unsent.
                let readback = crate::db::sqlite::opencode_submission_get_by_client(
                    &pool,
                    &submission.scope_id,
                    &submission.native_session_id,
                    &submission.client_id,
                )
                .await;
                match readback {
                    Ok(Some(actual))
                        if actual.id == submission.id
                            && actual.input_fingerprint == submission.input_fingerprint
                            && actual.input_json == submission.input_json =>
                    {
                        if matches!(
                            actual.state,
                            OpenCodeSubmissionState::Completed
                                | OpenCodeSubmissionState::Failed
                                | OpenCodeSubmissionState::Interrupted
                        ) {
                            return Ok(TurnAdmission::Rejected { reason: "the original OpenCode receipt already settled; it was not sent again".into() });
                        }
                        submission = actual;
                    }
                    _ => {
                        // Preserve the receipt and hold the logical fence even when SQLite
                        // cannot currently tell us which side of CAS it committed. No observer
                        // may infer never-sent from this speculative clone and no POST is made.
                        submission.state = OpenCodeSubmissionState::Unknown;
                        let (cancelled, _) = watch::channel(false);
                        shared.state().active = Some(Active {
                            submission: submission.clone(),
                            cancelled,
                        });
                        shared.unknown(thread, &submission.id, &reason);
                        if let Err(error) = shared.stop_process().await {
                            tracing::error!(%error, "OpenCode uncertain claim cleanup failed");
                        }
                        return Ok(TurnAdmission::Unknown {
                            turn_id: submission.id,
                            reason,
                        });
                    }
                }
                unknown_reason = Some(reason);
                false
            }
        };
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
        if let Some(reason) = unknown_reason {
            reconcile.unknown(thread, &id, &reason);
            return Ok(TurnAdmission::Unknown {
                turn_id: id,
                reason,
            });
        }
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
                && message["info"]["id"].as_str() == Some(&native_message_id)
                && message["info"]["sessionID"].as_str() == Some(&native)
                && message["info"]["role"].as_str() == Some("user")
            {
                return Ok(TurnAdmission::Accepted { turn_id: id });
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
        if self.is_attached() {
            return Err(CalmError::Conflict(
                "Stop is unavailable for a borrowed OpenCode session; use its original controller"
                    .into(),
            ));
        }
        let slot = self
            .shared
            .state()
            .active
            .as_ref()
            .filter(|a| a.submission.thread_id == thread && a.submission.id == turn)
            .map(|a| a.cancelled.clone());
        if let Some(slot) = slot
            && slot.send(true).is_err()
        {
            // A failed observer already stopped its process; repeat cleanup rather than
            // treating a send to a closed watch channel as successful interruption.
            self.shared.stop_process().await?;
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
        if self.is_attached() {
            let active = self
                .shared
                .state()
                .active
                .as_ref()
                .map(|active| (active.submission.id.clone(), active.submission.state));
            if let Some((id, state)) = active
                && state != OpenCodeSubmissionState::Prepared
            {
                crate::db::sqlite::opencode_submission_mark_unknown(
                    &self.shared.pool()?,
                    &id,
                    chrono::Utc::now().timestamp_millis(),
                )
                .await?;
            }
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
async fn validate_receipt(
    params: &OpenCodePlannerSessionParams,
    receipt: &OpenCodeSubmission,
    native: Option<&str>,
) -> Result<()> {
    use calm_types::worker::{WorkerContract, WorkerProviderKind};
    let card = params
        .repo
        .card_get(&params.card_id)
        .await?
        .ok_or_else(|| CalmError::NotFound("OpenCode owner card".into()))?;
    let binding = super::attachment::Binding::from_payload(&card.payload)?;
    let scope = binding
        .as_ref()
        .map(|binding| {
            params
                .host
                .resolve_binding(binding)
                .map(|connection| connection.scope_id.clone())
        })
        .transpose()?
        .unwrap_or_else(|| params.host.scope_id.clone());
    let expected_contract = if binding.is_some() {
        WorkerContract::Executor
    } else {
        WorkerContract::Planner
    };
    if binding.is_some()
        && (params.repo.card_role_get(&params.card_id).await?
            != Some(crate::model::CardRole::Worker)
            || !crate::plain_chat::card_is_plain_chat(&card, None, false))
    {
        return Err(CalmError::Conflict(
            "OpenCode attachment requires its original plain-chat role".into(),
        ));
    }
    let owner = params
        .repo
        .session_get(&WorkerSessionId(params.worker_session_id.clone()))
        .await?
        .ok_or_else(|| CalmError::NotFound("OpenCode recovery owner no longer exists".into()))?;
    let current = params
        .repo
        .session_projection_active_for_card(&params.card_id)
        .await?;
    if owner.provider != WorkerProviderKind::OpenCode
        || owner.contract != expected_contract
        || !owner.state.is_active_authority()
        || owner.card_id.as_ref().map(ToString::to_string).as_deref() != Some(&params.card_id)
        || owner.track_id.to_string() != params.track_id
        || current.as_ref().map(|s| s.id.as_str()) != Some(&params.worker_session_id)
        || receipt.card_id != params.card_id
        || receipt.scope_id != scope
        || binding
            .as_ref()
            .is_some_and(|binding| receipt.generation != binding.generation as i64)
        || native != Some(receipt.native_session_id.as_str())
    {
        return Err(CalmError::Conflict(
            "OpenCode recovery receipt does not match the live Planner owner/scope/native binding"
                .into(),
        ));
    }
    if let Some(original) = params
        .repo
        .session_get(&WorkerSessionId(receipt.worker_session_id.clone()))
        .await?
        && (original.provider != WorkerProviderKind::OpenCode
            || original.contract != expected_contract
            || original
                .card_id
                .as_ref()
                .map(ToString::to_string)
                .as_deref()
                != Some(&params.card_id)
            || original.thread_id.as_deref() != Some(&receipt.thread_id)
            || original.agent_session_id.as_deref() != Some(&receipt.native_session_id))
    {
        return Err(CalmError::Conflict(
            "OpenCode recovery receipt has conflicting original attribution".into(),
        ));
    }
    super::client::native_id(&receipt.native_session_id, "ses")?;
    super::client::native_id(&receipt.native_message_id, "msg")?;
    Ok(())
}
