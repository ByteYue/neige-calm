//! Attach an existing native operations conversation without Planner authority.
use crate::{
    actor::Actor,
    db::{
        sqlite::{card_create_with_id_tx, session_start_runtime_tx},
        write_with_event_typed,
    },
    error::{CalmError, ErrorBody, Result},
    event::{Event, EventScope},
    extract::{Json, JsonBody, Path},
    harness::initial_snapshot_with_goal,
    model::{Card, CardRole, NewCard, TrackConversationSummary, now_ms},
    opencode_planner::attachment::{Binding, ConnectionSummary, ConnectionsResponse, PAYLOAD_KEY},
    session_projection_repo::{
        AgentProvider, WorkerSessionInit, WorkerSessionKind, WorkerSessionState,
    },
    state::{AppState, CodexShellState, RouteState, WorkerState},
};
use axum::{
    Router,
    extract::State,
    http::{HeaderMap, StatusCode},
    routing::{get, post},
};
use serde::Deserialize;
use serde_json::json;
use utoipa::ToSchema;

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/api/opencode/connections", get(list_connections))
        .route(
            "/api/tracks/{track_id}/opencode-conversations",
            post(attach_conversation),
        )
}
#[derive(Debug, Clone, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct AttachOpenCodeBody {
    pub connection_id: String,
    pub session_id: String,
}
#[utoipa::path(get,path="/api/opencode/connections",tag="opencode",responses((status=200,body=ConnectionsResponse)))]
pub(crate) async fn list_connections(State(s): State<RouteState>) -> Json<ConnectionsResponse> {
    let mut connections: Vec<_> = s
        .opencode_planner
        .connections
        .values()
        .map(|connection| ConnectionSummary {
            id: connection.config.id.clone(),
            label: connection.config.label.clone(),
            directory: connection.config.directory.display().to_string(),
        })
        .collect();
    connections.sort_by(|a, b| a.id.cmp(&b.id));
    Json(ConnectionsResponse { connections })
}
#[utoipa::path(
    post, path="/api/tracks/{track_id}/opencode-conversations", tag="opencode",
    params(("track_id"=String,Path),
        ("Idempotency-Key"=String,Header,description="Required stable attachment request key")),
    request_body=AttachOpenCodeBody,
    responses((status=201,body=TrackConversationSummary),(status=400,body=ErrorBody),
        (status=403,body=ErrorBody),(status=404,body=ErrorBody),(status=409,body=ErrorBody))
)]
#[allow(deprecated)]
pub(crate) async fn attach_conversation(
    State(s): State<RouteState>,
    State(w): State<WorkerState>,
    State(cs): State<CodexShellState>,
    actor: Actor,
    headers: HeaderMap,
    Path(track_id): Path<String>,
    JsonBody(body): JsonBody<AttachOpenCodeBody>,
) -> Result<(StatusCode, Json<TrackConversationSummary>)> {
    super::track_report_blocks::require_rest_user_actor_for(
        &actor,
        "attach OpenCode session",
        "Only the owner can attach an existing native session.",
    )?;
    let key = super::idempotency_key::parse_idempotency_key_header(&headers)?.ok_or_else(|| {
        CalmError::BadRequest("Idempotency-Key is required when attaching a conversation".into())
    })?;
    super::super::opencode_planner::client::native_id(&body.session_id, "ses")
        .map_err(|_| CalmError::BadRequest("Invalid OpenCode session ID".into()))?;
    let connection = s.opencode_planner.connection(&body.connection_id)?;
    let binding = connection.binding(body.session_id.clone());
    let _delete = crate::per_card_lock::lock_key(&s.track_delete_locks, &track_id).await;
    let track = s
        .repo
        .track_get(&track_id)
        .await?
        .ok_or_else(|| CalmError::NotFound(format!("track {track_id}")))?;
    if track.closed_at.is_some() || track.purpose.as_deref() == Some(crate::AREA_CHAT_PURPOSE) {
        return Err(CalmError::Conflict(
            "Attach to an open, visible Track".into(),
        ));
    }
    let card_id = crate::conversation_keys::derive_track_conversation_keys(
        &track_id,
        &format!("opencode-attach:{key}"),
    )
    .card_id;
    let _attach = crate::per_card_lock::lock_card(
        &s.conversation_first_message_locks,
        &format!(
            "opencode-target:{}:{}",
            binding.directory.display(),
            binding.session_id
        ),
    )
    .await;
    let pool = w
        .repo
        .sqlite_pool()
        .ok_or_else(|| CalmError::Internal("OpenCode attachment requires durable SQLite".into()))?;
    s.opencode_planner
        .validate_external_directories(&s.workspace_root)
        .map_err(attachment_precondition)?;
    s.opencode_planner
        .validate_external_owned_directories(&pool)
        .await
        .map_err(attachment_precondition)?;
    let target: Option<String> = sqlx::query_scalar(concat!(
        "SELECT id FROM cards WHERE json_extract(payload,'$.opencode_attachment.directory')=?1 ",
        "AND json_extract(payload,'$.opencode_attachment.session_id')=?2"
    ))
    .bind(binding.directory.display().to_string())
    .bind(&binding.session_id)
    .fetch_optional(&pool)
    .await?;
    let unresolved: Option<String> = sqlx::query_scalar(concat!(
        "SELECT submission.card_id FROM opencode_submissions submission ",
        "JOIN opencode_external_scopes scope ON scope.scope_id=submission.scope_id ",
        "WHERE scope.directory=?1 AND submission.native_session_id=?2 ",
        "AND submission.state IN ('prepared','sending','unknown') LIMIT 1"
    ))
    .bind(binding.directory.display().to_string())
    .bind(&binding.session_id)
    .fetch_optional(&pool)
    .await?;
    if let Some(owner) = unresolved.as_ref()
        && target.as_deref() != Some(owner.as_str())
    {
        return Err(CalmError::Conflict("OpenCode outcome is unresolved. Reconnect the original retained binding; another controller cannot be created".into()));
    }
    let existing = if let Some(existing) = s.repo.card_get(&card_id).await? {
        Some(existing)
    } else if let Some(id) = target {
        s.repo.card_get(&id).await?
    } else {
        None
    };
    let card = if let Some(card) = existing {
        if card.track_id.as_str() != track_id
            || Binding::from_payload(&card.payload)?.as_ref() != Some(&binding)
        {
            return Err(CalmError::Conflict("This key or native target is already bound to another conversation; its original binding is retained".into()));
        }
        card
    } else {
        connection
            .validate_server()
            .await
            .map_err(attachment_precondition)?;
        let native = connection
            .client()
            .get(&format!("/session/{}", binding.session_id))
            .await
            .map_err(attachment_precondition)?;
        crate::opencode_planner::session_input::validate_native_session(
            &native,
            &binding.session_id,
            &binding.directory,
        )
        .map_err(attachment_precondition)?;
        let title = native["title"]
            .as_str()
            .filter(|title| !title.trim().is_empty())
            .map(str::to_owned);
        let worker_id = crate::model::new_id();
        let thread_id = uuid::Uuid::new_v4().to_string();
        let mut snapshot = initial_snapshot_with_goal(None);
        snapshot.phase = crate::harness::HarnessPhaseTag::Idle;
        snapshot.last_thread_id = Some(thread_id.clone());
        let new = NewCard {
            track_id: track.id.clone(),
            kind: "codex".into(),
            sort: None,
            title,
            payload: json!({"schemaVersion":1,"harness_profile":"plain_chat",(PAYLOAD_KEY):binding}),
        };
        let role_cache = s.write.role_cache().clone();
        let create_id = card_id.clone();
        let (card, _) = write_with_event_typed(
            s.repo.as_ref(),
            actor.to_actor_id(),
            EventScope::Card {
                card: card_id.clone().into(),
                track: track.id.clone(),
                area: track.area_id.clone(),
            },
            None,
            &s.events,
            &s.write,
            move |tx| {
                Box::pin(async move {
                    sqlx::query(concat!(
                        "INSERT OR IGNORE INTO opencode_external_scopes",
                        "(scope_id,connection_id,generation,port,directory) VALUES(?1,?2,?3,?4,?5)"
                    ))
                    .bind(format!(
                        "external:{}:{}:{}:{}",
                        binding.connection_id,
                        binding.generation,
                        binding.port,
                        binding.directory.display()
                    ))
                    .bind(&binding.connection_id)
                    .bind(binding.generation as i64)
                    .bind(binding.port)
                    .bind(binding.directory.display().to_string())
                    .execute(&mut **tx)
                    .await?;
                    let card = card_create_with_id_tx(
                        tx,
                        create_id,
                        new,
                        CardRole::Worker,
                        true,
                        &role_cache,
                    )
                    .await?;
                    session_start_runtime_tx(
                        tx,
                        WorkerSessionInit {
                            id: worker_id,
                            card_id: card.id.to_string(),
                            kind: WorkerSessionKind::OpenCodeCard,
                            agent_provider: Some(AgentProvider::OpenCode),
                            status: WorkerSessionState::Idle,
                            terminal_run_id: None,
                            thread_id: Some(thread_id),
                            session_id: Some(binding.session_id),
                            active_turn_id: None,
                            handle_state_json: Some(serde_json::to_value(snapshot)?),
                            spawn_op_id: None,
                            now_ms: now_ms(),
                        },
                    )
                    .await?;
                    Ok((card.clone(), Event::CardAdded(card)))
                })
            },
        )
        .await?;
        card
    };
    // The row is durable before observer startup. A cancelled HTTP response is recovered at
    // boot or the next same-key attach; attaching never puts a prompt into the queue.
    // Recovery owns the same deletion fence and the per-card lock used by Send/reset.
    // Resolve its fresh candidate there instead of replacing an owner after a stale miss.
    drop(_delete);
    #[cfg(feature = "fixtures")]
    crate::test_seams::pause_point(
        crate::test_seams::OPENCODE_ATTACH_RECOVERY,
        card.id.as_str(),
    )
    .await;
    let (runtime, _harness, _recovery_guard) =
        super::planner_session::ensure_planner_session(&s, &w, &cs, &card.id, &actor).await?;
    Ok((
        StatusCode::CREATED,
        Json(summary(
            &card,
            Some(runtime.status),
            runtime.last_turn_completed_ms,
        )),
    ))
}
pub(crate) fn summary(
    card: &Card,
    state: Option<WorkerSessionState>,
    last_turn_completed_at: Option<i64>,
) -> TrackConversationSummary {
    TrackConversationSummary {
        source_card_id: card.payload["side_source_card_id"]
            .as_str()
            .map(ToOwned::to_owned),
        id: card.id.to_string(),
        track_id: card.track_id.to_string(),
        title: card.title.clone(),
        kind: "track-opencode".into(),
        state,
        updated_at: card.updated_at,
        last_turn_completed_at,
    }
}

// All these checks happen before the first durable binding write.
fn attachment_precondition(error: CalmError) -> CalmError {
    CalmError::BadRequest(format!("OpenCode attachment precondition failed: {error}"))
}
