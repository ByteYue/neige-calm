//! Read-only snapshot synchronization for borrowed sessions. It never admits, aborts,
//! answers an interaction, changes native configuration, or creates a journal receipt.
use super::{
    attachment::{AttachedStatus, latest_model, native_status, session_model},
    client::Client,
    session::Shared,
    translate::TurnProjection,
};
use crate::{
    codex_appserver::Notification,
    error::{CalmError, Result},
    harness::transcript::{ItemMetadata, TranscriptItem, TranscriptOwner},
};
use serde_json::Value;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

pub(crate) async fn messages(client: &Client, native: &str) -> Result<Vec<Value>> {
    let mut messages = Vec::new();
    let mut cursor: Option<String> = None;
    let mut cursors = HashSet::new();
    let mut bytes = 0usize;
    loop {
        let mut path = format!("/session/{native}/message?limit=256");
        if let Some(before) = &cursor {
            let query = url::form_urlencoded::Serializer::new(String::new())
                .append_pair("before", before)
                .finish();
            path.push('&');
            path.push_str(&query);
        }
        let page = client.page(&path).await?;
        let rows = page
            .value
            .as_array()
            .ok_or_else(|| CalmError::Conflict("OpenCode history is malformed".into()))?;
        for message in rows {
            if message["info"]["sessionID"].as_str() != Some(native)
                || message["info"]["id"].as_str().is_none()
                || message["info"]["time"]["created"].as_i64().is_none()
                || !message["parts"].is_array()
            {
                return Err(CalmError::Conflict(
                    "OpenCode history identity or content is incomplete".into(),
                ));
            }
            bytes = bytes.saturating_add(serde_json::to_vec(message)?.len());
            if bytes > 16 * 1024 * 1024 || messages.len() + rows.len() > 16_384 {
                return Err(CalmError::Conflict(
                    "OpenCode history exceeds its bounded synchronization budget".into(),
                ));
            }
        }
        messages.splice(0..0, rows.iter().cloned());
        let Some(next) = page.next_cursor else {
            return Ok(messages);
        };
        if next.len() > 2048 || !cursors.insert(next.clone()) {
            return Err(CalmError::Conflict(
                "OpenCode history pagination failed to advance".into(),
            ));
        }
        cursor = Some(next);
    }
}

pub(crate) async fn observe(shared: Arc<Shared>) {
    let mut projections = HashMap::<String, TurnProjection>::new();
    while !shared.state().shutting_down {
        let result = sync(&shared, &mut projections).await;
        if let Err(error) = result {
            if let Some(metadata) = shared
                .attached_metadata
                .lock()
                .expect("attached metadata")
                .as_mut()
            {
                metadata.status = AttachedStatus::Unavailable;
                metadata.can_submit = false;
            }
            tracing::debug!(%error,"Existing OpenCode observation unavailable; native activity is retained");
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}
async fn sync(shared: &Shared, projections: &mut HashMap<String, TurnProjection>) -> Result<()> {
    // Admission cannot race a stale passive snapshot into writes while the submission
    // driver is settling its durable receipt. That driver owns all active-turn projection.
    let _issue = shared.issue.lock().await;
    if shared.state().active.is_some() {
        return Ok(());
    }
    let (binding, connection) = shared.attachment.as_ref().expect("attached observer");
    let client = shared.ensure_server().await?;
    let row = shared
        .params
        .repo
        .session_get(&calm_types::worker::WorkerSessionId(
            shared.params.worker_session_id.clone(),
        ))
        .await?
        .ok_or_else(|| CalmError::NotFound("OpenCode attachment owner".into()))?;
    if !row.state.is_active_authority() {
        return Err(CalmError::Conflict(
            "OpenCode attachment owner is closed".into(),
        ));
    }
    let thread = row
        .thread_id
        .ok_or_else(|| CalmError::Conflict("OpenCode attachment thread is missing".into()))?;
    if row.agent_session_id.as_deref() != Some(binding.session_id.as_str()) {
        return Err(CalmError::Conflict(
            "OpenCode observer native owner binding changed".into(),
        ));
    }
    let native = client
        .get(&format!("/session/{}", binding.session_id))
        .await?;
    super::session_input::validate_native_session(
        &native,
        &binding.session_id,
        &binding.directory,
    )?;
    let messages = messages(&client, &binding.session_id).await?;
    let status = native_status(&client, &binding.session_id).await?;
    if shared.state().shutting_down {
        return Ok(());
    }
    let receipts: Vec<(String, String, String, Option<String>)> = sqlx::query_as(concat!(
        "SELECT native_message_id,id,client_id,outcome_json FROM opencode_submissions ",
        "WHERE scope_id=?1 AND native_session_id=?2 AND card_id=?3"
    ))
    .bind(&connection.scope_id)
    .bind(&binding.session_id)
    .bind(&shared.params.card_id)
    .fetch_all(&shared.pool()?)
    .await?;
    let mut anchors = HashMap::new();
    let mut internal_users = HashSet::new();
    for (native_message, _, _, outcome) in &receipts {
        if let Some(outcome) = outcome {
            let outcome: Value = serde_json::from_str(outcome)?;
            if let Some(anchor) = outcome.get("nativeReturnAnchor") {
                let anchor: super::native_return::ReturnAnchor =
                    serde_json::from_value(anchor.clone())?;
                internal_users.extend(anchor.internal_user_ids.iter().cloned());
                anchors.insert(native_message.clone(), anchor);
            }
        }
    }
    for user in messages
        .iter()
        .filter(|message| message["info"]["role"].as_str() == Some("user"))
    {
        let id = user["info"]["id"].as_str().expect("validated message id");
        if internal_users.contains(id) {
            continue;
        }
        if !projections.contains_key(id) {
            let (turn, client) = receipts
                .iter()
                .find(|(native, _, _, _)| native == id)
                .map(|(_, turn, client, _)| (turn.clone(), client.clone()))
                .unwrap_or_else(|| {
                    (
                        format!("opencode-history-{id}"),
                        format!("opencode-history-{id}"),
                    )
                });
            let original = user["parts"]
                .as_array()
                .expect("validated parts")
                .iter()
                .filter(|part| part["type"].as_str() == Some("text"))
                .filter_map(|part| part["text"].as_str().map(str::to_owned))
                .collect();
            projections.insert(
                id.into(),
                TurnProjection::new_external(
                    thread.clone(),
                    turn,
                    id.into(),
                    client,
                    original,
                    binding.directory.display().to_string(),
                    0,
                )?,
            );
        }
        let mut candidate = projections.get(id).expect("history projection").clone();
        candidate.return_anchor = anchors.get(id).cloned();
        for notification in candidate.snapshot(&messages) {
            if shared.state().shutting_down {
                return Ok(());
            }
            let Notification::Item { method, params } = &notification else {
                continue;
            };
            // Durable equality avoids duplicate rows after restarts. Changed partial output is
            // appended through the same Harness owner path as other provider item updates.
            let previous:Option<(String,String)>=sqlx::query_as("SELECT method,params FROM harness_items WHERE card_id=?1 AND thread_id=?2 AND item_uuid=?3 ORDER BY id DESC LIMIT 1")
                .bind(&shared.params.card_id).bind(&thread).bind(params["item"]["id"].as_str()).fetch_optional(&shared.pool()?).await?;
            if let Some((previous_method, previous_params)) = previous
                && previous_method == *method
                && serde_json::from_str::<Value>(&previous_params)? == *params
            {
                continue;
            }
            persist_item(shared, &notification).await?;
        }
        projections.insert(id.into(), candidate);
    }
    if let Some(metadata) = shared
        .attached_metadata
        .lock()
        .expect("attached metadata")
        .as_mut()
    {
        metadata.status = status;
        metadata.model = session_model(&native).or_else(|| latest_model(&messages));
        metadata.can_submit = status == AttachedStatus::Idle;
    }
    Ok(())
}

async fn persist_item(shared: &Shared, notification: &Notification) -> Result<()> {
    let Notification::Item { method, params } = notification else {
        return Ok(());
    };
    let item = &params["item"];
    let thread = params["threadId"]
        .as_str()
        .ok_or_else(|| CalmError::Conflict("Native history has no thread".into()))?;
    let turn = params["turnId"].as_str();
    let id = item["id"].as_str();
    let kind = item["type"].as_str();
    let serialized = serde_json::to_string(params)?;
    let owner = TranscriptOwner {
        repo: shared.params.repo.as_ref(),
        events: &shared.params.events,
        write: shared.params.write.clone(),
        worker_session_id: &shared.params.worker_session_id,
        card_id: &shared.params.card_id,
        track_id: &shared.params.track_id,
    };
    let row = TranscriptItem {
        thread_id: thread,
        metadata: ItemMetadata {
            turn_id: turn,
            item_uuid: id,
            item_type: kind,
            method,
        },
        params_json: &serialized,
        legacy_segments_json: None,
        projection_client_id: item["clientId"].as_str(),
    };
    let db_id = owner.record(&row).await?;
    let track = shared
        .params
        .repo
        .track_get(&shared.params.track_id)
        .await?
        .ok_or_else(|| CalmError::NotFound("Native history Track was deleted".into()))?;
    owner
        .announce(
            crate::event::EventScope::Card {
                card: shared.params.card_id.clone().into(),
                track: track.id,
                area: track.area_id,
            },
            db_id,
            &row.metadata,
        )
        .await
}
