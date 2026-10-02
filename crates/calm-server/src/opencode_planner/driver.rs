//! Native evidence drives settlement. Polling, reconnection and stop never submit a prompt.
use super::{
    client::Client,
    session::Shared,
    translate::{Outcome, TurnProjection},
};
use crate::error::{CalmError, Result};
use calm_truth::opencode_submission::{OpenCodeSubmission, OpenCodeSubmissionState};
use serde_json::Value;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;

pub(crate) async fn drive(
    shared: Arc<Shared>,
    submission: OpenCodeSubmission,
    mut cancelled: watch::Receiver<bool>,
    dispatch: Option<(Client, Value)>,
) {
    if let Err(error) = run(Arc::clone(&shared), &submission, &mut cancelled, dispatch).await {
        // Every observer exit is fail-closed for its owned serve/tools, including initial
        // identity queries, denied-control errors and SQLite errors. The durable fence stays.
        if let Err(stop) = shared.stop_process().await {
            tracing::error!(%stop, "OpenCode observer cleanup failed");
        }
        if let Ok(pool) = shared.pool() {
            let _ = crate::db::sqlite::opencode_submission_mark_unknown(
                &pool,
                &submission.id,
                chrono::Utc::now().timestamp_millis(),
            )
            .await;
        }
        shared.unknown(
            &submission.thread_id,
            &submission.id,
            &format!("OpenCode native outcome is unknown: {error}"),
        );
    }
}

async fn run(
    shared: Arc<Shared>,
    submission: &OpenCodeSubmission,
    cancelled: &mut watch::Receiver<bool>,
    dispatch: Option<(Client, Value)>,
) -> Result<()> {
    let pool = shared.pool()?;
    let prior_tokens = shared.state().total_tokens;
    let original_text = submission.input_json["parts"]
        .as_array()
        .ok_or_else(|| {
            CalmError::Conflict("OpenCode durable submission has no input parts".into())
        })?
        .iter()
        .map(|part| {
            part["text"]
                .as_str()
                .filter(|_| part["type"].as_str() == Some("text"))
                .map(str::to_owned)
                .ok_or_else(|| {
                    CalmError::Conflict("OpenCode durable submission has invalid text parts".into())
                })
        })
        .collect::<Result<Vec<_>>>()?;
    let mut projection = TurnProjection::new(
        submission.thread_id.clone(),
        submission.id.clone(),
        submission.native_message_id.clone(),
        submission.client_id.clone(),
        original_text,
        shared.params.cwd.to_string_lossy().into_owned(),
        prior_tokens,
    )?;
    if submission.state == OpenCodeSubmissionState::Prepared && dispatch.is_none() {
        // Only Sending can have crossed the POST boundary: Prepared is durable proof that
        // the CAS was never claimed. Do not resurrect an abandoned queued operational act.
        let outcome = Outcome::Failed(
            "OpenCode submission was never sent: recovery found a prepared intent before dispatch"
                .into(),
        );
        shared.send(projection.started());
        return settle(&shared, submission, &projection, &outcome, &[]).await;
    }
    let recovered = dispatch.is_none();
    let client = shared.ensure_server().await?;
    // Validate the existing native identity under this directory before any observations
    // or control. GET does not implicitly create a missing native session.
    let native = shared
        .native_session(&client, &submission.thread_id)
        .await?;
    if native != submission.native_session_id {
        return Err(CalmError::Conflict(
            "OpenCode recovery native session mismatch".into(),
        ));
    }
    let send = dispatch.map(|(client, payload)| {
        let native = native.clone();
        tokio::spawn(async move {
            client
                .request(
                    "POST",
                    &format!("/session/{native}/message"),
                    Some(&payload),
                    Duration::from_secs(3600),
                )
                .await
        })
    });
    let mut send = AbortOnDrop(send);
    if recovered {
        crate::db::sqlite::opencode_submission_mark_unknown(
            &pool,
            &submission.id,
            chrono::Utc::now().timestamp_millis(),
        )
        .await?;
        shared.send(projection.started());
        shared.unknown(
            &submission.thread_id,
            &submission.id,
            "recovering the original native submission; no prompt was resent",
        );
    }
    let mut stop_until = None;
    let mut failed_transport = false;
    let mut native_return = None;
    let mut denial = None;
    loop {
        if shared.state().shutting_down && shared.attachment.is_some() {
            return Ok(());
        }
        if *cancelled.borrow() && stop_until.is_none() {
            stop_until = Some(tokio::time::Instant::now() + Duration::from_secs(5));
            // A failed abort is ambiguous too; the bounded process sweep below still runs.
            let _ = client
                .request(
                    "POST",
                    &format!("/session/{native}/abort"),
                    Some(&serde_json::json!({})),
                    Duration::from_secs(3),
                )
                .await;
        }
        if send.0.as_ref().is_some_and(|task| task.is_finished()) {
            let result = send.0.take().expect("finished send").await;
            if let Ok(Ok(response)) = result {
                native_return = Some(response);
            } else {
                failed_transport = true;
                crate::db::sqlite::opencode_submission_mark_unknown(
                    &pool,
                    &submission.id,
                    chrono::Utc::now().timestamp_millis(),
                )
                .await?;
                shared.unknown(&submission.thread_id,&submission.id,"OpenCode submission response was lost; reconciling the original native message");
            }
        }
        let observation = async {
            if shared.attachment.is_none()
                && let Some(rejected) =
                    reject_pending(&shared, &client, &native, submission).await?
            {
                // Retain a confirmed control receipt even if the following snapshot fails.
                denial = Some(rejected);
            }
            snapshot(&client, &native, submission).await
        };
        let observed = if let Some(until) = stop_until {
            tokio::time::timeout_at(until, observation)
                .await
                .unwrap_or_else(|_| {
                    Err(CalmError::Conflict(
                        "OpenCode stop observation budget expired".into(),
                    ))
                })
        } else {
            tokio::select! {
                result = observation => result,
                _ = cancelled.changed() => continue,
            }
        };
        match observed {
            Ok(messages) => {
                for frame in projection.snapshot(&messages) {
                    shared.send(frame);
                }
                let outcome = projection.outcome(&messages).or_else(|| {
                    match (native_return.as_ref(), denial.as_deref()) {
                        (Some(response), Some(reason)) => {
                            projection.denied_loop_return(&messages, response, &native, reason)
                        }
                        _ => None,
                    }
                });
                if let Some(outcome) = outcome {
                    return settle(&shared, submission, &projection, &outcome, &messages).await;
                }
            }
            Err(error) => {
                if !failed_transport {
                    failed_transport = true;
                    crate::db::sqlite::opencode_submission_mark_unknown(
                        &pool,
                        &submission.id,
                        chrono::Utc::now().timestamp_millis(),
                    )
                    .await?;
                    shared.unknown(
                        &submission.thread_id,
                        &submission.id,
                        &format!("OpenCode observation is unavailable: {error}"),
                    );
                }
            }
        }
        if shared.state().shutting_down
            || stop_until.is_some_and(|until| tokio::time::Instant::now() >= until)
        {
            send.0.take().inspect(|task| task.abort());
            shared.stop_process().await?;
            return Err(CalmError::Conflict("OpenCode process stopped without correlated terminal evidence; execution remains unknown".into()));
        }
        tokio::select! {_=tokio::time::sleep(Duration::from_millis(300))=>{},_=cancelled.changed()=>{}}
    }
}

async fn settle(
    shared: &Shared,
    submission: &OpenCodeSubmission,
    projection: &TurnProjection,
    outcome: &Outcome,
    messages: &[Value],
) -> Result<()> {
    let value = projection.terminal_value(outcome);
    let state = match outcome {
        Outcome::Completed => OpenCodeSubmissionState::Completed,
        Outcome::Failed(_) => OpenCodeSubmissionState::Failed,
        Outcome::Interrupted => OpenCodeSubmissionState::Interrupted,
    };
    shared.stop_process().await?;
    crate::harness::turn_outcome::record(
        shared.params.repo.as_ref(),
        &shared.params.worker_session_id,
        &shared.params.card_id,
        &shared.params.track_id,
        &submission.thread_id,
        &submission.id,
        &value,
    )
    .await?;
    crate::db::sqlite::opencode_submission_settle(
        &shared.pool()?,
        &submission.id,
        state,
        &value,
        chrono::Utc::now().timestamp_millis(),
    )
    .await?;
    let usage = projection.usage(messages);
    {
        let mut slot = shared.state();
        if slot
            .active
            .as_ref()
            .is_some_and(|a| a.submission.id == submission.id)
        {
            slot.active = None;
            if let crate::codex_appserver::Notification::Other { params, .. } = &usage {
                slot.total_tokens = params["tokenUsage"]["total"]["totalTokens"]
                    .as_i64()
                    .ok_or_else(|| {
                        CalmError::Internal("OpenCode token projection has no total".into())
                    })?;
            }
        }
    }
    shared.send(usage);
    shared.send(projection.completed(outcome));
    Ok(())
}

async fn reject_pending(
    shared: &Shared,
    client: &Client,
    native: &str,
    submission: &OpenCodeSubmission,
) -> Result<Option<String>> {
    let mut rejected = None;
    for (kind, list) in [("permission", "/permission"), ("question", "/question")] {
        let pending = client.get(list).await?;
        let Some(pending) = pending.as_array() else {
            return Err(CalmError::Conflict(
                "OpenCode pending request list is malformed".into(),
            ));
        };
        for request in pending
            .iter()
            .filter(|r| r["sessionID"].as_str() == Some(native))
        {
            let id = request["id"].as_str().ok_or_else(|| {
                CalmError::Conflict("OpenCode pending request has no identity".into())
            })?;
            if id.is_empty()
                || id.len() > 160
                || !id.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
            {
                return Err(CalmError::Conflict(
                    "OpenCode pending request identity is invalid".into(),
                ));
            }
            let (path, payload) = if kind == "permission" {
                (
                    format!("/permission/{id}/reply"),
                    serde_json::json!({"reply":"reject"}),
                )
            } else {
                (format!("/question/{id}/reject"), serde_json::json!({}))
            };
            let reply = client
                .request("POST", &path, Some(&payload), Duration::from_secs(3))
                .await?;
            if reply != Value::Bool(true) {
                return Err(CalmError::Conflict(
                    "OpenCode did not confirm its pending request rejection".into(),
                ));
            }
            rejected = Some(format!(
                "OpenCode {kind} request was rejected: this Planner has no interactive approval channel"
            ));
            shared.send(crate::codex_appserver::Notification::Other {method:"opencode/request/denied".into(),params:serde_json::json!({"threadId":submission.thread_id,"turnId":submission.id,"message":format!("OpenCode {kind} request was rejected: this Planner has no interactive approval channel")})});
            shared.send(crate::codex_appserver::Notification::Item {
                method:"item/completed".into(),
                params:serde_json::json!({"threadId":submission.thread_id,"turnId":submission.id,"completedAtMs":chrono::Utc::now().timestamp_millis(),"item":{
                    "id":format!("opencode-denied-{id}"),"type":"dynamicToolCall","tool":format!("opencode.{kind}"),"arguments":request,"status":"failed",
                    "error":{"message":format!("OpenCode {kind} request was rejected: this Planner has no interactive approval channel")},
                }}),
            });
        }
    }
    Ok(rejected)
}

pub(crate) async fn snapshot(
    client: &Client,
    native: &str,
    submission: &OpenCodeSubmission,
) -> Result<Vec<Value>> {
    let user = client
        .get(&format!(
            "/session/{native}/message/{}",
            submission.native_message_id
        ))
        .await?;
    if user["info"]["id"].as_str() != Some(&submission.native_message_id)
        || user["info"]["sessionID"].as_str() != Some(native)
        || user["info"]["role"].as_str() != Some("user")
    {
        return Err(CalmError::Conflict(
            "OpenCode exact message evidence mismatched".into(),
        ));
    }
    let created = user["info"]["time"]["created"].as_i64().ok_or_else(|| {
        CalmError::Conflict("OpenCode original message has no creation time".into())
    })?;
    let mut found_original = false;
    let mut inspected = 0usize;
    let mut messages = Vec::new();
    let mut cursor: Option<String> = None;
    let mut seen_cursors = std::collections::HashSet::new();
    // Bound the current turn, rather than rejecting a long previous conversation. The
    // pinned API serves newest pages first and supplies an opaque backward cursor header.
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
        let page_messages = page
            .value
            .as_array()
            .ok_or_else(|| CalmError::Conflict("OpenCode message snapshot is malformed".into()))?;
        inspected = inspected.saturating_add(page_messages.len());
        if inspected > 65_536 {
            return Err(CalmError::Conflict(
                "OpenCode reconciliation exceeded its bounded timestamp group".into(),
            ));
        }
        let mut relevant = Vec::new();
        for message in page_messages {
            if message["info"]["sessionID"].as_str() != Some(native) {
                return Err(CalmError::Conflict(
                    "OpenCode snapshot contained another session".into(),
                ));
            }
            if message["info"]["time"]["created"].as_i64().is_none() {
                return Err(CalmError::Conflict(
                    "OpenCode message snapshot has no creation time".into(),
                ));
            }
            let original = message["info"]["id"] == user["info"]["id"];
            found_original |= original;
            if original
                || message["info"]["parentID"].as_str() == Some(&submission.native_message_id)
            {
                bytes = bytes.saturating_add(serde_json::to_vec(message)?.len());
                if bytes > 16 * 1024 * 1024
                    || messages.len().saturating_add(relevant.len()) >= 16_384
                {
                    return Err(CalmError::Conflict(
                        "OpenCode current-turn tail exceeds the bounded reconciliation budget"
                            .into(),
                    ));
                }
                relevant.push(message.clone());
            }
        }
        // Native pages sort by (created,id). A same-millisecond assistant can sort before
        // its UUID parent, so cover the entire equal-time group, including older pages.
        messages.splice(0..0, relevant);
        let crossed_original_time = page_messages
            .first()
            .and_then(|m| m["info"]["time"]["created"].as_i64())
            .is_some_and(|at| at < created);
        if found_original && (crossed_original_time || page.next_cursor.is_none()) {
            return Ok(messages);
        }
        let Some(next) = page.next_cursor else {
            return Err(CalmError::Conflict(
                "OpenCode snapshot omitted the original native message; history is incomplete"
                    .into(),
            ));
        };
        if next.len() > 2048 || !seen_cursors.insert(next.clone()) {
            return Err(CalmError::Conflict(
                "OpenCode pagination cursor failed to advance".into(),
            ));
        }
        cursor = Some(next);
    }
}

struct AbortOnDrop(Option<tokio::task::JoinHandle<Result<Value>>>);
impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        if let Some(task) = self.0.take() {
            task.abort();
        }
    }
}
