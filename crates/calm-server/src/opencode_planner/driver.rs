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
    let mut projection = TurnProjection::new(
        submission.thread_id.clone(),
        submission.id.clone(),
        submission.native_message_id.clone(),
        submission.client_id.clone(),
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
    loop {
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
            if !matches!(result, Ok(Ok(_))) {
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
            reject_pending(&shared, &client, &native, submission).await?;
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
                if let Some(outcome) = projection.outcome(&messages) {
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
    shared
        .params
        .repo
        .harness_turn_outcome_put(
            &shared.params.worker_session_id,
            &shared.params.card_id,
            &shared.params.track_id,
            &submission.thread_id,
            &submission.id,
            &serde_json::to_string(&value)?,
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
) -> Result<()> {
    for (kind, list) in [("permission", "/permission"), ("question", "/question")] {
        let Ok(pending) = client.get(list).await else {
            continue;
        };
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
            client
                .request("POST", &path, Some(&payload), Duration::from_secs(3))
                .await?;
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
    Ok(())
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
    {
        return Err(CalmError::Conflict(
            "OpenCode exact message evidence mismatched".into(),
        ));
    }
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
        let original = page_messages
            .iter()
            .position(|m| m["info"]["id"] == user["info"]["id"]);
        let relevant = &page_messages[original.unwrap_or(0)..];
        if messages.len().saturating_add(relevant.len()) > 16_384 {
            return Err(CalmError::Conflict(
                "OpenCode current-turn tail exceeds the bounded reconciliation budget".into(),
            ));
        }
        for message in relevant {
            if message["info"]["sessionID"].as_str() != Some(native) {
                return Err(CalmError::Conflict(
                    "OpenCode snapshot contained another session".into(),
                ));
            }
            bytes = bytes.saturating_add(serde_json::to_vec(message)?.len());
            if bytes > 16 * 1024 * 1024 || messages.len() >= 16_384 {
                return Err(CalmError::Conflict(
                    "OpenCode current-turn tail exceeds the bounded reconciliation budget".into(),
                ));
            }
        }
        // Pages are ascending internally but retrieved newest first. Preserve native
        // transcript order by prepending each older page; trim previous turns once found.
        messages.splice(0..0, relevant.iter().cloned());
        if original.is_some() {
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
