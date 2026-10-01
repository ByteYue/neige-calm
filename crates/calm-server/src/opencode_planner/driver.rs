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
    let mut projection = TurnProjection::new(
        submission.thread_id.clone(),
        submission.id.clone(),
        submission.native_message_id.clone(),
        submission.client_id.clone(),
        shared.params.cwd.to_string_lossy().into_owned(),
        shared.params.prior_total_tokens,
    );
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
        reject_pending(&shared, &client, &native, submission).await?;
        match snapshot(&client, &native, submission).await {
            Ok(messages) => {
                for frame in projection.snapshot(&messages) {
                    shared.send(frame);
                }
                if let Some(outcome) = projection.outcome(&messages) {
                    // Persist evidence before broadcasting completion. A provider process may
                    // remain for subsequent turns, but all associated native tools are settled.
                    let value = projection.terminal_value(&outcome);
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
                        &pool,
                        &submission.id,
                        state,
                        &value,
                        chrono::Utc::now().timestamp_millis(),
                    )
                    .await?;
                    let mut slot = shared.state();
                    if slot
                        .active
                        .as_ref()
                        .is_some_and(|a| a.submission.id == submission.id)
                    {
                        slot.active = None;
                    }
                    shared.send(projection.usage(&messages));
                    shared.send(projection.completed(&outcome));
                    return Ok(());
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
                        &format!("OpenCode snapshot is unavailable: {error}"),
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

async fn snapshot(
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
    let messages = client
        .get(&format!("/session/{native}/message?limit=256"))
        .await?;
    let mut messages = messages
        .as_array()
        .cloned()
        .ok_or_else(|| CalmError::Conflict("OpenCode message snapshot is malformed".into()))?;
    if messages.len() >= 256 {
        return Err(CalmError::Conflict(
            "OpenCode snapshot is truncated; terminal evidence cannot be established".into(),
        ));
    }
    if messages
        .iter()
        .any(|m| m["info"]["sessionID"].as_str() != Some(native))
    {
        return Err(CalmError::Conflict(
            "OpenCode snapshot contained another session".into(),
        ));
    }
    if !messages
        .iter()
        .any(|m| m["info"]["id"] == user["info"]["id"])
    {
        messages.push(user);
    }
    Ok(messages)
}

struct AbortOnDrop(Option<tokio::task::JoinHandle<Result<Value>>>);
impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        if let Some(task) = self.0.take() {
            task.abort();
        }
    }
}
