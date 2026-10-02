//! Journal state changes commit before the adapter may send a native request.

use serde_json::{Map, Value};
use sqlx::{Row, SqlitePool, sqlite::SqliteRow};

use crate::error::{CalmError, Result};
use crate::opencode_submission::{
    OpenCodeSubmission, OpenCodeSubmissionIntent, OpenCodeSubmissionState,
};

fn decode(row: &SqliteRow) -> Result<OpenCodeSubmission> {
    Ok(OpenCodeSubmission {
        id: row.try_get("id")?,
        worker_session_id: row.try_get("worker_session_id")?,
        card_id: row.try_get("card_id")?,
        scope_id: row.try_get("scope_id")?,
        generation: row.try_get("generation")?,
        thread_id: row.try_get("thread_id")?,
        native_session_id: row.try_get("native_session_id")?,
        client_id: row.try_get("client_id")?,
        native_message_id: row.try_get("native_message_id")?,
        input_json: serde_json::from_str(&row.try_get::<String, _>("input_json")?)?,
        input_fingerprint: row.try_get("input_fingerprint")?,
        state: OpenCodeSubmissionState::try_from(row.try_get::<String, _>("state")?)
            .map_err(CalmError::Internal)?,
        outcome_json: row
            .try_get::<Option<String>, _>("outcome_json")?
            .map(|value| serde_json::from_str(&value))
            .transpose()?,
        created_at_ms: row.try_get("created_at_ms")?,
        updated_at_ms: row.try_get("updated_at_ms")?,
    })
}

fn canonical(value: &Value) -> Value {
    match value {
        Value::Object(map) => {
            let mut keys: Vec<_> = map.keys().collect();
            keys.sort_unstable();
            let mut sorted = Map::new();
            for key in keys {
                sorted.insert(key.clone(), canonical(&map[key]));
            }
            Value::Object(sorted)
        }
        Value::Array(values) => Value::Array(values.iter().map(canonical).collect()),
        other => other.clone(),
    }
}

async fn unresolved(
    pool: &SqlitePool,
    key: &str,
    value: &str,
) -> Result<Option<OpenCodeSubmission>> {
    let row = sqlx::query(&format!(
        "SELECT * FROM opencode_submissions WHERE {key} = ?1 AND state IN ('prepared','sending','unknown')"
    )).bind(value).fetch_optional(pool).await?;
    row.as_ref().map(decode).transpose()
}

pub async fn opencode_submission_get_unresolved(
    pool: &SqlitePool,
    worker_session_id: &str,
) -> Result<Option<OpenCodeSubmission>> {
    unresolved(pool, "worker_session_id", worker_session_id).await
}

/// Card lookup also finds a predecessor's intent after its worker runtime has retired.
pub async fn opencode_submission_get_unresolved_by_card(
    pool: &SqlitePool,
    card_id: &str,
) -> Result<Option<OpenCodeSubmission>> {
    unresolved(pool, "card_id", card_id).await
}

pub async fn opencode_submission_get_by_client(
    pool: &SqlitePool,
    scope_id: &str,
    native_session_id: &str,
    client_id: &str,
) -> Result<Option<OpenCodeSubmission>> {
    let row = sqlx::query(concat!(
        "SELECT * FROM opencode_submissions WHERE scope_id = ?1 ",
        "AND native_session_id = ?2 AND client_id = ?3"
    ))
    .bind(scope_id)
    .bind(native_session_id)
    .bind(client_id)
    .fetch_optional(pool)
    .await?;
    row.as_ref().map(decode).transpose()
}

pub async fn opencode_submission_prepare(
    pool: &SqlitePool,
    intent: &OpenCodeSubmissionIntent,
) -> Result<OpenCodeSubmission> {
    let input = serde_json::to_string(&canonical(&intent.input_json))?;
    if !intent.input_json.is_object() {
        return Err(CalmError::Conflict(
            "OpenCode submission input must be an object",
        ));
    }
    let fingerprint = blake3::hash(input.as_bytes()).to_hex().to_string();
    let mut tx = pool.begin().await?;
    let existing = sqlx::query(concat!(
        "SELECT * FROM opencode_submissions WHERE scope_id = ?1 ",
        "AND native_session_id = ?2 AND client_id = ?3"
    ))
    .bind(&intent.scope_id)
    .bind(&intent.native_session_id)
    .bind(&intent.client_id)
    .fetch_optional(&mut *tx)
    .await?;
    if let Some(row) = existing {
        let old = decode(&row)?;
        if old.id != intent.id
            || old.worker_session_id != intent.worker_session_id
            || old.card_id != intent.card_id
            || old.generation != intent.generation
            || old.thread_id != intent.thread_id
            || old.native_message_id != intent.native_message_id
            || old.input_json != intent.input_json
            || old.input_fingerprint != fingerprint
            || old.created_at_ms != intent.created_at_ms
        {
            return Err(CalmError::Conflict(
                "OpenCode client ID names a different immutable intent",
            ));
        }
        tx.commit().await?;
        return Ok(old);
    }
    let result = sqlx::query(
        r#"INSERT INTO opencode_submissions
            (id,worker_session_id,card_id,scope_id,generation,thread_id,native_session_id,
             client_id,native_message_id,input_json,input_fingerprint,state,created_at_ms,updated_at_ms)
            SELECT ?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,'prepared',?12,?12
            FROM worker_sessions ws
            WHERE ws.id = ?2 AND ws.card_id = ?3 AND ws.thread_id = ?6
              AND ws.agent_session_id = ?7 AND ws.provider = 'opencode' AND (ws.contract = 'planner' OR (ws.contract = 'executor' AND EXISTS (SELECT 1 FROM cards c WHERE c.id = ws.card_id AND c.kind = 'codex' AND c.role = 'worker' AND json_extract(c.payload, '$.harness_profile') = 'plain_chat' AND json_extract(c.payload, '$.opencode_attachment.session_id') = ws.agent_session_id)))
              AND ws.state IN ('starting','running','idle','turn_pending')"#,
    ).bind(&intent.id).bind(&intent.worker_session_id).bind(&intent.card_id).bind(&intent.scope_id)
        .bind(intent.generation).bind(&intent.thread_id).bind(&intent.native_session_id)
        .bind(&intent.client_id).bind(&intent.native_message_id).bind(input).bind(fingerprint)
        .bind(intent.created_at_ms).execute(&mut *tx).await?;
    if result.rows_affected() != 1 {
        return Err(CalmError::Conflict(
            "OpenCode submission has no matching active Planner authority",
        ));
    }
    let row = sqlx::query("SELECT * FROM opencode_submissions WHERE id = ?1")
        .bind(&intent.id)
        .fetch_one(&mut *tx)
        .await?;
    let prepared = decode(&row)?;
    tx.commit().await?;
    Ok(prepared)
}

/// Only a fresh Prepared intent can authorize one HTTP POST. Recovery must never call this.
pub async fn opencode_submission_claim_prepared(
    pool: &SqlitePool,
    id: &str,
    now_ms: i64,
) -> Result<bool> {
    let result = sqlx::query(
        r#"UPDATE opencode_submissions SET state = 'sending', updated_at_ms = ?2
            WHERE id = ?1 AND state = 'prepared'
              AND EXISTS (SELECT 1 FROM worker_sessions ws
                  WHERE ws.id = opencode_submissions.worker_session_id
                    AND ws.provider = 'opencode' AND (ws.contract = 'planner' OR (ws.contract = 'executor' AND EXISTS (SELECT 1 FROM cards c WHERE c.id = ws.card_id AND c.kind = 'codex' AND c.role = 'worker' AND json_extract(c.payload, '$.harness_profile') = 'plain_chat' AND json_extract(c.payload, '$.opencode_attachment.session_id') = ws.agent_session_id)))
                    AND ws.card_id = opencode_submissions.card_id
                    AND ws.thread_id = opencode_submissions.thread_id
                    AND ws.agent_session_id = opencode_submissions.native_session_id
                    AND ws.state IN ('starting','running','idle','turn_pending'))"#,
    )
    .bind(id)
    .bind(now_ms)
    .execute(pool)
    .await?;
    Ok(result.rows_affected() == 1)
}

pub async fn opencode_submission_mark_unknown(
    pool: &SqlitePool,
    id: &str,
    now_ms: i64,
) -> Result<()> {
    let result = sqlx::query(concat!(
        "UPDATE opencode_submissions SET state = 'unknown', updated_at_ms = ?2 ",
        "WHERE id = ?1 AND state IN ('sending','unknown')"
    ))
    .bind(id)
    .bind(now_ms)
    .execute(pool)
    .await?;
    if result.rows_affected() != 1 {
        return Err(CalmError::Conflict(
            "OpenCode intent is not Sending or Unknown",
        ));
    }
    Ok(())
}

/// The adapter owns native completion/abort evidence. Losing HTTP or an idle status alone
/// cannot justify settlement. Prepared may only settle as a local failure or cancellation.
pub async fn opencode_submission_settle(
    pool: &SqlitePool,
    id: &str,
    state: OpenCodeSubmissionState,
    outcome_json: &Value,
    now_ms: i64,
) -> Result<()> {
    if !state.is_terminal() || outcome_json.is_null() {
        return Err(CalmError::Conflict(
            "OpenCode settlement requires a terminal state and evidence",
        ));
    }
    let mut tx = pool.begin().await?;
    let row = sqlx::query("SELECT * FROM opencode_submissions WHERE id = ?1")
        .bind(id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| CalmError::NotFound(format!("OpenCode submission {id}")))?;
    let old = decode(&row)?;
    if old.state.is_terminal() {
        if old.state == state && old.outcome_json.as_ref() == Some(outcome_json) {
            tx.commit().await?;
            return Ok(());
        }
        return Err(CalmError::Conflict(
            "OpenCode submission already has an immutable outcome",
        ));
    }
    if old.state == OpenCodeSubmissionState::Prepared && state == OpenCodeSubmissionState::Completed
    {
        return Err(CalmError::Conflict(
            "Unsent OpenCode intent cannot complete",
        ));
    }
    sqlx::query("UPDATE opencode_submissions SET state = ?2, outcome_json = ?3, updated_at_ms = ?4 WHERE id = ?1")
        .bind(id).bind(state.as_db_str()).bind(serde_json::to_string(&canonical(outcome_json))?)
        .bind(now_ms).execute(&mut *tx).await?;
    tx.commit().await?;
    Ok(())
}
