//! Recover durable provider admission into the kernel's queue and correlation state.
//! Queue ownership is proved by the exact pre-dispatch batch, never by matching user text.
use super::{HarnessPhaseTag, HarnessSnapshot, PlannerBackend, QueueEntry};
use crate::db::Repo;
use crate::error::{CalmError, Result};
use calm_truth::opencode_submission::OpenCodeSubmission;
use serde_json::{Value, json};

/// Internal projection metadata produced from the real drained queue entries.
pub fn claimed_queue_entries(entries: &[QueueEntry]) -> Value {
    Value::Array(entries.iter().map(claimed_entry).collect())
}

fn claimed_entry(entry: &QueueEntry) -> Value {
    let kind = match entry {
        QueueEntry::User { .. } => "user",
        QueueEntry::LegacyUser { .. } => "legacy-user",
        QueueEntry::System { .. } => "system",
    };
    let user = entry.user_view();
    json!({
        "kind":kind,
        "id":entry.id(),
        "observation":entry.observation(),
        "envelopeId":entry.envelope_id(),
        "messageIds":entry.message_ids(),
        "attachments":entry.attachments(),
        "rev":user.as_ref().map(|view| view.rev),
        "queuedAtMs":user.as_ref().map(|view| view.queued_at_ms),
    })
}

fn retire_claimed_prefix(snapshot: &mut HarnessSnapshot, proof: &Value) -> Result<()> {
    let claims = proof
        .as_array()
        .filter(|claims| !claims.is_empty())
        .ok_or_else(|| {
            CalmError::Conflict("OpenCode recovery has no exact queued-batch proof".into())
        })?;
    let entries = snapshot.pending_entries();
    if entries.len() < claims.len()
        || entries
            .iter()
            .zip(claims)
            .any(|(entry, claim)| claimed_entry(entry) != *claim)
    {
        return Err(CalmError::Conflict(
            "OpenCode recovery queued batch changed; refusing to discard unrelated input".into(),
        ));
    }
    snapshot.set_pending_entries(entries.into_iter().skip(claims.len()).collect());
    Ok(())
}

async fn batch_proof(repo: &dyn Repo, receipt: &OpenCodeSubmission) -> Result<Value> {
    let mut cursor = 0;
    // The original projection may precede the native tool tail. Walk the storage owner's
    // bounded pages instead of assuming the most recent user echo still has the proof.
    for _ in 0..64 {
        let rows = repo
            .harness_item_list_by_card(&receipt.card_id, cursor, 500, true)
            .await?;
        for row in rows.iter().rev() {
            if row.worker_session_id != receipt.worker_session_id
                || row.thread_id != receipt.thread_id
                || row.item_type.as_deref() != Some("userMessage")
                || row.method != "item/completed"
            {
                continue;
            }
            let params: Value = serde_json::from_str(&row.params)?;
            if (row.item_uuid.as_deref() == Some(&receipt.client_id)
                || params["item"]["clientId"].as_str() == Some(&receipt.client_id))
                && let Some(proof) = params.get("calmQueueEntries")
            {
                return Ok(proof.clone());
            }
        }
        let Some(first) = rows.first() else { break };
        if rows.len() < 500 {
            break;
        }
        cursor = first.id;
    }
    Err(CalmError::Conflict(
        "OpenCode recovery could not find its queued-batch proof".into(),
    ))
}

/// Called before Harness installation, so a recovered observer cannot race queue issuance.
pub(crate) async fn adopt(
    repo: &dyn Repo,
    backend: &PlannerBackend,
    snapshot: &mut HarnessSnapshot,
) -> Result<Option<String>> {
    let PlannerBackend::OpenCode(session) = backend else {
        return Ok(None);
    };
    let receipt = session
        .recovery_submission(snapshot.projection_client_id.as_ref().map(|id| id.as_str()))
        .await?;
    let Some(receipt) = receipt else {
        return Ok(None);
    };
    session.adopt_recovery_binding(&receipt).await?;
    // IssuingTurn is the pre-drain checkpoint. Later phases already committed the drain;
    // their pending queue belongs to later input, even when its text happens to match.
    if snapshot.phase == HarnessPhaseTag::IssuingTurn && !snapshot.pending_entries().is_empty() {
        retire_claimed_prefix(snapshot, &batch_proof(repo, &receipt).await?)?;
    }
    snapshot.last_thread_id = Some(receipt.thread_id.clone());
    snapshot.last_turn_id = Some(receipt.id);
    snapshot.issued_input_segments = None;
    if receipt.state.is_terminal() {
        snapshot.phase = HarnessPhaseTag::TurnCompleted;
        snapshot.projection_client_id = None;
    } else {
        snapshot.phase = HarnessPhaseTag::TurnRunning;
    }
    Ok(Some(receipt.thread_id))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn claimed_batch_retires_original_identity_and_preserves_equal_later_text() {
        let original = QueueEntry::user_message("same words".into(), None, Vec::new());
        let later = QueueEntry::user_message("same words".into(), None, Vec::new());
        let proof = claimed_queue_entries(std::slice::from_ref(&original));
        let mut snapshot = HarnessSnapshot::initial(0, vec![original, later.clone()]);
        retire_claimed_prefix(&mut snapshot, &proof).unwrap();
        assert_eq!(snapshot.pending_entries(), vec![later]);
    }

    #[test]
    fn claimed_batch_changed_input_fails_closed_without_queue_mutation() {
        let original = QueueEntry::user_message("original".into(), None, Vec::new());
        let proof = claimed_queue_entries(std::slice::from_ref(&original));
        let mut changed = original;
        if let QueueEntry::User { text, .. } = &mut changed {
            *text = "changed".into();
        }
        let mut snapshot = HarnessSnapshot::initial(0, vec![changed.clone()]);
        assert!(retire_claimed_prefix(&mut snapshot, &proof).is_err());
        assert_eq!(snapshot.pending_entries(), vec![changed]);
    }
}
