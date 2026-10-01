use super::runtime_read_flip_support::{create_card_in_tx, fresh_repo};
use super::*;
use crate::opencode_submission::{OpenCodeSubmissionIntent, OpenCodeSubmissionState as State};
use crate::session_projection_repo::{AgentProvider, ThreadAttribution, WorkerSessionInit};
use calm_types::worker::WorkerSessionState;
use serde_json::json;

async fn bind(repo: &SqlxRepo, card: &str, worker: &str, native: &str) {
    let mut tx = repo.pool().begin().await.unwrap();
    if let Some(old) = session_projection_active_for_card_tx(&mut tx, card)
        .await
        .unwrap()
    {
        session_supersede_active_tx(&mut tx, &old.id, 2)
            .await
            .unwrap();
    }
    session_start_runtime_tx(
        &mut tx,
        WorkerSessionInit::shared_planner(
            worker.into(),
            card.into(),
            AgentProvider::OpenCode,
            WorkerSessionState::Idle,
            Some(format!("thread-{worker}")),
            json!({"mode":"harness"}),
            1,
        ),
    )
    .await
    .unwrap();
    session_bind_attribution_tx(
        &mut tx,
        &worker.into(),
        ThreadAttribution {
            worker_session_id: worker.into(),
            provider: AgentProvider::OpenCode,
            thread_id: Some(format!("thread-{worker}")),
            session_id: Some(native.into()),
            active_turn_id: None,
        },
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
}

async fn setup() -> (SqlxRepo, String, OpenCodeSubmissionIntent) {
    let repo = fresh_repo().await;
    let mut tx = repo.pool().begin().await.unwrap();
    let card = create_card_in_tx(&repo, &mut tx, "opencode-journal", "codex").await;
    tx.commit().await.unwrap();
    bind(&repo, &card, "worker-1", "native-1").await;
    let intent = OpenCodeSubmissionIntent {
        id: "intent-1".into(),
        worker_session_id: "worker-1".into(),
        card_id: card.clone(),
        scope_id: "scope-1".into(),
        generation: 0,
        thread_id: "thread-worker-1".into(),
        native_session_id: "native-1".into(),
        client_id: "client-1".into(),
        native_message_id: "msg-1".into(),
        input_json: json!({"messageID":"msg-1","parts":[]}),
        created_at_ms: 1,
    };
    (repo, card, intent)
}

#[tokio::test]
async fn opencode_unknown_intent_never_reclaims_send_and_blocks_successor_or_native_alias() {
    let (repo, card, intent) = setup().await;
    let prepared = opencode_submission_prepare(repo.pool(), &intent)
        .await
        .unwrap();
    assert_eq!(prepared.state, State::Prepared);
    assert!(
        opencode_submission_claim_prepared(repo.pool(), &intent.id, 2)
            .await
            .unwrap()
    );
    assert!(
        !opencode_submission_claim_prepared(repo.pool(), &intent.id, 3)
            .await
            .unwrap()
    );
    opencode_submission_mark_unknown(repo.pool(), &intent.id, 4)
        .await
        .unwrap();
    assert!(
        !opencode_submission_claim_prepared(repo.pool(), &intent.id, 5)
            .await
            .unwrap()
    );
    let replay = opencode_submission_prepare(repo.pool(), &intent)
        .await
        .unwrap();
    assert_eq!(
        replay.state,
        State::Unknown,
        "same intent preserves uncertain acceptance"
    );
    assert_eq!(replay.input_fingerprint, prepared.input_fingerprint);
    assert_eq!(
        opencode_submission_get_unresolved(repo.pool(), "worker-1")
            .await
            .unwrap(),
        Some(replay.clone())
    );
    assert_eq!(
        opencode_submission_get_unresolved_by_card(repo.pool(), &card)
            .await
            .unwrap(),
        Some(replay.clone())
    );
    let mut changed = intent.clone();
    changed.input_json["parts"] = json!([{"text":"different"}]);
    assert!(
        opencode_submission_prepare(repo.pool(), &changed)
            .await
            .is_err()
    );

    bind(&repo, &card, "worker-2", "native-2").await;
    let mut successor = intent.clone();
    successor.id = "intent-2".into();
    successor.worker_session_id = "worker-2".into();
    successor.thread_id = "thread-worker-2".into();
    successor.native_session_id = "native-2".into();
    successor.native_message_id = "msg-2".into();
    successor.client_id = "client-2".into();
    assert!(
        opencode_submission_prepare(repo.pool(), &successor)
            .await
            .is_err(),
        "card unresolved fence survives retirement"
    );
    assert_eq!(
        opencode_submission_get_unresolved_by_card(repo.pool(), &card)
            .await
            .unwrap(),
        Some(replay)
    );

    let mut tx = repo.pool().begin().await.unwrap();
    let alias_card = create_card_in_tx(&repo, &mut tx, "opencode-alias", "codex").await;
    tx.commit().await.unwrap();
    bind(&repo, &alias_card, "worker-3", "native-1").await;
    successor.card_id = alias_card;
    successor.worker_session_id = "worker-3".into();
    successor.thread_id = "thread-worker-3".into();
    successor.native_session_id = "native-1".into();
    assert!(
        opencode_submission_prepare(repo.pool(), &successor)
            .await
            .is_err(),
        "native unresolved fence covers different cards"
    );
}

#[tokio::test]
async fn opencode_journal_rejects_unsent_completion_and_keeps_terminal_evidence_immutable() {
    let (repo, _, intent) = setup().await;
    opencode_submission_prepare(repo.pool(), &intent)
        .await
        .unwrap();
    let outcome = json!({"native_message_id":"msg-1","assistant_message_id":"reply-1"});
    assert!(
        opencode_submission_settle(repo.pool(), &intent.id, State::Completed, &outcome, 2)
            .await
            .is_err()
    );
    assert!(
        opencode_submission_claim_prepared(repo.pool(), &intent.id, 3)
            .await
            .unwrap()
    );
    opencode_submission_mark_unknown(repo.pool(), &intent.id, 4)
        .await
        .unwrap();
    opencode_submission_settle(repo.pool(), &intent.id, State::Completed, &outcome, 5)
        .await
        .unwrap();
    opencode_submission_settle(repo.pool(), &intent.id, State::Completed, &outcome, 6)
        .await
        .unwrap();
    assert!(
        opencode_submission_get_unresolved_by_card(repo.pool(), &intent.card_id)
            .await
            .unwrap()
            .is_none()
    );
    let settled = opencode_submission_get_by_client(
        repo.pool(),
        &intent.scope_id,
        &intent.native_session_id,
        &intent.client_id,
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(settled.state, State::Completed);
    assert_eq!(settled.outcome_json, Some(outcome.clone()));
    assert_eq!(
        settled.updated_at_ms, 5,
        "idempotent read cannot rewrite settlement"
    );
    assert!(
        opencode_submission_settle(repo.pool(), &intent.id, State::Failed, &outcome, 7)
            .await
            .is_err()
    );
    assert!(
        opencode_submission_mark_unknown(repo.pool(), &intent.id, 8)
            .await
            .is_err()
    );
    for query in [
        "UPDATE opencode_submissions SET input_json = '{}' WHERE id = 'intent-1'",
        "UPDATE opencode_submissions SET state = 'prepared', outcome_json = NULL WHERE id = 'intent-1'",
        "DELETE FROM opencode_submissions WHERE id = 'intent-1'",
    ] {
        assert!(
            sqlx::query(query).execute(repo.pool()).await.is_err(),
            "journal mutation denied: {query}"
        );
    }
}

#[tokio::test]
async fn opencode_journal_requires_matching_live_native_authority_before_send() {
    let (repo, card, intent) = setup().await;
    let mut mismatch = intent.clone();
    mismatch.native_session_id = "foreign-native".into();
    assert!(
        opencode_submission_prepare(repo.pool(), &mismatch)
            .await
            .is_err()
    );
    opencode_submission_prepare(repo.pool(), &intent)
        .await
        .unwrap();
    bind(&repo, &card, "worker-2", "native-2").await;
    assert!(
        !opencode_submission_claim_prepared(repo.pool(), &intent.id, 2)
            .await
            .unwrap(),
        "superseded runtime cannot acquire send permission"
    );
}
