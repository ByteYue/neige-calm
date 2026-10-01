//! #1791: a Planner session row persists the provider its mint input names.

use super::session_projection::{runtime_get_by_id_from_pool, runtimes_active_for_kind_from_pool};
use super::*;
use crate::session_projection_repo::{
    AgentProvider, WorkerSessionInit, WorkerSessionKind, WorkerSessionProjectionRepoError,
};
use calm_types::worker::WorkerSessionState;
use serde_json::json;

use super::runtime_read_flip_support::{create_card_in_tx, fresh_repo};

async fn stored_identity(repo: &SqlxRepo, id: &str) -> (String, String, String) {
    sqlx::query_as("SELECT provider, mode, contract FROM worker_sessions WHERE id = ?1")
        .bind(id)
        .fetch_one(repo.pool())
        .await
        .expect("session row")
}

#[tokio::test]
async fn a_planner_mint_persists_the_provider_it_names() {
    let repo = fresh_repo().await;
    for (label, provider, stored) in [
        ("planner-claude", AgentProvider::Claude, "claude"),
        ("planner-codex", AgentProvider::Codex, "codex"),
        ("planner-opencode", AgentProvider::OpenCode, "opencode"),
    ] {
        let mut tx = repo.pool().begin().await.expect("begin");
        let card_id = create_card_in_tx(&repo, &mut tx, label, "codex").await;
        let id = format!("rt-{label}");
        session_start_runtime_tx(
            &mut tx,
            WorkerSessionInit::shared_planner(
                id.clone(),
                card_id,
                provider.clone(),
                WorkerSessionState::Idle,
                Some(format!("thread-{label}")),
                json!({"mode": "harness"}),
                1_000,
            ),
        )
        .await
        .expect("start planner runtime");
        tx.commit().await.expect("commit");

        assert_eq!(
            stored_identity(&repo, &id).await,
            (stored.into(), "resumable".into(), "planner".into()),
            "{label}"
        );
        let projected = runtime_get_by_id_from_pool(repo.pool(), &id)
            .await
            .expect("by-id read")
            .expect("row");
        assert_eq!(projected.kind, WorkerSessionKind::SharedPlanner, "{label}");
        assert_eq!(projected.agent_provider, Some(provider), "{label}");
    }
    let mut planners: Vec<String> =
        runtimes_active_for_kind_from_pool(repo.pool(), WorkerSessionKind::SharedPlanner)
            .await
            .expect("active planners")
            .into_iter()
            .map(|runtime| runtime.id)
            .collect();
    planners.sort();
    assert_eq!(
        planners,
        [
            "rt-planner-claude",
            "rt-planner-codex",
            "rt-planner-opencode"
        ]
    );
}

#[tokio::test]
async fn a_deferred_claude_placeholder_is_a_claude_row() {
    let repo = fresh_repo().await;
    let mut tx = repo.pool().begin().await.expect("begin");
    let card_id = create_card_in_tx(&repo, &mut tx, "deferred-claude", "codex").await;
    session_prepare_deferred_planner_tx(
        &mut tx,
        &WorkerSessionInit::shared_planner(
            "rt-deferred-claude".into(),
            card_id,
            AgentProvider::Claude,
            WorkerSessionState::Starting,
            None,
            json!({"mode": "harness"}),
            1_000,
        ),
    )
    .await
    .expect("prepare deferred placeholder");
    tx.commit().await.expect("commit");
    assert_eq!(
        stored_identity(&repo, "rt-deferred-claude").await,
        ("claude".into(), "resumable".into(), "planner".into())
    );
}

#[tokio::test]
async fn a_planner_init_without_a_provider_writes_nothing() {
    let repo = fresh_repo().await;
    let mut tx = repo.pool().begin().await.expect("begin");
    let card_id = create_card_in_tx(&repo, &mut tx, "planner-unnamed", "codex").await;
    let mut init = WorkerSessionInit::shared_planner(
        "rt-planner-unnamed".into(),
        card_id,
        AgentProvider::Codex,
        WorkerSessionState::Idle,
        Some("thread-unnamed".into()),
        json!({"mode": "harness"}),
        1_000,
    );
    init.agent_provider = None;
    let err = session_start_runtime_tx(&mut tx, init)
        .await
        .expect_err("a planner row needs its provider");
    assert_eq!(
        err,
        WorkerSessionProjectionRepoError::Message {
            message: "planner runtime init rt-planner-unnamed names no provider".into()
        }
    );
}

#[tokio::test]
async fn owned_planner_session_revocation_preserves_other_credentials() {
    for (provider, stored) in [
        (AgentProvider::Claude, "claude"),
        (AgentProvider::OpenCode, "opencode"),
    ] {
        for scope in ["session", "missing", "track", "all"] {
            let repo = fresh_repo().await;
            let mut tx = repo.pool().begin().await.unwrap();
            for (id, row_provider) in [
                ("target", provider.clone()),
                ("other", provider.clone()),
                ("codex-control", AgentProvider::Codex),
            ] {
                let card = create_card_in_tx(&repo, &mut tx, id, "codex").await;
                session_start_runtime_tx(
                    &mut tx,
                    WorkerSessionInit::shared_planner(
                        id.into(),
                        card,
                        row_provider,
                        WorkerSessionState::Idle,
                        Some(format!("thread-{id}")),
                        json!({"mode":"harness"}),
                        1,
                    ),
                )
                .await
                .unwrap();
                session_mcp_token_set_if_active_tx(&mut tx, id, &format!("token-{id}"))
                    .await
                    .unwrap();
            }
            let track: String =
                sqlx::query_scalar("SELECT track_id FROM worker_sessions WHERE id = 'target'")
                    .fetch_one(&mut *tx)
                    .await
                    .unwrap();
            let scope = match scope {
                "session" => OwnedPlannerScope::Session("target"),
                "missing" => OwnedPlannerScope::Session("does-not-exist"),
                "track" => OwnedPlannerScope::Track(&track),
                "all" => OwnedPlannerScope::All,
                _ => unreachable!(),
            };
            let ids = match provider {
                AgentProvider::Claude => claude_planner_revoke_tx(&mut tx, scope).await,
                AgentProvider::OpenCode => opencode_planner_revoke_tx(&mut tx, scope).await,
                AgentProvider::Codex => unreachable!(),
            }
            .expect("scoped revocation executes");
            let expected_ids = match scope {
                OwnedPlannerScope::Session("does-not-exist") => vec![],
                OwnedPlannerScope::All => vec!["other", "target"],
                _ => vec!["target"],
            };
            assert_eq!(ids, expected_ids, "{stored}/{scope:?}: returned sweep IDs");
            let rows: Vec<(String, Option<String>)> =
                sqlx::query_as("SELECT id,mcp_token_hash FROM worker_sessions ORDER BY id")
                    .fetch_all(&mut *tx)
                    .await
                    .unwrap();
            for (id, token) in rows {
                let expected = if expected_ids.contains(&id.as_str()) {
                    None
                } else {
                    Some(format!("token-{id}"))
                };
                assert_eq!(token, expected, "{stored}/{scope:?}: credential for {id}");
            }
            tx.commit().await.unwrap();
        }
    }
}
