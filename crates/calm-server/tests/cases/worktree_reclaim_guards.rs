//! #1815 review round 1: what the released-worktree reclaim must not take — work left in the
//! checkout, a worktree a live terminal works in — and how it decides: the lease owner's attempt
//! over other tasks of the card, a replaced failed attempt, refusals remembered off the cap.
use std::path::{Path, PathBuf};
use std::sync::Arc;

use calm_server::db::sqlite::card_with_terminal_create_tx;
use calm_server::db::write_in_tx_typed;
use calm_server::model::{CardRole, Task, new_id};
use calm_server::operation::claude_adapter::ClaudeWorkerAdapter;
use calm_server::operation::{OperationKey, OperationRepo as _, SqlxOperationRepo};
use calm_server::session_projection_repo::AgentProvider;
use calm_server::state::CodexClient;
use calm_server::test_seams::{
    KernelWorkspaceLease, provision_workspace_lease_for_test,
    release_workspace_lease_for_card_for_test,
};
use calm_types::report_blocks::tasks::PLANNER_DECLARATION_AUTHOR;
use serde_json::json;

use super::git_delivery::*;
use super::task_replace::{prepare_replace, replace, replace_args};
use super::worktree_reclaim::*;
use crate::mcp_track_report::call_tool;
use crate::task_recovery::{current, declare};

/// A tracked file modified and an untracked file each keep the checkout (and its branch)
/// exactly as they were; an ignored file does not.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn work_left_in_the_checkout_keeps_it() {
    let fx = fixture().await;
    let (modified, _, modified_lease) = delivered(&fx, "modified").await;
    let (untracked, _, untracked_lease) = delivered(&fx, "untracked").await;
    let (ignored, _, ignored_lease) = delivered(&fx, "ignored").await;
    std::fs::write(
        modified_lease.path.join("README.md"),
        "edited after the report\n",
    )
    .unwrap();
    std::fs::write(untracked_lease.path.join("notes.txt"), "not committed\n").unwrap();
    let exclude = ignored_lease.git_common_dir.join("info").join("exclude");
    let mut patterns = std::fs::read_to_string(&exclude).unwrap_or_default();
    patterns.push_str("*.log\n");
    std::fs::write(&exclude, patterns).unwrap();
    std::fs::write(ignored_lease.path.join("build.log"), "ignored\n").unwrap();
    let ignored_tip = slice_tip(&fx, &ignored_lease, &ignored.card_id);

    tick(&fx).await;
    assert_kept(&fx, &modified_lease, &modified.card_id).await;
    assert_eq!(
        std::fs::read_to_string(modified_lease.path.join("README.md")).unwrap(),
        "edited after the report\n"
    );
    assert_kept(&fx, &untracked_lease, &untracked.card_id).await;
    assert_eq!(
        std::fs::read_to_string(untracked_lease.path.join("notes.txt")).unwrap(),
        "not committed\n"
    );
    assert_reclaimed(&fx, &ignored_lease, &ignored.card_id, &ignored_tip).await;
}

/// A terminal opened in the worktree (cwd at or under it) holds it until the terminal exits; a
/// terminal whose cwd merely shares the path as a string prefix holds nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn live_terminal_in_the_worktree_keeps_it_until_exit() {
    let fx = fixture().await;
    let (served, _, served_lease) = delivered(&fx, "served").await;
    let (sibling, _, sibling_lease) = delivered(&fx, "sibling").await;
    let dev_server = open_terminal(&fx, &served_lease.path.join("web")).await;
    let mut prefixed = sibling_lease.path.clone().into_os_string();
    prefixed.push("X");
    let _unrelated = open_terminal(&fx, Path::new(&prefixed)).await;
    let served_tip = slice_tip(&fx, &served_lease, &served.card_id);
    let sibling_tip = slice_tip(&fx, &sibling_lease, &sibling.card_id);

    tick(&fx).await;
    assert_kept(&fx, &served_lease, &served.card_id).await;
    assert_reclaimed(&fx, &sibling_lease, &sibling.card_id, &sibling_tip).await;

    fx.boot
        .repo
        .terminal_set_exit(&dev_server, Some(0), false)
        .await
        .unwrap();
    tick(&fx).await;
    assert_reclaimed(&fx, &served_lease, &served.card_id, &served_tip).await;
}

/// The lease owner's worker op names the attempt: its status decides, whatever other task rows
/// carry the same `worker_card_id`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_lease_owners_attempt_decides() {
    let fx = fixture().await;
    // The owner's attempt is done; another task stamped with the card is not finished.
    let (owner_done, owned_done) = owner_op_lease(&fx, "owner-done").await;
    stranger_task(&fx, "stranger-a", &owned_done.card, "failed").await;
    set_task_status(&fx, &owner_done.id, "done").await;
    // The owner's attempt is not finished; another task stamped with the card is done.
    let (owner_open, owned_open) = owner_op_lease(&fx, "owner-open").await;
    stranger_task(&fx, "stranger-b", &owned_open.card, "done").await;
    set_task_status(&fx, &owner_open.id, "failed").await;
    let tip = slice_tip(&fx, &owned_done.lease, &owned_done.card);

    tick(&fx).await;
    assert_reclaimed(&fx, &owned_done.lease, &owned_done.card, &tip).await;
    assert_kept(&fx, &owned_open.lease, &owned_open.card).await;
}

/// A failed attempt the Planner replaced (`calm.task.replace`) gives up its worktree; a failed
/// attempt nobody replaced keeps it for recovery.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replaced_failed_attempt_is_reclaimed() {
    let fx = fixture().await;
    prepare_replace(&fx).await;
    let (replaced_worker, replaced_task, replaced_lease) = failed_attempt(&fx, "replaced").await;
    let (kept_worker, _, kept_lease) = failed_attempt(&fx, "unreplaced").await;
    let receipt = replace(&fx, replace_args(&replaced_task, "replace-1"))
        .await
        .expect("replace admitted");
    assert_eq!(receipt["successor"]["key"], "replaced.2", "{receipt}");
    let tip = slice_tip(&fx, &replaced_lease, &replaced_worker.card_id);

    tick(&fx).await;
    assert_reclaimed(&fx, &replaced_lease, &replaced_worker.card_id, &tip).await;
    assert_kept(&fx, &kept_lease, &kept_worker.card_id).await;
}

/// Refused entries (here: untracked work) do not use up the per-pass cap: sixteen of them
/// ahead of a clean one still leave room for it on the first pass.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn refusals_do_not_count_toward_the_cap() {
    let fx = fixture().await;
    let mut dirty = Vec::new();
    for n in 0..16 {
        let (worker, _, lease) =
            released_without_delivery(&fx, &format!("dirty-{n}"), "done").await;
        std::fs::write(lease.path.join("scratch.txt"), "work\n").unwrap();
        dirty.push((worker, lease));
    }
    let (clean, _, clean_lease) = released_without_delivery(&fx, "clean", "done").await;
    let tip = slice_tip(&fx, &clean_lease, &clean.card_id);

    tick(&fx).await;
    assert_reclaimed(&fx, &clean_lease, &clean.card_id, &tip).await;
    for (worker, lease) in &dirty {
        assert_kept(&fx, lease, &worker.card_id).await;
    }
}

/// A running terminal (the production composite `calm.terminal.open` writes) whose cwd is `cwd`;
/// returns the terminal id.
async fn open_terminal(fx: &Fx, cwd: &Path) -> String {
    let card_id = new_id();
    let track_id = fx.boot.track_id.clone();
    let cache = fx.boot.card_role_cache.clone();
    let cwd = cwd.to_string_lossy().to_string();
    let (_, terminal) = write_in_tx_typed(fx.boot.repo.as_ref(), move |tx| {
        Box::pin(async move {
            card_with_terminal_create_tx(
                tx,
                card_id,
                &new_id(),
                None,
                track_id,
                None,
                None,
                "/bin/sh".into(),
                cwd,
                json!({}),
                CardRole::Worker,
                true,
                &cache,
                calm_server::routes::theme::RequestTheme::default_dark(),
                false,
            )
            .await
        })
    })
    .await
    .unwrap();
    terminal.id
}

/// A worker whose attempt failed through `calm.task.fail` (the production release).
async fn failed_attempt(
    fx: &Fx,
    name: &str,
) -> (
    calm_server::mcp_server::registry::ToolCallIdentity,
    Task,
    KernelWorkspaceLease,
) {
    let worker = fx.new_worker(name, AgentProvider::Claude).await;
    let lease = fx.kernel_lease(&worker.card_id).await;
    let task = fx
        .running_task(name, "claude", &worker.card_id, json!({}))
        .await;
    call_tool(
        &fx.boot,
        "calm.task.fail",
        worker.clone(),
        json!({"idempotency_key": task.id, "reason": "worker gave up"}),
    )
    .await
    .unwrap();
    assert_eq!(lease_state(fx, &lease.lease_id).await, "released");
    (worker, current(&fx.boot, name).await, lease)
}

struct OwnedLease {
    card: String,
    lease: KernelWorkspaceLease,
}

/// The production worker sequence up to the lease: the scheduler's payload, an operations row
/// keyed by the attempt, the real Claude worker adapter's `prepare_tx` (creates the worker card
/// with its PTY row and takes the lease with the op as `lease_owner`), the spawn's provisioning,
/// the worker process exiting, and the report's release. Returns the attempt and the lease.
async fn owner_op_lease(fx: &Fx, key: &str) -> (Task, OwnedLease) {
    declare(
        &fx.boot,
        json!({
            "key": key, "kind": "claude", "goal": format!("work {key}"),
            "declared_by": PLANNER_DECLARATION_AUTHOR, "ready": true,
            "no_gate_reason": "reclaim fixture",
        }),
    )
    .await;
    let task = current(&fx.boot, key).await;
    let pool = fx.pool();
    sqlx::query("UPDATE tasks SET status = 'dispatched' WHERE id = ?1")
        .bind(&task.id)
        .execute(&pool)
        .await
        .unwrap();
    let (kind, payload) = calm_server::scheduler::build_worker_payload(&task).unwrap();
    let ops = SqlxOperationRepo::new(pool.clone());
    let op_id = ops
        .insert_operation(
            kind,
            OperationKey {
                operation_key: format!("worker:{}", task.id),
                idempotency_key: Some(task.id.clone()),
                payload_hash: "hash".into(),
            },
            payload,
        )
        .await
        .unwrap();
    let op = ops
        .claim_operation_for_recovery(&op_id)
        .await
        .unwrap()
        .unwrap();
    let adapter = ClaudeWorkerAdapter::new(
        fx.boot.repo.clone(),
        Arc::new(CodexClient::new_stub()),
        None,
        fx.boot.card_role_cache.clone(),
        calm_server::track_area_cache::TrackAreaCache::new(),
        fx.workspace_root.clone(),
    );
    let (op, _) = ops
        .prepare_tx_and_advance(&op, &adapter)
        .await
        .unwrap()
        .unwrap();
    let card = op
        .target_id
        .clone()
        .expect("the worker card is the op target");
    let (lease_id, owner, path, base_sha, git_common_dir): (
        String,
        String,
        String,
        String,
        String,
    ) = sqlx::query_as(
        "SELECT lease_id, lease_owner, path, base_sha, git_common_dir \
         FROM workspace_leases WHERE card_id = ?1",
    )
    .bind(&card)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(owner, op.id, "the lease owner is the worker op");
    provision_workspace_lease_for_test(&pool, fx.track(), &card, &fx.workspace_root)
        .await
        .unwrap();
    let terminal: String = sqlx::query_scalar("SELECT id FROM terminals WHERE card_id = ?1")
        .bind(&card)
        .fetch_one(&pool)
        .await
        .unwrap();
    fx.boot
        .repo
        .terminal_set_exit(&terminal, Some(0), false)
        .await
        .unwrap();
    assert!(
        release_workspace_lease_for_card_for_test(
            fx.boot.repo.as_ref(),
            &fx.boot.ctx.events,
            &card
        )
        .await
        .unwrap()
    );
    sqlx::query("UPDATE tasks SET worker_card_id = ?1 WHERE id = ?2")
        .bind(&card)
        .bind(&task.id)
        .execute(&pool)
        .await
        .unwrap();
    let path = PathBuf::from(path);
    let repo_root = path.ancestors().nth(4).unwrap().to_path_buf();
    let lease = KernelWorkspaceLease {
        lease_id,
        path,
        repo_root,
        base_sha,
        git_common_dir: PathBuf::from(git_common_dir),
    };
    (task, OwnedLease { card, lease })
}

/// Another attempt of this Track stamped with `card` as its worker, in `status`.
async fn stranger_task(fx: &Fx, key: &str, card: &str, status: &str) {
    declare(
        &fx.boot,
        json!({
            "key": key, "kind": "claude", "goal": format!("work {key}"),
            "declared_by": PLANNER_DECLARATION_AUTHOR, "ready": true,
            "no_gate_reason": "reclaim fixture",
        }),
    )
    .await;
    let task = current(&fx.boot, key).await;
    sqlx::query("UPDATE tasks SET worker_card_id = ?1 WHERE id = ?2")
        .bind(card)
        .bind(&task.id)
        .execute(&fx.pool())
        .await
        .unwrap();
    set_task_status(fx, &task.id, status).await;
}
