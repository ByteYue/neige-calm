//! #1815: the arms of the released-worktree reclaim that keep the worktree — the delivery or a
//! gate may still read it, a recovery may still need it, or the card uses it again.
use calm_server::model::TaskStatus;
use calm_server::operation::PhaseTag;
use calm_server::session_projection_repo::AgentProvider;
use calm_server::test_seams::release_workspace_lease_for_card_for_test;
use serde_json::json;

use super::git_delivery::*;
use super::worktree_reclaim::*;
use crate::mcp_track_report::call_tool;

/// The report committed, its delivery never submitted (a kernel that died after the report
/// transaction): the boot sweep sees it unsettled and keeps the worktree; once the delivery
/// settles, the next tick reclaims it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pending_delivery_keeps_until_settled() {
    let mut fx = fixture().await;
    fx.dispatcher.abort_event_listener_for_test();
    let worker = fx.claude_worker();
    let lease = fx.kernel_lease(&worker.card_id).await;
    let task = fx
        .running_task("pending", "claude", &worker.card_id, json!({}))
        .await;
    std::fs::write(lease.path.join("worker.txt"), "pending\n").unwrap();
    fx.report_only(&worker, &task.id).await;
    assert_eq!(fx.task_columns(&task.id).await.status, TaskStatus::Done);
    assert_eq!(lease_state(&fx, &lease.lease_id).await, "released");
    assert!(
        fx.delivery_row(&task.id)
            .await
            .unwrap()
            .settlement
            .is_none()
    );
    assert_eq!(fx.forge_op_count().await, 0);

    reboot(&mut fx).await;
    assert_kept(&fx, &lease, &worker.card_id).await;

    let settled = fx.wait_settled(&task.id).await;
    let row = fx.delivery_row(&task.id).await.unwrap();
    assert_eq!(row.settlement.as_deref(), Some("candidate"), "{settled:?}");
    tick(&fx).await;
    assert_reclaimed(&fx, &lease, &worker.card_id).await;
}

/// A failure the Planner may still retry needs the worktree.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retryable_failed_delivery_keeps() {
    let fx = fixture().await;
    let (worker, task, lease) = fx.hook_failing_task("retryable", json!({})).await;
    fx.wait_settled(&task.id).await;
    let row = fx.delivery_row(&task.id).await.unwrap();
    assert_eq!(row.settlement.as_deref(), Some("failed"));
    assert_eq!(row.retry_allowed, Some(1));
    assert_eq!(fx.task_columns(&task.id).await.status, TaskStatus::Done);

    tick(&fx).await;
    assert_kept(&fx, &lease, &worker.card_id).await;
}

/// A failed attempt keeps its worktree for recovery guidance.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_task_keeps() {
    let fx = fixture().await;
    let worker = fx.new_worker("failed", AgentProvider::Claude).await;
    let lease = fx.kernel_lease(&worker.card_id).await;
    let task = fx
        .running_task("failed", "claude", &worker.card_id, json!({}))
        .await;
    call_tool(
        &fx.boot,
        "calm.task.fail",
        worker.clone(),
        json!({"idempotency_key": task.id, "reason": "worker gave up"}),
    )
    .await
    .unwrap();
    assert_eq!(fx.task_columns(&task.id).await.status, TaskStatus::Failed);
    assert_eq!(lease_state(&fx, &lease.lease_id).await, "released");
    assert_eq!(fx.delivery_count(&task.id).await, 0);

    tick(&fx).await;
    assert_kept(&fx, &lease, &worker.card_id).await;
}

/// A gate reads the worktree while its task is `verifying`, even with the candidate settled.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn verifying_task_keeps() {
    let fx = fixture().await;
    let (worker, task, lease) = delivered(&fx, "verifying").await;
    set_task_status(&fx, &task.id, "verifying").await;
    assert_eq!(
        fx.delivery_row(&task.id)
            .await
            .unwrap()
            .settlement
            .as_deref(),
        Some("candidate")
    );

    tick(&fx).await;
    assert_kept(&fx, &lease, &worker.card_id).await;
}

/// A card without a task (nothing says the work is finished) keeps its worktree.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn card_without_task_keeps() {
    let fx = fixture().await;
    let worker = fx.new_worker("taskless", AgentProvider::Claude).await;
    let lease = fx.kernel_lease(&worker.card_id).await;
    assert!(
        release_workspace_lease_for_card_for_test(
            fx.boot.repo.as_ref(),
            &fx.boot.ctx.events,
            &worker.card_id
        )
        .await
        .unwrap()
    );

    tick(&fx).await;
    assert_kept(&fx, &lease, &worker.card_id).await;
}

/// A card that holds a lease again uses the same path: its older released lease is not
/// reclaimed out from under the held one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn held_lease_of_the_card_keeps() {
    let fx = fixture().await;
    let (worker, _, released) = released_without_delivery(&fx, "held", "done").await;
    let held = fx.kernel_lease(&worker.card_id).await;
    assert_eq!(held.path, released.path);
    assert_eq!(lease_state(&fx, &released.lease_id).await, "released");
    assert_eq!(lease_state(&fx, &held.lease_id).await, "held");

    tick(&fx).await;
    assert_kept(&fx, &held, &worker.card_id).await;
}

/// No reclaim on a Track while a forge action runs on it (its cwd may be a lease worktree);
/// the tick after it ends reclaims.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn active_forge_action_on_the_track_keeps() {
    let fx = fixture().await;
    let (done, _, done_lease) = delivered(&fx, "settled").await;

    let flag = fx.track_root.parent().unwrap().join("release-hook");
    let blocked = fx.new_worker("blocked", AgentProvider::Claude).await;
    let blocked_lease = fx.kernel_lease(&blocked.card_id).await;
    install_pre_commit(&blocked_lease, &hook_waiting_for(&flag, 0));
    let blocked_task = fx
        .running_task("blocked", "claude", &blocked.card_id, json!({}))
        .await;
    std::fs::write(blocked_lease.path.join("worker.txt"), "blocked\n").unwrap();
    fx.complete(&blocked, &blocked_task.id).await;
    let row = fx.delivery_row(&blocked_task.id).await.unwrap();
    let op = fx
        .forge_op(&row.forge_idempotency_key)
        .await
        .expect("the report submitted the delivery");
    assert!(
        !matches!(
            op.phase.tag(),
            PhaseTag::Succeeded | PhaseTag::Failed | PhaseTag::Stuck
        ),
        "{:?}",
        op.phase
    );

    tick(&fx).await;
    assert_kept(&fx, &done_lease, &done.card_id).await;

    std::fs::write(&flag, "").unwrap();
    fx.wait_settled(&blocked_task.id).await;
    remove_pre_commit(&blocked_lease);
    tick(&fx).await;
    assert_reclaimed(&fx, &done_lease, &done.card_id).await;
    assert_reclaimed(&fx, &blocked_lease, &blocked.card_id).await;
}

/// A legacy lease (no kernel delivery; the auto-commit runs after the release) keeps its
/// worktree for at least the forge deadline after its release, then is reclaimed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn legacy_lease_keeps_inside_the_grace() {
    let fx = fixture().await;
    let worker = fx.new_worker("legacy", AgentProvider::Claude).await;
    let lease = fx.kernel_lease(&worker.card_id).await;
    sqlx::query("UPDATE workspace_leases SET delivery_policy = NULL WHERE lease_id = ?1")
        .bind(&lease.lease_id)
        .execute(&fx.pool())
        .await
        .unwrap();
    let task = fx
        .running_task("legacy", "claude", &worker.card_id, json!({}))
        .await;
    fx.complete(&worker, &task.id).await;
    assert_eq!(fx.delivery_count(&task.id).await, 0);
    assert_eq!(fx.task_columns(&task.id).await.status, TaskStatus::Done);
    assert_eq!(lease_state(&fx, &lease.lease_id).await, "released");

    tick(&fx).await;
    assert_kept(&fx, &lease, &worker.card_id).await;

    // The default forge deadline of a parked action is 900 s: released 901 s ago is outside.
    sqlx::query(
        "UPDATE workspace_leases SET released_at_ms = released_at_ms - 901000 WHERE lease_id = ?1",
    )
    .bind(&lease.lease_id)
    .execute(&fx.pool())
    .await
    .unwrap();
    tick(&fx).await;
    assert_reclaimed(&fx, &lease, &worker.card_id).await;
}
