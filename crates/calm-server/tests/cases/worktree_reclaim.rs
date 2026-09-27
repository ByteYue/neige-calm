//! #1815: a released lease's worktree is reclaimed by the scheduler's reconcile sweep (boot and
//! every tick) once nothing can still read it — the attempt is terminal, its latest delivery is
//! settled or abandoned, and no forge action runs on the Track. Real git repositories, the
//! production report/delivery path, and the production boot sweep and reconcile tick. The arms
//! that keep the worktree are in `worktree_reclaim_kept.rs`.
use std::path::Path;
use std::time::Duration;

use calm_server::mcp_server::registry::ToolCallIdentity;
use calm_server::model::{Task, TaskStatus, now_ms};
use calm_server::session_projection_repo::AgentProvider;
use calm_server::test_seams::{KernelWorkspaceLease, release_workspace_lease_for_card_for_test};
use serde_json::json;

use super::git_delivery::*;

/// Whether `git worktree list` in `repo_root` names `path`.
pub(super) fn worktree_registered(repo_root: &Path, path: &Path) -> bool {
    let listed = git(repo_root, &["worktree", "list", "--porcelain"]);
    let path = path.to_string_lossy();
    listed
        .lines()
        .filter_map(|line| line.strip_prefix("worktree "))
        .any(|listed| listed == path)
}

pub(super) fn branch_exists(repo_root: &Path, branch: &str) -> bool {
    git_output(
        repo_root,
        &[
            "show-ref",
            "--verify",
            "--quiet",
            &format!("refs/heads/{branch}"),
        ],
    )
    .status
    .success()
}

pub(super) async fn worktree_removed_events(fx: &Fx, card: &str) -> i64 {
    sqlx::query_scalar(
        "SELECT COUNT(*) FROM events WHERE kind = 'worktree.removed' AND scope_card = ?1",
    )
    .bind(card)
    .fetch_one(&fx.pool())
    .await
    .unwrap()
}

/// Poll until the worktree at `path` is unregistered, its directory gone and its card has one
/// `worktree.removed`; panics with the observed state after [`WAIT`].
async fn wait_reclaimed(fx: &Fx, repo_root: &Path, path: &Path, card: &str) {
    let deadline = tokio::time::Instant::now() + WAIT;
    loop {
        let registered = worktree_registered(repo_root, path);
        let exists = path.exists();
        let removed = worktree_removed_events(fx, card).await;
        if !registered && !exists && removed == 1 {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "worktree {} not reclaimed: registered={registered} exists={exists} \
             worktree.removed={removed}",
            path.display()
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Just past the reclaim grace (the default forge deadline of a parked action, 900 s).
const PAST_GRACE_MS: i64 = 901_000;

/// Move every release so far back past the reclaim grace — what the passage of time does.
pub(super) async fn age_releases(fx: &Fx) {
    sqlx::query(
        "UPDATE workspace_leases SET released_at_ms = released_at_ms - ?1 WHERE state = 'released'",
    )
    .bind(PAST_GRACE_MS)
    .execute(&fx.pool())
    .await
    .unwrap();
}

/// One reconcile tick (`sweep_all`, the periodic backstop) with every release so far past the
/// grace, and the reclaim it started.
pub(super) async fn tick(fx: &Fx) {
    age_releases(fx).await;
    tick_within_grace(fx).await;
}

/// One reconcile tick without moving any release past the grace.
pub(super) async fn tick_within_grace(fx: &Fx) {
    let scheduler = fx.scheduler();
    scheduler.mark_boot_sweep_complete();
    scheduler.sweep_all().await;
    scheduler.worktree_reclaim_idle_for_test().await;
}

/// A kernel restart (recovery, then the boot sweep) with every release so far past the grace,
/// and the reclaim it started.
pub(super) async fn reboot(fx: &mut Fx) {
    age_releases(fx).await;
    fx.reboot().await;
    fx.scheduler().worktree_reclaim_idle_for_test().await;
}

/// The slice branch's commit, read before a reclaim so [`assert_reclaimed`] can pin it.
pub(super) fn slice_tip(fx: &Fx, lease: &KernelWorkspaceLease, card: &str) -> String {
    git(
        &lease.repo_root,
        &[
            "rev-parse",
            &format!("refs/heads/{}", fx.slice_branch(card)),
        ],
    )
}

/// The checkout and its registration are gone, one `worktree.removed`; the slice branch stays
/// at `tip` (it may hold the only copy of the attempt's commits).
pub(super) async fn assert_reclaimed(fx: &Fx, lease: &KernelWorkspaceLease, card: &str, tip: &str) {
    assert!(
        !lease.path.exists(),
        "{} still on disk",
        lease.path.display()
    );
    assert!(!worktree_registered(&lease.repo_root, &lease.path));
    assert_eq!(slice_tip(fx, lease, card), tip, "the slice branch is kept");
    assert_eq!(worktree_removed_events(fx, card).await, 1);
}

pub(super) async fn assert_kept(fx: &Fx, lease: &KernelWorkspaceLease, card: &str) {
    assert!(lease.path.is_dir(), "{} was removed", lease.path.display());
    assert!(worktree_registered(&lease.repo_root, &lease.path));
    assert!(branch_exists(&lease.repo_root, &fx.slice_branch(card)));
    assert_eq!(worktree_removed_events(fx, card).await, 0);
}

pub(super) async fn lease_state(fx: &Fx, lease_id: &str) -> String {
    sqlx::query_scalar("SELECT state FROM workspace_leases WHERE lease_id = ?1")
        .bind(lease_id)
        .fetch_one(&fx.pool())
        .await
        .unwrap()
}

pub(super) async fn set_task_status(fx: &Fx, task_id: &str, status: &str) {
    sqlx::query("UPDATE tasks SET status = ?1, finished_at_ms = ?2 WHERE id = ?3")
        .bind(status)
        .bind(now_ms())
        .bind(task_id)
        .execute(&fx.pool())
        .await
        .unwrap();
}

/// A worker that completed with an edit: the production report releases the lease and the
/// kernel delivery settles; returns once it has.
pub(super) async fn delivered(
    fx: &Fx,
    name: &str,
) -> (ToolCallIdentity, Task, KernelWorkspaceLease) {
    let worker = fx.new_worker(name, AgentProvider::Claude).await;
    let lease = fx.kernel_lease(&worker.card_id).await;
    let task = fx
        .running_task(name, "claude", &worker.card_id, json!({}))
        .await;
    std::fs::write(lease.path.join("worker.txt"), format!("{name}\n")).unwrap();
    fx.complete(&worker, &task.id).await;
    fx.wait_settled(&task.id).await;
    (worker, task, lease)
}

/// A worker whose lease was released through the production release with no report (so no
/// delivery row), its task then set to `status`.
pub(super) async fn released_without_delivery(
    fx: &Fx,
    name: &str,
    status: &str,
) -> (ToolCallIdentity, Task, KernelWorkspaceLease) {
    let worker = fx.new_worker(name, AgentProvider::Claude).await;
    let lease = fx.kernel_lease(&worker.card_id).await;
    let task = fx
        .running_task(name, "claude", &worker.card_id, json!({}))
        .await;
    assert!(
        release_workspace_lease_for_card_for_test(
            fx.boot.repo.as_ref(),
            &fx.boot.ctx.events,
            &worker.card_id
        )
        .await
        .unwrap()
    );
    set_task_status(fx, &task.id, status).await;
    assert_eq!(fx.delivery_count(&task.id).await, 0);
    (worker, task, lease)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn boot_sweep_reclaims_done_candidate_worktree() {
    let mut fx = fixture().await;
    let worker = fx.claude_worker();
    let lease = fx.kernel_lease(&worker.card_id).await;
    let task = fx
        .running_task("reclaim", "claude", &worker.card_id, json!({}))
        .await;
    std::fs::write(lease.path.join("worker.txt"), "delivered\n").unwrap();
    fx.complete(&worker, &task.id).await;
    fx.wait_settled(&task.id).await;

    // The state #1815 measured: a done attempt, a settled candidate, a released lease whose
    // worktree is still registered on disk.
    assert_eq!(
        fx.task_columns(&task.id).await.status,
        calm_server::model::TaskStatus::Done
    );
    let row = fx.delivery_row(&task.id).await.unwrap();
    assert_eq!(row.settlement.as_deref(), Some("candidate"));
    let state: String =
        sqlx::query_scalar("SELECT state FROM workspace_leases WHERE lease_id = ?1")
            .bind(&lease.lease_id)
            .fetch_one(&fx.pool())
            .await
            .unwrap();
    assert_eq!(state, "released");
    assert!(lease.path.is_dir());
    assert!(worktree_registered(&lease.repo_root, &lease.path));
    assert_eq!(worktree_removed_events(&fx, &worker.card_id).await, 0);

    let tip = slice_tip(&fx, &lease, &worker.card_id);
    age_releases(&fx).await;
    fx.reboot().await;

    wait_reclaimed(&fx, &lease.repo_root, &lease.path, &worker.card_id).await;
    // The candidate stays pinned by its ref (D9), and the slice branch stays where it was.
    let candidate = fx.candidate_row(&task.id).await.unwrap();
    assert_eq!(
        ref_target(&lease.git_common_dir, &candidate.ref_name).as_deref(),
        Some(candidate.commit_sha.as_str())
    );
    assert_eq!(slice_tip(&fx, &lease, &worker.card_id), tip);
    assert_eq!(tip, candidate.commit_sha);
}

/// The tick reclaims; `calm.plan.list` then reads `removed: true`; a second tick finds nothing
/// to do (no second `worktree.removed`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tick_reclaims_once_and_plan_list_reads_removed() {
    let fx = fixture().await;
    let (worker, task, lease) = delivered(&fx, "tick").await;
    assert_eq!(fx.task_columns(&task.id).await.status, TaskStatus::Done);
    let before = fx.plan_entry("tick").await;
    assert_eq!(before["worktree"]["removed"], false, "{before}");

    let tip = slice_tip(&fx, &lease, &worker.card_id);
    tick(&fx).await;
    assert_reclaimed(&fx, &lease, &worker.card_id, &tip).await;
    let after = fx.plan_entry("tick").await;
    assert_eq!(after["worktree"]["removed"], true, "{after}");
    assert!(after["worktree"].get("path").is_none(), "{after}");
    assert_eq!(after["worktree"]["state"], "released");

    tick(&fx).await;
    assert_eq!(worktree_removed_events(&fx, &worker.card_id).await, 1);
    assert_eq!(lease_state(&fx, &lease.lease_id).await, "released");
}

/// A kernel lease keeps its worktree for the grace after its release (the Planner may open a
/// preview in it after its wake), then is reclaimed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kernel_lease_inside_the_grace_is_kept() {
    let fx = fixture().await;
    let (worker, task, lease) = delivered(&fx, "fresh").await;
    assert_eq!(fx.task_columns(&task.id).await.status, TaskStatus::Done);
    let policy: Option<String> =
        sqlx::query_scalar("SELECT delivery_policy FROM workspace_leases WHERE lease_id = ?1")
            .bind(&lease.lease_id)
            .fetch_one(&fx.pool())
            .await
            .unwrap();
    assert_eq!(policy.as_deref(), Some("kernel"));

    tick_within_grace(&fx).await;
    assert_kept(&fx, &lease, &worker.card_id).await;

    let tip = slice_tip(&fx, &lease, &worker.card_id);
    tick(&fx).await;
    assert_reclaimed(&fx, &lease, &worker.card_id, &tip).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn done_without_delivery_and_canceled_are_reclaimed() {
    let fx = fixture().await;
    let (done, _, done_lease) = released_without_delivery(&fx, "no-delivery", "done").await;
    let (canceled, _, canceled_lease) =
        released_without_delivery(&fx, "canceled", "canceled").await;

    let done_tip = slice_tip(&fx, &done_lease, &done.card_id);
    let canceled_tip = slice_tip(&fx, &canceled_lease, &canceled.card_id);
    tick(&fx).await;
    assert_reclaimed(&fx, &done_lease, &done.card_id, &done_tip).await;
    assert_reclaimed(&fx, &canceled_lease, &canceled.card_id, &canceled_tip).await;
}

/// A deleted card's worktree goes whatever its attempt's status (here `failed`, which would keep
/// it while the card exists).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deleted_card_is_reclaimed() {
    let fx = fixture().await;
    let (worker, _, lease) = released_without_delivery(&fx, "deleted", "failed").await;
    sqlx::query("DELETE FROM cards WHERE id = ?1")
        .bind(&worker.card_id)
        .execute(&fx.pool())
        .await
        .unwrap();

    let tip = slice_tip(&fx, &lease, &worker.card_id);
    tick(&fx).await;
    assert_reclaimed(&fx, &lease, &worker.card_id, &tip).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn abandoned_delivery_is_reclaimed() {
    let mut fx = fixture().await;
    // A delivery that committed but died before pinning its ref settles `failed`, retryable,
    // with the checkout clean; the Planner abandons it.
    let (worker, task, lease) = crashed_before_ref(&mut fx).await;
    fx.reboot().await;
    fx.wait_settled(&task.id).await;
    let row = fx.delivery_row(&task.id).await.unwrap();
    assert_eq!(row.settlement.as_deref(), Some("failed"));
    assert_eq!(row.retry_allowed, Some(1));
    fx.abandon(&task, &row.delivery_id, "abandon-1", Some("not needed"))
        .await
        .expect("abandon admitted");
    assert!(fx.abandonment_row(&row.delivery_id).await.is_some());
    assert_eq!(fx.task_columns(&task.id).await.status, TaskStatus::Done);

    let tip = slice_tip(&fx, &lease, &worker.card_id);
    tick(&fx).await;
    assert_reclaimed(&fx, &lease, &worker.card_id, &tip).await;
}

/// The latest delivery decides: a retryable failure followed by a candidate retry is settled.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn candidate_retry_after_retryable_failure_is_reclaimed() {
    let fx = fixture().await;
    let (worker, task, lease) = fx.hook_failing_task("retried", json!({})).await;
    fx.wait_settled(&task.id).await;
    let first = fx.delivery_row(&task.id).await.unwrap();
    assert_eq!(first.retry_allowed, Some(1));
    remove_pre_commit(&lease);
    fx.retry(&task, &first.delivery_id, "retry-1", None)
        .await
        .expect("retry admitted");
    fx.wait_settled_nth(&task.id, 2).await;
    let first_now = fx.delivery_row_at(&task.id, 1).await.unwrap();
    assert_eq!(first_now.settlement.as_deref(), Some("failed"));
    assert_eq!(first_now.retry_allowed, Some(1));
    let second = fx.delivery_row(&task.id).await.unwrap();
    assert_eq!(second.ordinal, 2);
    assert_eq!(second.settlement.as_deref(), Some("candidate"));
    assert_eq!(fx.task_columns(&task.id).await.status, TaskStatus::Done);

    let tip = slice_tip(&fx, &lease, &worker.card_id);
    tick(&fx).await;
    assert_reclaimed(&fx, &lease, &worker.card_id, &tip).await;
}

/// A delivery that failed without retry (`workspace_missing`: the directory went away) settles
/// the lease; the reclaim prunes the registration left behind and keeps the slice branch.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unretryable_failed_delivery_is_reclaimed() {
    let mut fx = fixture().await;
    fx.dispatcher.abort_event_listener_for_test();
    let worker = fx.codex_worker();
    let lease = fx.kernel_lease(&worker.card_id).await;
    let task = fx
        .running_task("gone", "codex", &worker.card_id, json!({}))
        .await;
    fx.report_only(&worker, &task.id).await;
    std::fs::remove_dir_all(&lease.path).unwrap();
    // The boot sweep sees the delivery unsettled (kept), then settles it.
    reboot(&mut fx).await;
    fx.wait_settled(&task.id).await;
    let row = fx.delivery_row(&task.id).await.unwrap();
    assert_eq!(row.retry_allowed, Some(0));
    assert_eq!(fx.task_columns(&task.id).await.status, TaskStatus::Done);
    assert_eq!(worktree_removed_events(&fx, &worker.card_id).await, 0);
    assert!(worktree_registered(&lease.repo_root, &lease.path));

    let tip = slice_tip(&fx, &lease, &worker.card_id);
    tick(&fx).await;
    assert_reclaimed(&fx, &lease, &worker.card_id, &tip).await;
}

/// The boot sweep takes a backlog, at most 16 removals per pass; the next pass takes the rest.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn boot_backlog_is_reclaimed_sixteen_per_pass() {
    let mut fx = fixture().await;
    let mut released = Vec::new();
    for n in 0..17 {
        let (worker, _, lease) =
            released_without_delivery(&fx, &format!("backlog-{n}"), "done").await;
        let tip = slice_tip(&fx, &lease, &worker.card_id);
        released.push((worker, tip, lease));
    }

    reboot(&mut fx).await;
    let mut removed = 0;
    for (worker, _, _) in &released {
        removed += worktree_removed_events(&fx, &worker.card_id).await;
    }
    assert_eq!(removed, 16);

    tick(&fx).await;
    for (worker, tip, lease) in &released {
        assert_reclaimed(&fx, lease, &worker.card_id, tip).await;
    }
}

/// A lease path that no longer resolves to its recorded worktree (here moved behind a symlink)
/// is refused: nothing is removed, no event, and the moved checkout survives. The refusal is
/// remembered for the process: restoring the path does not bring it back on the next tick, a
/// restart does.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn identity_refusal_leaves_the_entry_untouched() {
    let mut fx = fixture().await;
    let (worker, _, lease) = delivered(&fx, "moved").await;
    let tip = slice_tip(&fx, &lease, &worker.card_id);
    let moved = lease.path.with_file_name("moved-away");
    std::fs::rename(&lease.path, &moved).unwrap();
    std::os::unix::fs::symlink(&moved, &lease.path).unwrap();

    tick(&fx).await;
    assert_eq!(worktree_removed_events(&fx, &worker.card_id).await, 0);
    assert!(lease.path.is_symlink());
    assert_eq!(
        std::fs::read_to_string(moved.join("worker.txt")).unwrap(),
        "moved\n"
    );
    assert_eq!(slice_tip(&fx, &lease, &worker.card_id), tip);

    std::fs::remove_file(&lease.path).unwrap();
    std::fs::rename(&moved, &lease.path).unwrap();
    tick(&fx).await;
    assert_eq!(
        worktree_removed_events(&fx, &worker.card_id).await,
        0,
        "a refused lease is not looked at again by this process"
    );
    assert!(lease.path.is_dir());

    reboot(&mut fx).await;
    assert_reclaimed(&fx, &lease, &worker.card_id, &tip).await;
}
