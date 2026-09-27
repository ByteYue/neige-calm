//! #1815: a released lease's worktree is reclaimed by the scheduler's reconcile sweep (boot and
//! every tick) once nothing can still read it — the attempt is terminal, its latest delivery is
//! settled or abandoned, and no forge action runs on the Track. Real git repositories, the
//! production report/delivery path, and the production boot sweep.
use std::path::Path;
use std::time::Duration;

use serde_json::json;

use super::git_delivery::*;

/// Whether `git worktree list` in `repo_root` names `path`.
fn worktree_registered(repo_root: &Path, path: &Path) -> bool {
    let listed = git(repo_root, &["worktree", "list", "--porcelain"]);
    let path = path.to_string_lossy();
    listed
        .lines()
        .filter_map(|line| line.strip_prefix("worktree "))
        .any(|listed| listed == path)
}

async fn worktree_removed_events(fx: &Fx, card: &str) -> i64 {
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

    fx.reboot().await;

    wait_reclaimed(&fx, &lease.repo_root, &lease.path, &worker.card_id).await;
    // The candidate stays pinned by its ref after the worktree and slice branch are gone (D9).
    let candidate = fx.candidate_row(&task.id).await.unwrap();
    assert_eq!(
        ref_target(&lease.git_common_dir, &candidate.ref_name).as_deref(),
        Some(candidate.commit_sha.as_str())
    );
    assert!(
        git_output(
            &lease.repo_root,
            &[
                "show-ref",
                "--verify",
                "--quiet",
                &format!("refs/heads/{}", fx.slice_branch(&worker.card_id)),
            ],
        )
        .status
        .code()
            == Some(1),
        "the slice branch was deleted with the worktree"
    );
}
