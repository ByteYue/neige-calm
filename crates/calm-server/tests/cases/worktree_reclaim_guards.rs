//! #1815 review round 1: what the released-worktree reclaim must not take — work left in the
//! checkout, a worktree a live terminal works in — and how it decides: the lease owner's attempt
//! over other tasks of the card, a replaced failed attempt, refusals remembered off the cap; the
//! production success path with the worker's own PTY still live; bounded, hook-free git.
use std::path::{Path, PathBuf};
use std::sync::Arc;

use calm_server::db::sqlite::card_with_terminal_create_tx;
use calm_server::db::write_in_tx_typed;
use calm_server::mcp_server::registry::ToolCallIdentity;
use calm_server::model::{CardRole, Task, TaskStatus, new_id};
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
use crate::mcp_track_report::{call_tool, worker_identity};
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

/// The production success path: a real worker prepare and provisioning, the worker's report
/// through `calm.task.complete` (release, kernel delivery, candidate), its PTY still live — as
/// it stays until the Track completes. After the grace the worktree is reclaimed with its slice
/// branch at the candidate; a second card's live terminal in a worktree keeps that one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn finished_worker_with_its_own_pty_live_is_reclaimed() {
    let fx = fixture().await;
    let (task, reported) = reported_worker(&fx, "reported").await;
    let (_, previewed) = reported_worker(&fx, "previewed").await;
    let _preview = open_terminal(&fx, &previewed.lease.path).await;
    let candidate = fx.candidate_row(&task.id).await.expect("candidate");
    assert_eq!(
        slice_tip(&fx, &reported.lease, &reported.card),
        candidate.commit_sha
    );

    tick(&fx).await;
    assert_reclaimed(&fx, &reported.lease, &reported.card, &candidate.commit_sha).await;
    assert!(worker_pty_live(&fx, &reported.card).await);
    assert_kept(&fx, &previewed.lease, &previewed.card).await;
}

/// [`prepared_worker`] run to a settled report: `running` on its card (the scheduler's stamp),
/// an edit, `calm.task.complete` as that worker, the delivery settled. Its PTY stays live.
async fn reported_worker(fx: &Fx, key: &str) -> (Task, OwnedLease) {
    let (task, owned) = prepared_worker(fx, key).await;
    fx.claim_running(&task.id, &owned.card).await;
    let session_id: String =
        sqlx::query_scalar("SELECT id FROM worker_sessions WHERE card_id = ?1")
            .bind(&owned.card)
            .fetch_one(&fx.pool())
            .await
            .unwrap();
    let worker = ToolCallIdentity {
        card_id: owned.card.clone(),
        provider: AgentProvider::Claude,
        session_id,
        thread_id: format!("{key}-thread"),
        ..worker_identity(&fx.boot)
    };
    std::fs::write(owned.lease.path.join("worker.txt"), format!("{key}\n")).unwrap();
    fx.complete(&worker, &task.id).await;
    fx.wait_settled(&task.id).await;
    assert_eq!(
        fx.delivery_row(&task.id)
            .await
            .unwrap()
            .settlement
            .as_deref(),
        Some("candidate")
    );
    assert_eq!(fx.task_columns(&task.id).await.status, TaskStatus::Done);
    assert_eq!(lease_state(fx, &owned.lease.lease_id).await, "released");
    assert!(
        worker_pty_live(fx, &owned.card).await,
        "the worker PTY outlives the report"
    );
    (task, owned)
}

/// The shared repository config names an fsmonitor hook and a clean filter for every path
/// (any worker can write both). The fsmonitor hook never runs; the clean filter runs only under
/// the bounded `status` pre-check, never under `worktree remove` (with `--force` git runs no
/// repository code).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reclaim_never_runs_the_fsmonitor_hook_or_a_clean_filter_in_the_remove() {
    let fx = fixture().await;
    let (worker, _, lease) = delivered(&fx, "fsmonitor").await;
    let scratch = fx.track_root.parent().unwrap().to_path_buf();
    let fsmonitor_ran = scratch.join("fsmonitor-ran");
    let filter_callers = scratch.join("filter-callers");
    let hook = scratch.join("fsmonitor-hook");
    let filter = scratch.join("clean-filter");
    write_executable(
        &hook,
        &format!(
            "#!/bin/sh\ntouch '{}'\nsleep 5\nexit 1\n",
            fsmonitor_ran.display()
        ),
    );
    // Records the command lines of its parent and grandparent: the git that ran it, directly or
    // through `sh -c`.
    write_executable(
        &filter,
        &format!(
            "#!/bin/sh\ngp=$(cut -d' ' -f4 /proc/$PPID/stat)\n\
             {{ tr '\\0' ' ' < /proc/$PPID/cmdline; echo; tr '\\0' ' ' < /proc/$gp/cmdline; echo; }} >> '{}'\n\
             cat\n",
            filter_callers.display()
        ),
    );
    git(
        &lease.repo_root,
        &["config", "core.fsmonitor", hook.to_str().unwrap()],
    );
    git(
        &lease.repo_root,
        &["config", "filter.slow.clean", filter.to_str().unwrap()],
    );
    std::fs::write(
        lease.git_common_dir.join("info").join("attributes"),
        "* filter=slow\n",
    )
    .unwrap();
    let tip = slice_tip(&fx, &lease, &worker.card_id);

    tick(&fx).await;
    assert_reclaimed(&fx, &lease, &worker.card_id, &tip).await;
    assert!(!fsmonitor_ran.exists(), "the fsmonitor hook ran");
    let callers = std::fs::read_to_string(&filter_callers).unwrap_or_default();
    assert!(
        callers.contains("status"),
        "the pre-check's status ran the filter (the fixture is live):\n{callers}"
    );
    assert!(
        !callers.contains(" worktree remove "),
        "the clean filter ran under worktree remove:\n{callers}"
    );
}

/// A clean checkout whose detached HEAD carries a commit no ref reaches keeps it: removing the
/// worktree would orphan that commit.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn detached_head_no_ref_reaches_keeps_the_worktree() {
    let fx = fixture().await;
    let (worker, _, lease) = delivered(&fx, "detached").await;
    git(&lease.path, &["checkout", "-q", "--detach"]);
    let orphan = commit_file(&lease.path, "later.txt", "after the report\n", "later work");
    assert!(
        git_output(&lease.path, &["symbolic-ref", "-q", "HEAD"])
            .status
            .code()
            == Some(1)
    );

    tick(&fx).await;
    assert_kept(&fx, &lease, &worker.card_id).await;
    assert_eq!(git(&lease.path, &["rev-parse", "HEAD"]), orphan);
    assert_eq!(
        git(&lease.repo_root, &["show", &format!("{orphan}:later.txt")]),
        "after the report"
    );
}

/// A detached HEAD some ref reaches holds nothing only it keeps: at the slice branch's tip, or
/// at the candidate after the slice branch moved elsewhere (only the candidate ref reaches it).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn detached_head_a_ref_reaches_is_reclaimed() {
    let fx = fixture().await;
    let (at_branch, _, at_branch_lease) = delivered(&fx, "at-branch").await;
    let (at_candidate, candidate_task, at_candidate_lease) = delivered(&fx, "at-candidate").await;
    git(&at_branch_lease.path, &["checkout", "-q", "--detach"]);
    git(&at_candidate_lease.path, &["checkout", "-q", "--detach"]);
    let slice = fx.slice_branch(&at_candidate.card_id);
    git(
        &at_candidate_lease.repo_root,
        &["branch", "-f", &slice, &at_candidate_lease.base_sha],
    );
    let candidate = fx.candidate_row(&candidate_task.id).await.unwrap();
    assert_eq!(
        git(&at_candidate_lease.path, &["rev-parse", "HEAD"]),
        candidate.commit_sha
    );
    let branch_tip = slice_tip(&fx, &at_branch_lease, &at_branch.card_id);

    tick(&fx).await;
    assert_reclaimed(&fx, &at_branch_lease, &at_branch.card_id, &branch_tip).await;
    assert_reclaimed(
        &fx,
        &at_candidate_lease,
        &at_candidate.card_id,
        &at_candidate_lease.base_sha,
    )
    .await;
}

/// A lease path that is a symlink is refused, even for a legacy lease without a recorded base
/// (no identity check): nothing is unlinked or pruned, the linked-to checkout is untouched.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn symlink_leaf_of_a_baseless_lease_is_refused() {
    let fx = fixture().await;
    let (worker, _, lease) = released_without_delivery(&fx, "linked", "done").await;
    sqlx::query(
        "UPDATE workspace_leases SET base_sha = NULL, base_source = NULL, base_attempt_id = NULL, \
         canonical_path = NULL, git_common_dir = NULL, delivery_policy = NULL WHERE lease_id = ?1",
    )
    .bind(&lease.lease_id)
    .execute(&fx.pool())
    .await
    .unwrap();
    let moved = lease.path.with_file_name("linked-away");
    std::fs::rename(&lease.path, &moved).unwrap();
    std::os::unix::fs::symlink(&moved, &lease.path).unwrap();

    tick(&fx).await;
    assert_eq!(worktree_removed_events(&fx, &worker.card_id).await, 0);
    assert!(lease.path.is_symlink());
    assert!(moved.join(".git").is_file());
    assert!(worktree_registered(&lease.repo_root, &lease.path));
}

/// A locked worktree (clean, but `worktree remove --force` refuses a lock) is a refusal, not
/// a failure: sixteen of them ahead of a clean one leave room for it on the first pass, and
/// stay untouched.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_removals_do_not_count_toward_the_cap() {
    let fx = fixture().await;
    let mut locked = Vec::new();
    for n in 0..16 {
        let (worker, _, lease) =
            released_without_delivery(&fx, &format!("locked-{n}"), "done").await;
        git(
            &lease.repo_root,
            &["worktree", "lock", lease.path.to_str().unwrap()],
        );
        locked.push((worker, lease));
    }
    let (clean, _, clean_lease) = released_without_delivery(&fx, "unlocked", "done").await;
    let tip = slice_tip(&fx, &clean_lease, &clean.card_id);

    tick(&fx).await;
    assert_reclaimed(&fx, &clean_lease, &clean.card_id, &tip).await;
    for (worker, lease) in &locked {
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
async fn failed_attempt(fx: &Fx, name: &str) -> (ToolCallIdentity, Task, KernelWorkspaceLease) {
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

/// The production worker sequence up to a provisioned worktree: the scheduler's payload, an
/// operations row keyed by the attempt, the real Claude worker adapter's `prepare_tx` (creates
/// the worker card with its PTY row — cwd = the worktree — and takes the lease with the op as
/// `lease_owner`), then the spawn's provisioning and a succeeded op. The attempt is
/// `dispatched`, the PTY live.
async fn prepared_worker(fx: &Fx, key: &str) -> (Task, OwnedLease) {
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
    // The spawn half: the worktree above, and the op's row as a successful spawn leaves it (this
    // fixture's runtime has no worker adapter to drive it).
    sqlx::query(
        "UPDATE operations SET phase = 'succeeded', completed_at_ms = updated_at_ms WHERE id = ?1",
    )
    .bind(&op.id)
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

/// [`prepared_worker`], then the production release with no report; the task names the card.
async fn owner_op_lease(fx: &Fx, key: &str) -> (Task, OwnedLease) {
    let (task, owned) = prepared_worker(fx, key).await;
    assert!(
        release_workspace_lease_for_card_for_test(
            fx.boot.repo.as_ref(),
            &fx.boot.ctx.events,
            &owned.card
        )
        .await
        .unwrap()
    );
    sqlx::query("UPDATE tasks SET worker_card_id = ?1 WHERE id = ?2")
        .bind(&owned.card)
        .bind(&task.id)
        .execute(&fx.pool())
        .await
        .unwrap();
    (task, owned)
}

/// Whether the card's own PTY row records no exit.
async fn worker_pty_live(fx: &Fx, card: &str) -> bool {
    sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM terminals \
         WHERE card_id = ?1 AND exit_code IS NULL AND signal_killed = 0)",
    )
    .bind(card)
    .fetch_one(&fx.pool())
    .await
    .unwrap()
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
