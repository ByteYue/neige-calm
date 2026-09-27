//! Reclaim of released lease worktrees (#1815).
//!
//! A worker's report releases only the lease row (`decision_sink`): the legacy `git.commit:auto`
//! action runs after the release, the kernel delivery settles later, and a gate reads the
//! worktree while its task is `verifying`. The scheduler's reconcile sweep (boot and every tick)
//! removes the checkout once nothing can read it any more, and only after a grace since the
//! release. Only a clean checkout goes, and the
//! slice branch `neige/<track>/<card>` stays: it can hold the only copy of a legacy attempt's
//! committed work (no candidate ref pins it), and Track deletion still sweeps it. A candidate
//! stays pinned by its `refs/neige/candidates/…` ref (D9).

use std::collections::HashSet;
use std::time::Duration;

use sqlx::SqlitePool;

use super::facts::worktree_removed_after_last_provision_sql;
use super::{
    RemovalOutcome, WORKSPACE_LEASE_COLUMNS, WorkspaceLease, WorkspaceLeaseTarget, WorktreeRemoval,
    any_track_has_active_forge_action, git_failed, git_worktree_prune, git_worktree_registered,
    persist_worktree_removed_for_lease, remove_workspace_worktree_for_lease_as,
    row_to_workspace_lease,
};
use crate::error::{CalmError, Result};
use crate::event::EventBus;
use crate::mcp_server::transport::forge_deadline_ms;
use crate::plugin_host::child_process::run_bounded;
use crate::workspace_materialize::isolated_git_command;

/// At most this many removals are attempted per pass; the rest wait for the next tick.
pub(crate) const RECLAIM_PER_PASS: usize = 16;

/// A live terminal (the `terminals_running` rule: no exit recorded, not killed) of another card
/// whose `cwd` is the lease worktree or under it — a terminal opened in the worktree (a dev
/// server, a Planner preview). `card`, `path` and `canonical` are SQL expressions for the lease's
/// `card_id`, `path` and `canonical_path` (a NULL `canonical_path` matches nothing). The lease's
/// own worker card is exempt: its PTY outlives a successful report until the Track completes,
/// and removing a finished worker's cwd loses no work — [`WorktreeRemoval::KeepWork`] never
/// removes a dirty tree and keeps the slice branch.
fn live_terminal_under_sql(card: &str, path: &str, canonical: &str) -> String {
    let under = |root: &str| {
        format!("(te.cwd = {root} OR substr(te.cwd, 1, length({root}) + 1) = {root} || '/')")
    };
    format!(
        "EXISTS (SELECT 1 FROM terminals te \
         WHERE te.exit_code IS NULL AND te.signal_killed = 0 AND te.card_id IS NOT {card} \
         AND ({} OR {}))",
        under(path),
        under(canonical)
    )
}

/// Whether the attempt row `t` is finished with its worktree: `done` / `canceled`, or `failed`
/// and replaced by `calm.task.replace` (the successor carries its candidate; the failed attempt
/// is never recovered once replaced). Any other `failed` attempt keeps it for recovery guidance.
fn attempt_finished_sql(t: &str) -> String {
    format!(
        "({t}.status IN ('done','canceled') OR ({t}.status = 'failed' AND EXISTS (\
         SELECT 1 FROM task_replacements r WHERE r.predecessor_attempt_id = {t}.id)))"
    )
}

/// The released leases whose worktree may be removed, oldest release first. Lease `L` of card
/// `C` on a live Track qualifies when:
/// - `L` is `C`'s latest released lease (every lease of a card shares its path), `C` holds no
///   `held`/`releasing` lease, and no other card's live terminal works in the worktree (both
///   re-checked before the git calls);
/// - `L`'s latest delivery (highest ordinal) is absent, a `candidate`, `failed` without retry,
///   or abandoned;
/// - `C`'s attempt — the task the lease owner's worker op was keyed by (the ownership proof of
///   `calm_truth::db::sqlite::worker_op_targets_card_tx`), else every task `C` worked — is
///   finished ([`attempt_finished_sql`]), or `C` is gone. A `failed` attempt nobody replaced,
///   a `verifying` one and a card without a task keep their worktree;
/// - the lease was released at or before `released_before_ms` ([`reclaim_grace_ms`] ago);
/// - the kernel has not removed `C`'s worktree since it was last provisioned.
///
/// The Track's forge actions are checked per entry in [`reclaim_released_workspace_worktrees`].
pub(crate) async fn reclaimable_released_workspace_leases(
    pool: &SqlitePool,
    released_before_ms: i64,
) -> Result<Vec<WorkspaceLease>> {
    let removed = worktree_removed_after_last_provision_sql("wl.card_id");
    let terminal = live_terminal_under_sql("wl.card_id", "wl.path", "wl.canonical_path");
    let owner_finished = attempt_finished_sql("t");
    let sql = format!(
        r#"SELECT {WORKSPACE_LEASE_COLUMNS} FROM workspace_leases wl
           WHERE wl.state = 'released'
             AND EXISTS (SELECT 1 FROM tracks tr WHERE tr.id = wl.track_id)
             AND wl.lease_id = (
               SELECT l.lease_id FROM workspace_leases l
               WHERE l.card_id = wl.card_id AND l.state = 'released'
               ORDER BY l.created_at_ms DESC, l.lease_id DESC LIMIT 1)
             AND NOT EXISTS (
               SELECT 1 FROM workspace_leases h
               WHERE h.card_id = wl.card_id AND h.state IN ('held','releasing'))
             AND NOT {terminal}
             AND NOT EXISTS (
               SELECT 1 FROM task_git_deliveries d
               WHERE d.delivery_id = (
                   SELECT latest.delivery_id FROM task_git_deliveries latest
                   WHERE latest.lease_id = wl.lease_id
                   ORDER BY latest.ordinal DESC, latest.created_at_ms DESC LIMIT 1)
                 AND NOT (
                   d.settlement IS 'candidate'
                   OR (d.settlement IS 'failed' AND d.retry_allowed IS 0)
                   OR EXISTS (
                     SELECT 1 FROM task_git_delivery_abandonments a
                     WHERE a.delivery_id = d.delivery_id)))
             AND (
               NOT EXISTS (SELECT 1 FROM cards c WHERE c.id = wl.card_id)
               OR COALESCE(
                 (SELECT {owner_finished}
                  FROM operations o JOIN tasks t ON t.id = o.idempotency_key
                  WHERE o.id = wl.lease_owner
                    AND o.kind IN ('codex-worker', 'terminal-worker', 'claude-worker', 'codex-isolated-worker')
                    AND o.target_type = 'card'
                    AND o.target_id = wl.card_id
                    AND json_extract(o.payload_json, '$.actor.kind') = 'KernelDispatcher'),
                 EXISTS (SELECT 1 FROM tasks t WHERE t.worker_card_id = wl.card_id)
                   AND NOT EXISTS (
                     SELECT 1 FROM tasks t
                     WHERE t.worker_card_id = wl.card_id AND NOT {owner_finished})))
             AND wl.released_at_ms <= ?1
             AND NOT {removed}
           ORDER BY wl.released_at_ms ASC, wl.lease_id ASC"#
    );
    let rows = sqlx::query(&sql)
        .bind(released_before_ms)
        .fetch_all(pool)
        .await?;
    rows.into_iter().map(row_to_workspace_lease).collect()
}

/// How long every lease keeps its worktree after its release: at least the deadline of any
/// forge action (a legacy lease's `git.commit:auto` commits it after the release), which also
/// leaves the Planner time to open a preview terminal in it after its wake.
pub(crate) fn reclaim_grace_ms() -> i64 {
    forge_deadline_ms(true).max(forge_deadline_ms(false))
}

/// What one reclaim pass did.
#[derive(Debug, Default)]
pub(crate) struct ReclaimPass {
    pub reclaimed: usize,
    /// Entries that will not be removed as they stand (a checkout with work in it, a path that
    /// no longer resolves to its worktree, …): `(lease_id, why)`. The caller leaves them out of
    /// later passes; they do not count toward [`RECLAIM_PER_PASS`].
    pub refused: Vec<(String, String)>,
}

/// Remove the worktrees of `leases` (from [`reclaimable_released_workspace_leases`]) not in
/// `skip`, at most [`RECLAIM_PER_PASS`] removals or failures, best effort per entry.
pub(crate) async fn reclaim_released_workspace_worktrees(
    pool: &SqlitePool,
    events: &EventBus,
    leases: Vec<WorkspaceLease>,
    skip: &HashSet<String>,
) -> ReclaimPass {
    let mut pass = ReclaimPass::default();
    let mut attempted = 0;
    for lease in leases {
        if attempted == RECLAIM_PER_PASS {
            break;
        }
        if skip.contains(&lease.lease_id) {
            continue;
        }
        match reclaim_one(pool, events, &lease).await {
            Ok(Reclaim::Skipped) => {}
            Ok(Reclaim::Refused(why)) => pass.refused.push((lease.lease_id.clone(), why)),
            Ok(Reclaim::Removed) => {
                attempted += 1;
                pass.reclaimed += 1;
            }
            Ok(Reclaim::TrackGone) => attempted += 1,
            Err(error) => {
                attempted += 1;
                tracing::warn!(
                    lease_id = %lease.lease_id,
                    card_id = %lease.card_id,
                    track_id = %lease.track_id,
                    path = %lease.path,
                    %error,
                    "released worktree reclaim failed; next sweep retries"
                );
            }
        }
    }
    pass
}

enum Reclaim {
    /// The card took a lease again, a terminal works in the worktree, or a forge action runs
    /// on the Track: nothing was touched; a later pass looks again.
    Skipped,
    /// Nothing was touched, and why.
    Refused(String),
    Removed,
    /// Removed, but the Track row was deleted meanwhile: its own sweep owns the events.
    TrackGone,
}

async fn reclaim_one(
    pool: &SqlitePool,
    events: &EventBus,
    lease: &WorkspaceLease,
) -> Result<Reclaim> {
    // Re-read right before the git calls: a new lease of the card shares the path, and a
    // terminal or a forge action may work in the worktree.
    if lease_in_use(pool, lease).await?
        || any_track_has_active_forge_action(pool, &[lease.track_id.as_str()]).await?
    {
        return Ok(Reclaim::Skipped);
    }
    let removal = lease.clone();
    let removed = tokio::task::spawn_blocking(move || {
        remove_workspace_worktree_for_lease_as(&removal, WorktreeRemoval::KeepWork)
    })
    .await
    .map_err(|error| CalmError::Internal(format!("worktree reclaim task: {error}")));
    // A failed or timed-out git run (a locked worktree, submodules, a hung filter, …) leaves the
    // entry as it was, like a refusal: the same state fails the same way on every pass.
    let outcome = match removed {
        Ok(Ok(outcome)) => outcome,
        Ok(Err(error)) | Err(error) => RemovalOutcome::Refused(error.to_string()),
    };
    // `Removed(false)` (nothing was left on disk) is recorded as removed too: otherwise the
    // entry would be selected again on every pass.
    if let RemovalOutcome::Refused(why) = outcome {
        return Ok(Reclaim::Refused(why));
    }
    match persist_worktree_removed_for_lease(pool, events, lease).await {
        Ok(()) => Ok(Reclaim::Removed),
        Err(CalmError::NotFound(_)) => Ok(Reclaim::TrackGone),
        Err(error) => Err(error),
    }
}

/// The card holds a lease again, or another card's live terminal works in the worktree.
async fn lease_in_use(pool: &SqlitePool, lease: &WorkspaceLease) -> Result<bool> {
    let canonical = lease
        .base
        .as_ref()
        .map(|base| base.canonical_path.to_string_lossy().to_string());
    let sql = format!(
        "SELECT EXISTS(SELECT 1 FROM workspace_leases \
         WHERE card_id = ?1 AND state IN ('held','releasing')) OR {}",
        live_terminal_under_sql("?1", "?2", "?3")
    );
    let in_use = sqlx::query_scalar(&sql)
        .bind(&lease.card_id)
        .bind(&lease.path)
        .bind(canonical)
        .fetch_one(pool)
        .await?;
    Ok(in_use)
}

/// [`WorktreeRemoval::KeepWork`] after the identity, repository, symlink-leaf and
/// foreign-registration checks of `remove_workspace_worktree_as`: a registered checkout goes
/// only when `git status` shows no tracked change and no untracked (non-ignored) file, through
/// `git worktree remove` without `--force` (which re-checks the same and refuses a tree that
/// changed since); a registration whose directory is gone is pruned; an unregistered directory
/// is left alone. The slice branch is never deleted here.
pub(super) fn remove_clean_worktree_keeping_branch(
    target: &WorkspaceLeaseTarget,
    link_removed: bool,
    registered: bool,
    path_existed: bool,
) -> Result<RemovalOutcome> {
    if !path_existed {
        if registered {
            // Drop the registration of the missing directory; touches no files.
            git_worktree_prune(&target.repo_root)?;
        }
        return Ok(RemovalOutcome::Removed(link_removed || registered));
    }
    if !registered {
        return Ok(RemovalOutcome::Refused(format!(
            "{} is not a registered worktree; its contents are left alone",
            target.path.display()
        )));
    }
    // Without its `.git` file, `git -C <path>` would find the attached repository above it and
    // read that checkout's status instead.
    if !target.path.is_dir() || !target.path.join(".git").is_file() {
        return Ok(RemovalOutcome::Refused(format!(
            "{} is not a worktree directory with a .git file; left alone",
            target.path.display()
        )));
    }
    if let Some(changes) = uncommitted_changes(target)? {
        return Ok(RemovalOutcome::Refused(format!(
            "worktree {} has uncommitted changes or untracked files ({changes}); left in place",
            target.path.display()
        )));
    }
    // Isolated and bounded: without `--force`, git runs `status` in the checkout (clean
    // filters); no fsmonitor — the `-c` reaches that child `status` too.
    let mut command = isolated_git_command();
    command
        .args(["-c", "core.fsmonitor=false", "-C"])
        .arg(&target.repo_root)
        .args(["worktree", "remove"])
        .arg(&target.path);
    let output = run_reclaim_git(command, "git worktree remove")?;
    if !output.status.success() && git_worktree_registered(target)? {
        return Err(git_failed(
            "git worktree remove",
            &target.repo_root,
            &output,
        ));
    }
    Ok(RemovalOutcome::Removed(true))
}

/// The first entries of `git status --porcelain` in the checkout (tracked changes and
/// untracked files that are not ignored), `None` when it is clean.
fn uncommitted_changes(target: &WorkspaceLeaseTarget) -> Result<Option<String>> {
    // Isolated: `status` runs the checkout's clean filters. No fsmonitor (a worker can point
    // the shared config at a command that never returns) and no index write.
    let mut command = isolated_git_command();
    command
        .arg("--no-optional-locks")
        .args(["-c", "core.fsmonitor=false", "-C"])
        .arg(&target.path)
        .args([
            "status",
            "--porcelain=v1",
            "-z",
            "--untracked-files=normal",
            "--ignore-submodules=none",
        ]);
    let output = run_reclaim_git(command, "git status")?;
    if !output.status.success() {
        return Err(git_failed("git status", &target.path, &output));
    }
    let entries: Vec<String> = output
        .stdout
        .split(|byte| *byte == 0)
        .filter(|entry| !entry.is_empty())
        .map(|entry| String::from_utf8_lossy(entry).to_string())
        .collect();
    if entries.is_empty() {
        return Ok(None);
    }
    let mut shown = entries
        .iter()
        .take(3)
        .cloned()
        .collect::<Vec<_>>()
        .join(", ");
    if entries.len() > 3 {
        shown.push_str(&format!(", … {} entries", entries.len()));
    }
    Ok(Some(shown))
}

/// Bound on each reclaim git run that executes repository code. The remove deletes the whole
/// checkout, ignored build output (`node_modules`, `target`) included, so it gets minutes, not
/// the gate sampler's seconds; it runs off the async runtime and outside any transaction.
const RECLAIM_GIT_TIMEOUT: Duration = Duration::from_secs(120);
const RECLAIM_GIT_OUTPUT_CAP: usize = 1024 * 1024;

/// One git run through the gate's bounded runner (process group, deadline, capped output),
/// driven from the blocking thread the removal runs on.
fn run_reclaim_git(command: std::process::Command, what: &str) -> Result<std::process::Output> {
    let deadline = tokio::time::Instant::now() + RECLAIM_GIT_TIMEOUT;
    tokio::runtime::Handle::current()
        .block_on(run_bounded(
            tokio::process::Command::from(command),
            deadline,
            RECLAIM_GIT_OUTPUT_CAP,
        ))
        .map_err(|error| CalmError::Internal(format!("{what}: {error:?}")))
}
