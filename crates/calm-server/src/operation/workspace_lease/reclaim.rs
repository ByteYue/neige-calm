//! Reclaim of released lease worktrees (#1815).
//!
//! A worker's report releases only the lease row (`decision_sink`): the legacy `git.commit:auto`
//! action runs after the release, the kernel delivery settles later, and a gate reads the
//! worktree while its task is `verifying`. The scheduler's reconcile sweep (boot and every tick)
//! removes the worktree and its slice branch once nothing can read them any more; a candidate
//! stays pinned by its `refs/neige/candidates/…` ref (D9).

use sqlx::SqlitePool;

use super::facts::worktree_removed_after_last_provision_sql;
use super::{
    WORKSPACE_LEASE_COLUMNS, WorkspaceLease, any_track_has_active_forge_action,
    persist_worktree_removed_for_lease, remove_workspace_worktree_for_lease,
    row_to_workspace_lease, workspace_lease_target_from_lease,
};
use crate::error::{CalmError, Result};
use crate::event::EventBus;
use crate::mcp_server::transport::forge_deadline_ms;

/// At most this many removals are attempted per pass; the rest wait for the next tick.
pub(crate) const RECLAIM_PER_PASS: usize = 16;

/// The released leases whose worktree may be removed, in random order (a refused entry stays
/// reclaimable and must not hold the per-pass cap on every tick). Lease `L` of card `C` on a
/// live Track qualifies when:
/// - `L` is `C`'s latest released lease (every lease of a card shares its path) and `C` holds
///   no `held`/`releasing` lease (re-checked before the git calls);
/// - `L`'s latest delivery (highest ordinal) is absent, a `candidate`, `failed` without retry,
///   or abandoned;
/// - `C`'s attempt — the task the lease owner's worker op was keyed by (the ownership proof of
///   `calm_truth::db::sqlite::worker_op_targets_card_tx`), else the tasks `C` worked — is
///   `done`/`canceled`, or `C` is gone. A `failed` or `verifying` attempt and a card without a
///   task keep their worktree (recovery guidance, the gate);
/// - a legacy lease (`delivery_policy IS NULL`, whose auto-commit runs after the release) was
///   released at or before `legacy_released_before_ms`;
/// - the kernel has not removed `C`'s worktree since it was last provisioned.
///
/// The Track's forge actions are checked per entry in [`reclaim_released_workspace_worktrees`].
pub(crate) async fn reclaimable_released_workspace_leases(
    pool: &SqlitePool,
    legacy_released_before_ms: i64,
) -> Result<Vec<WorkspaceLease>> {
    let removed = worktree_removed_after_last_provision_sql("wl.card_id");
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
                 (SELECT t.status IN ('done','canceled')
                  FROM operations o JOIN tasks t ON t.id = o.idempotency_key
                  WHERE o.id = wl.lease_owner
                    AND o.kind IN ('codex-worker', 'terminal-worker', 'claude-worker', 'codex-isolated-worker')
                    AND o.target_type = 'card'
                    AND o.target_id = wl.card_id
                    AND json_extract(o.payload_json, '$.actor.kind') = 'KernelDispatcher'),
                 EXISTS (SELECT 1 FROM tasks t WHERE t.worker_card_id = wl.card_id)
                   AND NOT EXISTS (
                     SELECT 1 FROM tasks t
                     WHERE t.worker_card_id = wl.card_id AND t.status NOT IN ('done','canceled'))))
             AND (wl.delivery_policy IS NOT NULL OR wl.released_at_ms <= ?1)
             AND NOT {removed}
           ORDER BY RANDOM()"#
    );
    let rows = sqlx::query(&sql)
        .bind(legacy_released_before_ms)
        .fetch_all(pool)
        .await?;
    rows.into_iter().map(row_to_workspace_lease).collect()
}

/// How long a legacy lease keeps its worktree after its release: at least the deadline of any
/// forge action, the legacy `git.commit:auto` that commits it after the release included.
pub(crate) fn legacy_reclaim_grace_ms() -> i64 {
    forge_deadline_ms(true).max(forge_deadline_ms(false))
}

/// Remove the worktrees of `leases` (from [`reclaimable_released_workspace_leases`]), at most
/// [`RECLAIM_PER_PASS`] attempts, best effort per entry. Returns how many were removed.
pub(crate) async fn reclaim_released_workspace_worktrees(
    pool: &SqlitePool,
    events: &EventBus,
    leases: Vec<WorkspaceLease>,
) -> usize {
    let mut attempted = 0;
    let mut reclaimed = 0;
    for lease in leases {
        if attempted == RECLAIM_PER_PASS {
            break;
        }
        match reclaim_one(pool, events, &lease).await {
            Ok(Reclaim::Skipped) => {}
            Ok(Reclaim::Removed) => {
                attempted += 1;
                reclaimed += 1;
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
    reclaimed
}

enum Reclaim {
    /// Not a lease worktree path, the card took a lease again, or a forge action runs on the
    /// Track: nothing was touched.
    Skipped,
    Removed,
    /// Removed, but the Track row was deleted meanwhile: its own sweep owns the events.
    TrackGone,
}

async fn reclaim_one(
    pool: &SqlitePool,
    events: &EventBus,
    lease: &WorkspaceLease,
) -> Result<Reclaim> {
    // Only a lease worktree (`<repo>/.claude/worktrees/<track>/<card>`) is reclaimed: a
    // relative legacy path would resolve against the kernel's own working directory.
    if workspace_lease_target_from_lease(lease)?.is_none() {
        return Ok(Reclaim::Skipped);
    }
    // Re-read right before the git calls: a new lease of the card shares the path, and a
    // forge action may run with the worktree as its cwd.
    if card_holds_active_lease(pool, &lease.card_id).await?
        || any_track_has_active_forge_action(pool, &[lease.track_id.as_str()]).await?
    {
        return Ok(Reclaim::Skipped);
    }
    let removal = lease.clone();
    // `Ok(false)` (nothing was left on disk) is recorded as removed too: otherwise the entry
    // would be selected again on every pass.
    tokio::task::spawn_blocking(move || remove_workspace_worktree_for_lease(&removal))
        .await
        .map_err(|error| CalmError::Internal(format!("worktree reclaim task: {error}")))??;
    match persist_worktree_removed_for_lease(pool, events, lease).await {
        Ok(()) => Ok(Reclaim::Removed),
        Err(CalmError::NotFound(_)) => Ok(Reclaim::TrackGone),
        Err(error) => Err(error),
    }
}

async fn card_holds_active_lease(pool: &SqlitePool, card_id: &str) -> Result<bool> {
    let held = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM workspace_leases \
         WHERE card_id = ?1 AND state IN ('held','releasing'))",
    )
    .bind(card_id)
    .fetch_one(pool)
    .await?;
    Ok(held)
}
