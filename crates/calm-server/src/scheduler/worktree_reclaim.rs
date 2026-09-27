//! The reconcile sweep's released-worktree reclaim (#1815): the selection runs inside the pass,
//! so it reads the same state as the rest of the pass; the removals (git and the file tree,
//! hundreds of MB for some checkouts) run in a spawned task, so neither boot nor a tick waits
//! for them. One reclaim at a time per scheduler; a lease it refused is not looked at again
//! until the process restarts.
use super::*;
use crate::operation::workspace_lease::reclaim::{
    legacy_reclaim_grace_ms, reclaim_released_workspace_worktrees,
    reclaimable_released_workspace_leases,
};

impl Scheduler {
    pub(super) async fn start_worktree_reclaim(&self) {
        let Ok(guard) = Arc::clone(&self.worktree_reclaim).try_lock_owned() else {
            tracing::debug!("scheduler sweep: worktree reclaim still running; skipped");
            return;
        };
        let Some(pool) = self.repo.sqlite_pool() else {
            return;
        };
        let leases = match reclaimable_released_workspace_leases(
            &pool,
            now_ms().saturating_sub(legacy_reclaim_grace_ms()),
        )
        .await
        {
            Ok(leases) => leases,
            Err(error) => {
                tracing::warn!(%error, "released worktree reclaim scan failed; next tick retries");
                return;
            }
        };
        if leases.is_empty() {
            return;
        }
        let events = self.events.clone();
        let refused = Arc::clone(&self.worktree_reclaim_refused);
        tokio::spawn(async move {
            let skip = refused.lock().expect("reclaim refusals lock").clone();
            let pass = reclaim_released_workspace_worktrees(&pool, &events, leases, &skip).await;
            if pass.reclaimed > 0 {
                tracing::info!(
                    reclaimed = pass.reclaimed,
                    "released lease worktrees reclaimed"
                );
            }
            let mut remembered = refused.lock().expect("reclaim refusals lock");
            for (lease_id, why) in pass.refused {
                tracing::info!(%lease_id, %why, "released worktree kept; not retried until restart");
                remembered.insert(lease_id);
            }
            drop(remembered);
            drop(guard);
        });
    }

    /// TEST seam: wait until no worktree reclaim started by a sweep is running.
    #[doc(hidden)]
    pub async fn worktree_reclaim_idle_for_test(&self) {
        drop(self.worktree_reclaim.lock().await);
    }
}
