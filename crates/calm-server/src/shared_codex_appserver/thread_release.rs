//! Releasing a thread from the shared daemon (#1853). `thread/start` and `thread/resume`
//! subscribe the kernel's one connection, and codex keeps a thread (and its MCP servers)
//! loaded while any connection is subscribed. A later `thread/resume` reloads a released
//! thread, so releasing one whose Card is gone or whose session ended is safe.
use super::*;

/// Per-call budget: a release runs after a delete commits and on the sweeper tick, and a wedged
/// daemon must not hold either for the client's default 30 s per thread.
const THREAD_UNSUBSCRIBE_TIMEOUT: Duration = Duration::from_secs(5);

impl SharedCodexAppServer {
    /// Drop `thread_id`'s attribution and unsubscribe the kernel connection from it. Best
    /// effort: an RPC failure is logged, and with no connection there is nothing to unsubscribe.
    /// The tombstone keeps a later `thread/started` for it from binding to a pending Card.
    async fn release_thread(&self, thread_id: &str) {
        self.thread_cache.remove(thread_id);
        self.forgotten_threads.lock().await.remember(thread_id);
        self.unsubscribe_thread(thread_id).await;
    }

    pub(super) async fn unsubscribe_thread(&self, thread_id: &str) {
        #[cfg(feature = "fixtures")]
        if let Some(fake) = self.fake.as_ref() {
            fake.unsubscribed_threads
                .lock()
                .expect("fake unsubscribed threads mutex")
                .push(thread_id.to_string());
            return;
        }
        let Some(client) = self.running_client().await else {
            return;
        };
        let deadline = tokio::time::Instant::now() + THREAD_UNSUBSCRIBE_TIMEOUT;
        match client.thread_unsubscribe(thread_id, deadline).await {
            Ok(response) => tracing::info!(
                target: "shared_codex_daemon::release_thread",
                %thread_id,
                status = %response.status,
                "released shared codex thread"
            ),
            Err(error) => tracing::warn!(
                target: "shared_codex_daemon::release_thread",
                %thread_id,
                %error,
                "thread/unsubscribe failed; codex may keep the thread loaded"
            ),
        }
    }

    /// Release every cached thread whose session POSITIVELY ended (`codex_threads_ended`); a
    /// thread no session row names yet stays.
    /// Holds `resume_replay_serial` from the read through the RPCs, so a system-error recovery,
    /// which revives a Failed row under the same lock, lands wholly before or after this pass.
    pub async fn release_ended_threads(&self) -> Result<usize> {
        let _replay_guard = self.resume_replay_serial.lock().await;
        let cached: Vec<String> = self
            .thread_cache
            .iter()
            .map(|entry| entry.key().clone())
            .collect();
        let ended = self.repo.codex_threads_ended(&cached).await?;
        for thread_id in &ended {
            self.release_thread(thread_id).await;
        }
        Ok(ended.len())
    }

    #[cfg(feature = "fixtures")]
    pub fn unsubscribed_threads_for_test(&self) -> Vec<String> {
        self.fake
            .as_ref()
            .expect("fake daemon")
            .unsubscribed_threads
            .lock()
            .expect("fake unsubscribed threads mutex")
            .clone()
    }
}
