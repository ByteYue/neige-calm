//! Reuse the verified marker sweep, with a provider namespace in every marker value.
//! The inherited environment key is the existing process primitive's wire contract;
//! an OpenCode worker id can never match a Claude worker id.
pub use crate::claude_planner::stop::{MarkerInstance, STOP_BOUND};
use crate::error::Result;

pub(crate) fn identity(worker_session_id: &str) -> String {
    format!("opencode:{worker_session_id}")
}
pub async fn stop(instance: &MarkerInstance, worker_session_id: &str) -> Result<()> {
    crate::claude_planner::stop::stop(instance, &identity(worker_session_id)).await
}
pub async fn sweep(instance: &MarkerInstance, ids: &[&str]) -> Result<()> {
    let ids: Vec<String> = ids.iter().map(|id| identity(id)).collect();
    let borrowed: Vec<&str> = ids.iter().map(String::as_str).collect();
    crate::claude_planner::stop::sweep(
        instance,
        &borrowed,
        crate::claude_planner::stop::SeamPolicy::Ignore,
    )
    .await
}
