//! Revoke before stopping. Process absence never rewrites a submitted native turn as completed.
use super::config::OpenCodePlannerHost;
use crate::db::sqlite::{OpenCodePlannerScope, opencode_planner_revoke_tx};
use crate::db::{RepoEventWrite, write_in_tx_typed};
use crate::error::Result;

pub async fn boot(repo: &dyn RepoEventWrite, host: &OpenCodePlannerHost) -> Result<()> {
    let ids = write_in_tx_typed(repo, |tx| {
        Box::pin(
            async move { Ok(opencode_planner_revoke_tx(tx, OpenCodePlannerScope::All).await?) },
        )
    })
    .await?;
    let ids: Vec<&str> = ids.iter().map(String::as_str).collect();
    super::stop::sweep(&host.instance, &ids).await
}
pub async fn sweep_track(
    repo: &dyn RepoEventWrite,
    host: &OpenCodePlannerHost,
    track_id: &str,
) -> Result<()> {
    let track_id = track_id.to_owned();
    let ids = write_in_tx_typed(repo, move |tx| {
        Box::pin(async move {
            Ok(opencode_planner_revoke_tx(tx, OpenCodePlannerScope::Track(&track_id)).await?)
        })
    })
    .await?;
    let ids: Vec<&str> = ids.iter().map(String::as_str).collect();
    super::stop::sweep(&host.instance, &ids).await
}
pub(crate) async fn revoke_session(repo: &dyn RepoEventWrite, id: &str) -> Result<()> {
    let id = id.to_owned();
    write_in_tx_typed(repo, move |tx| {
        Box::pin(async move {
            opencode_planner_revoke_tx(tx, OpenCodePlannerScope::Session(&id)).await?;
            Ok(())
        })
    })
    .await
}
pub async fn stop_session(
    repo: &dyn RepoEventWrite,
    host: &OpenCodePlannerHost,
    id: &str,
) -> Result<()> {
    revoke_session(repo, id).await?;
    super::stop::stop(&host.instance, id).await
}
