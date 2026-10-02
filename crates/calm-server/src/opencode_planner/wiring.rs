//! The existing Planner rendering remains authoritative. Project config is disabled to keep
//! process/MCP scope fixed, so workspace instruction files are injected explicitly.
use super::{
    config::OpenCodePlannerHost,
    session::{OpenCodePlannerSession, OpenCodePlannerSessionParams},
};
use crate::{
    db::Repo,
    error::{CalmError, Result},
    plugin_host::PluginHost,
    shared_codex_appserver::SharedCodexAppServer,
};
use std::path::Path;
use std::sync::Arc;

#[derive(Clone)]
pub struct OpenCodePlannerWiring {
    pub host: Arc<OpenCodePlannerHost>,
    pub plugin: Arc<PluginHost>,
    pub events: crate::event::EventBus,
    pub write: crate::state::WriteContext,
}
pub struct OpenCodePlannerRow<'a> {
    pub worker_session_id: &'a str,
    pub card_id: &'a str,
    pub track_id: &'a str,
    pub prior_total_tokens: i64,
}
impl OpenCodePlannerWiring {
    pub async fn open_session(
        &self,
        repo: Arc<dyn Repo>,
        seals: Arc<SharedCodexAppServer>,
        row: OpenCodePlannerRow<'_>,
    ) -> Result<Arc<OpenCodePlannerSession>> {
        let track = repo
            .track_get(row.track_id)
            .await?
            .ok_or_else(|| CalmError::NotFound(format!("track {}", row.track_id)))?;
        let card = repo
            .card_get(row.card_id)
            .await?
            .ok_or_else(|| CalmError::NotFound("OpenCode card".into()))?;
        let binding = super::attachment::Binding::from_payload(&card.payload)?;
        let cwd = binding
            .as_ref()
            .map(|binding| binding.directory.clone())
            .unwrap_or_else(|| std::path::PathBuf::from(track.workspace.agent_cwd()));
        let mut instructions = if binding.is_some() {
            String::new()
        } else {
            crate::operation::planner_harness_start_adapter::planner_instructions(
                repo.as_ref(),
                &self.plugin,
                row.track_id,
                row.card_id,
            )
            .await?
        };
        if binding.is_none() {
            instructions.push_str(&workspace_instructions(&cwd)?);
        }
        let settings = crate::routes::settings::load_settings(repo.as_ref()).await?;
        let proxy = SharedCodexAppServer::resolved_proxy_env_pairs(
            settings.http_proxy.as_deref(),
            settings.https_proxy.as_deref(),
            |key| std::env::var(key).ok(),
        )
        .into_iter()
        .map(|(upper, lower, value)| (upper.into(), lower.into(), value))
        .collect();
        Ok(Arc::new(
            OpenCodePlannerSession::open(OpenCodePlannerSessionParams {
                host: Arc::clone(&self.host),
                worker_session_id: row.worker_session_id.into(),
                card_id: row.card_id.into(),
                track_id: row.track_id.into(),
                cwd,
                instructions,
                proxy,
                prior_total_tokens: row.prior_total_tokens,
                repo,
                seals,
                events: self.events.clone(),
                write: self.write.clone(),
            })
            .await?,
        ))
    }
}
#[cfg(any(test, feature = "fixtures"))]
impl OpenCodePlannerWiring {
    pub fn unconfigured_for_test(repo: Arc<dyn Repo>) -> Self {
        let route: Arc<dyn crate::db::RouteRepo> = repo;
        Self {
            events: crate::event::EventBus::new(),
            write: crate::state::WriteContext::new(
                crate::card_role_cache::CardRoleCache::new(),
                crate::track_area_cache::TrackAreaCache::new(),
            ),
            host: Arc::new(OpenCodePlannerHost::unconfigured_scratch().expect("OpenCode host")),
            plugin: Arc::new(PluginHost::new_full(
                Arc::new(crate::plugin_host::PluginRegistry::empty()),
                route,
                std::path::PathBuf::new(),
                std::env::temp_dir().join("calm-opencode-planner-test-plugins-data"),
                Vec::new(),
                crate::event::EventBus::new(),
                crate::state::WriteContext::new(
                    crate::card_role_cache::CardRoleCache::new(),
                    crate::track_area_cache::TrackAreaCache::new(),
                ),
            )),
        }
    }
}
pub(crate) fn workspace_instructions(cwd: &Path) -> Result<String> {
    let mut directories = Vec::new();
    for dir in cwd.canonicalize()?.ancestors() {
        directories.push(dir.to_path_buf());
        if dir.join(".git").exists() {
            break;
        }
    }
    directories.reverse();
    let mut output = String::new();
    for dir in directories {
        for name in ["AGENTS.md", "CLAUDE.md", "CONTEXT.md"] {
            let path = dir.join(name);
            match std::fs::read_to_string(&path) {
                Ok(contents) => {
                    if output.len().saturating_add(contents.len()) > 1024 * 1024 {
                        return Err(CalmError::Conflict(
                            "workspace instructions exceed the OpenCode input limit".into(),
                        ));
                    }
                    output.push_str(&format!(
                        "\n\nInstructions from {}:\n{contents}",
                        path.display()
                    ));
                    break;
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
        }
    }
    Ok(output)
}
