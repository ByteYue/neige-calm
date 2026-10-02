//! Explicit operator registrations for existing native sessions. Connections are borrowed:
//! no profile mutation, process ownership, or Neige MCP authority is attached to them.
use super::{
    client::Client,
    config::{OpenCodePlannerHost, PINNED_VERSION},
    models::OpenCodeCatalog,
};
use crate::error::{CalmError, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::HashSet,
    path::{Path, PathBuf},
    sync::Arc,
};
use utoipa::ToSchema;

pub const PAYLOAD_KEY: &str = "opencode_attachment";

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConnectionsConfig {
    pub connections: Vec<ConnectionConfig>,
}
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConnectionConfig {
    pub id: String,
    pub label: String,
    pub generation: u64,
    pub directory: PathBuf,
    pub port: u16,
    pub password_file: PathBuf,
}
impl ConnectionsConfig {
    pub fn read(path: &Path) -> Result<Self> {
        Ok(serde_json::from_slice(&std::fs::read(path)?)?)
    }
}
#[derive(Clone, Debug)]
pub struct Connection {
    pub config: ConnectionConfig,
    client: Client,
    pub scope_id: String,
}
impl Connection {
    pub(crate) async fn validate_server(&self) -> Result<()> {
        let health = self.client.get("/global/health").await?;
        if health["healthy"] != true || health["version"].as_str() != Some(PINNED_VERSION) {
            return Err(CalmError::Conflict(format!(
                "Existing OpenCode server must report healthy version {PINNED_VERSION}"
            )));
        }
        Ok(())
    }
    pub(crate) fn client(&self) -> Client {
        self.client.clone()
    }
    pub(crate) async fn catalog(&self) -> Result<Arc<OpenCodeCatalog>> {
        self.validate_server().await?;
        OpenCodeCatalog::from_native(
            &self.client.get("/provider").await?,
            &self.client.get("/config").await?,
        )
        .map(Arc::new)
    }
    pub(crate) fn binding(&self, session_id: String) -> Binding {
        Binding {
            connection_id: self.config.id.clone(),
            generation: self.config.generation,
            port: self.config.port,
            directory: self.config.directory.clone(),
            session_id,
        }
    }
}
impl OpenCodePlannerHost {
    pub fn with_connections(mut self, config: ConnectionsConfig) -> Result<Self> {
        let mut ids = HashSet::new();
        let mut targets = HashSet::new();
        for mut entry in config.connections {
            if entry.id.is_empty()
                || entry.id.len() > 128
                || !entry
                    .id
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
                || entry.label.trim().is_empty()
                || (entry.generation == 0 || entry.generation > i64::MAX as u64)
                || entry.port == 0
                || !entry.directory.is_absolute()
                || !entry.password_file.is_absolute()
            {
                return Err(CalmError::BadRequest("OpenCode connection requires an identifier, label, positive generation/port and absolute directory/password_file".into()));
            }
            entry.directory = entry.directory.canonicalize()?;
            if !entry.directory.is_dir()
                || !ids.insert(entry.id.clone())
                || !targets.insert((entry.port, entry.directory.clone()))
            {
                return Err(CalmError::BadRequest(
                    "Duplicate OpenCode connection or invalid directory".into(),
                ));
            }
            let password = std::fs::read_to_string(&entry.password_file)?;
            let password = password.trim_end_matches(['\n', '\r']);
            if password.is_empty() || password.len() > 4096 || password.contains(['\n', '\r']) {
                return Err(CalmError::BadRequest(
                    "OpenCode password file must contain one nonempty password".into(),
                ));
            }
            let client = Client::new(entry.port, password, &entry.directory);
            // Scope includes the declared generation and target, never credential material.
            let scope_id = format!(
                "external:{}:{}:{}:{}",
                entry.id,
                entry.generation,
                entry.port,
                entry.directory.display()
            );
            self.connections.insert(
                entry.id.clone(),
                Arc::new(Connection {
                    config: entry,
                    client,
                    scope_id,
                }),
            );
        }
        Ok(self)
    }
    /// Borrowed sessions must not live in a directory Neige may recycle on deletion.
    pub fn validate_external_directories(&self, workspace_root: &Path) -> Result<()> {
        if self.connections.is_empty() {
            return Ok(());
        }
        self.reject_owned_directory(&workspace_root.canonicalize()?)
    }
    pub async fn validate_external_owned_directories(&self, pool: &sqlx::SqlitePool) -> Result<()> {
        if self.connections.is_empty() {
            return Ok(());
        }
        let paths: Vec<String> = sqlx::query_scalar(concat!(
            "SELECT workspace_worktree_path FROM tracks WHERE workspace_worktree_path IS NOT NULL ",
            "UNION SELECT path FROM workspace_leases ",
            "UNION SELECT canonical_path FROM workspace_leases WHERE canonical_path IS NOT NULL"
        ))
        .fetch_all(pool)
        .await?;
        for path in paths {
            match std::fs::canonicalize(path) {
                Ok(owned) => self.reject_owned_directory(&owned)?,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
        }
        Ok(())
    }
    fn reject_owned_directory(&self, owned: &Path) -> Result<()> {
        if self
            .connections
            .values()
            .any(|connection| connection.config.directory.starts_with(owned))
        {
            return Err(CalmError::BadRequest(
                "Existing OpenCode directory is owned by Neige workspace teardown".into(),
            ));
        }
        Ok(())
    }
    pub(crate) fn connection(&self, id: &str) -> Result<Arc<Connection>> {
        self.connections.get(id).cloned().ok_or_else(|| {
            CalmError::Conflict(format!("OpenCode connection {id} is not configured"))
        })
    }
    pub(crate) fn resolve_binding(&self, binding: &Binding) -> Result<Arc<Connection>> {
        let connection = self.connection(&binding.connection_id)?;
        if connection.config.generation != binding.generation
            || connection.config.port != binding.port
            || connection.config.directory != binding.directory
        {
            return Err(CalmError::Conflict("OpenCode connection configuration changed; the original session binding is retained and will not be repointed".into()));
        }
        Ok(connection)
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Binding {
    pub connection_id: String,
    pub generation: u64,
    pub port: u16,
    pub directory: PathBuf,
    pub session_id: String,
}
impl Binding {
    pub fn from_payload(payload: &Value) -> Result<Option<Self>> {
        payload
            .get(PAYLOAD_KEY)
            .map(|value| serde_json::from_value(value.clone()).map_err(CalmError::from))
            .transpose()
    }
}
#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct ConnectionSummary {
    pub id: String,
    pub label: String,
    pub directory: String,
}
#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct ConnectionsResponse {
    pub connections: Vec<ConnectionSummary>,
}
#[derive(Clone, Copy, Debug, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AttachedStatus {
    Idle,
    Running,
    Unavailable,
    Unknown,
}
#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct AttachedSession {
    pub connection_id: String,
    pub label: String,
    pub session_id: String,
    pub directory: String,
    pub status: AttachedStatus,
    pub can_submit: bool,
    pub can_stop: bool,
    pub model: Option<String>,
}
impl AttachedSession {
    pub(crate) fn initial(connection: &Connection, binding: &Binding) -> Self {
        Self {
            connection_id: binding.connection_id.clone(),
            label: connection.config.label.clone(),
            session_id: binding.session_id.clone(),
            directory: binding.directory.display().to_string(),
            status: AttachedStatus::Unknown,
            can_submit: false,
            can_stop: false,
            model: None,
        }
    }
}
pub(crate) fn session_model(native: &Value) -> Option<String> {
    Some(format!(
        "{}/{}",
        native["model"]["providerID"].as_str()?,
        native["model"]["id"]
            .as_str()
            .or(native["model"]["modelID"].as_str())?
    ))
}
pub(crate) fn latest_model(messages: &[Value]) -> Option<String> {
    messages.iter().rev().find_map(|message| {
        let info = &message["info"];
        let (provider, model) = if info["role"].as_str() == Some("user") {
            (&info["model"]["providerID"], &info["model"]["modelID"])
        } else {
            (&info["providerID"], &info["modelID"])
        };
        Some(format!("{}/{}", provider.as_str()?, model.as_str()?))
    })
}
pub(crate) async fn native_status(client: &Client, native: &str) -> Result<AttachedStatus> {
    let statuses = client.get("/session/status").await?;
    let object = statuses
        .as_object()
        .ok_or_else(|| CalmError::Conflict("OpenCode status snapshot is malformed".into()))?;
    match object
        .get(native)
        .and_then(|status| status["type"].as_str())
    {
        None if !object.contains_key(native) => Ok(AttachedStatus::Idle),
        Some("idle") => Ok(AttachedStatus::Idle),
        Some("busy" | "retry") => Ok(AttachedStatus::Running),
        _ => Ok(AttachedStatus::Unknown),
    }
}

impl super::session::OpenCodePlannerSession {
    pub fn attached_session(&self) -> Option<AttachedSession> {
        let mut metadata = self
            .shared
            .attached_metadata
            .lock()
            .expect("attached metadata")
            .clone()?;
        if self.shared.state().active.is_some() {
            metadata.status = AttachedStatus::Unknown;
        }
        metadata.can_submit = metadata.status == AttachedStatus::Idle
            && self.shared.state().active.is_none()
            && !self.shared.state().shutting_down;
        Some(metadata)
    }
    pub fn supports_interrupt(&self) -> bool {
        !self.is_attached()
    }
    pub fn is_attached(&self) -> bool {
        self.shared.attachment.is_some()
    }
    pub async fn check_can_submit(&self) -> Result<()> {
        if let Some((binding, _)) = &self.shared.attachment {
            self.shared
                .params
                .host
                .validate_external_owned_directories(&self.shared.pool()?)
                .await?;
            if !self
                .attached_session()
                .is_some_and(|metadata| metadata.can_submit)
            {
                return Err(CalmError::Conflict("The original OpenCode snapshot is not ready for submission; observe until it settles".into()));
            }
            if self.shared.state().active.is_some() {
                return Err(CalmError::Conflict("The original OpenCode submission is still unresolved; no additional prompt was sent".into()));
            }
            let client = self.shared.ensure_server().await?;
            let native = client
                .get(&format!("/session/{}", binding.session_id))
                .await?;
            super::session_input::validate_native_session(
                &native,
                &binding.session_id,
                &binding.directory,
            )?;
            if native_status(&client, &binding.session_id).await? != AttachedStatus::Idle {
                return Err(CalmError::Conflict("The existing OpenCode session is running or its state is unknown; observe it until it settles".into()));
            }
        }
        Ok(())
    }
}
