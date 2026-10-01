//! Declared access to the Track checkout. Missing selection preserves exclusive writes.
use serde::{Deserialize, Serialize};
use serde_json::Value;
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum WorkspaceAccess {
    ReadOnly,
    #[default]
    ReadWrite,
}
impl WorkspaceAccess {
    pub fn from_context(context: &Value) -> Result<Self, String> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Selection {
            access: WorkspaceAccess,
        }
        match context.get("neige_workspace") {
            None => Ok(Self::ReadWrite),
            Some(value) => serde_json::from_value::<Selection>(value.clone())
                .map(|selection| selection.access)
                .map_err(|error| format!("neige_workspace: {error}")),
        }
    }
    pub fn validate_task(self, task: &Value) -> Result<(), String> {
        if self == Self::ReadWrite {
            return Ok(());
        }
        if task.get("kind").and_then(Value::as_str) != Some("codex")
            || task
                .get("spawn")
                .and_then(Value::as_str)
                .is_some_and(|route| route != crate::task_recovery::TASK_IN_TRACK_ROUTE)
            || task.get("gate").is_some_and(|gate| !gate.is_null())
        {
            return Err(
                "neige_workspace: read_only requires a Codex task in this Track without a gate"
                    .into(),
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn workspace_access_requires_explicit_valid_read_intent() {
        assert_eq!(
            WorkspaceAccess::from_context(&json!({})).unwrap(),
            WorkspaceAccess::ReadWrite
        );
        assert_eq!(
            WorkspaceAccess::from_context(&json!({"neige_workspace":{"access":"read_only"}}))
                .unwrap(),
            WorkspaceAccess::ReadOnly
        );
        for selection in [
            json!(null),
            json!({}),
            json!({"access":"readonly"}),
            json!({"access":"read_only","source_task":"old-contract"}),
        ] {
            assert!(WorkspaceAccess::from_context(&json!({"neige_workspace":selection})).is_err());
        }
    }
}
