//! Native input and existing-session identity validation, with no session creation fallback.
use crate::{
    codex_appserver::InputItem,
    error::{CalmError, Result},
};
use serde_json::{Value, json};

pub(crate) fn parts(items: &[InputItem]) -> Result<Vec<Value>> {
    items
        .iter()
        .map(|item| match item {
            InputItem::Text { text } => Ok(json!({"type":"text","text":text})),
            InputItem::LocalImage { .. } => Err(CalmError::BadRequest(
                "OpenCode Planner currently accepts text input only".into(),
            )),
        })
        .collect()
}
pub(crate) fn validate_native_session(
    native: &Value,
    id: &str,
    cwd: &std::path::Path,
) -> Result<()> {
    let actual = native["directory"]
        .as_str()
        .ok_or_else(|| CalmError::Conflict("OpenCode session has no directory".into()))?;
    if native["id"].as_str() != Some(id)
        || std::fs::canonicalize(actual)? != std::fs::canonicalize(cwd)?
    {
        return Err(CalmError::Conflict(
            "OpenCode native session identity/directory does not match this Planner".into(),
        ));
    }
    Ok(())
}
