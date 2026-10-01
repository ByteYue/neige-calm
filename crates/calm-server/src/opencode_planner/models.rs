//! Native provider/model identifiers and declared variants, with no invented effort aliases.
use crate::error::{CalmError, Result};
use crate::planner_model::{CardModelSelection, TurnModelSelection};
use serde_json::Value;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OpenCodeModel {
    pub value: String,
    pub display_name: String,
    pub effort_levels: Vec<String>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OpenCodeCatalog {
    pub models: Vec<OpenCodeModel>,
    pub default_model: Option<String>,
    pub fetched_at_ms: i64,
}
impl OpenCodeCatalog {
    pub(crate) fn from_native(providers: &Value, config: &Value) -> Result<Self> {
        let connected = providers
            .get("connected")
            .and_then(Value::as_array)
            .ok_or_else(|| {
                CalmError::Conflict(
                    "OpenCode provider catalog has no connected providers field".into(),
                )
            })?;
        let all = providers
            .get("all")
            .and_then(Value::as_array)
            .ok_or_else(|| CalmError::Conflict("OpenCode provider catalog is malformed".into()))?;
        let mut models = Vec::new();
        for provider in all {
            let id = provider
                .get("id")
                .and_then(Value::as_str)
                .ok_or_else(|| CalmError::Conflict("OpenCode provider has no id".into()))?;
            if !connected.iter().any(|v| v.as_str() == Some(id)) {
                continue;
            }
            let entries = provider
                .get("models")
                .and_then(Value::as_object)
                .ok_or_else(|| CalmError::Conflict("OpenCode provider has no models".into()))?;
            for (model_id, model) in entries {
                models.push(OpenCodeModel {
                    value: format!("{id}/{model_id}"),
                    display_name: model
                        .get("name")
                        .and_then(Value::as_str)
                        .unwrap_or(model_id)
                        .into(),
                    effort_levels: model
                        .get("variants")
                        .and_then(Value::as_object)
                        .map(|m| m.keys().cloned().collect())
                        .unwrap_or_default(),
                });
            }
        }
        if models.is_empty() {
            return Err(CalmError::Conflict("OpenCode has no connected provider models; configure this dedicated Planner profile".into()));
        }
        let default_model = config
            .get("model")
            .and_then(Value::as_str)
            .map(str::to_owned);
        Ok(Self {
            models,
            default_model,
            fetched_at_ms: chrono::Utc::now().timestamp_millis(),
        })
    }
}
pub fn turn_selection(
    payload: &Value,
) -> std::result::Result<TurnModelSelection, (String, String)> {
    let selected = CardModelSelection::from_payload(payload).map_err(|e| {
        (
            e.to_string(),
            "This Planner's model selection cannot be read. Select a model again.".into(),
        )
    })?;
    if let Some(model) = selected.model.as_deref() {
        split_model(model).map_err(|e| {
            (
                e.to_string(),
                "Choose an OpenCode provider/model identifier.".into(),
            )
        })?;
    }
    Ok(TurnModelSelection {
        model: selected.model,
        effort: selected.reasoning_effort,
    })
}
pub(crate) fn split_model(value: &str) -> Result<(&str, &str)> {
    match value.split_once('/') {
        Some((provider, model))
            if !provider.is_empty()
                && !model.is_empty()
                && !provider.chars().any(char::is_whitespace)
                && !model.chars().any(char::is_whitespace) =>
        {
            Ok((provider, model))
        }
        _ => Err(CalmError::BadRequest(
            "OpenCode model must be provider/model".into(),
        )),
    }
}
