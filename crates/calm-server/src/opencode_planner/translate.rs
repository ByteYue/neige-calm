//! Snapshot projection. Native part identity is stable across polling and reconciliation.
use crate::codex_appserver::Notification;
use serde_json::{Value, json};
use std::collections::HashMap;

pub(crate) struct TurnProjection {
    pub(crate) thread: String,
    pub(crate) turn: String,
    pub(crate) message: String,
    pub(crate) client_id: String,
    cwd: String,
    seen: HashMap<String, Value>,
    prior_tokens: i64,
    mcp_names: HashMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Outcome {
    Completed,
    Failed(String),
    Interrupted,
}

impl Outcome {
    pub(crate) fn status(&self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::Failed(_) => "failed",
            Self::Interrupted => "interrupted",
        }
    }
}

impl TurnProjection {
    pub(crate) fn new(
        thread: String,
        turn: String,
        message: String,
        client_id: String,
        cwd: String,
        prior_tokens: i64,
    ) -> crate::error::Result<Self> {
        let registry = crate::mcp_server::build_default_registry();
        let descriptors = registry.descriptors_for_role(calm_types::model::CardRole::Planner);
        let mcp_names = mcp_name_map(descriptors.iter().map(|d| d.name.as_str()))?;
        Ok(Self {
            thread,
            turn,
            message,
            client_id,
            cwd,
            seen: HashMap::new(),
            prior_tokens,
            mcp_names,
        })
    }
    pub(crate) fn started(&self) -> Notification {
        Notification::TurnStarted {
            thread_id: self.thread.clone(),
            turn: json!({"id":self.turn,"status":"inProgress","error":null}),
        }
    }
    pub(crate) fn terminal_value(&self, outcome: &Outcome) -> Value {
        let error = match outcome {
            Outcome::Failed(message) => json!({"message":message}),
            _ => Value::Null,
        };
        json!({"id":self.turn,"status":outcome.status(),"error":error})
    }
    pub(crate) fn completed(&self, outcome: &Outcome) -> Notification {
        Notification::TurnCompleted {
            thread_id: self.thread.clone(),
            turn: self.terminal_value(outcome),
        }
    }
    pub(crate) fn snapshot(&mut self, messages: &[Value]) -> Vec<Notification> {
        let mut notifications = Vec::new();
        for message in messages {
            let info = &message["info"];
            let ours = info["id"].as_str() == Some(&self.message);
            let assistant = info["role"].as_str() == Some("assistant")
                && info["parentID"].as_str() == Some(&self.message);
            if !ours && !assistant {
                continue;
            }
            if ours {
                let content = message["parts"]
                    .as_array()
                    .map(|p| {
                        p.iter()
                            .filter(|p| p["type"].as_str() == Some("text"))
                            .map(|p| json!({"type":"text","text":p["text"]}))
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();
                let item = json!({"id":self.message,"type":"userMessage","clientId":self.client_id,"content":content});
                self.emit(
                    item,
                    true,
                    info["time"]["created"].as_i64().unwrap_or(0),
                    &mut notifications,
                );
            }
            if assistant && let Some(parts) = message["parts"].as_array() {
                for part in parts {
                    let Some(id) = part["id"].as_str() else {
                        continue;
                    };
                    let at = part["time"]["end"]
                        .as_i64()
                        .or(part["time"]["start"].as_i64())
                        .unwrap_or_else(|| chrono::Utc::now().timestamp_millis());
                    let (item, finished) = match part["type"].as_str() {
                        Some("text") => (
                            json!({"id":id,"type":"agentMessage","text":part["text"]}),
                            part["time"]["end"].is_number()
                                || info["time"]["completed"].is_number(),
                        ),
                        Some("reasoning") => (
                            json!({"id":id,"type":"reasoning","summary":[part["text"]],"content":[]}),
                            part["time"]["end"].is_number()
                                || info["time"]["completed"].is_number(),
                        ),
                        Some("tool") => self.tool(part),
                        Some("step-start" | "step-finish" | "snapshot") => continue,
                        _ => (
                            json!({"id":id,"type":"dynamicToolCall","tool":"opencode.nativePart","arguments":part,"status":"completed","result":{"content":[]}}),
                            true,
                        ),
                    };
                    self.emit(item, finished, at, &mut notifications);
                }
            }
        }
        notifications
    }
    fn emit(&mut self, item: Value, finished: bool, at: i64, out: &mut Vec<Notification>) {
        let Some(id) = item["id"].as_str() else {
            return;
        };
        let compared = json!({"finished":finished,"item":item});
        if self.seen.get(id) == Some(&compared) {
            return;
        }
        self.seen.insert(id.into(), compared);
        let method = if finished {
            "item/completed"
        } else {
            "item/started"
        };
        let mut params = json!({"threadId":self.thread,"turnId":self.turn,"item":item});
        params[if finished {
            "completedAtMs"
        } else {
            "startedAtMs"
        }] = json!(at);
        out.push(Notification::Item {
            method: method.into(),
            params,
        });
    }
    fn tool(&self, part: &Value) -> (Value, bool) {
        let state = &part["state"];
        let native_status = state["status"].as_str().unwrap_or("pending");
        let finished = matches!(native_status, "completed" | "error");
        let status = match native_status {
            "completed" => "completed",
            "error" => "failed",
            _ => "inProgress",
        };
        let tool = part["tool"].as_str().unwrap_or("opencode.unknownTool");
        let mut item = if tool == "bash" {
            json!({"id":part["id"],"type":"commandExecution","command":state["input"]["command"],"cwd":self.cwd,"status":status,"aggregatedOutput":state["output"],"exitCode":state["metadata"]["exit"]})
        } else if let Some(name) = self.mcp_names.get(tool) {
            json!({"id":part["id"],"type":"mcpToolCall","server":"calm","tool":name,"arguments":state["input"],"status":status,"result":{"content":[{"type":"text","text":state["output"]}]}})
        } else {
            json!({"id":part["id"],"type":"dynamicToolCall","tool":tool,"arguments":state["input"],"status":status,"result":{"content":[{"type":"text","text":state["output"]}]}})
        };
        if native_status == "error" {
            item["error"] = json!({"message":state["error"]});
        }
        if let (Some(start), Some(end)) = (
            state["time"]["start"].as_i64(),
            state["time"]["end"].as_i64(),
        ) {
            item["durationMs"] = json!(end.saturating_sub(start));
        }
        (item, finished)
    }
    pub(crate) fn outcome(&self, messages: &[Value]) -> Option<Outcome> {
        let assistants: Vec<&Value> = messages
            .iter()
            .filter(|m| {
                m["info"]["role"].as_str() == Some("assistant")
                    && m["info"]["parentID"].as_str() == Some(&self.message)
            })
            .collect();
        if assistants.iter().any(|m| {
            m["parts"].as_array().is_some_and(|parts| {
                parts.iter().any(|p| {
                    p["type"].as_str() == Some("tool")
                        && !matches!(p["state"]["status"].as_str(), Some("completed" | "error"))
                })
            })
        }) {
            return None;
        }
        let latest = assistants
            .into_iter()
            .max_by_key(|m| m["info"]["time"]["created"].as_i64().unwrap_or(0))?;
        let info = &latest["info"];
        if !info["time"]["completed"].is_number() {
            return None;
        }
        if let Some(error) = info.get("error").filter(|e| !e.is_null()) {
            if error["name"].as_str() == Some("MessageAbortedError") {
                return Some(Outcome::Interrupted);
            }
            return Some(Outcome::Failed(
                error["data"]["message"]
                    .as_str()
                    .unwrap_or("OpenCode reported a native error")
                    .into(),
            ));
        }
        match info["finish"].as_str() {
            Some("stop") => Some(Outcome::Completed),
            Some("tool-calls" | "unknown") | None => None,
            Some(reason) => Some(Outcome::Failed(format!("OpenCode finished with {reason}"))),
        }
    }
    pub(crate) fn usage(&self, messages: &[Value]) -> Notification {
        let assistants: Vec<&Value> = messages
            .iter()
            .filter(|m| {
                m["info"]["role"].as_str() == Some("assistant")
                    && m["info"]["parentID"].as_str() == Some(&self.message)
            })
            .collect();
        let total = assistants
            .iter()
            .fold(0i64, |sum, m| sum.saturating_add(message_tokens(m)));
        let last = assistants
            .into_iter()
            .max_by_key(|m| {
                (
                    m["info"]["time"]["created"].as_i64().unwrap_or(0),
                    m["info"]["id"].as_str().unwrap_or(""),
                )
            })
            .map(message_tokens)
            .unwrap_or(0);
        Notification::Other {
            method: "thread/tokenUsage/updated".into(),
            params: json!({"threadId":self.thread,"tokenUsage":{"total":{"totalTokens":self.prior_tokens.saturating_add(total)},"last":{"totalTokens":last},"modelContextWindow":null}}),
        }
    }
}

/// Pinned OpenCode sanitizes both MCP client/tool names. Reverse only the authoritative
/// registered names and reject collisions rather than guessing which underscores were dots.
pub(crate) fn mcp_name_map<'a>(
    names: impl Iterator<Item = &'a str>,
) -> crate::error::Result<HashMap<String, String>> {
    let mut map = HashMap::new();
    for name in names {
        let sanitized: String = name
            .chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() || c == '_' || c == '-' {
                    c
                } else {
                    '_'
                }
            })
            .collect();
        if let Some(prior) = map.insert(format!("calm_{sanitized}"), name.to_owned()) {
            return Err(crate::error::CalmError::Conflict(format!(
                "OpenCode MCP name collision: {prior} and {name}"
            )));
        }
    }
    Ok(map)
}

fn message_tokens(message: &Value) -> i64 {
    let tokens = &message["info"]["tokens"];
    [
        tokens["input"].as_i64(),
        tokens["output"].as_i64(),
        tokens["reasoning"].as_i64(),
        tokens["cache"]["read"].as_i64(),
        tokens["cache"]["write"].as_i64(),
    ]
    .into_iter()
    .flatten()
    .fold(0i64, i64::saturating_add)
}
