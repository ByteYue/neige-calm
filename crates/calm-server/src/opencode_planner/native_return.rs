//! A positive synchronous loop return can close a borrowed turn across the pinned
//! native auto-compaction bridge. Unmarked replay and foreign user input stay fenced.
use super::translate::Outcome;
use calm_truth::opencode_submission::OpenCodeSubmission;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ReturnAnchor {
    pub(crate) assistant_id: String,
    pub(crate) internal_user_ids: Vec<String>,
}

pub(crate) fn validate(
    submission: &OpenCodeSubmission,
    messages: &[Value],
    returned: &Value,
) -> Option<(ReturnAnchor, Outcome)> {
    let info = &returned["info"];
    let id = info["id"].as_str()?;
    if info["role"].as_str() != Some("assistant")
        || info["sessionID"].as_str() != Some(&submission.native_session_id)
        || !info["time"]["completed"].is_number()
    {
        return None;
    }
    let first = messages
        .iter()
        .position(|m| m["info"]["id"].as_str() == Some(&submission.native_message_id))?;
    let last = messages
        .iter()
        .position(|m| m["info"]["id"].as_str() == Some(id))?;
    if last <= first || &messages[last] != returned {
        return None;
    }
    let original = &messages[first];
    if original["info"]["role"].as_str() != Some("user") {
        return None;
    }
    let actual: Vec<_> = original["parts"]
        .as_array()?
        .iter()
        .filter(|p| p["type"] == "text")
        .map(|p| (&p["id"], &p["text"]))
        .collect();
    let expected: Vec<_> = submission.input_json["parts"]
        .as_array()?
        .iter()
        .map(|p| (&p["id"], &p["text"]))
        .collect();
    if actual != expected {
        return None;
    }
    let window = &messages[first + 1..=last];
    let users: Vec<_> = window
        .iter()
        .filter(|m| m["info"]["role"] == "user")
        .collect();
    let mut internal_user_ids = Vec::new();
    let parent = if users.is_empty() {
        submission.native_message_id.as_str()
    } else {
        if users.len() != 2 {
            return None;
        }
        let compaction = users[0];
        let continuation = users[1];
        let compaction_id = compaction["info"]["id"].as_str()?;
        let continuation_id = continuation["info"]["id"].as_str()?;
        let parts = compaction["parts"].as_array()?;
        if parts.len() != 1 || parts[0]["type"] != "compaction" || parts[0]["auto"] != true {
            return None;
        }
        let parts = continuation["parts"].as_array()?;
        if parts.len() != 1
            || parts[0]["type"] != "text"
            || parts[0]["synthetic"] != true
            || parts[0]["metadata"]["compaction_continue"] != true
        {
            return None;
        }
        let bridge: Vec<_> = window
            .iter()
            .filter(|m| m["info"]["parentID"].as_str() == Some(compaction_id))
            .collect();
        if bridge.len() != 1
            || bridge[0]["info"]["summary"] != true
            || bridge[0]["info"]["mode"] != "compaction"
            || bridge[0]["info"]["finish"] != "stop"
            || !bridge[0]["info"]["time"]["completed"].is_number()
            || window
                .iter()
                .position(|m| m["info"]["id"] == compaction["info"]["id"])?
                >= window
                    .iter()
                    .position(|m| m["info"]["id"] == bridge[0]["info"]["id"])?
            || window
                .iter()
                .position(|m| m["info"]["id"] == bridge[0]["info"]["id"])?
                >= window
                    .iter()
                    .position(|m| m["info"]["id"] == continuation["info"]["id"])?
        {
            return None;
        }
        internal_user_ids.extend([compaction_id.to_owned(), continuation_id.to_owned()]);
        continuation_id
    };
    if info["parentID"].as_str() != Some(parent) {
        return None;
    }
    let mut tool_error = None;
    for message in window.iter().filter(|m| m["info"]["role"] == "assistant") {
        let parent = message["info"]["parentID"].as_str()?;
        if parent != submission.native_message_id
            && !internal_user_ids.iter().any(|id| id == parent)
        {
            return None;
        }
        for part in message["parts"].as_array()? {
            if part["type"] == "tool" {
                match part["state"]["status"].as_str()? {
                    "completed" => {}
                    "error" => {
                        tool_error = Some(
                            part["state"]["error"]
                                .as_str()
                                .unwrap_or("OpenCode native tool failed")
                                .to_owned(),
                        )
                    }
                    _ => return None,
                }
            }
        }
    }
    let outcome = if let Some(error) = info.get("error").filter(|e| !e.is_null()) {
        Outcome::Failed(
            error["data"]["message"]
                .as_str()
                .unwrap_or("OpenCode native loop failed")
                .into(),
        )
    } else {
        match info["finish"].as_str()? {
            "stop" => Outcome::Completed,
            "tool-calls" => Outcome::Failed(tool_error?),
            _ => return None,
        }
    };
    Some((
        ReturnAnchor {
            assistant_id: id.into(),
            internal_user_ids,
        },
        outcome,
    ))
}

/// Without a synchronous anchor, only the original uninterrupted user turn can
/// provide terminal evidence. Compaction and concurrent input cannot be guessed.
pub(crate) fn uninterrupted_original(submission: &OpenCodeSubmission, messages: &[Value]) -> bool {
    let Some(first) = messages
        .iter()
        .position(|m| m["info"]["id"].as_str() == Some(&submission.native_message_id))
    else {
        return false;
    };
    !messages[first + 1..]
        .iter()
        .any(|m| m["info"]["role"] == "user")
}
