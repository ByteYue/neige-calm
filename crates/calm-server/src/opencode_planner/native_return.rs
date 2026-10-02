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
    pub(crate) assistant_ids: Vec<String>,
    pub(crate) internal_user_ids: Vec<String>,
}

fn original<'a>(submission: &OpenCodeSubmission, messages: &'a [Value]) -> Option<&'a Value> {
    if submission.input_json["messageID"].as_str() != Some(&submission.native_message_id) {
        return None;
    }
    let matching: Vec<_> = messages
        .iter()
        .filter(|m| m["info"]["id"].as_str() == Some(&submission.native_message_id))
        .collect();
    if matching.len() != 1 {
        return None;
    }
    let original = matching[0];
    if original["info"]["role"] != "user"
        || original["info"]["sessionID"].as_str() != Some(&submission.native_session_id)
    {
        return None;
    }
    let actual = original["parts"].as_array()?;
    let expected = submission.input_json["parts"].as_array()?;
    if actual.len() != expected.len() || actual.iter().zip(expected).any(|(actual, expected)| {
        actual["type"] != "text" || actual["type"] != expected["type"] || actual["text"] != expected["text"]
            || actual["text"].as_str().is_none() || actual["id"].as_str().is_none()
            // The native protocol mints omitted part IDs. Explicit client IDs stay exact.
            || expected.get("id").is_some_and(|id| actual.get("id") != Some(id))
    }) { return None; }
    Some(original)
}

pub(crate) fn validate(
    submission: &OpenCodeSubmission,
    messages: &[Value],
    returned: &Value,
) -> Option<(ReturnAnchor, Outcome)> {
    let info = &returned["info"];
    let id = info["id"].as_str()?;
    if info["role"] != "assistant"
        || info["sessionID"].as_str() != Some(&submission.native_session_id)
        || !info["time"]["completed"].is_number()
    {
        return None;
    }
    let original = original(submission, messages)?;
    let start = original["info"]["time"]["created"].as_i64()?;
    let end = info["time"]["created"].as_i64()?;
    if end < start
        || messages
            .iter()
            .filter(|m| m["info"]["id"].as_str() == Some(id))
            .count()
            != 1
        || messages
            .iter()
            .find(|m| m["info"]["id"].as_str() == Some(id))?
            != returned
    {
        return None;
    }
    // UUID client IDs are unrelated to native ascending IDs. Include the entire
    // admitted timestamp bucket, including assistants/users sorting before the UUID.
    let window: Vec<_> = messages
        .iter()
        .filter(|m| {
            m["info"]["id"] != original["info"]["id"]
                && m["info"]["time"]["created"]
                    .as_i64()
                    .is_some_and(|at| at >= start && at <= end)
        })
        .collect();
    let users: Vec<_> = window
        .iter()
        .copied()
        .filter(|m| m["info"]["role"] == "user")
        .collect();
    let mut internal_user_ids = Vec::new();
    let parent = if users.is_empty() {
        submission.native_message_id.as_str()
    } else {
        if users.len() != 2 {
            return None;
        }
        let compaction = users.iter().copied().find(|m| {
            m["parts"].as_array().is_some_and(|parts| {
                parts.len() == 1 && parts[0]["type"] == "compaction" && parts[0]["auto"] == true
            })
        })?;
        let continuation = users
            .iter()
            .copied()
            .find(|m| m["info"]["id"] != compaction["info"]["id"])?;
        let compaction_id = compaction["info"]["id"].as_str()?;
        let continuation_id = continuation["info"]["id"].as_str()?;
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
            .copied()
            .filter(|m| m["info"]["parentID"].as_str() == Some(compaction_id))
            .collect();
        if bridge.len() != 1 || bridge[0]["info"]["summary"] != true
            || bridge[0]["info"]["mode"] != "compaction" || bridge[0]["info"]["finish"] != "stop"
            || !bridge[0]["info"]["time"]["completed"].is_number()
            // These two native transitions have no parent link. A timestamp tie
            // cannot establish their causal order, even if sorted IDs look plausible.
            || start >= compaction["info"]["time"]["created"].as_i64()?
            || bridge[0]["info"]["time"]["created"].as_i64()? >= continuation["info"]["time"]["created"].as_i64()?
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
    let mut assistant_ids = Vec::new();
    for message in window {
        if message["info"]["role"] == "user" {
            continue;
        }
        if message["info"]["role"] != "assistant"
            || !message["info"]["time"]["completed"].is_number()
        {
            return None;
        }
        let parent = message["info"]["parentID"].as_str()?;
        if parent != submission.native_message_id
            && !internal_user_ids.iter().any(|id| id == parent)
        {
            return None;
        }
        assistant_ids.push(message["info"]["id"].as_str()?.to_owned());
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
    let outcome = if let Some(outcome) = Outcome::native_error(info) {
        outcome
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
            assistant_ids,
            internal_user_ids,
        },
        outcome,
    ))
}

/// Without a synchronous anchor, only the original uninterrupted user turn can
/// provide terminal evidence. Include equal-time users regardless of sorted ID.
pub(crate) fn uninterrupted_original(submission: &OpenCodeSubmission, messages: &[Value]) -> bool {
    let Some(original) = original(submission, messages) else {
        return false;
    };
    let Some(start) = original["info"]["time"]["created"].as_i64() else {
        return false;
    };
    !messages.iter().any(|m| {
        m["info"]["role"] == "user"
            && m["info"]["id"] != original["info"]["id"]
            && m["info"]["time"]["created"]
                .as_i64()
                .is_some_and(|at| at >= start)
    })
}
