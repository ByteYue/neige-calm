//! Durable send intent for OpenCode Planner HTTP turns.
//! A Sending or Unknown record is reconciled by reading its native message; it is never resent.

use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OpenCodeSubmissionState {
    Prepared,
    Sending,
    Unknown,
    Completed,
    Failed,
    Interrupted,
}

impl OpenCodeSubmissionState {
    pub fn as_db_str(self) -> &'static str {
        match self {
            Self::Prepared => "prepared",
            Self::Sending => "sending",
            Self::Unknown => "unknown",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Interrupted => "interrupted",
        }
    }

    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Interrupted)
    }
}

impl TryFrom<String> for OpenCodeSubmissionState {
    type Error = String;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        match value.as_str() {
            "prepared" => Ok(Self::Prepared),
            "sending" => Ok(Self::Sending),
            "unknown" => Ok(Self::Unknown),
            "completed" => Ok(Self::Completed),
            "failed" => Ok(Self::Failed),
            "interrupted" => Ok(Self::Interrupted),
            other => Err(format!("unknown OpenCode submission state {other}")),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct OpenCodeSubmissionIntent {
    pub id: String,
    pub worker_session_id: String,
    pub card_id: String,
    pub scope_id: String,
    /// Configuration generation of the owned provider scope.
    pub generation: i64,
    pub thread_id: String,
    pub native_session_id: String,
    pub client_id: String,
    pub native_message_id: String,
    /// The complete native request, including its preassigned message and part IDs.
    pub input_json: Value,
    pub created_at_ms: i64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct OpenCodeSubmission {
    pub id: String,
    pub worker_session_id: String,
    pub card_id: String,
    pub scope_id: String,
    pub generation: i64,
    pub thread_id: String,
    pub native_session_id: String,
    pub client_id: String,
    pub native_message_id: String,
    pub input_json: Value,
    pub input_fingerprint: String,
    pub state: OpenCodeSubmissionState,
    pub outcome_json: Option<Value>,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}
