//! The two notification sources of a track (#1829): an ask (the Planner moved the track to
//! `blocked`, or called `calm.user.notify`) and planner down (the Planner's newest finished turn
//! failed). A pure function of the rows `sql::notification_rows` read; nothing else is an item.

use serde::{Deserialize, Serialize};

/// What an item is: `ask` folds to `attention = input`, `planner_down` to `attention = failed`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NotificationSource {
    Ask,
    PlannerDown,
}

/// One thing addressed to the user and not yet handled. `key` carries the evidence row's id, so the
/// same source happening again is a new key; `text` is the kernel's words for it, shown verbatim.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActivityItem {
    pub source: NotificationSource,
    pub key: String,
    pub text: String,
    pub at_ms: i64,
}

/// N1 — the track's newest lifecycle edge into `blocked`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockedEdge {
    pub event_id: i64,
    pub at_ms: i64,
    /// `payload.agent_message`; every Planner write carries one.
    pub message: Option<String>,
}

/// N3, notify arm — one successful `calm.user.notify` call of the Planner card.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NotifyRow {
    pub row_id: i64,
    pub at_ms: i64,
    /// `$.item.arguments.text`; the tool refuses a call without it.
    pub text: Option<String>,
}

/// N3, turn arm — the Planner card's newest `turn/completed` row that is not `interrupted`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LastTurn {
    pub row_id: i64,
    pub at_ms: i64,
    pub status: Option<String>,
    /// `$.error.message`.
    pub error_message: Option<String>,
}

/// Everything the two sources read. A track without a Planner card has no notify rows, no last
/// turn and no U.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct NotificationRows {
    pub blocked_edge: Option<BlockedEdge>,
    /// L — the newest edge OUT of `blocked`, by any actor.
    pub left_blocked_at: Option<i64>,
    /// U — the newest user message to the Planner card.
    pub user_sent_at: Option<i64>,
    pub notifies: Vec<NotifyRow>,
    pub last_turn: Option<LastTurn>,
}

/// One item, or none when its required text decoded to NULL: only that item is dropped, with a warn
/// naming its key; the other items and the overlay are written as usual.
fn item(
    track_id: &str,
    source: NotificationSource,
    key: String,
    text: Option<&str>,
    at_ms: i64,
) -> Option<ActivityItem> {
    let Some(text) = text else {
        tracing::warn!(
            track_id = %track_id,
            key = %key,
            "track_activity: notification evidence has no text; item dropped"
        );
        return None;
    };
    Some(ActivityItem {
        source,
        key,
        text: text.to_string(),
        at_ms,
    })
}

/// The open items, newest first then by key. An ask is open while `at_ms > MAX(U, L)`; planner down
/// is open while the newest non-interrupted turn is `failed`. No lifecycle filter: a done or archived
/// track keeps what is still addressed to the user.
pub fn notifications(track_id: &str, rows: &NotificationRows) -> Vec<ActivityItem> {
    let answered = rows.user_sent_at.max(rows.left_blocked_at);
    let open = |at_ms: i64| answered.is_none_or(|closed| at_ms > closed);
    let mut items = Vec::new();

    if let Some(edge) = &rows.blocked_edge
        && open(edge.at_ms)
    {
        items.extend(item(
            track_id,
            NotificationSource::Ask,
            format!("ask:lifecycle:{}", edge.event_id),
            edge.message.as_deref(),
            edge.at_ms,
        ));
    }
    for notify in rows.notifies.iter().filter(|n| open(n.at_ms)) {
        items.extend(item(
            track_id,
            NotificationSource::Ask,
            format!("ask:notify:{}", notify.row_id),
            notify.text.as_deref().map(str::trim),
            notify.at_ms,
        ));
    }
    if let Some(turn) = &rows.last_turn
        && turn.status.as_deref() == Some("failed")
    {
        items.extend(item(
            track_id,
            NotificationSource::PlannerDown,
            format!("planner_down:{}", turn.row_id),
            turn.error_message.as_deref(),
            turn.at_ms,
        ));
    }

    items.sort_by(|a, b| b.at_ms.cmp(&a.at_ms).then_with(|| a.key.cmp(&b.key)));
    items
}
