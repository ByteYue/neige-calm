//! OpenCode's generated transcript frames normalized at its adapter boundary.
use crate::codex_appserver::Notification;
use provider::events::{ItemPhase, PlannerEvent, PlannerEventKind};

pub(crate) fn from_notification(notification: Notification) -> PlannerEvent {
    let thread_id = notification.thread_id().map(str::to_owned);
    let kind = match notification {
        Notification::TurnStarted { turn, .. } => match turn["id"].as_str() {
            Some(id) => PlannerEventKind::TurnStarted { turn_id: id.into() },
            None => PlannerEventKind::Ignored,
        },
        Notification::TurnCompleted { turn, .. } => PlannerEventKind::TurnCompleted { turn },
        Notification::Item { method, params } => match method.as_str() {
            "item/started" => PlannerEventKind::Item {
                phase: ItemPhase::Started,
                params,
                questions: Vec::new(),
            },
            "item/completed" => PlannerEventKind::Item {
                phase: ItemPhase::Completed,
                params,
                // Native permission and question controls remain with the OpenCode controller.
                questions: Vec::new(),
            },
            _ => PlannerEventKind::Ignored,
        },
        Notification::Other { method, params } if method == "thread/tokenUsage/updated" => {
            PlannerEventKind::TokenUsage { params }
        }
        Notification::Other { method, params } if method == "opencode/submission/unknown" => {
            match (params["turnId"].as_str(), params["message"].as_str()) {
                (Some(turn_id), Some(reason)) => PlannerEventKind::SubmissionUnknown {
                    turn_id: turn_id.into(),
                    reason: reason.into(),
                },
                _ => PlannerEventKind::Ignored,
            }
        }
        Notification::Other { .. }
        | Notification::ThreadStarted { .. }
        | Notification::ThreadStatusChanged { .. } => PlannerEventKind::Ignored,
    };
    PlannerEvent { thread_id, kind }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn unknown_admission_keeps_exact_correlation_and_reason() {
        let event = from_notification(Notification::Other {
            method: "opencode/submission/unknown".into(),
            params: json!({"threadId":"thread","turnId":"turn","message":"readback lost"}),
        });
        assert_eq!(event.thread_id.as_deref(), Some("thread"));
        assert!(
            matches!(event.kind,PlannerEventKind::SubmissionUnknown {turn_id,reason} if turn_id=="turn" && reason=="readback lost")
        );
    }

    #[test]
    fn completed_item_preserves_transcript_envelope() {
        let params = json!({"threadId":"thread","turnId":"turn","item":{"id":"native-part","type":"agentMessage","text":"progress"}});
        let event = from_notification(Notification::Item {
            method: "item/completed".into(),
            params: params.clone(),
        });
        assert_eq!(event.thread_id.as_deref(), Some("thread"));
        assert!(
            matches!(event.kind,PlannerEventKind::Item {phase:ItemPhase::Completed,params:actual,questions} if actual==params && questions.is_empty())
        );
    }
}
