//! Item persistence shared by live Harness notifications and passive provider history.
//! Callers retain notification admission, state transitions, and snapshot reconciliation.
use super::{HarnessPhaseTag, HarnessSnapshot};
use crate::{
    db::Repo,
    error::Result,
    event::{Event, EventBus, EventScope},
    ids::ActorId,
    state::WriteContext,
};

/// Native settlement does not retire the kernel consumer's queued item frames.
/// A correlated live turn retains its transcript ownership through an uncertain wedge;
/// only the durable idle/completed checkpoint releases it to passive reconciliation.
pub(crate) fn live_turn_pending(
    snapshot: &HarnessSnapshot,
    active_turn_id: Option<&str>,
    turn_id: &str,
    client_id: &str,
) -> bool {
    active_turn_id == Some(turn_id)
        || (!matches!(
            snapshot.phase,
            HarnessPhaseTag::Idle | HarnessPhaseTag::TurnCompleted
        ) && (snapshot.last_turn_id.as_deref() == Some(turn_id)
            || snapshot.projection_client_id.as_ref().map(|id| id.as_str()) == Some(client_id)))
}

pub(crate) struct TranscriptOwner<'a> {
    pub repo: &'a dyn Repo,
    pub events: &'a EventBus,
    pub write: WriteContext,
    pub worker_session_id: &'a str,
    pub card_id: &'a str,
    pub track_id: &'a str,
}

pub(crate) struct ItemMetadata<'a> {
    pub turn_id: Option<&'a str>,
    pub item_uuid: Option<&'a str>,
    pub item_type: Option<&'a str>,
    pub method: &'a str,
}

pub(crate) struct TranscriptItem<'a> {
    pub thread_id: &'a str,
    pub metadata: ItemMetadata<'a>,
    pub params_json: &'a str,
    /// Only a live notification for a turn issued by an older binary supplies this.
    pub legacy_segments_json: Option<&'a str>,
    /// The provider echo's correlation with the already-persisted user projection.
    pub projection_client_id: Option<&'a str>,
}

pub(crate) fn is_user_message_type(item_type: Option<&str>) -> bool {
    matches!(item_type, Some("userMessage" | "user_message"))
}

impl TranscriptOwner<'_> {
    pub(crate) async fn record(&self, item: &TranscriptItem<'_>) -> Result<i64> {
        let metadata = &item.metadata;
        if is_user_message_type(metadata.item_type)
            && metadata.method == "item/completed"
            && let (Some(client_id), Some(item_uuid)) =
                (item.projection_client_id, metadata.item_uuid)
            && let Some(row_id) = self
                .repo
                .transcript_projection_upgrade(
                    self.card_id,
                    client_id,
                    metadata.turn_id,
                    item_uuid,
                    item.params_json,
                )
                .await?
        {
            return Ok(row_id);
        }
        Ok(self
            .repo
            .harness_item_insert(
                self.worker_session_id,
                self.card_id,
                self.track_id,
                item.thread_id,
                metadata.turn_id,
                metadata.item_uuid,
                metadata.item_type,
                metadata.method,
                item.params_json,
                item.legacy_segments_json,
            )
            .await?)
    }

    /// Scope resolution stays with the caller: live notifications use their runtime's
    /// cached scope, while passive history checks that its bound track still exists.
    #[allow(deprecated)]
    pub(crate) async fn announce(
        &self,
        scope: EventScope,
        item_db_id: i64,
        metadata: &ItemMetadata<'_>,
    ) -> Result<()> {
        self.repo
            .log_pure_event(
                ActorId::Kernel,
                scope,
                None,
                self.events,
                self.write.role_cache(),
                self.write.area_cache(),
                Event::HarnessItemAdded {
                    worker_session_id: self.worker_session_id.into(),
                    card_id: self.card_id.into(),
                    track_id: self.track_id.into(),
                    item_db_id,
                    item_uuid: metadata.item_uuid.map(str::to_owned),
                    item_type: metadata.item_type.map(str::to_owned),
                    turn_id: metadata.turn_id.map(str::to_owned),
                    method: metadata.method.into(),
                },
            )
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn durable_kernel_turn_ownership_survives_native_settlement_and_wedge() {
        let mut snapshot = HarnessSnapshot::initial(0, vec![]);
        snapshot.last_turn_id = Some("owned-turn".into());
        snapshot.phase = HarnessPhaseTag::TurnRunning;
        assert!(live_turn_pending(&snapshot, None, "owned-turn", "client"));
        assert!(!live_turn_pending(&snapshot, None, "other-turn", "client"));
        snapshot.phase = HarnessPhaseTag::Wedged;
        assert!(live_turn_pending(&snapshot, None, "owned-turn", "client"));
        snapshot.phase = HarnessPhaseTag::TurnCompleted;
        assert!(!live_turn_pending(&snapshot, None, "owned-turn", "client"));
        snapshot.phase = HarnessPhaseTag::Idle;
        assert!(!live_turn_pending(&snapshot, None, "owned-turn", "client"));
        assert!(live_turn_pending(
            &snapshot,
            Some("owned-turn"),
            "owned-turn",
            "client"
        ));
    }

    #[test]
    fn issuing_projection_retains_only_its_exact_client_until_kernel_retirement() {
        let mut snapshot = HarnessSnapshot::initial(0, vec![]);
        snapshot.last_turn_id = Some("previous-turn".into());
        snapshot.phase = HarnessPhaseTag::IssuingTurn;
        snapshot.projection_client_id = Some(super::super::QueueEntryId::from_wire(
            "queued-client".into(),
        ));
        assert!(live_turn_pending(
            &snapshot,
            None,
            "new-turn",
            "queued-client"
        ));
        assert!(!live_turn_pending(
            &snapshot,
            None,
            "new-turn",
            "different-client"
        ));
        snapshot.phase = HarnessPhaseTag::TurnCompleted;
        assert!(!live_turn_pending(
            &snapshot,
            None,
            "new-turn",
            "queued-client"
        ));
    }
}
