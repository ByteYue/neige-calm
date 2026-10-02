use calm_server::event::{BroadcastEnvelope, Event, EventScope};
use serde_json::{Value, json};

pub(super) enum ItemEventScope {
    BoundCard,
    System,
}

pub(super) enum ItemSelection<'a> {
    All,
    NativeUuid(&'a str),
}

pub(super) fn assert_item_announcements(
    events: &mut tokio::sync::broadcast::Receiver<BroadcastEnvelope>,
    rows: &Value,
    card: &str,
    track: &str,
    expected_scope: ItemEventScope,
    selection: ItemSelection<'_>,
) {
    let rows = rows.as_array().unwrap();
    let mut announcements = Vec::new();
    while let Ok(envelope) = events.try_recv() {
        if let Event::HarnessItemAdded {
            item_db_id,
            card_id,
            track_id,
            item_uuid,
            item_type,
            turn_id,
            method,
            ..
        } = envelope.event
        {
            if let ItemSelection::NativeUuid(id) = selection
                && item_uuid.as_deref() != Some(id)
            {
                continue;
            }
            announcements.push((
                item_db_id,
                card_id,
                track_id,
                item_uuid,
                item_type,
                turn_id,
                method,
                envelope.id,
                envelope.scope,
            ));
        }
    }
    let mut announced_rows: Vec<_> = announcements.iter().map(|(id, ..)| *id).collect();
    let mut persisted_rows: Vec<_> = rows.iter().map(|row| row["id"].as_i64().unwrap()).collect();
    announced_rows.sort_unstable();
    persisted_rows.sort_unstable();
    assert_eq!(
        announced_rows, persisted_rows,
        "each persisted item is announced once, including after an unchanged native snapshot"
    );
    for (id, card_id, track_id, item_uuid, item_type, turn_id, method, event_id, scope) in
        announcements
    {
        assert_eq!(card_id.as_str(), card);
        assert_eq!(track_id.as_str(), track);
        assert!(event_id > 0, "the broadcast must have a durable event row");
        let row = rows
            .iter()
            .find(|row| row["id"] == id)
            .expect("the announced item is persisted");
        assert_eq!(row["item_uuid"], json!(item_uuid));
        assert_eq!(row["item_type"], json!(item_type));
        assert_eq!(row["turn_id"], json!(turn_id));
        assert_eq!(row["method"], method);
        assert!(match expected_scope {
            ItemEventScope::BoundCard => matches!(scope, EventScope::Card {
                card: scoped_card, track: scoped_track, ..
            } if scoped_card.as_str() == card && scoped_track.as_str() == track),
            ItemEventScope::System => scope == EventScope::System,
        });
    }
}
