//! The report snapshot readers serve the payload mirror once a CRDT blob exists (#1859).

use super::{ReportBlock, TrackReportPayload, report_blocks_snapshot, report_blocks_snapshot_tx};
use crate::db::RepoSyncDomainRaw;
use crate::db::sqlite::SqlxRepo;
use crate::error::CalmError;
use crate::model::{NewArea, NewCard, NewTrack, RequestTheme};
use crate::track_report_doc::ReportDoc;
use serde_json::json;

/// A track whose report card holds `payload` and `body_crdt`, written straight to the row.
async fn report_row(payload: &TrackReportPayload, body_crdt: &[u8]) -> (SqlxRepo, String) {
    let repo = SqlxRepo::open("sqlite::memory:").await.unwrap();
    let area = repo
        .area_create(NewArea {
            name: "one".into(),
            color: "#123456".into(),
            sort: None,
        })
        .await
        .unwrap();
    let track = repo
        .track_create(NewTrack {
            area_id: area.id.as_str().into(),
            title: "Report".into(),
            sort: None,
            cwd: "/tmp".into(),
            template_id: None,
            plugin_scope: None,
            template_input: None,
            attach_folder: false,
            theme: RequestTheme::default_dark(),
        })
        .await
        .unwrap();
    let card = repo
        .card_create(NewCard {
            track_id: track.id.as_str().into(),
            kind: "track-report".into(),
            sort: Some(-1.0),
            payload: serde_json::to_value(TrackReportPayload::initial()).unwrap(),
            title: Some("Report".into()),
        })
        .await
        .unwrap();
    sqlx::query("UPDATE cards SET payload=json(?1),body_crdt=?2 WHERE id=?3")
        .bind(serde_json::to_string(payload).unwrap())
        .bind(body_crdt)
        .bind(card.id.as_str())
        .execute(repo.pool())
        .await
        .unwrap();
    (repo, track.id.as_str().to_owned())
}

fn mirrored_payload() -> TrackReportPayload {
    let mut payload = TrackReportPayload::new("mirror summary", "# Mirror\n");
    payload.blocks = Some(vec![
        ReportBlock {
            id: "b_prose".into(),
            kind: "prose".into(),
            rev: 1,
            payload: json!({ "markdown": "# Mirror\n" }),
        },
        ReportBlock {
            id: "b_task".into(),
            kind: "task".into(),
            rev: 2,
            payload: json!({ "key": "t", "kind": "terminal", "goal": "ls" }),
        },
    ]);
    payload
}

/// The blob is unreadable garbage, so any Automerge load on this path turns the read into an
/// error; the mirror is served, with the legacy terminal `goal` normalized like every read side.
#[tokio::test]
async fn snapshot_serves_the_payload_mirror_without_loading_the_crdt() {
    let payload = mirrored_payload();
    let (repo, track_id) = report_row(&payload, &[0]).await;
    let expected = calm_types::report_blocks::tasks::normalize_legacy_terminal_task_blocks(
        payload.blocks.as_deref().unwrap(),
    );
    assert_eq!(expected[1].payload["command"], "ls");

    let pooled = report_blocks_snapshot(repo.pool(), &track_id)
        .await
        .unwrap();
    assert_eq!(pooled, ("mirror summary".to_owned(), expected.clone()));
    let mut tx = repo.pool().begin().await.unwrap();
    let in_tx = report_blocks_snapshot_tx(&mut tx, &track_id).await.unwrap();
    assert_eq!(in_tx, ("mirror summary".to_owned(), expected));
}

/// A blob without a mirror is a row no production writer produces; it is an invariant
/// violation, not a cue to fall back to loading the (here perfectly readable) CRDT.
#[tokio::test]
async fn snapshot_with_a_crdt_but_no_mirror_is_internal() {
    let payload = mirrored_payload();
    let blob = ReportDoc::from_payload(&payload).to_bytes();
    let unmirrored = TrackReportPayload {
        blocks: None,
        ..payload
    };
    let (repo, track_id) = report_row(&unmirrored, &blob).await;

    for error in [
        report_blocks_snapshot(repo.pool(), &track_id)
            .await
            .unwrap_err(),
        report_blocks_snapshot_tx(&mut repo.pool().begin().await.unwrap(), &track_id)
            .await
            .unwrap_err(),
    ] {
        assert!(
            matches!(&error, CalmError::Internal(message) if message.contains("has a CRDT but no block mirror")),
            "got {error:?}"
        );
    }
}
