//! Provider capabilities are exercised through the existing production REST router.
use super::*;

async fn opencode() -> Boot {
    boot_with_provider(TrackWorkspaceKind::Managed, "opencode").await
}

async fn request(
    b: &Boot,
    method: &str,
    suffix: &str,
    payload: Option<Value>,
) -> (StatusCode, Value) {
    let response = b
        .app
        .clone()
        .oneshot(
            Request::builder()
                .method(method)
                .uri(format!("/api/cards/{}/planner/{suffix}", b.planner_card.id))
                .header(header::CONTENT_TYPE, "application/json")
                .header("Idempotency-Key", "opencode-provider-capability")
                .body(payload.map_or_else(Body::empty, |value| Body::from(value.to_string())))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&bytes).unwrap())
}

#[tokio::test]
async fn opencode_run_declares_text_only_input() {
    let b = opencode().await;
    let (status, run) = request(&b, "GET", "run", None).await;
    assert_eq!(status, StatusCode::OK, "{run}");
    assert_eq!(run["attachments_supported"], false, "{run}");
}

#[tokio::test]
async fn opencode_upload_refuses_before_storing_image_bytes() {
    let b = opencode().await;
    let (status, body) = upload(
        &b.app,
        b.planner_card.id.as_str(),
        Some("user"),
        "image/png",
        png(b"refused"),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert!(
        body["error"].as_str().unwrap().contains("text input only"),
        "{body}"
    );
    assert!(
        !b.attachments_dir().exists(),
        "refused upload must not create attachment storage"
    );
}

#[tokio::test]
async fn opencode_image_input_refuses_before_harness_or_queue_admission() {
    let b = opencode().await;
    let (status, body) = request(&b, "POST", "input", Some(json!({"text":"image request", "attachments":["0189bc3f-2b1a-4c7d-9e4f-1a2b3c4d5e04.png"]}))).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert!(
        body["error"].as_str().unwrap().contains("text input only"),
        "{body}"
    );
    assert!(
        b.repo
            .session_projection_active_for_card(&b.planner_card.id.to_string())
            .await
            .unwrap()
            .is_none()
    );
    let (status, run) = request(&b, "GET", "run", None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        run["pending"],
        json!([]),
        "refused attachment must not enter the pending queue"
    );
    assert!(!b.attachments_dir().exists());
}

#[tokio::test]
async fn opencode_historical_attachment_bytes_remain_readable() {
    let b = opencode().await;
    let locks = Default::default();
    let stored = calm_server::planner_attachments::store::store_upload(
        &b.attachments_dir(),
        &b.workspace,
        &b.planner_card.id,
        &locks,
        Duration::from_secs(5),
        Body::from(png(b"historical image")),
    )
    .await
    .unwrap();
    let (status, _, bytes) =
        read_back(&b.app, b.planner_card.id.as_str(), stored.id.as_str()).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(bytes, png(b"historical image"));
}
