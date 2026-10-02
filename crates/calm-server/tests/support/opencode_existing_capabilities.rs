use super::*;

#[tokio::test]
async fn native_empty_session_preserves_explicit_agent_and_variant_on_first_input() {
    let fixture = Fixture::new().await;
    fixture.native.0.lock().unwrap().messages.clear();
    let stack = Stack::boot(&fixture).await;
    let track = stack.track(&fixture).await;
    let card = stack.attach(&track, "empty").await;
    stack.wait_submit(&card).await;
    assert_eq!(
        stack.run(&card).await["attached_session"]["model"],
        "fixture/original-model"
    );
    let (status, body) = stack
        .request(
            "POST",
            &format!("/api/cards/{card}/planner/input"),
            Some(json!({"text":"first audit"})),
            None,
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    stack.wait_text(&card, "fresh audit progress").await;
    let posts = fixture.posts();
    assert_eq!(posts.len(), 1);
    assert_eq!(posts[0]["input"]["agent"], "audit");
    assert_eq!(posts[0]["input"]["variant"], "max");
    assert_eq!(
        posts[0]["input"]["model"],
        json!({"providerID":"fixture","modelID":"original-model"})
    );
    assert!(posts[0]["input"].get("system").is_none());
    stack.shutdown().await;
}

#[tokio::test]
async fn attached_watchdog_preserves_running_turn_and_accepts_later_native_completion() {
    let fixture = Fixture::new().await;
    fixture.native.0.lock().unwrap().hold = true;
    let stack = Stack::boot(&fixture).await;
    let track = stack.track(&fixture).await;
    let card = stack.attach(&track, "watchdog").await;
    stack.wait_submit(&card).await;
    let (status, body) = stack
        .request(
            "POST",
            &format!("/api/cards/{card}/planner/input"),
            Some(json!({"text":"long operational ETL"})),
            None,
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    stack.wait_text(&card, "fresh audit progress").await;
    let run = stack.run(&card).await;
    assert_eq!(run["phase"], "turn_running");
    let harness = stack
        .state
        .harness
        .get(&run["worker_session_id"].as_str().unwrap().to_owned())
        .unwrap();
    harness.expire_turn_duration_for_test().await.unwrap();
    let snapshot = harness.snapshot().await;
    assert_eq!(
        snapshot.phase,
        calm_server::harness::HarnessPhaseTag::TurnRunning
    );
    assert!(snapshot.interruption_intent.is_none());
    assert_eq!(
        fixture.posts().len(),
        1,
        "watchdog must not control native execution"
    );
    {
        let mut native = fixture.native.0.lock().unwrap();
        native.busy = false;
        let last = native.messages.last_mut().unwrap();
        last["info"]["finish"] = json!("stop");
        last["info"]["time"]["completed"] = json!(100);
        last["parts"][0]["time"]["end"] = json!(100);
    }
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while stack.journals().await != vec![(SESSION.into(), "completed".into())] {
        assert!(
            tokio::time::Instant::now() < deadline,
            "{}",
            stack.run(&card).await
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    stack.wait_submit(&card).await;
    assert_eq!(fixture.posts().len(), 1);
    stack.shutdown().await;
}
