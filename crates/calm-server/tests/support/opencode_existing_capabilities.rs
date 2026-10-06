use super::*;

#[tokio::test]
async fn attached_model_catalog_refuses_before_managed_process_or_native_writes() {
    use std::os::unix::fs::PermissionsExt;

    let fixture = Fixture::new().await;
    let stack = Stack::boot(&fixture).await;
    let track = stack.track(&fixture).await;
    let card = stack.attach(&track, "native-model-ownership").await;
    stack.wait_submit(&card).await;
    let native_messages = fixture.native.0.lock().unwrap().messages.clone();
    let binary = fixture.path().join("managed-opencode-trap");
    let invoked = fixture.path().join("managed-opencode-invoked");
    std::fs::write(
        &binary,
        format!(
            "#!/bin/sh\nprintf invoked > '{}'\nexit 1\n",
            invoked.display()
        ),
    )
    .unwrap();
    std::fs::set_permissions(&binary, std::fs::Permissions::from_mode(0o700)).unwrap();
    let state = stack.state.clone().with_opencode_planner_config(
        calm_server::opencode_planner::config::OpenCodePlannerConfig {
            opencode_binary: binary,
            opencode_version: "1.18.34".into(),
            config_dir: fixture.path().join("managed-profile"),
        },
    );
    let app = routes::router()
        .layer(axum::middleware::from_fn(
            calm_server::actor::actor_middleware,
        ))
        .layer(axum::middleware::from_fn(owner))
        .with_state(state);
    let models = Stack {
        state: stack.state.clone(),
        app,
    };
    for query in [
        format!("card_id={card}"),
        format!("card_id={card}&provider=opencode"),
    ] {
        let (status, error) = models
            .request("GET", &format!("/api/models?{query}"), None, None)
            .await;
        assert_eq!(status, StatusCode::CONFLICT, "{error}");
        assert_eq!(error["code"], "conflict");
        assert!(
            !invoked.exists(),
            "attached catalog reads must not even check the managed binary"
        );
    }
    assert_eq!(fixture.native.0.lock().unwrap().messages, native_messages);
    assert!(fixture.posts().is_empty());
    assert!(stack.journals().await.is_empty());
    drop(models);
    stack.shutdown().await;
}

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
            Some("first-audit-intent"),
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
            Some("long-etl-intent"),
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

#[tokio::test]
async fn attached_input_key_replays_unknown_intent_after_restart_without_resending() {
    let fixture = Fixture::new().await;
    let stack = Stack::boot(&fixture).await;
    let track = stack.track(&fixture).await;
    let card = stack.attach(&track, "keyed-unknown").await;
    stack.wait_submit(&card).await;
    let path = format!("/api/cards/{card}/planner/input");
    let input = json!({"text":"one keyed operational side effect"});
    let (missing, _) = stack
        .request("POST", &path, Some(input.clone()), None)
        .await;
    assert_eq!(missing, StatusCode::BAD_REQUEST);
    assert!(fixture.posts().is_empty());
    assert!(stack.journals().await.is_empty());
    fixture.native.0.lock().unwrap().lost = true;
    let (accepted, answer) = stack
        .request(
            "POST",
            &path,
            Some(input.clone()),
            Some("one-native-intent"),
        )
        .await;
    assert_eq!(accepted, StatusCode::OK, "{answer}");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    while stack.journals().await != vec![(SESSION.into(), "unknown".into())] {
        assert!(tokio::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(fixture.posts().len(), 1);
    let (replayed, replay) = stack
        .request(
            "POST",
            &path,
            Some(input.clone()),
            Some("one-native-intent"),
        )
        .await;
    assert_eq!(replayed, StatusCode::OK);
    assert_eq!(replay, answer);
    let (changed, body) = stack
        .request(
            "POST",
            &path,
            Some(json!({"text":"a different intent"})),
            Some("one-native-intent"),
        )
        .await;
    assert_eq!(changed, StatusCode::CONFLICT);
    assert_eq!(body["code"], "idempotency_key_reused");
    stack.shutdown().await;
    let reboot = Stack::boot(&fixture).await;
    let (replayed, replay) = reboot
        .request("POST", &path, Some(input), Some("one-native-intent"))
        .await;
    assert_eq!(replayed, StatusCode::OK);
    assert_eq!(replay, answer);
    tokio::time::sleep(Duration::from_millis(700)).await;
    assert_eq!(fixture.posts().len(), 1);
    assert_eq!(
        reboot.journals().await,
        vec![(SESSION.into(), "unknown".into())]
    );
    reboot.shutdown().await;
}

#[tokio::test]
async fn attached_replace_turn_refuses_before_native_history_or_receipts_change() {
    let fixture = Fixture::new().await;
    let stack = Stack::boot(&fixture).await;
    let track = stack.track(&fixture).await;
    let card = stack.attach(&track, "read-only-native-history").await;
    stack.wait_text(&card, "original progress").await;
    stack.wait_submit(&card).await;
    let original_items = stack.items(&card).await;
    let original_messages = fixture.native.0.lock().unwrap().messages.clone();
    let (status, error) = stack.request(
        "POST", &format!("/api/cards/{card}/planner/input"),
        Some(json!({"text":"replace the existing progress", "replaces_turn":"opencode-history-msg_original"})),
        Some("unsupported-native-edit"),
    ).await;
    assert_eq!(status, StatusCode::CONFLICT, "{error}");
    assert_eq!(error["code"], "planner_turn_not_replaceable");
    assert_eq!(stack.items(&card).await, original_items);
    assert_eq!(fixture.native.0.lock().unwrap().messages, original_messages);
    assert!(stack.journals().await.is_empty());
    assert!(fixture.posts().is_empty());
    let key_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM planner_input_idempotency WHERE card_id=?1 AND idempotency_key=?2",
    )
    .bind(&card)
    .bind("unsupported-native-edit")
    .fetch_one(&stack.state.raw_repo().sqlite_pool().unwrap())
    .await
    .unwrap();
    assert_eq!(
        key_count, 0,
        "A refused native edit cannot claim the send key"
    );
    assert_eq!(stack.run(&card).await["phase"], "idle");
    stack.shutdown().await;
}

#[tokio::test]
async fn attached_manual_compaction_refuses_without_native_writes_and_keeps_continuation() {
    let fixture = Fixture::new().await;
    let stack = Stack::boot(&fixture).await;
    let track = stack.track(&fixture).await;
    let card = stack.attach(&track, "compact-ownership").await;
    for (index, (key, text)) in [
        ("before-compact", "first audit"),
        ("after-compact", "second audit"),
    ]
    .into_iter()
    .enumerate()
    {
        stack.wait_submit(&card).await;
        let (status, body) = stack
            .request(
                "POST",
                &format!("/api/cards/{card}/planner/input"),
                Some(json!({"text":text})),
                Some(key),
            )
            .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        while stack.journals().await != vec![(SESSION.into(), "completed".into()); index + 1]
            || stack.run(&card).await["phase"] != "turn_completed"
        {
            assert!(
                tokio::time::Instant::now() < deadline,
                "turn did not settle"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        stack.wait_submit(&card).await;
        if key == "after-compact" {
            break;
        }
        let run = stack.run(&card).await;
        let items = stack.items(&card).await;
        let journal = stack.journals().await;
        let native_messages = fixture.native.0.lock().unwrap().messages.clone();
        let native_writes = fixture.posts();
        let (status, error) = stack
            .request(
                "POST",
                &format!("/api/cards/{card}/planner/compact"),
                None,
                None,
            )
            .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{error}");
        assert_eq!(stack.run(&card).await, run);
        assert_eq!(stack.items(&card).await, items);
        assert_eq!(stack.journals().await, journal);
        assert_eq!(fixture.native.0.lock().unwrap().messages, native_messages);
        assert_eq!(
            fixture.posts(),
            native_writes,
            "no native compact, prompt or abort"
        );
    }
    assert_eq!(
        fixture.posts().len(),
        2,
        "only the two explicit audit prompts"
    );
    stack.shutdown().await;
}

#[tokio::test]
async fn running_foreign_turn_is_observed_but_never_stopped_or_sent_into() {
    let fixture = Fixture::new().await;
    {
        let mut native = fixture.native.0.lock().unwrap();
        native.busy = true;
        native.messages[1] = assistant(
            "msg_reply",
            "msg_original",
            "foreign partial output",
            2,
            false,
        );
    }
    let stack = Stack::boot(&fixture).await;
    let track = stack.track(&fixture).await;
    let card = stack.attach(&track, "attach").await;
    stack.wait_text(&card, "foreign partial output").await;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while stack.run(&card).await["attached_session"]["status"] != "running" {
        assert!(
            tokio::time::Instant::now() < deadline,
            "native status did not settle"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let run = stack.run(&card).await;
    assert_eq!(run["attached_session"]["status"], "running");
    assert_eq!(run["attached_session"]["can_submit"], false);
    let (status, _) = stack
        .request(
            "POST",
            &format!("/api/cards/{card}/planner/input"),
            Some(json!({"text":"do another ETL"})),
            Some("foreign-busy-input"),
        )
        .await;
    assert_eq!(status, StatusCode::CONFLICT);
    for path in [
        format!("/api/cards/{card}/planner/interrupt"),
        format!("/api/cards/{card}/planner/reset"),
    ] {
        let (status, _) = stack.request("POST", &path, Some(json!({})), None).await;
        assert_eq!(status, StatusCode::CONFLICT, "{path}");
    }
    assert!(stack.journals().await.is_empty());
    assert!(fixture.posts().is_empty());
    let (status, body) = stack
        .request("DELETE", &format!("/api/cards/{card}"), None, None)
        .await;
    assert!(status.is_success(), "{status} {body}");
    assert!(fixture.native.0.lock().unwrap().busy);
    assert!(fixture.posts().is_empty());
    stack.shutdown().await;
    assert!(fixture.native.0.lock().unwrap().busy);
    assert!(fixture.posts().is_empty());
}
