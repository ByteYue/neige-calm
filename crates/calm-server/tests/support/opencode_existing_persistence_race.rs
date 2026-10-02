use super::*;
use calm_server::harness::{
    Observation,
    run_loop::{
        PlannerHarnessObservationRaceHook, install_planner_harness_observation_race_hook_for_test,
    },
};

#[tokio::test]
async fn settled_native_reply_waits_for_kernel_transcript_before_passive_import() {
    let fixture = Fixture::new().await;
    fixture.native.0.lock().unwrap().hold = true;
    let stack = Stack::boot(&fixture).await;
    let track = stack.track(&fixture).await;
    let card = stack.attach(&track, "kernel-persistence-order").await;
    stack.wait_submit(&card).await;
    let (status, body) = stack
        .request(
            "POST",
            &format!("/api/cards/{card}/planner/input"),
            Some(json!({"text":"audit native persistence order"})),
            None,
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    stack.wait_text(&card, "fresh audit progress").await;
    let run = stack.run(&card).await;
    assert_eq!(run["phase"], "turn_running");
    let runtime = run["worker_session_id"].as_str().unwrap().to_owned();
    let harness = stack.state.harness.get(&runtime).unwrap();
    // The observation only parks the real consumer; it must not issue another prompt.
    harness.pause_issuance_for_dev();
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    install_planner_harness_observation_race_hook_for_test(
        &runtime,
        PlannerHarnessObservationRaceHook {
            entered: entered.clone(),
            release: release.clone(),
        },
    );
    harness
        .observe(Observation::TrackGoal {
            text: "fixture persistence barrier".into(),
        })
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), entered.notified())
        .await
        .unwrap();
    let mut events = stack.state.events.subscribe();
    let native_part = {
        let mut native = fixture.native.0.lock().unwrap();
        let reply = native.messages.last_mut().unwrap();
        let completed = reply["info"]["time"]["created"].as_i64().unwrap() + 1;
        reply["info"]["time"]["completed"] = json!(completed);
        reply["info"]["finish"] = json!("stop");
        reply["parts"][0]["time"]["end"] = json!(completed);
        let part = reply["parts"][0]["id"].as_str().unwrap().to_owned();
        native.busy = false;
        part
    };
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while stack.journals().await != vec![(SESSION.into(), "completed".into())] {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the native driver must settle while the kernel consumer is held"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(stack.run(&card).await["phase"], "turn_running");
    let history_reads = || {
        fixture
            .requests()
            .iter()
            .filter(|request| {
                request["method"] == "GET"
                    && request["path"] == format!("/session/{SESSION}/message")
            })
            .count()
    };
    let before = history_reads();
    // Three paginated snapshots after native settlement give the passive observer its window.
    while history_reads() < before + 6 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the passive observer must inspect the settled native snapshot"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    release.notify_one();
    while stack.run(&card).await["phase"] != "turn_completed" {
        assert!(tokio::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    tokio::time::sleep(Duration::from_millis(700)).await;
    let rows = stack.items(&card).await;
    let completed: Vec<_> = rows
        .as_array()
        .unwrap()
        .iter()
        .filter(|row| row["item_uuid"] == native_part && row["method"] == "item/completed")
        .collect();
    assert_eq!(
        completed.len(),
        1,
        "the same native item must not be persisted by both passive history and queued Harness notification: {completed:?}"
    );
    transcript_assertions::assert_item_announcements(
        &mut events,
        &json!(completed),
        &card,
        &track,
        transcript_assertions::ItemEventScope::System,
        transcript_assertions::ItemSelection::NativeUuid(&native_part),
    );
    assert_eq!(fixture.posts().len(), 1);
    stack.shutdown().await;
}
