use super::*;
use calm_server::harness::{
    Observation,
    run_loop::{
        PlannerHarnessObservationRaceHook, install_planner_harness_observation_race_hook_for_test,
    },
};

#[tokio::test]
async fn terminal_receipt_recovery_persists_kernel_retirement_before_history_import() {
    let fixture = Fixture::new().await;
    let stack = Stack::boot(&fixture).await;
    let track = stack.track(&fixture).await;
    let card = stack.attach(&track, "recover-terminal-checkpoint").await;
    stack.wait_submit(&card).await;
    fixture.native.0.lock().unwrap().lost = true;
    let (status, body) = stack
        .request(
            "POST",
            &format!("/api/cards/{card}/planner/input"),
            Some(json!({"text":"audit once across kernel crash"})),
            Some("crash-checkpoint-intent"),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    let runtime = loop {
        let run = stack.run(&card).await;
        if run["phase"] == "turn_running"
            && stack.journals().await == vec![(SESSION.into(), "unknown".into())]
        {
            break run["worker_session_id"].as_str().unwrap().to_owned();
        }
        assert!(tokio::time::Instant::now() < deadline, "{run}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    let harness = stack.state.harness.get(&runtime).unwrap();
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
            text: "fixture crash checkpoint barrier".into(),
        })
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), entered.notified())
        .await
        .unwrap();
    let native_part = {
        let input = fixture.posts()[0]["input"].clone();
        let mut native = fixture.native.0.lock().unwrap();
        native.messages.push(persisted_user(&input, 20));
        let reply = assistant(
            "msg_crash_terminal",
            input["messageID"].as_str().unwrap(),
            "completed native audit across crash",
            21,
            true,
        );
        let part = reply["parts"][0]["id"].as_str().unwrap().to_owned();
        native.messages.push(reply);
        native.busy = false;
        part
    };
    while stack.journals().await != vec![(SESSION.into(), "completed".into())] {
        assert!(tokio::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let repo = stack.state.raw_repo();
    let worker = repo
        .session_projection_by_id(&runtime)
        .await
        .unwrap()
        .unwrap();
    let snapshot = worker.handle_state_json.as_ref().unwrap();
    assert_eq!(snapshot["phase"], "turn_running");
    assert!(snapshot["projection_client_id"].is_string(), "{snapshot}");
    assert!(
        stack
            .items(&card)
            .await
            .as_array()
            .unwrap()
            .iter()
            .all(|row| { row["item_uuid"] != native_part || row["method"] != "item/completed" }),
        "the actual consumer must still be parked before the terminal item write"
    );
    // SQLite captures the real atomic crash state: terminal journal, retained client,
    // and no consumed terminal frame. No reconstructed provider or queue policy.
    let pool = repo.sqlite_pool().unwrap();
    let crash_db = fixture.path().join("crash-checkpoint.db");
    sqlx::query("VACUUM INTO ?")
        .bind(crash_db.to_str().unwrap())
        .execute(&pool)
        .await
        .unwrap();
    release.notify_one();
    drop(harness);
    stack.shutdown().await;
    pool.close().await;
    std::fs::copy(&crash_db, fixture.path().join("neige.db")).unwrap();
    for suffix in ["neige.db-wal", "neige.db-shm"] {
        let path = fixture.path().join(suffix);
        if path.exists() {
            std::fs::remove_file(path).unwrap();
        }
    }
    let reboot = Stack::boot(&fixture).await;
    assert_eq!(reboot.run(&card).await["phase"], "turn_completed");
    let recovered = reboot
        .state
        .raw_repo()
        .session_projection_by_id(&runtime)
        .await
        .unwrap()
        .unwrap();
    let checkpoint = recovered.handle_state_json.as_ref().unwrap();
    assert_eq!(
        checkpoint["phase"], "turn_completed",
        "boot must publish the adopted terminal checkpoint to the durable history owner"
    );
    assert!(checkpoint["projection_client_id"].is_null());
    assert!(recovered.active_turn_id.is_none());
    reboot
        .wait_text(&card, "completed native audit across crash")
        .await;
    let rows = reboot.items(&card).await;
    assert_eq!(
        rows.as_array()
            .unwrap()
            .iter()
            .filter(|row| { row["item_uuid"] == native_part && row["method"] == "item/completed" })
            .count(),
        1
    );
    assert_eq!(
        fixture.posts().len(),
        1,
        "recovery must send zero new native POSTs"
    );
    reboot.shutdown().await;
}
