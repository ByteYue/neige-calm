use super::*;
use calm_server::{
    harness::{
        Observation,
        run_loop::{PlannerHarnessDrainRaceHook, install_planner_harness_drain_race_hook_for_test},
    },
    test_seams::{OPENCODE_ATTACH_RECOVERY, PausePoint, install_pause_for_test},
};

#[tokio::test]
async fn repeated_attach_preserves_send_recovered_owner_and_durable_queued_text() {
    let fixture = Fixture::new().await;
    let stack = Stack::boot(&fixture).await;
    let track = stack.track(&fixture).await;
    let card = stack.attach(&track, "attach-recovery-race").await;
    stack.wait_submit(&card).await;
    let runtime = stack.run(&card).await["worker_session_id"]
        .as_str()
        .unwrap()
        .to_owned();
    // Leave the durable active row with no process-local owner, as after a lost observer.
    stack
        .state
        .harness
        .remove(&runtime)
        .unwrap()
        .shutdown()
        .await
        .unwrap();
    let attach_entered = Arc::new(tokio::sync::Notify::new());
    let attach_release = Arc::new(tokio::sync::Notify::new());
    install_pause_for_test(
        OPENCODE_ATTACH_RECOVERY,
        &card,
        PausePoint {
            entered: attach_entered.clone(),
            release: attach_release.clone(),
        },
    );
    let drain_entered = Arc::new(tokio::sync::Notify::new());
    let drain_release = Arc::new(tokio::sync::Notify::new());
    install_planner_harness_drain_race_hook_for_test(
        &runtime,
        PlannerHarnessDrainRaceHook {
            entered: drain_entered.clone(),
            release: drain_release.clone(),
        },
    );
    {
        let attach = stack.attach(&track, "attach-recovery-race");
        tokio::pin!(attach);
        tokio::select! {
            _ = attach_entered.notified() => {}
            result = &mut attach => panic!("attach returned before its recovery pause: {result}"),
            _ = tokio::time::sleep(Duration::from_secs(5)) => panic!("attach missed recovery pause"),
        }
        // The competing HTTP Send installs the owner and commits its queued text while attach waits.
        let text = "retain acknowledged audit intent across repeated attachment";
        let input = json!({"text":text});
        let path = format!("/api/cards/{card}/planner/input");
        let (status, answer) = stack
            .request(
                "POST",
                &path,
                Some(input.clone()),
                Some("attach-race-intent"),
            )
            .await;
        assert_eq!(status, StatusCode::OK, "{answer}");
        assert_eq!(answer["worker_session_id"], runtime);
        assert!(answer["entry_id"].is_string());
        tokio::time::timeout(Duration::from_secs(5), drain_entered.notified())
            .await
            .unwrap();
        let owner = stack.state.harness.get(&runtime).unwrap();
        owner.pause_issuance_for_dev();
        assert!(
            fixture.posts().is_empty(),
            "the queue must still be undrained"
        );
        attach_release.notify_one();
        assert_eq!(attach.await, card);
        // A stale attachment used to replace this owner and persist its old empty snapshot.
        let retained = stack
            .state
            .raw_repo()
            .session_projection_by_id(&runtime)
            .await
            .unwrap()
            .unwrap();
        assert!(
            retained.handle_state_json.unwrap()["pending_queue"]
                .to_string()
                .contains(text),
            "attachment must retain the durably acknowledged queue"
        );
        assert!(
            owner
                .observe(Observation::TrackGoal {
                    text: "retained owner probe".into(),
                })
                .is_ok(),
            "attachment must not close or replace the Send-recovered owner"
        );
        let (status, replay) = stack
            .request("POST", &path, Some(input), Some("attach-race-intent"))
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(replay, answer);
        assert!(fixture.posts().is_empty());
        drain_release.notify_one();
        drop(owner);
    }
    stack.shutdown().await;
}
