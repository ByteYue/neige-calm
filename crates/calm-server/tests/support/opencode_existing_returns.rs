use super::*;

#[derive(Clone, Copy)]
pub(super) enum Reply {
    Compaction,
    Denied,
    Ordinary,
    Replay,
    Lost,
    DirectTie,
    ForeignTie,
    ForeignTieLost,
    CompactionTie,
    Aborted,
}

pub(super) async fn reply(
    native: Native,
    kind: Reply,
    input: Value,
    authorized: bool,
    query: std::collections::HashMap<String, String>,
) -> Response {
    let original = input["messageID"].as_str().unwrap().to_owned();
    let release = {
        let mut state = native.0.lock().unwrap();
        state.requests.push(
            json!({"method":"POST","path":format!("/session/{SESSION}/message"),"input":input}),
        );
        assert!(authorized);
        assert_eq!(query["directory"], state.directory.display().to_string());
        state.messages.push(persisted_user(&input, 10));
        state.busy = true;
        if matches!(
            kind,
            Reply::Compaction
                | Reply::CompactionTie
                | Reply::Ordinary
                | Reply::Replay
                | Reply::Lost
        ) {
            let mut compaction = user("msg_auto", "", 11);
            compaction["parts"] = json!([{"id":"prt_compaction","type":"compaction","auto":true}]);
            state.messages.push(compaction);
            let mut summary = assistant(
                "msg_summary",
                "msg_auto",
                "native compacted summary",
                12,
                true,
            );
            summary["info"]["summary"] = json!(true);
            summary["info"]["mode"] = json!("compaction");
            state.messages.push(summary);
        }
        state.release.clone()
    };
    release.notified().await;
    let mut state = native.0.lock().unwrap();
    let result = if matches!(
        kind,
        Reply::DirectTie | Reply::ForeignTie | Reply::ForeignTieLost | Reply::Aborted
    ) {
        if matches!(kind, Reply::ForeignTie | Reply::ForeignTieLost) {
            state
                .messages
                .push(user("msg_000_foreign", "different native client input", 10));
        }
        let mut result = assistant(
            "msg_000_tied",
            &original,
            "same timestamp native result",
            10,
            true,
        );
        if matches!(kind, Reply::Aborted) {
            result["info"]["time"]["created"] = json!(16);
            result["info"]["time"]["completed"] = json!(17);
            result["parts"][0]["time"] = json!({"start":16,"end":17});
            result["info"]["error"] = json!({"name":"MessageAbortedError","data":{"message":"Native operator stopped execution"}});
        }
        result
    } else if matches!(kind, Reply::Denied) {
        let mut message = assistant("msg_denied", &original, "", 14, true);
        message["info"]["finish"] = json!("tool-calls");
        message["parts"] = json!([{"id":"prt_denied","type":"tool","tool":"calm_calm_terminal_open","state":{"status":"error","input":{"command":"audit"},"error":"Permission rejected by native operator","time":{"start":13,"end":14}}}]);
        message
    } else {
        let mut continuation = user("msg_continue", "Continue", 13);
        if matches!(kind, Reply::Compaction | Reply::CompactionTie | Reply::Lost) {
            continuation["parts"][0]["synthetic"] = json!(true);
            continuation["parts"][0]["metadata"] = json!({"compaction_continue":true});
        } else if matches!(kind, Reply::Replay) {
            continuation["parts"][0]["text"] = input["parts"][0]["text"].clone();
        }
        state.messages.push(continuation);
        if matches!(kind, Reply::Compaction | Reply::CompactionTie) {
            let mut tools = assistant("msg_continuation_tool", "msg_continue", "", 14, true);
            tools["info"]["finish"] = json!("tool-calls");
            tools["parts"] = json!([{"id":"prt_continuation_tool","type":"tool","tool":"bash","state":{"status":"completed","input":{"command":"audit"},"output":"native audit tool progress","metadata":{"exit":0},"time":{"start":14,"end":15}}}]);
            state.messages.push(tools);
        }
        assistant(
            "msg_final",
            "msg_continue",
            "final native compacted progress",
            16,
            true,
        )
    };
    state.messages.push(result.clone());
    if matches!(kind, Reply::CompactionTie) {
        for message in state
            .messages
            .iter_mut()
            .filter(|m| m["info"]["time"]["created"].as_i64().unwrap() >= 10)
        {
            message["info"]["time"]["created"] = json!(10);
        }
    }
    let result = state.messages.last().unwrap().clone();
    state.busy = false;
    if matches!(kind, Reply::Lost | Reply::ForeignTieLost) {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }
    axum::Json(result).into_response()
}

async fn start(kind: Reply) -> (Fixture, Stack, String) {
    let fixture = Fixture::new().await;
    fixture.native.0.lock().unwrap().reply = Some(kind);
    let stack = Stack::boot(&fixture).await;
    let track = stack.track(&fixture).await;
    let card = stack.attach(&track, "returned-loop").await;
    stack.wait_submit(&card).await;
    let (status, value) = stack
        .request(
            "POST",
            &format!("/api/cards/{card}/planner/input"),
            Some(json!({"text":"audit native progress"})),
            None,
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{value}");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while fixture.posts().is_empty() {
        assert!(tokio::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    (fixture, stack, card)
}

async fn wait_state(stack: &Stack, expected: &str) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(8);
    while stack.journals().await != vec![(SESSION.into(), expected.into())] {
        assert!(
            tokio::time::Instant::now() < deadline,
            "expected {expected}, actual {:?}",
            stack.journals().await
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
async fn native_compaction_return_projects_final_once_across_restart() {
    let (fixture, stack, card) = start(Reply::Compaction).await;
    tokio::time::sleep(Duration::from_millis(800)).await;
    assert!(
        matches!(stack.journals().await[0].1.as_str(), "sending" | "unknown"),
        "summary must not complete owned turn"
    );
    fixture.native.0.lock().unwrap().release.notify_one();
    wait_state(&stack, "completed").await;
    stack
        .wait_text(&card, "final native compacted progress")
        .await;
    stack.wait_text(&card, "native audit tool progress").await;
    stack.wait_submit(&card).await;
    tokio::time::sleep(Duration::from_millis(700)).await;
    let before = stack.items(&card).await;
    assert_eq!(
        before
            .as_array()
            .unwrap()
            .iter()
            .filter(|row| row["item_uuid"] == "prt_continuation_tool")
            .count(),
        1
    );
    stack.shutdown().await;
    let restarted = Stack::boot(&fixture).await;
    restarted.wait_submit(&card).await;
    assert_eq!(restarted.items(&card).await, before);
    assert_eq!(fixture.posts().len(), 1);
    restarted.shutdown().await;
}

#[tokio::test]
async fn externally_denied_native_loop_finishes_failed_without_control() {
    let (fixture, stack, card) = start(Reply::Denied).await;
    fixture.native.0.lock().unwrap().release.notify_one();
    wait_state(&stack, "failed").await;
    stack.wait_text(&card, "prt_denied").await;
    let items = stack.items(&card).await;
    let native_tool_rows: Vec<_> = items
        .as_array()
        .unwrap()
        .iter()
        .filter(|row| row["item_uuid"] == "prt_denied")
        .collect();
    assert!(!native_tool_rows.is_empty());
    assert!(
        native_tool_rows
            .iter()
            .all(|row| row["item_type"] == "dynamicToolCall"),
        "External tool must keep native identity: {items}"
    );
    assert!(native_tool_rows.iter().all(|row| {
        row["params"]
            .as_str()
            .unwrap()
            .contains("calm_calm_terminal_open")
    }));
    assert_eq!(fixture.posts().len(), 1);
    stack.shutdown().await;
}

#[tokio::test]
async fn ambiguous_compaction_replay_and_foreign_input_remain_unknown() {
    for kind in [
        Reply::Ordinary,
        Reply::Replay,
        Reply::Lost,
        Reply::CompactionTie,
        Reply::ForeignTie,
        Reply::ForeignTieLost,
    ] {
        let (fixture, stack, card) = start(kind).await;
        fixture.native.0.lock().unwrap().release.notify_one();
        wait_blocked(
            &stack,
            &card,
            if matches!(kind, Reply::Lost | Reply::ForeignTieLost) {
                "submission response was lost"
            } else {
                "returned loop cannot be correlated"
            },
        )
        .await;
        wait_state(&stack, "unknown").await;
        stack.shutdown().await;
        let restarted = Stack::boot(&fixture).await;
        tokio::time::sleep(Duration::from_millis(600)).await;
        assert_eq!(restarted.journals().await[0].1, "unknown");
        assert_eq!(
            restarted.run(&card).await["attached_session"]["can_submit"],
            false
        );
        assert_eq!(fixture.posts().len(), 1);
        restarted.shutdown().await;
    }
}

async fn wait_blocked(stack: &Stack, card: &str, reason: &str) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(8);
    loop {
        let run = stack.run(card).await;
        if run["blocked_reason"]
            .as_str()
            .is_some_and(|value| value.contains(reason))
        {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "Missing final-response fence {reason}: {run}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
#[tokio::test]
async fn direct_native_reply_before_uuid_parent_in_same_timestamp_bucket_completes() {
    let (fixture, stack, card) = start(Reply::DirectTie).await;
    fixture.native.0.lock().unwrap().release.notify_one();
    wait_state(&stack, "completed").await;
    stack.wait_text(&card, "same timestamp native result").await;
    assert_eq!(fixture.posts().len(), 1);
    stack.shutdown().await;
}
#[tokio::test]
async fn native_operator_aborted_return_remains_interrupted_without_neige_control() {
    let (fixture, stack, _card) = start(Reply::Aborted).await;
    fixture.native.0.lock().unwrap().release.notify_one();
    wait_state(&stack, "interrupted").await;
    assert_eq!(fixture.posts().len(), 1);
    stack.shutdown().await;
}
