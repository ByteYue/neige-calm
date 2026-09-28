//! R1 review regressions through public read and real queued Planner input.
use super::*;

/// One C1 → reject → repair → C2 → R2 pipeline, read at every point a report, notice or briefing is judged. Each
/// checkpoint's fault is undone before the next, and a later positive read proves the undo was exact, so no
/// checkpoint can pass on an earlier one's leftover state.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn candidate_repair_r2_report_notice_and_briefing_follow_current_state() {
    use super::super::settlement::{notices, observed};
    use crate::isolated_codex_retry::recovery_wake::{planner, planner_with_daemon};
    use calm_server::{codex_appserver::InputItem, shared_codex_appserver::SharedCodexAppServer};
    let (fx, producer, r1, _, _) = rejected().await;
    let pair = request(&fx, "Fix findings").await.unwrap();
    let (c2, publication, machine, r2) = produce_c2(&fx, &pair).await;
    let pool = fx.boot.repo.sqlite_pool().unwrap();

    // R2 reports must answer every original finding exactly, from R2's own execution.
    let identity = review_identity(&fx, &r2).await;
    let mut bad = vec![
        json!({"$neige_result_presentation":"worker-summary-v1","summary":"review claims pass","details":passed()}),
    ];
    let mut value = passed();
    value.as_object_mut().unwrap().remove("finding_responses");
    bad.push(value);
    for responses in [
        json!([]),
        json!([passed()["finding_responses"][0]]),
        json!([
            passed()["finding_responses"][0],
            passed()["finding_responses"][0]
        ]),
    ] {
        let mut value = passed();
        value["finding_responses"] = responses;
        bad.push(value);
    }
    for (pointer, value) in [
        ("/finding_responses/1/finding_index", json!(2)),
        ("/finding_responses/1/status", json!("unresolved")),
        ("/finding_responses/1/evidence", json!(" ")),
        ("/blocking_findings", json!(["new blocker"])),
    ] {
        let mut report = passed();
        *report.pointer_mut(pointer).unwrap() = value;
        bad.push(report);
    }
    for result in bad {
        let response = call_tool(
            &fx.boot,
            "calm.task.complete",
            identity.clone(),
            json!({"idempotency_key":r2.id,"result":result,"artifacts":[]}),
        )
        .await;
        assert!(
            response.is_err(),
            "accepted invalid repair report: {response:?}"
        );
    }
    assert!(
        call_tool(
            &fx.boot,
            "calm.task.complete",
            review_identity(&fx, &r1).await,
            json!({"idempotency_key":r2.id,"result":passed(),"artifacts":[]})
        )
        .await
        .is_err()
    );
    // Fault injection at the persisted current task: deleting the optional reference
    // cannot turn this registered R2 into an ordinary two-field reviewer.
    sqlx::query("UPDATE tasks SET context_json=json_remove(context_json,'$.neige_execution.repair') WHERE id=?1")
        .bind(&r2.id).execute(&pool).await.unwrap();
    let downgrade = call_tool(&fx.boot,"calm.task.complete",identity.clone(),json!({"idempotency_key":r2.id,"result":{"passed":true,"blocking_findings":[]},"artifacts":[]})).await.unwrap_err();
    assert!(
        downgrade.message.contains("repair reference"),
        "{downgrade:?}"
    );
    assert_eq!(current(&fx.boot, &r2.key).await.status, TaskStatus::Running);
    sqlx::query("UPDATE tasks SET context_json=?1 WHERE id=?2")
        .bind(&r2.context_json)
        .bind(&r2.id)
        .execute(&pool)
        .await
        .unwrap();
    // An old R1 replay is still an R1 report and cannot qualify C2.
    call_tool(&fx.boot,"calm.task.complete",review_identity(&fx,&r1).await,json!({"idempotency_key":r1.id,"result":{"passed":false,"blocking_findings":FINDINGS},"artifacts":[]})).await.unwrap();
    assert!(verdict(&fx, &c2, "accepted").await.is_err());

    // The real R2 report, submitted while the controlled provider still owns its run: Done is not settled.
    std::fs::write(
        workspace(&fx, &r2).await.join("report-result.json"),
        passed().to_string(),
    )
    .unwrap();
    call_tool(
        &fx.boot,
        "calm.task.complete",
        identity,
        json!({"idempotency_key":r2.id,"result":passed(),"artifacts":[]}),
    )
    .await
    .unwrap();
    assert_eq!(current(&fx.boot, &r2.key).await.status, TaskStatus::Done);
    assert!(verdict(&fx, &c2, "accepted").await.is_err());
    assert!(verdict(&fx, &producer, "accepted").await.is_err());
    let operation = fx
        .state
        .operation_runtime
        .find_by_kind_and_idempotency("codex-isolated-worker", &r2.id)
        .await
        .unwrap()
        .unwrap();
    let unsettled = briefing(&fx, &r2, &operation.id).await;
    // Always stop the real fixture worker before asserting reader results.
    settle(&fx, &r2, true).await;
    assert_acceptance_refused(&unsettled, "unavailable");

    let event=fx.boot.repo.events_for_track(fx.boot.track_id.as_str(), &["task.execution_settled"], None).await.unwrap()
        .into_iter().find(|e|matches!(&e.event,calm_server::event::Event::TaskExecutionSettled{task_id,..} if task_id==&r2.id)).unwrap();
    let calm_server::event::Event::TaskExecutionSettled { operation_id, .. } = &event.event else {
        unreachable!()
    };
    let relevant = || {
        calm_server::file_delivery::candidate_review_notice_relevant(
            fx.boot.repo.as_ref(),
            &fx.boot.track_id,
            &r2.id,
            operation_id,
        )
    };
    let withdraw = |id: &str, at: Option<i64>| {
        sqlx::query("UPDATE tasks SET context_stale_at_ms=?1 WHERE id=?2")
            .bind(at)
            .bind(id.to_owned())
            .execute(&pool)
    };

    // Dispatcher replay of the settlement rechecks the ORIGINAL C1/R1 lineage, not only R2's own.
    supersede_planner(&fx).await;
    let handle = planner(&fx).await;
    // Accept the real preceding R2 completion before withdrawal. Otherwise an
    // unrelated old C1 prefix can prevent replay before the R2 relevance gate.
    let completed=fx.boot.repo.events_for_track(fx.boot.track_id.as_str(), &["task.completed"], None).await.unwrap()
        .into_iter().find(|e|matches!(&e.event,calm_server::event::Event::TaskCompleted{idempotency_key,..} if idempotency_key==&r2.id)).unwrap();
    fx.state
        .dispatcher
        .catch_up_push(fx.boot.track_id.clone(), completed.event, completed.id)
        .await;
    assert!(
        observed(&handle, &r2.id, false).await,
        "real completion prefix must be consumed"
    );
    for original in [&producer.id, &r1.id] {
        withdraw(original, Some(1)).await.unwrap();
        assert!(!relevant().await.unwrap(), "withdrawn {original}");
        withdraw(original, None).await.unwrap();
    }
    assert!(
        relevant().await.unwrap(),
        "restored lineage is relevant again"
    );
    withdraw(&producer.id, Some(1)).await.unwrap();
    fx.state
        .dispatcher
        .catch_up_push(fx.boot.track_id.clone(), event.event.clone(), event.id)
        .await;
    let count = notices(&handle.snapshot().await, &r2.id);
    handle.shutdown().await.unwrap();
    assert_eq!(count, 0, "withdrawn produce: replay must be irrelevant");
    withdraw(&producer.id, None).await.unwrap();

    // A notice queued while valid is re-read at delivery: the actual Planner input reflects a later withdrawal.
    supersede_planner(&fx).await;
    let daemon = SharedCodexAppServer::new_fake_running_with_pending(fx.boot.repo.clone(), None);
    let handle = planner_with_daemon(&fx, daemon.clone()).await;
    calm_server::semantic_recovery::test_support::register_thread(
        fx.boot.repo.as_ref(),
        fx.boot.planner_card_id.as_str(),
        "planner-observer",
    )
    .await
    .unwrap();
    let observation = calm_server::file_delivery::candidate_review_notice_observation(
        fx.boot.repo.as_ref(),
        &fx.boot.track_id,
        &r2.id,
        operation_id,
    )
    .await
    .unwrap()
    .unwrap();
    handle.observe_envelope(observation, event.id).unwrap();
    let queued = observed(&handle, &r2.id, true).await;
    withdraw(&producer.id, Some(1)).await.unwrap();
    handle
        .force_phase_for_dev(calm_server::harness::HarnessPhaseTag::Idle)
        .await
        .unwrap();
    let issued = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            for (_, items) in daemon.started_turns_for_test() {
                for item in items {
                    if let InputItem::Text { text } = item
                        && let Some(brief) = parse_briefing(&text, &r2.id, Some(event.id))
                    {
                        return brief;
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    if issued.is_err() {
        // Keep exact started/pending event identities available on a failure.
        eprintln!(
            "Expected R2 {}/{}; started turns: {:?}; queue snapshot: {:?}",
            r2.id,
            event.id,
            daemon.started_turns_for_test(),
            handle.snapshot().await
        );
    }
    handle.shutdown().await.unwrap();
    assert!(queued, "valid R2 notice must queue before withdrawal");
    let brief = issued.expect("actual Planner input must contain the exact R2 settlement");
    assert_eq!(brief["current_authority"], false, "{brief}");
    assert!(
        brief["authority_reason"]
            .as_str()
            .is_some_and(|r| !r.is_empty())
    );
    assert_acceptance_refused(&brief, "unavailable");
    withdraw(&producer.id, None).await.unwrap();

    // Acceptance-ready names the exact C2 evidence and offers only an explicit producer verdict.
    let brief = briefing(&fx, &r2, operation_id).await;
    let phase = &brief["repair_acceptance"];
    assert_eq!(phase["state"], "acceptance-ready", "{brief}");
    assert_eq!(phase["next_action"]["tool"], "calm.task.verdict");
    assert_eq!(phase["next_action"]["arguments"]["idempotency_key"], c2.id);
    assert_eq!(phase["next_action"]["arguments"]["status"], "accepted");
    assert_eq!(brief["subject"]["producer_attempt_id"], c2.id);
    assert_eq!(brief["subject"]["publication_operation_id"], publication);
    assert_eq!(
        brief["subject"]["verification_operation_id"],
        machine["verification_operation_id"]
    );
    assert_eq!(
        brief["delivery"]["candidate"]["snapshot"],
        machine["candidate"]["snapshot"]
    );
    assert!(brief["delivery"]["candidate"]["snapshot"].is_string());
    assert_eq!(brief["delivery"]["review"]["review_attempt_id"], r2.id);
    assert_eq!(
        brief["delivery"]["review"]["finding_responses"],
        passed()["finding_responses"]
    );
    assert_eq!(
        brief["delivery"]["repair"]["blocking_findings"],
        json!(FINDINGS)
    );
    assert_eq!(brief["delivery"]["verification"]["passed"], true);
    assert_eq!(
        brief["delivery"]["review"]["operation"]["state"],
        "succeeded"
    );
    let decisions = || {
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM task_candidate_decisions")
            .fetch_one(&pool)
    };
    assert_eq!(
        decisions().await.unwrap(),
        0,
        "briefing must not record acceptance"
    );
    let mut args = phase["next_action"]["arguments"].clone();
    args["message"] = json!("Accept C2 after checking both resolved findings and fresh checks.");
    call_tool(
        &fx.boot,
        "calm.task.verdict",
        planner_identity(&fx.boot),
        args,
    )
    .await
    .unwrap();
    assert_eq!(
        decisions().await.unwrap(),
        1,
        "explicit producer verdict records acceptance"
    );
    assert_acceptance_decided(&briefing(&fx, &r2, operation_id).await, "accepted");

    // The report event is re-read as evidence: stripping its responses refuses a repeated acceptance.
    let report_event:i64=sqlx::query_scalar("SELECT json_extract(event_json,'$.data.result.candidate_acceptance.review.report_event_id') FROM task_candidate_decisions ORDER BY event_id DESC LIMIT 1").fetch_one(&pool).await.unwrap();
    let report: String = sqlx::query_scalar("SELECT payload FROM events WHERE id=?1")
        .bind(report_event)
        .fetch_one(&pool)
        .await
        .unwrap();
    sqlx::query(
        "UPDATE events SET payload=json_remove(payload,'$.result.finding_responses') WHERE id=?1",
    )
    .bind(report_event)
    .execute(&pool)
    .await
    .unwrap();
    assert!(verdict(&fx, &c2, "accepted").await.is_err());
    let view = listed(&fx).await;
    let task = view["tasks"]
        .as_array()
        .unwrap()
        .iter()
        .find(|t| t["key"] == c2.key)
        .unwrap();
    assert_eq!(task["file_delivery"]["qualified"], false, "{view}");
    sqlx::query("UPDATE events SET payload=?1 WHERE id=?2")
        .bind(report)
        .bind(report_event)
        .execute(&pool)
        .await
        .unwrap();
    verdict(&fx, &c2, "accepted").await.unwrap();

    // A failed R2 Operation blocks acceptance even after a decision exists.
    let recorded: String = sqlx::query_scalar("SELECT phase FROM operations WHERE id=?1")
        .bind(operation_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    for phase in ["failed", recorded.as_str()] {
        sqlx::query("UPDATE operations SET phase=?1 WHERE id=?2")
            .bind(phase)
            .bind(operation_id)
            .execute(&pool)
            .await
            .unwrap();
        let brief = briefing(&fx, &r2, operation_id).await;
        if phase == "failed" {
            assert_acceptance_refused(&brief, "blocked");
        } else {
            assert_acceptance_decided(&brief, "accepted");
        }
    }

    verdict(&fx, &c2, "rejected").await.unwrap();
    assert_acceptance_decided(&briefing(&fx, &r2, operation_id).await, "rejected");

    sqlx::query("DELETE FROM task_candidate_repairs")
        .execute(&pool)
        .await
        .unwrap();
    assert_acceptance_refused(&briefing(&fx, &r2, operation_id).await, "unavailable");
}

async fn supersede_planner(fx: &Fixture) {
    let pool = fx.boot.repo.sqlite_pool().unwrap();
    let mut tx = calm_server::db::sqlite::begin_immediate_tx(&pool)
        .await
        .unwrap();
    calm_server::db::sqlite::session_supersede_active_tx(
        &mut tx,
        &planner_identity(&fx.boot).session_id,
        calm_server::model::now_ms(),
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
}

/// The production settlement context for R2, as the Planner queue would deliver it now.
async fn briefing(fx: &Fixture, r2: &Task, operation: &str) -> Value {
    let observation = calm_server::file_delivery::candidate_review_notice_observation(
        fx.boot.repo.as_ref(),
        &fx.boot.track_id,
        &r2.id,
        operation,
    )
    .await
    .unwrap()
    .unwrap();
    let calm_server::harness::Observation::SystemContext { text } = observation else {
        panic!("expected production settlement context")
    };
    parse_briefing(&text, &r2.id, None).expect("exact R2 briefing")
}

fn assert_acceptance_refused(brief: &Value, state: &str) {
    let phase = &brief["repair_acceptance"];
    assert_eq!(phase["state"], state, "{brief}");
    assert!(phase["next_action"].is_null(), "{brief}");
    assert!(
        phase["reason"].as_str().is_some_and(|s| !s.is_empty()),
        "{brief}"
    );
}

fn assert_acceptance_decided(brief: &Value, state: &str) {
    let phase = &brief["repair_acceptance"];
    assert_eq!(phase["state"], "already-decided", "{brief}");
    assert!(phase["next_action"].is_null(), "{brief}");
    assert_eq!(brief["delivery"]["decision"]["state"], state);
    assert!(brief["delivery"]["decision"]["event_id"].is_i64());
}

// Historical R1 and current R2 notices can share a turn or arrive in different
// turns. Select the notified identity, never the first turn's last settlement.
fn parse_briefing(text: &str, attempt: &str, event: Option<i64>) -> Option<Value> {
    text.split("Candidate review execution settled (kernel snapshot):\n")
        .skip(1)
        .find_map(|section| {
            let (body, _) = section.split_once("\nEnd candidate review settlement.")?;
            let brief: Value = serde_json::from_str(body).expect("production briefing JSON");
            (brief["review_attempt_id"] == attempt
                && event.is_none_or(|id| brief["event_id"] == id))
            .then_some(brief)
        })
}
