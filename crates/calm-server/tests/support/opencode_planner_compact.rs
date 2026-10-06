use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn managed_manual_compaction_refuses_without_native_writes_and_keeps_continuation() {
    let root = Root::new("complete");
    let stack = Stack::boot(&root).await;
    let (_, card) = stack.create().await;
    stack.input(&card, "first audit").await;
    stack.completed(&card, 1).await;
    let run = stack.run(&card).await;
    let items = stack.rows(&card).await;
    let pool = stack.repo().sqlite_pool().unwrap();
    let journal = sqlx::query_as::<_, (String, String, String)>(
        "SELECT id,input_json,state FROM opencode_submissions WHERE card_id=? ORDER BY id",
    )
    .bind(&card)
    .fetch_all(&pool)
    .await
    .unwrap();
    let native_writes = root.records("native-writes.jsonl");
    let (status, error) = stack
        .request("POST", &format!("/api/cards/{card}/planner/compact"), None)
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{error}");
    assert_eq!(stack.run(&card).await, run);
    assert_eq!(stack.rows(&card).await, items);
    assert_eq!(
        sqlx::query_as::<_, (String, String, String)>(
            "SELECT id,input_json,state FROM opencode_submissions WHERE card_id=? ORDER BY id"
        )
        .bind(&card)
        .fetch_all(&pool)
        .await
        .unwrap(),
        journal
    );
    assert_eq!(root.records("native-writes.jsonl"), native_writes);
    stack.input(&card, "second audit").await;
    stack.completed(&card, 2).await;
    stack.shutdown().await;
}
