//! OpenCode extends the provider CHECK without losing existing rows or inbound references.

use calm_truth::MIGRATOR;
use sqlx::{Connection, Row, SqliteConnection, migrate::Migrate, sqlite::SqliteConnectOptions};

async fn apply_through(db: &mut SqliteConnection, last: i64) {
    let applied: Vec<i64> = sqlx::query_scalar("SELECT version FROM _sqlx_migrations")
        .fetch_all(&mut *db)
        .await
        .unwrap();
    for migration in MIGRATOR.iter().filter(|m| {
        m.version <= last && !applied.contains(&m.version) && !m.migration_type.is_down_migration()
    }) {
        db.apply(migration).await.unwrap();
    }
}

async fn snapshot(db: &mut SqliteConnection) -> Vec<Vec<String>> {
    sqlx::query("SELECT * FROM worker_sessions ORDER BY id")
        .fetch_all(db)
        .await
        .unwrap()
        .iter()
        .map(|row| {
            (0..row.len())
                .map(|i| {
                    use sqlx::ValueRef;
                    let value = row.try_get_raw(i).unwrap();
                    if value.is_null() {
                        "NULL".into()
                    } else if let Ok(text) = row.try_get::<String, _>(i) {
                        text
                    } else {
                        row.get::<i64, _>(i).to_string()
                    }
                })
                .collect()
        })
        .collect()
}

#[tokio::test]
async fn opencode_migration_preserves_rows_self_references_and_all_inbound_links() {
    let mut db = SqliteConnection::connect_with(
        &SqliteConnectOptions::new()
            .in_memory(true)
            .foreign_keys(true),
    )
    .await
    .unwrap();
    db.ensure_migrations_table().await.unwrap();
    apply_through(&mut db, 129).await;
    sqlx::raw_sql(include_str!("fixtures/opencode_migration_before.sql"))
        .execute(&mut db)
        .await
        .unwrap();
    let before = snapshot(&mut db).await;
    let indexes_before: Vec<(String, String)> = sqlx::query_as(
        "SELECT name,sql FROM sqlite_schema WHERE type='index' AND tbl_name='worker_sessions' AND sql IS NOT NULL ORDER BY name",
    ).fetch_all(&mut db).await.unwrap();

    apply_through(&mut db, 130).await;

    assert_eq!(
        snapshot(&mut db).await,
        before,
        "every session column survives"
    );
    let indexes_after: Vec<(String, String)> = sqlx::query_as(
        "SELECT name,sql FROM sqlite_schema WHERE type='index' AND tbl_name='worker_sessions' AND sql IS NOT NULL ORDER BY name",
    ).fetch_all(&mut db).await.unwrap();
    assert_eq!(
        indexes_after, indexes_before,
        "all index definitions survive"
    );
    for (table, column) in [
        ("cards", "session_id"),
        ("tracks", "root_session_id"),
        ("worker_flow_items", "worker_session_id"),
    ] {
        let value: String = sqlx::query_scalar(&format!("SELECT {column} FROM {table}"))
            .fetch_one(&mut db)
            .await
            .unwrap();
        assert_eq!(value, "s", "{table}.{column} survives DROP actions");
    }
    assert!(
        sqlx::query("PRAGMA foreign_key_check")
            .fetch_all(&mut db)
            .await
            .unwrap()
            .is_empty()
    );
    for (id, mode, contract, accepted) in [
        ("oc-planner", "resumable", "planner", true),
        ("oc-executor", "resumable", "executor", false),
        ("oc-validator", "resumable", "validator", false),
        ("oc-ephemeral", "ephemeral", "planner", false),
    ] {
        let result = sqlx::query(concat!(
            "INSERT INTO worker_sessions (id,track_id,provider,mode,contract,state,",
            "created_at_ms,updated_at_ms) VALUES (?1,'t','opencode',?2,?3,'idle',1,1)"
        ))
        .bind(id)
        .bind(mode)
        .bind(contract)
        .execute(&mut db)
        .await;
        assert_eq!(result.is_ok(), accepted, "OpenCode {mode}/{contract}");
    }
}
