//! Durable write references for execution that can outlive a model turn.
use super::*;
use crate::db::sqlite::{begin_immediate_tx, track_get_tx};
use calm_types::workspace_access::WorkspaceAccess;

/// Never cloned or released by Drop: an uncertain provider outcome retains its lease.
pub(crate) struct ExecutionWriteGuard {
    pool: SqlitePool,
    id: String,
}
impl ExecutionWriteGuard {
    pub(crate) async fn acquire_native(
        pool: &SqlitePool,
        card: &str,
        thread: &str,
        except_attempt: &str,
    ) -> Result<Self> {
        let mut tx = begin_immediate_tx(pool).await?;
        let track: String = sqlx::query_scalar("SELECT track_id FROM cards WHERE id=?1")
            .bind(card)
            .fetch_one(&mut *tx)
            .await?;
        if !crate::db::sqlite::track_available(
            &mut tx,
            &track,
            except_attempt,
            WorkspaceAccess::ReadWrite,
        )
        .await?
        {
            return Err(CalmError::Conflict(
                "workspace write guard is waiting for current readers or writers".into(),
            ));
        }
        let duplicate: bool = sqlx::query_scalar(
            r#"
SELECT EXISTS(SELECT 1 FROM workspace_leases WHERE holder_kind='native' AND holder_id=?1 AND
state IN ('held', 'releasing'))
"#,
        )
        .bind(thread)
        .fetch_one(&mut *tx)
        .await?;
        if duplicate {
            return Err(CalmError::Conflict(
                "native execution still holds its write guard".into(),
            ));
        }
        let id = acquire_execution_write_tx(&mut tx, &track, card, thread, "native").await?;
        tx.commit().await?;
        Ok(Self {
            pool: pool.clone(),
            id,
        })
    }
    pub(crate) async fn started(self, turn: &str) -> Result<()> {
        sqlx::query(
            r#"
UPDATE workspace_leases SET holder_phase='running', lease_owner=?2, updated_at_ms=?3 WHERE
lease_id=?1 AND state='held' AND holder_phase='issuing'
"#,
        )
        .bind(&self.id)
        .bind(turn)
        .bind(now_ms())
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}

/// The caller's transaction reserves the Track checkout before spawning any write execution.
pub(crate) async fn acquire_execution_write_tx(
    tx: &mut Tx<'_>,
    track: &str,
    card: &str,
    holder: &str,
    kind: &str,
) -> Result<String> {
    if !matches!(kind, "native" | "terminal" | "forge") {
        return Err(CalmError::Internal("invalid execution guard kind".into()));
    }
    let track_row = track_get_tx(tx, &TrackId::from(track)).await?;
    let path = std::fs::canonicalize(track_row.workspace.agent_cwd()).map_err(|error| {
        CalmError::Conflict(format!("workspace guard path unavailable: {error}"))
    })?;
    let path = path
        .to_str()
        .ok_or_else(|| CalmError::Conflict("workspace guard path is not UTF-8".into()))?;
    let id = new_id();
    sqlx::query(
        r#"
INSERT INTO workspace_leases(lease_id, card_id, track_id, path, state, lease_owner, boot_id,
created_at_ms, updated_at_ms, access_mode, holder_kind, holder_id, holder_phase) VALUES(?1, ?2,
?3, ?4, 'held', ?5, ?6, ?7, ?7, 'read_write', ?8, ?5, 'issuing')
"#,
    )
    .bind(&id)
    .bind(card)
    .bind(track)
    .bind(path)
    .bind(holder)
    .bind(read_boot_id())
    .bind(now_ms())
    .bind(kind)
    .execute(&mut **tx)
    .await?;
    Ok(id)
}

/// Only a provider's confirmed process stop may call this; completion notifications are insufficient.
pub(crate) async fn release_stopped_execution(
    pool: &SqlitePool,
    kind: &str,
    holder: &str,
) -> Result<()> {
    sqlx::query(
        r#"
UPDATE workspace_leases SET state='released', holder_phase='stopped', released_at_ms=?3,
updated_at_ms=?3 WHERE holder_kind=?1 AND holder_id=?2 AND state IN ('held', 'releasing')
"#,
    )
    .bind(kind)
    .bind(holder)
    .bind(now_ms())
    .execute(pool)
    .await?;
    Ok(())
}
