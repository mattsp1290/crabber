//! Backend-local persistence; transaction and failure ordering are unchanged.
use super::{PostgresStore, db, decode, decode_record};
use crate::StoreError;
use crabber_core::{EventCursor, EventRecord, RunId, Session, SessionId, ToolCallRecord};
use sqlx::Row;

impl PostgresStore {
    pub(in crate::postgres) async fn read_session_revision(
        &self,
        session: &SessionId,
    ) -> Result<crabber_core::SessionRevision, StoreError> {
        let value: i64 = sqlx::query_scalar("SELECT public_revision FROM sessions WHERE id=$1")
            .bind(&session.0)
            .fetch_optional(&self.pool)
            .await
            .map_err(db)?
            .ok_or(StoreError::NotFound)?;
        crabber_core::SessionRevision::new(
            u64::try_from(value)
                .map_err(|_| StoreError::Validation("invalid public session revision".into()))?,
        )
    }
    pub(in crate::postgres) async fn read_session(
        &self,
        id: &SessionId,
    ) -> Result<Option<Session>, StoreError> {
        sqlx::query("SELECT data FROM sessions WHERE id=$1")
            .bind(&id.0)
            .fetch_optional(&self.pool)
            .await
            .map_err(db)?
            .map(|row| decode(row.get("data")))
            .transpose()
    }
    pub(in crate::postgres) async fn read_events(
        &self,
        id: &SessionId,
        after: Option<EventCursor>,
        limit: usize,
    ) -> Result<Vec<EventRecord>, StoreError> {
        let rows = sqlx::query(
            "SELECT seq,data FROM events WHERE session_id=$1 AND seq>$2 ORDER BY seq LIMIT $3",
        )
        .bind(&id.0)
        .bind(after.map_or(0, |value| i64::try_from(value.0).unwrap_or(i64::MAX)))
        .bind(i64::try_from(limit).unwrap_or(i64::MAX))
        .fetch_all(&self.pool)
        .await
        .map_err(db)?;
        rows.into_iter()
            .map(|row| {
                let mut event: EventRecord = crate::public_state::decode_event(row.get("data"))?;
                event.cursor = Some(EventCursor(
                    u64::try_from(row.get::<i64, _>("seq"))
                        .map_err(|_| StoreError::Validation("invalid event cursor".into()))?,
                ));
                Ok(event)
            })
            .collect()
    }
    pub(in crate::postgres) async fn read_unfinished_calls(
        &self,
        run: &RunId,
    ) -> Result<Vec<ToolCallRecord>, StoreError> {
        sqlx::query("SELECT snapshot_record FROM tool_calls WHERE run_id=$1 AND status IN ('pending','running','waiting') ORDER BY id")
            .bind(&run.0).fetch_all(&self.pool).await.map_err(db)?.into_iter().map(|row| decode_record(&row.get::<String, _>("snapshot_record"))).collect()
    }
}
