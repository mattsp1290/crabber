//! Newest suffix reads: indexed metadata first, payloads only within the shared cap.
use super::{PostgresStore, StoreError, db};
use crate::public_history::newest::Window;
use crate::transaction::Transaction;
use crate::{PublicNewestHistoryOutcome, SnapshotLimit, SnapshotRequest, SnapshotUsage};
use crabber_core::{
    EventCursor, ImportedMessageProvenance, ImportedToolCall, Message, ToolCallId, ToolCallRecord,
};
use serde::de::DeserializeOwned;
use sqlx::{Postgres, Row, postgres::PgRow};

impl PostgresStore {
    pub(super) async fn read_newest_history(
        &self,
        request: SnapshotRequest,
    ) -> Result<PublicNewestHistoryOutcome, StoreError> {
        crate::public_history::newest::validate(&request)?;
        let mut tx =
            crate::transaction::begin_scoped(&self.pool, crate::transaction::StoreScope::Snapshot)
                .await
                .map_err(db)?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        let result = scan(&mut tx, &request).await;
        tx.rollback().await.map_err(db)?;
        result
    }
}
async fn scan(
    tx: &mut Transaction<'_, Postgres>,
    request: &SnapshotRequest,
) -> Result<PublicNewestHistoryOutcome, StoreError> {
    let revision: i64 = sqlx::query_scalar("SELECT public_revision FROM sessions WHERE id=$1")
        .bind(&request.session_id.0)
        .fetch_optional(&mut **tx)
        .await
        .map_err(db)?
        .ok_or(StoreError::NotFound)?;
    let high_water: i64 =
        sqlx::query_scalar("SELECT coalesce(max(seq),0) FROM events WHERE session_id=$1")
            .bind(&request.session_id.0)
            .fetch_one(&mut **tx)
            .await
            .map_err(db)?;
    let mut window = Window::new(
        crate::attempt::sql::revision(revision).map_err(|_| invalid())?,
        EventCursor(u64::try_from(high_water).map_err(|_| invalid())?),
        request.limits,
    );
    let mut before = i64::MAX;
    loop {
        let row = sqlx::query("SELECT seq,snapshot_parts,snapshot_text,snapshot_bytes FROM messages WHERE session_id=$1 AND seq<=$2 ORDER BY seq DESC LIMIT 1")
            .bind(&request.session_id.0).bind(before).fetch_optional(&mut **tx).await.map_err(db)?;
        let Some(row) = row else {
            return Ok(window.finish(None));
        };
        before = row
            .try_get::<i64, _>("seq")
            .map_err(db)?
            .checked_sub(1)
            .ok_or_else(invalid)?;
        let (message, cost) =
            match payload::<Message>(tx, request, &window, &row, Kind::Message).await? {
                Ok(message) => message,
                Err(limit) => return Ok(window.finish(Some(limit))),
            };
        if message.session_id != request.session_id {
            return Err(invalid());
        }
        let ids = match window.message(&message, Some(cost)) {
            Ok(ids) => ids,
            Err(limit) => return Ok(window.finish(Some(limit))),
        };
        if let Err(limit) = dependencies(tx, request, &mut window, &message, ids).await? {
            return Ok(window.finish(Some(limit)));
        }
        window.commit();
    }
}
async fn dependencies(
    tx: &mut Transaction<'_, Postgres>,
    request: &SnapshotRequest,
    window: &mut Window,
    message: &Message,
    ids: Vec<ToolCallId>,
) -> Result<Result<(), SnapshotLimit>, StoreError> {
    for id in ids {
        let row = sqlx::query("SELECT t.seq,t.snapshot_parts,t.snapshot_text,t.snapshot_bytes FROM tool_calls t JOIN runs r ON r.id=t.run_id WHERE r.session_id=$1 AND t.id=$2")
            .bind(&request.session_id.0).bind(&id.0).fetch_optional(&mut **tx).await.map_err(db)?;
        let outcome = if let Some(row) = row {
            match payload::<ToolCallRecord>(tx, request, window, &row, Kind::Native).await? {
                Ok((call, cost)) => {
                    if call.id != id {
                        return Err(invalid());
                    }
                    window.native(&call, Some(cost))
                }
                Err(limit) => Err(limit),
            }
        } else {
            imported(tx, request, window, &id).await?
        };
        if let Err(limit) = outcome {
            return Ok(Err(limit));
        }
    }
    let row = sqlx::query("SELECT ordinal AS seq,CAST(0 AS BIGINT) AS snapshot_parts,CAST(0 AS BIGINT) AS snapshot_text,snapshot_bytes FROM imported_message_provenance WHERE session_id=$1 AND message_id=$2")
        .bind(&request.session_id.0).bind(&message.id.0).fetch_optional(&mut **tx).await.map_err(db)?;
    if let Some(row) = row {
        match payload::<ImportedMessageProvenance>(tx, request, window, &row, Kind::Provenance)
            .await?
        {
            Ok((record, cost)) => {
                if record.session_id != request.session_id || record.message_id != message.id {
                    return Err(invalid());
                }
                return Ok(window.provenance(&record, Some(cost)));
            }
            Err(limit) => return Ok(Err(limit)),
        }
    }
    Ok(Ok(()))
}
async fn imported(
    tx: &mut Transaction<'_, Postgres>,
    request: &SnapshotRequest,
    window: &mut Window,
    id: &ToolCallId,
) -> Result<Result<(), SnapshotLimit>, StoreError> {
    let row = sqlx::query("SELECT ordinal AS seq,snapshot_parts,snapshot_text,snapshot_bytes FROM imported_tool_calls WHERE session_id=$1 AND id=$2")
        .bind(&request.session_id.0).bind(&id.0).fetch_optional(&mut **tx).await.map_err(db)?;
    let Some(row) = row else {
        return Ok(Ok(()));
    };
    match payload::<ImportedToolCall>(tx, request, window, &row, Kind::Imported).await? {
        Ok((call, cost)) => {
            if call.session_id != request.session_id || call.id != *id {
                return Err(invalid());
            }
            Ok(window.imported(&call, Some(cost)))
        }
        Err(limit) => Ok(Err(limit)),
    }
}
#[derive(Clone, Copy)]
enum Kind {
    Message,
    Native,
    Imported,
    Provenance,
}
async fn payload<T: DeserializeOwned>(
    tx: &mut Transaction<'_, Postgres>,
    request: &SnapshotRequest,
    window: &Window,
    row: &PgRow,
    kind: Kind,
) -> Result<Result<(T, SnapshotUsage), SnapshotLimit>, StoreError> {
    let size = |name| -> Result<usize, StoreError> {
        usize::try_from(row.try_get::<i64, _>(name).map_err(db)?).map_err(|_| invalid())
    };
    let cost = SnapshotUsage {
        messages: usize::from(matches!(kind, Kind::Message)),
        tool_calls: usize::from(matches!(kind, Kind::Native | Kind::Imported)),
        parts: size("snapshot_parts")?,
        text_bytes: size("snapshot_text")?,
        encoded_bytes: size("snapshot_bytes")?,
    };
    if let Err(limit) = window.preflight(cost) {
        return Ok(Err(limit));
    }
    let query = match kind {
        Kind::Message => {
            "SELECT CASE WHEN octet_length(snapshot_record)=$3 THEN snapshot_record END FROM messages WHERE session_id=$1 AND seq=$2"
        }
        Kind::Native => {
            "SELECT CASE WHEN octet_length(t.snapshot_record)=$3 THEN t.snapshot_record END FROM tool_calls t JOIN runs r ON r.id=t.run_id WHERE r.session_id=$1 AND t.seq=$2"
        }
        Kind::Imported => {
            "SELECT CASE WHEN octet_length(data_json)=$3 THEN data_json END FROM imported_tool_calls WHERE session_id=$1 AND ordinal=$2"
        }
        Kind::Provenance => {
            "SELECT CASE WHEN octet_length(data_json)=$3 THEN data_json END FROM imported_message_provenance WHERE session_id=$1 AND ordinal=$2"
        }
    };
    let record: Option<String> = sqlx::query_scalar(query)
        .bind(&request.session_id.0)
        .bind(row.try_get::<i64, _>("seq").map_err(db)?)
        .bind(i64::try_from(cost.encoded_bytes).map_err(|_| invalid())?)
        .fetch_one(&mut **tx)
        .await
        .map_err(db)?;
    let record = record.ok_or_else(invalid)?;
    Ok(Ok((
        serde_json::from_str(&record).map_err(|_| invalid())?,
        cost,
    )))
}
fn invalid() -> StoreError {
    StoreError::Validation("invalid newest history record".into())
}

#[cfg(test)]
mod tests;
