//! Metadata-first bounded reads in a consistent WAL snapshot.
use super::{SqliteStore, StoreError, db};
use crate::sql_snapshot::{Boundary, RecordKind, append_imported, digest, invalid, parse, token};
pub(super) use crate::sql_snapshot::{call_record, message_record};
use crate::{SnapshotOutcome, SnapshotPage, SnapshotRequest, SnapshotUsage};
use crabber_core::EventCursor;
use sqlx::Row;

impl SqliteStore {
    #[allow(clippy::too_many_lines)] // One metadata-first repeatable-read transaction.
    pub(super) async fn read_snapshot(
        &self,
        request: SnapshotRequest,
    ) -> Result<SnapshotOutcome, StoreError> {
        // Reject oversized untrusted input before even opening a database transaction.
        if request
            .continuation
            .as_ref()
            .is_some_and(|t| t.0.len() > 2048)
        {
            return Err(invalid());
        }
        let mut tx = self.readers.begin().await.map_err(db)?;
        let result = async {
            let (revision, public_revision): (i64, i64) =
                sqlx::query_as("SELECT snapshot_revision,public_revision FROM sessions WHERE id=$1")
                    .bind(&request.session_id.0)
                    .fetch_optional(&mut *tx)
                    .await
                    .map_err(db)?
                    .ok_or(StoreError::NotFound)?;
            let key: String =
                sqlx::query_scalar("SELECT secret FROM snapshot_auth WHERE singleton")
                    .fetch_one(&mut *tx)
                    .await
                    .map_err(db)?;
            let session = digest(&[&request.session_id.0]);
            let mut b = if let Some(t) = &request.continuation {
                parse(t, &key, &session)?
            } else {
                let row = sqlx::query(
                    "SELECT coalesce((SELECT max(seq) FROM messages WHERE session_id=$1),0) AS \
                        messages, coalesce((SELECT max(t.seq) FROM tool_calls t JOIN runs r ON \
                        r.id=t.run_id WHERE r.session_id=$1),0) AS calls, coalesce((SELECT max(ordinal) FROM imported_tool_calls WHERE session_id=$1),0) AS imported, coalesce((SELECT max(ordinal) FROM imported_message_provenance WHERE session_id=$1),0) AS provenance, coalesce((SELECT max(seq) \
                        FROM events WHERE session_id=$1),0) AS high_water",
                )
                .bind(&request.session_id.0)
                .fetch_one(&mut *tx)
                .await
                .map_err(db)?;
                Boundary {
                    session,
                    revision,
                public_revision,
                    messages: row.try_get("messages").map_err(db)?,
                    calls: row.try_get("calls").map_err(db)?,
                    imported: row.try_get("imported").map_err(db)?,
                    provenance: row.try_get("provenance").map_err(db)?,
                    message_after: 0,
                    call_after: 0,
                    imported_after: 0,
                    provenance_after: 0,
                    high_water: EventCursor(
                        u64::try_from(row.try_get::<i64, _>("high_water").map_err(db)?)
                            .map_err(|_| invalid())?,
                    ),
                }
            };
            if revision != b.revision {
                return Ok(SnapshotOutcome::Invalidated {
                    high_water: b.high_water,
                });
            }
            let mut page = SnapshotPage {
            captured_revision: crate::attempt::sql::revision(b.public_revision).map_err(|_| invalid())?,
                high_water: b.high_water,
                messages: Vec::new(),
                tool_calls: Vec::new(),
            imported_calls: Vec::new(),
            imported_provenance: Vec::new(),
                usage: SnapshotUsage::default(),
                continuation: None,
            };
            loop {
                // Only scalar accounting is fetched before all caps pass.
                let message = sqlx::query(
                    "SELECT seq,snapshot_parts,snapshot_text,snapshot_bytes FROM messages WHERE \
                        session_id=$1 AND seq>$2 AND seq<=$3 ORDER BY seq LIMIT 1",
                )
                .bind(&request.session_id.0)
                .bind(b.message_after)
                .bind(b.messages)
                .fetch_optional(&mut *tx)
                .await
                .map_err(db)?;
                let is_message = message.is_some();
                let row = if let Some(row) = message {
                    Some(row)
                } else {
                    sqlx::query(
                        "SELECT t.seq,t.snapshot_parts,t.snapshot_text,t.snapshot_bytes FROM \
                            tool_calls t JOIN runs r ON r.id=t.run_id WHERE r.session_id=$1 AND \
                            t.seq>$2 AND t.seq<=$3 ORDER BY t.seq LIMIT 1",
                    )
                    .bind(&request.session_id.0)
                    .bind(b.call_after)
                    .bind(b.calls)
                    .fetch_optional(&mut *tx)
                    .await
                    .map_err(db)?
                };
                let (kind, row) = if is_message { (RecordKind::Message, row) } else if row.is_some() { (RecordKind::Native, row) } else {
    let imported = sqlx::query("SELECT ordinal AS seq,CASE WHEN length(CAST(id AS BLOB))<=512 THEN id END AS identity,snapshot_parts,snapshot_text,snapshot_bytes FROM imported_tool_calls WHERE session_id=$1 AND ordinal>$2 AND ordinal<=$3 ORDER BY ordinal LIMIT 1")
        .bind(&request.session_id.0).bind(b.imported_after).bind(b.imported).fetch_optional(&mut *tx).await.map_err(db)?;
    if imported.is_some() { (RecordKind::Imported, imported) } else {
        let provenance = sqlx::query("SELECT ordinal AS seq,CASE WHEN length(CAST(message_id AS BLOB))<=512 THEN message_id END AS identity,CAST(0 AS BIGINT) AS snapshot_parts,CAST(0 AS BIGINT) AS snapshot_text,snapshot_bytes FROM imported_message_provenance WHERE session_id=$1 AND ordinal>$2 AND ordinal<=$3 ORDER BY ordinal LIMIT 1")
            .bind(&request.session_id.0).bind(b.provenance_after).bind(b.provenance).fetch_optional(&mut *tx).await.map_err(db)?;
        (RecordKind::Provenance, provenance)
    }
};
                let Some(row) = row else {
                    return Ok(SnapshotOutcome::Page(page));
                };
                let mut usage = page.usage;
                if is_message {
                    usage.messages = usage.messages.saturating_add(1);
                } else if kind != RecordKind::Provenance {
                    usage.tool_calls = usage.tool_calls.saturating_add(1);
                }
                usage.parts = usage.parts.saturating_add(
                    usize::try_from(row.try_get::<i64, _>("snapshot_parts").map_err(db)?)
                        .map_err(|_| invalid())?,
                );
                usage.text_bytes = usage.text_bytes.saturating_add(
                    usize::try_from(row.try_get::<i64, _>("snapshot_text").map_err(db)?)
                        .map_err(|_| invalid())?,
                );
                usage.encoded_bytes = usage.encoded_bytes.saturating_add(
                    usize::try_from(row.try_get::<i64, _>("snapshot_bytes").map_err(db)?)
                        .map_err(|_| invalid())?,
                );
                if let Some(limit) = usage.exceeded(request.limits) {
                    let continuation = token(&b, &key)?;
                    if page.usage.encoded_bytes == 0 {
                        return Ok(SnapshotOutcome::Limited {
                            high_water: b.high_water,
                            limit,
                            continuation,
                        });
                    }
                    page.continuation = Some(continuation);
                    return Ok(SnapshotOutcome::Page(page));
                }
                let seq: i64 = row.try_get("seq").map_err(db)?;
if matches!(kind, RecordKind::Message | RecordKind::Native) {
    let query = if is_message { "SELECT data FROM messages WHERE seq=$1" } else { "SELECT data FROM tool_calls WHERE seq=$1" };
    let record: String = sqlx::query_scalar(query).bind(seq).fetch_one(&mut *tx).await.map_err(db)?;
    if is_message { page.messages.push(serde_json::from_str(&record).map_err(|_| invalid())?); b.message_after = seq; }
    else { page.tool_calls.push(serde_json::from_str(&record).map_err(|_| invalid())?); b.call_after = seq; }
} else {
    let bytes: i64 = row.try_get("snapshot_bytes").map_err(db)?;
    let query = if kind == RecordKind::Imported {
        "SELECT CASE WHEN length(CAST(data_json AS BLOB))=$3 THEN data_json END FROM imported_tool_calls WHERE session_id=$1 AND ordinal=$2"
    } else {
        "SELECT CASE WHEN length(CAST(data_json AS BLOB))=$3 THEN data_json END FROM imported_message_provenance WHERE session_id=$1 AND ordinal=$2"
    };
    let record: Option<String> = sqlx::query_scalar(query).bind(&request.session_id.0).bind(seq).bind(bytes).fetch_one(&mut *tx).await.map_err(db)?;
    let identity: Option<String> = row.try_get("identity").map_err(db)?;
    append_imported(&mut page, kind, &record.ok_or_else(invalid)?, &request.session_id, &identity.ok_or_else(invalid)?,
        (row.try_get("snapshot_parts").map_err(db)?, row.try_get("snapshot_text").map_err(db)?, bytes))?;
    if kind == RecordKind::Imported { b.imported_after = seq; } else { b.provenance_after = seq; }
}
page.usage = usage;
            }
        }
        .await;
        super::transactions::finish(tx, result, std::convert::identity).await
    }
}
