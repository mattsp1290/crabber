//! Backend-local transfer of one scalar-preflighted history record.
use super::{Boundary, RecordKind, StoreError, db, invalid};
use crate::sql_snapshot::append_imported;
use crate::{SnapshotPage, SnapshotRequest};
use sqlx::sqlite::SqliteRow;
use sqlx::{Row, Sqlite, Transaction};

pub(super) async fn next(
    tx: &mut Transaction<'_, Sqlite>,
    request: &SnapshotRequest,
    b: &Boundary,
) -> Result<Option<(RecordKind, SqliteRow)>, StoreError> {
    let message = sqlx::query(
        "SELECT seq,snapshot_parts,snapshot_text,snapshot_bytes FROM messages WHERE \
                        session_id=$1 AND seq>$2 AND seq<=$3 ORDER BY seq LIMIT 1",
    )
    .bind(&request.session_id.0)
    .bind(b.message_after)
    .bind(b.messages)
    .fetch_optional(&mut **tx)
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
        .fetch_optional(&mut **tx)
        .await
        .map_err(db)?
    };
    let (kind, row) = if is_message {
        (RecordKind::Message, row)
    } else if row.is_some() {
        (RecordKind::Native, row)
    } else {
        let imported = sqlx::query("SELECT ordinal AS seq,CASE WHEN length(CAST(id AS BLOB))<=512 THEN id END AS identity,snapshot_parts,snapshot_text,snapshot_bytes FROM imported_tool_calls WHERE session_id=$1 AND ordinal>$2 AND ordinal<=$3 ORDER BY ordinal LIMIT 1")
        .bind(&request.session_id.0).bind(b.imported_after).bind(b.imported).fetch_optional(&mut **tx).await.map_err(db)?;
        if imported.is_some() {
            (RecordKind::Imported, imported)
        } else {
            let provenance = sqlx::query("SELECT ordinal AS seq,CASE WHEN length(CAST(message_id AS BLOB))<=512 THEN message_id END AS identity,CAST(0 AS BIGINT) AS snapshot_parts,CAST(0 AS BIGINT) AS snapshot_text,snapshot_bytes FROM imported_message_provenance WHERE session_id=$1 AND ordinal>$2 AND ordinal<=$3 ORDER BY ordinal LIMIT 1")
            .bind(&request.session_id.0).bind(b.provenance_after).bind(b.provenance).fetch_optional(&mut **tx).await.map_err(db)?;
            (RecordKind::Provenance, provenance)
        }
    };
    Ok(row.map(|row| (kind, row)))
}

pub(super) async fn append(
    tx: &mut Transaction<'_, Sqlite>,
    request: &SnapshotRequest,
    b: &mut Boundary,
    page: &mut SnapshotPage,
    row: &SqliteRow,
    kind: RecordKind,
) -> Result<(), StoreError> {
    let seq: i64 = row.try_get("seq").map_err(db)?;
    if matches!(kind, RecordKind::Message | RecordKind::Native) {
        let query = if kind == RecordKind::Message {
            "SELECT data FROM messages WHERE seq=$1"
        } else {
            "SELECT data FROM tool_calls WHERE seq=$1"
        };
        let record: String = sqlx::query_scalar(query)
            .bind(seq)
            .fetch_one(&mut **tx)
            .await
            .map_err(db)?;
        if kind == RecordKind::Message {
            page.messages
                .push(serde_json::from_str(&record).map_err(|_| invalid())?);
            b.message_after = seq;
        } else {
            page.tool_calls
                .push(serde_json::from_str(&record).map_err(|_| invalid())?);
            b.call_after = seq;
        }
    } else {
        let bytes: i64 = row.try_get("snapshot_bytes").map_err(db)?;
        let query = if kind == RecordKind::Imported {
            "SELECT CASE WHEN length(CAST(data_json AS BLOB))=$3 THEN data_json END FROM imported_tool_calls WHERE session_id=$1 AND ordinal=$2"
        } else {
            "SELECT CASE WHEN length(CAST(data_json AS BLOB))=$3 THEN data_json END FROM imported_message_provenance WHERE session_id=$1 AND ordinal=$2"
        };
        let record: Option<String> = sqlx::query_scalar(query)
            .bind(&request.session_id.0)
            .bind(seq)
            .bind(bytes)
            .fetch_one(&mut **tx)
            .await
            .map_err(db)?;
        let identity: Option<String> = row.try_get("identity").map_err(db)?;
        append_imported(
            page,
            kind,
            &record.ok_or_else(invalid)?,
            &request.session_id,
            &identity.ok_or_else(invalid)?,
            (
                row.try_get("snapshot_parts").map_err(db)?,
                row.try_get("snapshot_text").map_err(db)?,
                bytes,
            ),
        )?;
        if kind == RecordKind::Imported {
            b.imported_after = seq;
        } else {
            b.provenance_after = seq;
        }
    }

    Ok(())
}
