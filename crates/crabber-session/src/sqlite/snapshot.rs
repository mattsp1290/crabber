//! Metadata-first bounded reads in a consistent WAL snapshot.
use super::{SqliteStore, StoreError, db};
use crate::sql_snapshot::{Boundary, Profile, RecordKind, digest, invalid, parse_profile, token_profile};
pub(super) use crate::sql_snapshot::{call_record, message_record};
use crate::{SnapshotOutcome, SnapshotPage, SnapshotRequest, SnapshotUsage};
use crabber_core::EventCursor;
use sqlx::Row;

#[path = "snapshot/record.rs"]
mod record;

impl SqliteStore {
    #[allow(clippy::too_many_lines)] // One metadata-first repeatable-read transaction.
    pub(super) async fn read_snapshot(
        &self,
        request: SnapshotRequest,
    ) -> Result<SnapshotOutcome, StoreError> {
        self.read_snapshot_profile(request, Profile::Raw).await
    }
    #[allow(clippy::too_many_lines)] // One bounded coherent history transaction.
    pub(super) async fn read_snapshot_profile(
        &self,
        request: SnapshotRequest,
        profile: Profile,
    ) -> Result<SnapshotOutcome, StoreError> {
        // Reject oversized untrusted input before even opening a database transaction.
        if request
            .continuation
            .as_ref()
            .is_some_and(|t| t.0.len() > profile.token_cap())
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
                parse_profile(t, &key, &session, profile)?
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
                    let Some((kind, row)) = record::next(&mut tx, &request, &b).await? else {
                    return Ok(SnapshotOutcome::Page(page));
                };
                let mut usage = page.usage;
                if kind == RecordKind::Message {
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
                    let continuation = token_profile(&b, &key, profile)?;
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
                record::append(&mut tx, &request, &mut b, &mut page, &row, kind).await?;
            page.usage = usage;
            }
        }
        .await;
        super::transactions::finish(tx, result, std::convert::identity).await
    }
}
