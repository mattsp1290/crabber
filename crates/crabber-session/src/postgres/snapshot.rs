//! Metadata-first bounded reads. Canonical serde text avoids JSONB's whitespace
//! and number normalization changing exact encoded-record accounting.
use super::{
    EventCursor, Message, Postgres, PostgresStore, Row, Serialize, StoreError, ToolCallRecord,
    Transaction, db, decode,
};
use crate::{SnapshotContinuation, SnapshotOutcome, SnapshotPage, SnapshotRequest, SnapshotUsage};
use hmac::{Hmac, Mac};
use serde::Deserialize;
use sha2::{Digest, Sha256};

pub(super) struct Accounting {
    pub record: String,
    pub parts: i64,
    pub text: i64,
    pub bytes: i64,
}
fn accounting<T: Serialize>(
    record: &T,
    parts: usize,
    text: usize,
) -> Result<Accounting, StoreError> {
    let record = serde_json::to_string(record).map_err(|_| invalid())?;
    Ok(Accounting {
        parts: i64::try_from(parts).map_err(|_| invalid())?,
        text: i64::try_from(text).map_err(|_| invalid())?,
        bytes: i64::try_from(record.len()).map_err(|_| invalid())?,
        record,
    })
}
pub(super) fn message_record(message: &Message) -> Result<Accounting, StoreError> {
    accounting(
        message,
        message.parts.len(),
        message.parts.iter().fold(0usize, |n, p| {
            n.saturating_add(crate::snapshot::text_bytes(&p.content))
        }),
    )
}
pub(super) fn call_record(call: &ToolCallRecord) -> Result<Accounting, StoreError> {
    accounting(
        call,
        0,
        call.result.as_ref().map_or(0, |r| {
            r.content.iter().fold(0usize, |n, b| {
                n.saturating_add(crate::snapshot::text_bytes(b))
            })
        }),
    )
}

pub(super) async fn migrate(tx: &mut Transaction<'_, Postgres>) -> Result<(), StoreError> {
    let done: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM schema_version WHERE version=3)")
            .fetch_one(&mut **tx)
            .await
            .map_err(db)?;
    if done {
        return Ok(());
    }
    // No concurrent legacy writer can change a record between decode and backfill.
    sqlx::raw_sql(
        "LOCK TABLE sessions,runs,messages,tool_calls,events,inbox IN ACCESS EXCLUSIVE MODE",
    )
    .execute(&mut **tx)
    .await
    .map_err(db)?;
    sqlx::raw_sql(include_str!("../../migrations/0003_snapshots.sql"))
        .execute(&mut **tx)
        .await
        .map_err(db)?;
    sqlx::query("INSERT INTO snapshot_auth(secret) VALUES($1)")
        .bind(uuid::Uuid::new_v4().to_string())
        .execute(&mut **tx)
        .await
        .map_err(db)?;
    // Legacy migration deliberately decodes ONE record at a time, not a history
    // vector. Subsequent bounded reads never decode an over-budget record.
    for table in ["messages", "tool_calls", "inbox"] {
        let mut after = 0i64;
        loop {
            let query = format!("SELECT seq,data FROM {table} WHERE seq>$1 ORDER BY seq LIMIT 1");
            let Some(row) = sqlx::query(&query)
                .bind(after)
                .fetch_optional(&mut **tx)
                .await
                .map_err(db)?
            else {
                break;
            };
            after = row.get("seq");
            let a = if table == "tool_calls" {
                call_record(&decode::<ToolCallRecord>(row.get("data"))?)?
            } else {
                message_record(&decode::<Message>(row.get("data"))?)?
            };
            if table == "inbox" {
                sqlx::query("UPDATE inbox SET snapshot_record=$2 WHERE seq=$1")
                    .bind(after)
                    .bind(a.record)
                    .execute(&mut **tx)
                    .await
                    .map_err(db)?;
            } else {
                let query = format!(
                    "UPDATE {table} SET snapshot_record=$2,snapshot_parts=$3,snapshot_text=$4,snapshot_bytes=$5 WHERE seq=$1"
                );
                sqlx::query(&query)
                    .bind(after)
                    .bind(a.record)
                    .bind(a.parts)
                    .bind(a.text)
                    .bind(a.bytes)
                    .execute(&mut **tx)
                    .await
                    .map_err(db)?;
            }
        }
    }
    sqlx::raw_sql(include_str!("../../migrations/0003_snapshot_guards.sql"))
        .execute(&mut **tx)
        .await
        .map_err(db)?;
    Ok(())
}

#[derive(Serialize, Deserialize)]
struct Boundary {
    session: String,
    revision: i64,
    messages: i64,
    calls: i64,
    message_after: i64,
    call_after: i64,
    high_water: EventCursor,
}
fn invalid() -> StoreError {
    StoreError::Validation("invalid snapshot continuation or record".into())
}
fn digest(parts: &[&str]) -> String {
    let mut hash = Sha256::new();
    for part in parts {
        hash.update(part.len().to_le_bytes());
        hash.update(part.as_bytes());
    }
    format!("{:x}", hash.finalize())
}
fn signature(key: &str, json: &str) -> Result<String, StoreError> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key.as_bytes()).map_err(|_| invalid())?;
    mac.update(json.as_bytes());
    Ok(format!("{:x}", mac.finalize().into_bytes()))
}
fn token(boundary: &Boundary, key: &str) -> Result<SnapshotContinuation, StoreError> {
    let json = serde_json::to_string(boundary).map_err(|_| invalid())?;
    Ok(SnapshotContinuation(format!(
        "{}:{json}",
        signature(key, &json)?
    )))
}
fn parse(token: &SnapshotContinuation, key: &str, session: &str) -> Result<Boundary, StoreError> {
    if token.0.len() > 2048 {
        return Err(invalid());
    }
    let (supplied_signature, json) = token.0.split_once(':').ok_or_else(invalid)?;
    let expected = signature(key, json)?;
    if supplied_signature.len() != expected.len()
        || supplied_signature
            .bytes()
            .zip(expected.bytes())
            .fold(0u8, |diff, (a, b)| diff | (a ^ b))
            != 0
    {
        return Err(invalid());
    }
    let b: Boundary = serde_json::from_str(json).map_err(|_| invalid())?;
    if b.session != session
        || b.revision < 0
        || b.message_after < 0
        || b.call_after < 0
        || b.message_after > b.messages
        || b.call_after > b.calls
    {
        return Err(invalid());
    }
    Ok(b)
}
impl PostgresStore {
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
        let mut tx = self.pool.begin().await.map_err(db)?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        let revision: i64 =
            sqlx::query_scalar("SELECT snapshot_revision FROM sessions WHERE id=$1")
                .bind(&request.session_id.0)
                .fetch_optional(&mut *tx)
                .await
                .map_err(db)?
                .ok_or(StoreError::NotFound)?;
        let key: String = sqlx::query_scalar("SELECT secret FROM snapshot_auth WHERE singleton")
            .fetch_one(&mut *tx)
            .await
            .map_err(db)?;
        let session = digest(&[&request.session_id.0]);
        let mut b = if let Some(t) = &request.continuation {
            parse(t, &key, &session)?
        } else {
            let row = sqlx::query("SELECT coalesce((SELECT max(seq) FROM messages WHERE session_id=$1),0) AS messages, coalesce((SELECT max(t.seq) FROM tool_calls t JOIN runs r ON r.id=t.run_id WHERE r.session_id=$1),0) AS calls, coalesce((SELECT max(seq) FROM events WHERE session_id=$1),0) AS high_water").bind(&request.session_id.0).fetch_one(&mut *tx).await.map_err(db)?;
            Boundary {
                session,
                revision,
                messages: row.get("messages"),
                calls: row.get("calls"),
                message_after: 0,
                call_after: 0,
                high_water: EventCursor(
                    u64::try_from(row.get::<i64, _>("high_water")).map_err(|_| invalid())?,
                ),
            }
        };
        if revision != b.revision {
            return Ok(SnapshotOutcome::Invalidated {
                high_water: b.high_water,
            });
        }
        let mut page = SnapshotPage {
            high_water: b.high_water,
            messages: Vec::new(),
            tool_calls: Vec::new(),
            usage: SnapshotUsage::default(),
            continuation: None,
        };
        loop {
            // Only scalar accounting crosses the network before all caps pass.
            let message = sqlx::query("SELECT seq,snapshot_parts,snapshot_text,snapshot_bytes FROM messages WHERE session_id=$1 AND seq>$2 AND seq<=$3 ORDER BY seq LIMIT 1").bind(&request.session_id.0).bind(b.message_after).bind(b.messages).fetch_optional(&mut *tx).await.map_err(db)?;
            let is_message = message.is_some();
            let row = if let Some(row) = message {
                Some(row)
            } else {
                sqlx::query("SELECT t.seq,t.snapshot_parts,t.snapshot_text,t.snapshot_bytes FROM tool_calls t JOIN runs r ON r.id=t.run_id WHERE r.session_id=$1 AND t.seq>$2 AND t.seq<=$3 ORDER BY t.seq LIMIT 1").bind(&request.session_id.0).bind(b.call_after).bind(b.calls).fetch_optional(&mut *tx).await.map_err(db)?
            };
            let Some(row) = row else {
                tx.commit().await.map_err(db)?;
                return Ok(SnapshotOutcome::Page(page));
            };
            let mut usage = page.usage;
            if is_message {
                usage.messages = usage.messages.saturating_add(1);
            } else {
                usage.tool_calls = usage.tool_calls.saturating_add(1);
            }
            usage.parts = usage.parts.saturating_add(
                usize::try_from(row.get::<i64, _>("snapshot_parts")).map_err(|_| invalid())?,
            );
            usage.text_bytes = usage.text_bytes.saturating_add(
                usize::try_from(row.get::<i64, _>("snapshot_text")).map_err(|_| invalid())?,
            );
            usage.encoded_bytes = usage.encoded_bytes.saturating_add(
                usize::try_from(row.get::<i64, _>("snapshot_bytes")).map_err(|_| invalid())?,
            );
            if let Some(limit) = usage.exceeded(request.limits) {
                let continuation = token(&b, &key)?;
                tx.commit().await.map_err(db)?;
                if page.usage.messages == 0 && page.usage.tool_calls == 0 {
                    return Ok(SnapshotOutcome::Limited {
                        high_water: b.high_water,
                        limit,
                        continuation,
                    });
                }
                page.continuation = Some(continuation);
                return Ok(SnapshotOutcome::Page(page));
            }
            let seq: i64 = row.get("seq");
            let query = if is_message {
                "SELECT snapshot_record FROM messages WHERE seq=$1"
            } else {
                "SELECT snapshot_record FROM tool_calls WHERE seq=$1"
            };
            let record: String = sqlx::query_scalar(query)
                .bind(seq)
                .fetch_one(&mut *tx)
                .await
                .map_err(db)?;
            if is_message {
                page.messages
                    .push(serde_json::from_str(&record).map_err(|_| invalid())?);
                b.message_after = seq;
            } else {
                page.tool_calls
                    .push(serde_json::from_str(&record).map_err(|_| invalid())?);
                b.call_after = seq;
            }
            page.usage = usage;
        }
    }
}

#[cfg(test)]
mod token_tests {
    use super::*;
    #[test]
    fn another_database_key_cannot_authenticate_a_boundary() {
        let boundary = Boundary {
            session: digest(&["session"]),
            revision: 0,
            messages: 1,
            calls: 0,
            message_after: 0,
            call_after: 0,
            high_water: EventCursor(7),
        };
        let token = token(&boundary, "database-one-secret").unwrap();
        assert!(parse(&token, "database-one-secret", &boundary.session).is_ok());
        assert!(parse(&token, "database-two-secret", &boundary.session).is_err());
    }
}
