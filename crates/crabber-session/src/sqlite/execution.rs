//! Fenced execution effects; each call owns one immediate write transaction.
use super::records::{
    call_status, decode_row, inbox_kind, insert_event, insert_message, insert_part, save_run,
};
use super::transactions::finish;
use super::{SqliteStore, StoreError, admission_execution, db, snapshot, text};
use crate::{ExecutionStore, InboxKind};
use async_trait::async_trait;
use crabber_core::{
    ContextEpoch, EpochId, EventRecord, Message, Part, PartKind, RunFence, RunStatus, ToolCallId,
    ToolCallRecord, ToolCallStatus, ToolResult, ToolResultStatus, Usage,
};
use sqlx::Row;
use time::OffsetDateTime;

pub(super) struct SqliteExecution {
    pub(super) store: SqliteStore,
    pub(super) fence: RunFence,
}

#[async_trait]
impl ExecutionStore for SqliteExecution {
    async fn begin_admission_execution(&self) -> Result<(), crate::AdmissionExecutionError> {
        admission_execution::begin(self).await
    }
    async fn renew_lease(&self, until: OffsetDateTime) -> Result<(), StoreError> {
        let mut tx = self.store.begin_write().await?;
        let result = async {
            let mut run = self.store.ownership_fenced(&mut tx, &self.fence).await?;
            let now = self.store.clock.now();
            if until <= now {
                return Err(StoreError::Validation(
                    "lease must extend into the future".into(),
                ));
            }
            run.lease_until = until;
            run.updated_at = now;
            save_run(&mut tx, &run).await?;
            Ok(())
        }
        .await;
        finish(tx, result, std::convert::identity).await
    }
    async fn append_message(&self, message: Message) -> Result<(), StoreError> {
        let mut tx = self.store.begin_write().await?;
        let result = async {
            let run = self.store.fenced(&mut tx, &self.fence).await?;
            insert_message(&mut tx, &run, &message).await?;
            Ok(())
        }
        .await;
        finish(tx, result, std::convert::identity).await
    }
    async fn append_part(&self, part: Part) -> Result<(), StoreError> {
        let mut tx = self.store.begin_write().await?;
        let result = async {
            let run = self.store.fenced(&mut tx, &self.fence).await?;
            let row = sqlx::query("SELECT data FROM messages WHERE id=$1")
                .bind(&part.message_id.0)
                .fetch_optional(&mut *tx)
                .await
                .map_err(db)?
                .ok_or(StoreError::NotFound)?;
            let mut message: Message = decode_row(&row, "data")?;
            if message.session_id != run.session_id || message.run_id.as_ref() != Some(&run.id) {
                return Err(StoreError::Validation("part belongs to another run".into()));
            }
            if message
                .parts
                .iter()
                .any(|old| old.id == part.id || old.ordinal == part.ordinal)
            {
                return Err(StoreError::Conflict);
            }
            insert_part(&mut tx, &part).await?;
            message.parts.push(part);
            message.parts.sort_by_key(|part| part.ordinal);
            text(&message)?;
            let accounting = snapshot::message_record(&message)?;
            sqlx::query(
                "UPDATE messages SET \
                    data=$2,snapshot_parts=$3,snapshot_text=$4,snapshot_bytes=$5 WHERE id=$1",
            )
            .bind(&message.id.0)
            .bind(&accounting.record)
            .bind(accounting.parts)
            .bind(accounting.text)
            .bind(accounting.bytes)
            .execute(&mut *tx)
            .await
            .map_err(db)?;
            Ok(())
        }
        .await;
        finish(tx, result, std::convert::identity).await
    }
    async fn append_event(&self, event: EventRecord) -> Result<(), StoreError> {
        let mut tx = self.store.begin_write().await?;
        let result = async {
            let run = self.store.fenced(&mut tx, &self.fence).await?;
            insert_event(&mut tx, &run, &event).await?;
            Ok(())
        }
        .await;
        finish(tx, result, std::convert::identity).await
    }
    async fn create_tool_call(
        &self,
        call: ToolCallRecord,
        pending_event: EventRecord,
    ) -> Result<(), StoreError> {
        let mut tx = self.store.begin_write().await?;
        let result = async {
            let run = self.store.fenced(&mut tx, &self.fence).await?;
            if call.run_id != run.id || call.status != ToolCallStatus::Pending {
                return Err(StoreError::Validation("invalid new tool call".into()));
            }
            insert_event(&mut tx, &run, &pending_event).await?;
            text(&call)?;
            let accounting = snapshot::call_record(&call)?;
            sqlx::query(
                "INSERT INTO \
                    tool_calls(id,run_id,status,data,snapshot_parts,snapshot_text,snapshot_bytes) \
                    VALUES($1,$2,$3,$4,$5,$6,$7)",
            )
            .bind(&call.id.0)
            .bind(&run.id.0)
            .bind(call_status(call.status))
            .bind(&accounting.record)
            .bind(accounting.parts)
            .bind(accounting.text)
            .bind(accounting.bytes)
            .execute(&mut *tx)
            .await
            .map_err(db)?;
            Ok(())
        }
        .await;
        finish(tx, result, std::convert::identity).await
    }
    async fn claim_tool_call(
        &self,
        id: &ToolCallId,
        running_event: EventRecord,
    ) -> Result<(), StoreError> {
        let mut tx = self.store.begin_write().await?;
        let result = async {
            let run = self.store.fenced(&mut tx, &self.fence).await?;
            let row = sqlx::query("SELECT data FROM tool_calls WHERE id=$1")
                .bind(&id.0)
                .fetch_optional(&mut *tx)
                .await
                .map_err(db)?
                .ok_or(StoreError::NotFound)?;
            let mut call: ToolCallRecord = decode_row(&row, "data")?;
            if call.run_id != run.id || call.status != ToolCallStatus::Pending {
                return Err(StoreError::Conflict);
            }
            insert_event(&mut tx, &run, &running_event).await?;
            call.status = ToolCallStatus::Running;
            text(&call)?;
            let accounting = snapshot::call_record(&call)?;
            sqlx::query(
                "UPDATE tool_calls SET \
                    status=$2,data=$3,snapshot_parts=$4,snapshot_text=$5,snapshot_bytes=$6 \
                    WHERE id=$1",
            )
            .bind(&id.0)
            .bind(call_status(call.status))
            .bind(&accounting.record)
            .bind(accounting.parts)
            .bind(accounting.text)
            .bind(accounting.bytes)
            .execute(&mut *tx)
            .await
            .map_err(db)?;
            Ok(())
        }
        .await;
        finish(tx, result, std::convert::identity).await
    }
    async fn settle_tool_call(
        &self,
        id: &ToolCallId,
        result: ToolResult,
        result_message: Message,
        terminal_event: EventRecord,
    ) -> Result<(), StoreError> {
        let mut tx = self.store.begin_write().await?;
        let result = async {
            let run = self.store.fenced(&mut tx, &self.fence).await?;
            let row = sqlx::query("SELECT data FROM tool_calls WHERE id=$1")
                .bind(&id.0)
                .fetch_optional(&mut *tx)
                .await
                .map_err(db)?
                .ok_or(StoreError::NotFound)?;
            let mut call: ToolCallRecord = decode_row(&row, "data")?;
            if call.run_id != run.id || call.status != ToolCallStatus::Running {
                return Err(StoreError::Conflict);
            }
            if !result_message.parts.iter().any(|part| {
                matches!(&part.content,
        crabber_core::ContentBlock::ToolResult { call_id, .. } if call_id == id)
            }) {
                return Err(StoreError::Validation(
                    "tool result message lacks matching result part".into(),
                ));
            }
            insert_message(&mut tx, &run, &result_message).await?;
            insert_event(&mut tx, &run, &terminal_event).await?;
            call.status = match result.status {
                ToolResultStatus::Completed => ToolCallStatus::Completed,
                ToolResultStatus::Failed => ToolCallStatus::Failed,
                ToolResultStatus::Interrupted => ToolCallStatus::Interrupted,
            };
            call.result = Some(result);
            text(&call)?;
            let accounting = snapshot::call_record(&call)?;
            sqlx::query(
                "UPDATE tool_calls SET \
    status=$2,data=$3,snapshot_parts=$4,snapshot_text=$5,snapshot_bytes=$6 \
    WHERE id=$1",
            )
            .bind(&id.0)
            .bind(call_status(call.status))
            .bind(&accounting.record)
            .bind(accounting.parts)
            .bind(accounting.text)
            .bind(accounting.bytes)
            .execute(&mut *tx)
            .await
            .map_err(db)?;
            Ok(())
        }
        .await;
        finish(tx, result, std::convert::identity).await
    }
    async fn start_epoch(&self, epoch: ContextEpoch) -> Result<(), StoreError> {
        let mut tx = self.store.begin_write().await?;
        let result = async {
            let mut run = self.store.fenced(&mut tx, &self.fence).await?;
            if epoch.run_id != run.id
                || epoch.session_id != run.session_id
                || epoch.parent.as_ref() != Some(&run.epoch_id)
            {
                return Err(StoreError::Validation("invalid context epoch".into()));
            }
            sqlx::query("INSERT INTO epochs(id,session_id,run_id,data) VALUES($1,$2,$3,$4)")
                .bind(&epoch.id.0)
                .bind(&epoch.session_id.0)
                .bind(&run.id.0)
                .bind(text(&epoch)?)
                .execute(&mut *tx)
                .await
                .map_err(db)?;
            run.epoch_id = epoch.id;
            save_run(&mut tx, &run).await?;
            Ok(())
        }
        .await;
        finish(tx, result, std::convert::identity).await
    }
    async fn finish_epoch(&self, id: &EpochId, summary: Message) -> Result<(), StoreError> {
        let mut tx = self.store.begin_write().await?;
        let result = async {
            let run = self.store.fenced(&mut tx, &self.fence).await?;
            let row = sqlx::query("SELECT data FROM epochs WHERE id=$1")
                .bind(&id.0)
                .fetch_optional(&mut *tx)
                .await
                .map_err(db)?
                .ok_or(StoreError::NotFound)?;
            let mut epoch: ContextEpoch = decode_row(&row, "data")?;
            if epoch.run_id != run.id || epoch.summary_message_id.is_some() || run.epoch_id != *id {
                return Err(StoreError::Conflict);
            }
            if !summary
                .parts
                .iter()
                .any(|part| part.kind == PartKind::CompactionSummary)
            {
                return Err(StoreError::Validation(
                    "summary message lacks compaction part".into(),
                ));
            }
            insert_message(&mut tx, &run, &summary).await?;
            epoch.summary_message_id = Some(summary.id);
            sqlx::query("UPDATE epochs SET data=$2 WHERE id=$1")
                .bind(&id.0)
                .bind(text(&epoch)?)
                .execute(&mut *tx)
                .await
                .map_err(db)?;
            Ok(())
        }
        .await;
        finish(tx, result, std::convert::identity).await
    }
    async fn pause_run(
        &self,
        checkpoint: serde_json::Value,
        event: EventRecord,
    ) -> Result<(), StoreError> {
        let mut tx = self.store.begin_write().await?;
        let result = async {
            let mut run = self.store.fenced(&mut tx, &self.fence).await?;
            insert_event(&mut tx, &run, &event).await?;
            let now = self.store.clock.now();
            run.status = RunStatus::Paused;
            run.checkpoint = Some(checkpoint);
            run.lease_until = now;
            run.updated_at = now;
            save_run(&mut tx, &run).await?;
            Ok(())
        }
        .await;
        finish(tx, result, std::convert::identity).await
    }
    async fn settle_run(
        &self,
        status_value: RunStatus,
        error: Option<String>,
        usage: Usage,
        event: EventRecord,
    ) -> Result<(), StoreError> {
        let mut tx = self.store.begin_write().await?;
        let result = async {
            let mut run = self.store.fenced(&mut tx, &self.fence).await?;
            if !status_value.is_terminal() {
                return Err(StoreError::Validation(
                    "run settlement must be terminal".into(),
                ));
            }
            if status_value == RunStatus::Completed {
                sqlx::query("SELECT 1 FROM sessions WHERE id=$1")
                    .bind(&run.session_id.0)
                    .fetch_one(&mut *tx)
                    .await
                    .map_err(db)?;
                let pending = sqlx::query(
                    "SELECT 1 FROM inbox WHERE session_id=$1 AND consumed_by_run IS NULL LIMIT 1",
                )
                .bind(&run.session_id.0)
                .fetch_optional(&mut *tx)
                .await
                .map_err(db)?
                .is_some();
                if pending {
                    return Err(StoreError::PendingInput);
                }
            }
            insert_event(&mut tx, &run, &event).await?;
            run.status = status_value;
            run.error = error;
            run.usage = usage;
            run.updated_at = self.store.clock.now();
            save_run(&mut tx, &run).await?;
            Ok(())
        }
        .await;
        finish(tx, result, std::convert::identity).await
    }
    async fn put_extension_state(
        &self,
        extension_id: &str,
        entries: Vec<(String, Option<String>)>,
    ) -> Result<(), StoreError> {
        crate::storable::ensure_storable_text(extension_id)?;
        let mut tx = self.store.begin_write().await?;
        let result = async {
            let run = self.store.fenced(&mut tx, &self.fence).await?;
            for (key, value) in entries {
                crate::storable::ensure_storable_text(&key)?;
                if let Some(value) = &value {
                    crate::storable::ensure_storable_text(value)?;
                }
                if let Some(value) = value {
                    sqlx::query(
                        "INSERT INTO extension_state(session_id,extension_id,key,value) \
                            VALUES($1,$2,$3,$4) ON CONFLICT(session_id,extension_id,key) DO UPDATE SET \
                            value=excluded.value",
                    )
                    .bind(&run.session_id.0)
                    .bind(extension_id)
                    .bind(&key)
                    .bind(&value)
                    .execute(&mut *tx)
                    .await
                    .map_err(db)?;
                } else {
                    sqlx::query(
                        "DELETE FROM extension_state WHERE session_id=$1 AND extension_id=$2 AND \
                            key=$3",
                    )
                    .bind(&run.session_id.0)
                    .bind(extension_id)
                    .bind(&key)
                    .execute(&mut *tx)
                    .await
                    .map_err(db)?;
                }
            }
            let rows = sqlx::query(
                "SELECT key,value FROM extension_state WHERE session_id=$1 AND extension_id=$2",
            )
            .bind(&run.session_id.0)
            .bind(extension_id)
            .fetch_all(&mut *tx)
            .await
            .map_err(db)?;
            let bytes = rows.iter().try_fold(0usize, |total, row| {
                let key: String = row.try_get("key").map_err(db)?;
                let value: String = row.try_get("value").map_err(db)?;
                Ok::<_, StoreError>(total.saturating_add(key.len()).saturating_add(value.len()))
            })?;
            if rows.len() > self.store.limits.max_state_entries
                || bytes > self.store.limits.max_state_bytes
            {
                return Err(StoreError::Limit("extension state".into()));
            }
            Ok(())
        }
        .await;
        finish(tx, result, std::convert::identity).await
    }
    async fn claim_inbox(&self, kind: InboxKind) -> Result<Vec<Message>, StoreError> {
        self.claim(kind, false).await
    }
    async fn claim_inbox_into_history(&self, kind: InboxKind) -> Result<Vec<Message>, StoreError> {
        self.claim(kind, true).await
    }
}
impl SqliteExecution {
    async fn claim(&self, kind: InboxKind, into_history: bool) -> Result<Vec<Message>, StoreError> {
        let mut tx = self.store.begin_write().await?;
        let result = async {
            let run = self.store.fenced(&mut tx, &self.fence).await?;
            let rows = sqlx::query(
                "SELECT seq,data FROM inbox WHERE session_id=$1 AND kind=$2 AND \
                    consumed_by_run IS NULL ORDER BY seq",
            )
            .bind(&run.session_id.0)
            .bind(inbox_kind(kind))
            .fetch_all(&mut *tx)
            .await
            .map_err(db)?;
            let mut claimed = Vec::with_capacity(rows.len());
            for row in rows {
                let mut message: Message = decode_row(&row, "data")?;
                if into_history {
                    message.run_id = Some(run.id.clone());
                    insert_message(&mut tx, &run, &message).await?;
                }
                sqlx::query("UPDATE inbox SET consumed_by_run=$2 WHERE seq=$1")
                    .bind(row.try_get::<i64, _>("seq").map_err(db)?)
                    .bind(&run.id.0)
                    .execute(&mut *tx)
                    .await
                    .map_err(db)?;
                claimed.push(message);
            }
            Ok(claimed)
        }
        .await;
        finish(tx, result, std::convert::identity).await
    }
}
