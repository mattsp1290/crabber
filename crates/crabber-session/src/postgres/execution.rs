//! Legacy fenced execution effects retain native access checks and transaction order.
use super::{
    PostgresExecution, admission_execution, attempt, bump_public_revision, call_status, db, decode,
    decode_record, frontend, inbox_kind, insert_event, insert_message, insert_part, json, phase,
    run_control, save_run, snapshot,
};
use crate::{ExecutionStore, InboxKind, StoreError};
use async_trait::async_trait;
use crabber_core::{
    ContextEpoch, EpochId, EventRecord, Message, Part, PartKind, RunStatus, ToolCallId,
    ToolCallRecord, ToolCallStatus, ToolResult, ToolResultStatus, Usage,
};
use sqlx::Row;
use time::OffsetDateTime;

#[async_trait]
impl ExecutionStore for PostgresExecution {
    async fn stage_frontend_call(
        &self,
        request: crate::FrontendCallStaging,
        authority: crate::AttemptAuthority,
    ) -> Result<crate::FrontendStagingReceipt, crate::ToolContinuationError> {
        self.stage_frontend(request, authority).await
    }
    async fn suspend_tool_continuation(
        &self,
        batch: crate::ToolContinuationSuspension,
        authority: crate::ToolSuspensionAuthority,
    ) -> Result<crate::ToolContinuationReceipt, crate::ToolContinuationError> {
        Box::pin(self.suspend_tools(batch, authority)).await
    }

    async fn observe_run_controls(
        &self,
        turn: Option<&crabber_core::TurnId>,
    ) -> Result<crate::RunControlBoundary, crate::RunControlError> {
        self.control_boundary(turn).await
    }

    async fn start_tool_phase(
        &self,
        start: crate::PhaseStart,
        authority: Option<crate::PhaseAuthority>,
    ) -> Result<crate::PhaseMutationOutcome, crate::PhaseError> {
        self.start_phase(&start, authority).await
    }

    async fn suspend_for_approval(
        &self,
        batch: crate::ApprovalSuspension,
        authority: crate::AttemptAuthority,
    ) -> Result<crate::ApprovalReceipt, crate::ApprovalError> {
        Box::pin(self.suspend_approval(batch, &authority)).await
    }

    async fn conclude_attempt(
        &self,
        conclusion: crate::AttemptConclusion,
        authority: Option<crate::AttemptAuthority>,
    ) -> Result<crate::AttemptMutationOutcome, crate::AttemptError> {
        self.conclude_owned_attempt(conclusion, authority).await
    }

    async fn settle_tool_phase(
        &self,
        settlement: crate::PhaseSettlement,
        authority: Option<crate::PhaseAuthority>,
    ) -> Result<crate::PhaseMutationOutcome, crate::PhaseError> {
        self.settle_phase(&settlement, authority).await
    }

    async fn mutate_phase(
        &self,
        request: crate::PhaseMutation,
        authority: Option<crate::PhaseWriteAuthority>,
    ) -> Result<crate::PhaseMutationOutcome, crate::PhaseError> {
        self.change_phase(request, authority).await
    }

    async fn mutate_attempt(
        &self,
        request: crate::AttemptMutation,
        authority: Option<crate::AttemptAuthority>,
    ) -> Result<crate::AttemptMutationOutcome, crate::AttemptError> {
        self.change_attempt(request, authority).await
    }

    async fn mutate_public_source(
        &self,
        mutation: crate::PublicSourceMutation,
        authority: crate::PublicSourceAuthority,
    ) -> Result<crate::PublicStateReceipt, crate::PublicStateError> {
        if matches!(&authority, crate::PublicSourceAuthority::Invocation(_)) {
            return Err(crate::PublicStateError::Unsupported);
        }
        self.store
            .write_public_source(mutation, authority, Some(&self.fence))
            .await
    }
    async fn mutate_public_state(
        &self,
        mutation: crate::PublicStateMutation,
    ) -> Result<crate::PublicStateReceipt, crate::PublicStateError> {
        self.change_public_state(mutation).await
    }

    async fn begin_admission_execution(&self) -> Result<(), crate::AdmissionExecutionError> {
        admission_execution::begin(self).await
    }
    async fn renew_lease(&self, until: OffsetDateTime) -> Result<(), StoreError> {
        let (mut tx, mut run) = self
            .store
            .ownership_fenced_scoped(&self.fence, crate::transaction::StoreScope::Lease)
            .await?;
        let now = self.store.clock.now();
        if until <= now {
            return Err(StoreError::Validation(
                "lease must extend into the future".into(),
            ));
        }
        run.lease_until = until;
        run.updated_at = now;
        save_run(&mut tx, &run).await?;
        attempt::renew(&mut tx, &run).await?;
        tx.commit().await.map_err(db)
    }
    async fn append_message(&self, message: Message) -> Result<(), StoreError> {
        let (mut tx, run) = self.store.fenced(&self.fence).await?;
        insert_message(&mut tx, &run, &message).await?;
        tx.commit().await.map_err(db)
    }
    async fn append_part(&self, part: Part) -> Result<(), StoreError> {
        let (mut tx, run) = self.store.fenced(&self.fence).await?;
        let row = sqlx::query(
            "SELECT snapshot_record FROM messages WHERE id=$1 AND run_id=$2 FOR UPDATE",
        )
        .bind(&part.message_id.0)
        .bind(&run.id.0)
        .fetch_optional(&mut *tx)
        .await
        .map_err(db)?
        .ok_or(StoreError::NotFound)?;
        let mut message: Message = decode_record(&row.get::<String, _>("snapshot_record"))?;
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
        let accounting = snapshot::message_record(&message)?;
        sqlx::query("UPDATE messages SET data=$2,snapshot_record=$3,snapshot_parts=$4,snapshot_text=$5,snapshot_bytes=$6 WHERE id=$1")
            .bind(&message.id.0)
            .bind(json(&message)?)
            .bind(&accounting.record).bind(accounting.parts).bind(accounting.text).bind(accounting.bytes)
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        bump_public_revision(&mut tx, &run.session_id).await?;
        tx.commit().await.map_err(db)
    }
    async fn append_event(&self, event: EventRecord) -> Result<(), StoreError> {
        let (mut tx, run) = self
            .store
            .fenced_scoped(&self.fence, crate::transaction::StoreScope::Events)
            .await?;
        insert_event(&mut tx, &run, &event).await?;
        tx.commit().await.map_err(db)
    }
    async fn create_tool_call(
        &self,
        call: ToolCallRecord,
        pending_event: EventRecord,
    ) -> Result<(), StoreError> {
        let (mut tx, run) = self.store.fenced(&self.fence).await?;
        if frontend::declared(&mut tx, &run, &call.name)
            .await
            .map_err(|_| StoreError::Validation("frontend declaration integrity failure".into()))?
        {
            return Err(StoreError::Validation(
                "frontend calls require atomic staging".into(),
            ));
        }
        if call.run_id != run.id || call.status != ToolCallStatus::Pending {
            return Err(StoreError::Validation("invalid new tool call".into()));
        }
        let imported: bool =
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM imported_tool_calls WHERE id=$1)")
                .bind(&call.id.0)
                .fetch_one(&mut *tx)
                .await
                .map_err(db)?;
        if imported {
            return Err(StoreError::Conflict);
        }
        let accounting = snapshot::call_record(&call)?;
        sqlx::query("INSERT INTO tool_calls(id,run_id,status,data,snapshot_record,snapshot_parts,snapshot_text,snapshot_bytes) VALUES($1,$2,$3,$4,$5,$6,$7,$8)")
            .bind(&call.id.0)
            .bind(&run.id.0)
            .bind(call_status(call.status))
            .bind(json(&call)?)
            .bind(&accounting.record).bind(accounting.parts).bind(accounting.text).bind(accounting.bytes)
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        insert_event(&mut tx, &run, &pending_event).await?;
        tx.commit().await.map_err(db)
    }
    async fn claim_tool_call(
        &self,
        id: &ToolCallId,
        running_event: EventRecord,
    ) -> Result<(), StoreError> {
        self.claim_legacy_with_proof(
            id,
            &running_event,
            &mut crate::public_session::v2::proof::Proof::bounded(32 << 20),
        )
        .await
    }

    async fn settle_tool_call(
        &self,
        id: &ToolCallId,
        result: ToolResult,
        result_message: Message,
        terminal_event: EventRecord,
    ) -> Result<(), StoreError> {
        let (mut tx, run) = self.store.fenced(&self.fence).await?;
        phase::legacy_guard(&mut tx, &run, id).await?;
        let row = sqlx::query(
            "SELECT snapshot_record FROM tool_calls WHERE id=$1 AND run_id=$2 FOR UPDATE",
        )
        .bind(&id.0)
        .bind(&run.id.0)
        .fetch_optional(&mut *tx)
        .await
        .map_err(db)?
        .ok_or(StoreError::NotFound)?;
        let mut call: ToolCallRecord = decode_record(&row.get::<String, _>("snapshot_record"))?;
        if call.run_id != run.id || call.status != ToolCallStatus::Running {
            return Err(StoreError::Conflict);
        }
        if !result_message.parts.iter().any(|part| matches!(&part.content, crabber_core::ContentBlock::ToolResult { call_id, .. } if call_id == id)) { return Err(StoreError::Validation("tool result message lacks matching result part".into())); }
        insert_message(&mut tx, &run, &result_message).await?;
        insert_event(&mut tx, &run, &terminal_event).await?;
        call.status = match result.status {
            ToolResultStatus::Completed => ToolCallStatus::Completed,
            ToolResultStatus::Failed => ToolCallStatus::Failed,
            ToolResultStatus::Interrupted => ToolCallStatus::Interrupted,
        };
        call.result = Some(result);
        let accounting = snapshot::call_record(&call)?;
        sqlx::query("UPDATE tool_calls SET status=$2,data=$3,snapshot_record=$4,snapshot_parts=$5,snapshot_text=$6,snapshot_bytes=$7 WHERE id=$1")
            .bind(&id.0)
            .bind(call_status(call.status))
            .bind(json(&call)?)
            .bind(&accounting.record).bind(accounting.parts).bind(accounting.text).bind(accounting.bytes)
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        tx.commit().await.map_err(db)
    }
    async fn start_epoch(&self, epoch: ContextEpoch) -> Result<(), StoreError> {
        let (mut tx, mut run) = self.store.fenced(&self.fence).await?;
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
            .bind(json(&epoch)?)
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        run.epoch_id = epoch.id;
        save_run(&mut tx, &run).await?;
        bump_public_revision(&mut tx, &run.session_id).await?;
        tx.commit().await.map_err(db)
    }
    async fn finish_epoch(&self, id: &EpochId, summary: Message) -> Result<(), StoreError> {
        let (mut tx, run) = self.store.fenced(&self.fence).await?;
        let row = sqlx::query("SELECT data FROM epochs WHERE id=$1 AND run_id=$2 FOR UPDATE")
            .bind(&id.0)
            .bind(&run.id.0)
            .fetch_optional(&mut *tx)
            .await
            .map_err(db)?
            .ok_or(StoreError::NotFound)?;
        let mut epoch: ContextEpoch = decode(row.get("data"))?;
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
            .bind(json(&epoch)?)
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        tx.commit().await.map_err(db)
    }
    async fn pause_run(
        &self,
        checkpoint: serde_json::Value,
        event: EventRecord,
    ) -> Result<(), StoreError> {
        let (mut tx, mut run) = self.store.fenced(&self.fence).await?;
        let native: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM execution_attempts WHERE run_id=$1) OR EXISTS(SELECT 1 FROM tool_phases WHERE run_id=$1) OR EXISTS(SELECT 1 FROM continuation_barriers WHERE session_id=$2) OR EXISTS(SELECT 1 FROM tool_continuation_barriers WHERE session_id=$2)",
        )
        .bind(&run.id.0)
        .bind(&run.session_id.0)
        .fetch_one(&mut *tx)
        .await
        .map_err(db)?;
        if native {
            return Err(StoreError::Conflict);
        }
        insert_event(&mut tx, &run, &event).await?;
        let now = self.store.clock.now();
        run.status = RunStatus::Paused;
        run.checkpoint = Some(checkpoint);
        run.lease_until = now;
        run.updated_at = now;
        save_run(&mut tx, &run).await?;
        tx.commit().await.map_err(db)
    }
    async fn settle_run(
        &self,
        status_value: RunStatus,
        error: Option<String>,
        usage: Usage,
        event: EventRecord,
    ) -> Result<(), StoreError> {
        let (mut tx, mut run) = self.store.fenced(&self.fence).await?;
        let native: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM execution_attempts WHERE run_id=$1) OR EXISTS(SELECT 1 FROM tool_phases WHERE run_id=$1) OR EXISTS(SELECT 1 FROM continuation_barriers WHERE session_id=$2) OR EXISTS(SELECT 1 FROM tool_continuation_barriers WHERE session_id=$2)",
        )
        .bind(&run.id.0)
        .bind(&run.session_id.0)
        .fetch_one(&mut *tx)
        .await
        .map_err(db)?;
        if native {
            return Err(StoreError::Conflict);
        }
        if !status_value.is_terminal() {
            return Err(StoreError::Validation(
                "run settlement must be terminal".into(),
            ));
        }
        if status_value == RunStatus::Completed {
            sqlx::query("SELECT 1 FROM sessions WHERE id=$1 FOR UPDATE")
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
            if pending
                || run_control::pending(&mut tx, &run)
                    .await
                    .map_err(crate::run_control::store_error)?
            {
                return Err(StoreError::PendingInput);
            }
        }
        insert_event(&mut tx, &run, &event).await?;
        run.status = status_value;
        run.error = error;
        run.usage = usage;
        run.updated_at = self.store.clock.now();
        save_run(&mut tx, &run).await?;
        run_control::finalize(&mut tx, &run, self.store.clock.now())
            .await
            .map_err(crate::run_control::store_error)?;
        tx.commit().await.map_err(db)
    }
    async fn put_extension_state(
        &self,
        extension_id: &str,
        entries: Vec<(String, Option<String>)>,
    ) -> Result<(), StoreError> {
        crate::storable::ensure_storable_text(extension_id)?;
        let (mut tx, run) = self.store.fenced(&self.fence).await?;
        for (key, value) in entries {
            crate::storable::ensure_storable_text(&key)?;
            if let Some(value) = &value {
                crate::storable::ensure_storable_text(value)?;
            }
            if let Some(value) = value {
                sqlx::query("INSERT INTO extension_state(session_id,extension_id,key,value) VALUES($1,$2,$3,$4) ON CONFLICT(session_id,extension_id,key) DO UPDATE SET value=excluded.value")
                    .bind(&run.session_id.0).bind(extension_id).bind(&key).bind(&value).execute(&mut *tx).await.map_err(db)?;
            } else {
                sqlx::query("DELETE FROM extension_state WHERE session_id=$1 AND extension_id=$2 AND key=$3")
                    .bind(&run.session_id.0).bind(extension_id).bind(&key).execute(&mut *tx).await.map_err(db)?;
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
        if rows.len() > self.store.limits.max_state_entries
            || rows
                .iter()
                .map(|row| row.get::<String, _>("key").len() + row.get::<String, _>("value").len())
                .sum::<usize>()
                > self.store.limits.max_state_bytes
        {
            return Err(StoreError::Limit("extension state".into()));
        }
        tx.commit().await.map_err(db)
    }
    async fn claim_inbox(&self, kind: InboxKind) -> Result<Vec<Message>, StoreError> {
        self.claim(kind, false).await
    }
    async fn claim_inbox_into_history(&self, kind: InboxKind) -> Result<Vec<Message>, StoreError> {
        self.claim(kind, true).await
    }
}
impl PostgresExecution {
    async fn claim(&self, kind: InboxKind, into_history: bool) -> Result<Vec<Message>, StoreError> {
        let (mut tx, run) = self.store.fenced(&self.fence).await?;
        let rows = sqlx::query("SELECT seq,snapshot_record FROM inbox WHERE session_id=$1 AND kind=$2 AND consumed_by_run IS NULL ORDER BY seq FOR UPDATE")
            .bind(&run.session_id.0).bind(inbox_kind(kind)).fetch_all(&mut *tx).await.map_err(db)?;
        let mut claimed = Vec::with_capacity(rows.len());
        for row in rows {
            let mut message: Message = decode_record(&row.get::<String, _>("snapshot_record"))?;
            if into_history {
                message.run_id = Some(run.id.clone());
                insert_message(&mut tx, &run, &message).await?;
            }
            sqlx::query("UPDATE inbox SET consumed_by_run=$2 WHERE seq=$1")
                .bind(row.get::<i64, _>("seq"))
                .bind(&run.id.0)
                .execute(&mut *tx)
                .await
                .map_err(db)?;
            claimed.push(message);
        }
        if !into_history && !claimed.is_empty() {
            bump_public_revision(&mut tx, &run.session_id).await?;
        }
        tx.commit().await.map_err(db)?;
        Ok(claimed)
    }
}
