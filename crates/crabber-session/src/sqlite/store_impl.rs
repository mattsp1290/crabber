//! Store contract, with read-only calls routed through the reader pool.
use super::records::{
    decode_row, inbox_kind, load_run, lookup_receipt, messages, project, save_run,
};
use super::transactions::finish;
use super::{SqliteExecution, SqliteStore, StoreError, abandon, admission_execution, db, text};
use crate::{
    AdmitOutcome, AdmitRequest, ExecutionStore, InboxKind, KeyedAdmitOutcome, KeyedAdmitRequest,
    Store,
};
use async_trait::async_trait;
use crabber_core::{
    AdmissionKey, AdmissionReceipt, EpochId, EventCursor, EventRecord, Message, Run, RunFence,
    RunId, Session, SessionId, ToolCallRecord,
};
use sqlx::Row;
use std::collections::BTreeMap;

#[async_trait]
impl Store for SqliteStore {
    async fn terminate_approval(
        &self,
        request: crate::ApprovalTermination,
    ) -> Result<crate::ApprovalReceipt, crate::ApprovalError> {
        self.terminate_approval_inner(&request).await
    }
    async fn mark_attempt_uncertain(
        &self,
        request: crate::AttemptUncertainty,
    ) -> Result<crate::AttemptControlOutcome, crate::AttemptError> {
        self.host_attempt(crate::attempt_control::Control::Uncertain(request))
            .await
    }
    async fn retire_attempt(
        &self,
        request: crate::AttemptRetirement,
    ) -> Result<crate::AttemptControlOutcome, crate::AttemptError> {
        self.host_attempt(crate::attempt_control::Control::Retire(request))
            .await
    }

    async fn reclaim_approval_continuation(
        &self,
        request: crate::ApprovalContinuationRecovery,
    ) -> Result<crate::ApprovalClaimOutcome, crate::ApprovalError> {
        self.recover_approval(&request).await
    }
    async fn manage_session(
        &self,
        request: crate::ManageSession,
    ) -> Result<crate::ManagementReceipt, crate::ManagementError> {
        self.manage(request).await
    }
    async fn lookup_session_management(
        &self,
        session: &SessionId,
        id: &crabber_core::ControlId,
    ) -> Result<Option<crate::ManagementReceipt>, crate::ManagementError> {
        self.management_receipt(session, id).await
    }
    async fn session_summary(
        &self,
        session: &SessionId,
    ) -> Result<crate::SessionSummary, crate::ManagementError> {
        self.read_summary(session).await
    }
    async fn list_sessions(
        &self,
        request: crate::ListSessions,
    ) -> Result<crate::SessionListPage, crate::ManagementError> {
        self.summaries(request).await
    }

    async fn claim_approval_continuation(
        &self,
        claim: crate::ApprovalContinuationClaim,
    ) -> Result<crate::ApprovalClaimOutcome, crate::ApprovalError> {
        self.claim_approval(&claim).await
    }

    async fn answer_approval(
        &self,
        answer: crate::ApprovalAnswer,
    ) -> Result<crate::ApprovalReceipt, crate::ApprovalError> {
        self.answer_approval_inner(&answer).await
    }

    async fn approval_snapshot(
        &self,
        session: &SessionId,
        run: &RunId,
    ) -> Result<Option<crate::ApprovalSnapshot>, crate::ApprovalError> {
        self.read_approval(session, run).await
    }
    async fn lookup_approval(
        &self,
        session: &SessionId,
        id: &crabber_core::ControlId,
    ) -> Result<Option<crate::ApprovalReceipt>, crate::ApprovalError> {
        self.approval_receipt(session, id).await
    }
    fn supports_owned_attempts(&self) -> bool {
        true
    }
    fn supports_owned_tool_phases(&self) -> bool {
        true
    }
    fn supports_pre_effect_approvals(&self) -> bool {
        true
    }

    async fn mark_phase_uncertain(
        &self,
        request: crate::PhaseUncertainty,
    ) -> Result<crate::PhaseControlOutcome, crate::PhaseError> {
        request.fingerprint()?;
        self.host_phase(
            &request.run_id,
            request.mutation(),
            crate::phase::PhaseEffect::Uncertainty(&request),
        )
        .await
    }
    async fn record_phase_cleanup(
        &self,
        request: crate::PhaseCleanup,
    ) -> Result<crate::PhaseControlOutcome, crate::PhaseError> {
        request.fingerprint()?;
        self.host_phase(
            &request.run_id,
            request.mutation(),
            crate::phase::PhaseEffect::Cleanup(&request),
        )
        .await
    }
    async fn list_tool_phases(
        &self,
        request: crate::PhasePageRequest,
    ) -> Result<crate::PhasePage, crate::PhaseError> {
        self.phase_page(&request).await
    }
    async fn resolve_uncertain_phase(
        &self,
        resolution: crate::PhaseResolution,
    ) -> Result<crate::PhaseResolutionOutcome, crate::PhaseError> {
        self.resolve_phase(&resolution).await
    }

    async fn tool_phase(
        &self,
        run: &RunId,
        id: &crabber_core::PhaseId,
    ) -> Result<crate::VersionedPhase, crate::PhaseError> {
        self.read_phase(run, id).await
    }
    async fn lookup_phase_mutation(
        &self,
        session: &SessionId,
        id: &crabber_core::ControlId,
    ) -> Result<Option<crate::PhaseReceipt>, crate::PhaseError> {
        self.phase_receipt(session, id).await
    }

    async fn execution_attempt(
        &self,
        run: &RunId,
        id: &crabber_core::AttemptId,
    ) -> Result<crate::VersionedAttempt, crate::AttemptError> {
        self.read_attempt(run, Some(id))
            .await?
            .ok_or(crate::AttemptError::NotFound)
    }
    async fn latest_execution_attempt(
        &self,
        run: &RunId,
    ) -> Result<Option<crate::VersionedAttempt>, crate::AttemptError> {
        self.read_attempt(run, None).await
    }
    async fn lookup_attempt_mutation(
        &self,
        session: &SessionId,
        id: &crabber_core::ControlId,
    ) -> Result<Option<crate::AttemptReceipt>, crate::AttemptError> {
        self.attempt_receipt(session, id).await
    }

    async fn public_state(
        &self,
        session: &SessionId,
    ) -> Result<crate::PublicStateSnapshot, crate::PublicStateError> {
        self.read_public_state(session).await
    }
    async fn lookup_public_state_mutation(
        &self,
        session: &SessionId,
        id: &crabber_core::ControlId,
    ) -> Result<Option<crate::PublicStateReceipt>, crate::PublicStateError> {
        self.public_receipt(session, id).await
    }

    async fn session_revision(
        &self,
        session: &SessionId,
    ) -> Result<crabber_core::SessionRevision, StoreError> {
        let value: i64 = sqlx::query_scalar("SELECT public_revision FROM sessions WHERE id=$1")
            .bind(&session.0)
            .fetch_optional(&self.readers)
            .await
            .map_err(db)?
            .ok_or(StoreError::NotFound)?;
        crabber_core::SessionRevision::new(
            u64::try_from(value)
                .map_err(|_| StoreError::Validation("invalid public session revision".into()))?,
        )
    }
    async fn admit_run(&self, request: AdmitRequest) -> Result<AdmitOutcome, StoreError> {
        self.admit_with_binding(request, None).await
    }

    async fn admit_keyed_run(
        &self,
        request: KeyedAdmitRequest,
    ) -> Result<KeyedAdmitOutcome, StoreError> {
        self.admit_keyed_with_binding(request, None).await
    }
    async fn admit_bound_run(
        &self,
        request: crate::BoundAdmitRequest,
    ) -> Result<AdmitOutcome, StoreError> {
        self.admit_with_binding(request.request, Some(request.binding))
            .await
    }
    async fn admit_bound_keyed_run(
        &self,
        request: crate::BoundKeyedAdmitRequest,
    ) -> Result<KeyedAdmitOutcome, StoreError> {
        self.admit_keyed_with_binding(request.keyed, Some(request.binding))
            .await
    }
    async fn execution_binding(
        &self,
        session: &SessionId,
        run: &RunId,
    ) -> Result<Option<crate::RetainedExecutionBinding>, StoreError> {
        self.read_execution_binding(session, run).await
    }

    async fn lookup_admission(
        &self,
        session: &SessionId,
        key: &AdmissionKey,
    ) -> Result<Option<AdmissionReceipt>, StoreError> {
        lookup_receipt(
            &mut *self.readers.acquire().await.map_err(db)?,
            session,
            key,
        )
        .await
    }
    async fn load_admission_execution(
        &self,
        session: &SessionId,
        key: &AdmissionKey,
    ) -> Result<Option<crate::AdmissionExecutionRecord>, crate::AdmissionExecutionError> {
        let mut tx = self
            .readers
            .begin()
            .await
            .map_err(|_| crate::AdmissionExecutionError::UnknownStoreFailure)?;
        let result = async {
            let receipt = lookup_receipt(&mut tx, session, key)
                .await
                .map_err(|_| crate::AdmissionExecutionError::UnknownStoreFailure)?;
            let record = if let Some(receipt) = receipt {
                admission_execution::load_record(&mut tx, &receipt.run_id)
                    .await
                    .map_err(|_| crate::AdmissionExecutionError::UnknownStoreFailure)?
            } else {
                None
            };
            Ok(record)
        }
        .await;
        finish(tx, result, |_| {
            crate::AdmissionExecutionError::UnknownStoreFailure
        })
        .await
    }
    async fn claim_unstarted_admission(
        &self,
        request: crate::ClaimUnstartedAdmissionRequest,
    ) -> Result<crate::ClaimedAdmission, crate::AdmissionExecutionError> {
        admission_execution::claim(self, request).await
    }
    async fn abandon_run(
        &self,
        request: crabber_core::AbandonRequest,
    ) -> Result<crabber_core::AbandonOutcome, crabber_core::AbandonError> {
        abandon::abandon(self, request).await
    }
    async fn execution(&self, fence: RunFence) -> Result<Box<dyn ExecutionStore>, StoreError> {
        let mut tx = self.readers.begin().await.map_err(db)?;
        let result = self.ownership_fenced(&mut tx, &fence).await;
        finish(tx, result, std::convert::identity).await?;
        Ok(Box::new(SqliteExecution {
            store: self.clone(),
            fence,
        }))
    }
    async fn get_session(&self, id: &SessionId) -> Result<Option<Session>, StoreError> {
        sqlx::query("SELECT data FROM sessions WHERE id=$1")
            .bind(&id.0)
            .fetch_optional(&self.readers)
            .await
            .map_err(db)?
            .map(|row| decode_row(&row, "data"))
            .transpose()
    }
    async fn get_run(&self, id: &RunId) -> Result<Option<Run>, StoreError> {
        sqlx::query("SELECT data FROM runs WHERE id=$1")
            .bind(&id.0)
            .fetch_optional(&self.readers)
            .await
            .map_err(db)?
            .map(|row| decode_row(&row, "data"))
            .transpose()
    }
    async fn list_messages(
        &self,
        id: &SessionId,
        epoch: Option<EpochId>,
    ) -> Result<Vec<Message>, StoreError> {
        project(&mut *self.readers.acquire().await.map_err(db)?, id, epoch).await
    }
    async fn snapshot(
        &self,
        request: crate::SnapshotRequest,
    ) -> Result<crate::SnapshotOutcome, StoreError> {
        self.read_snapshot(request).await
    }
    async fn list_all_messages(&self, id: &SessionId) -> Result<Vec<Message>, StoreError> {
        messages(&mut *self.readers.acquire().await.map_err(db)?, id).await
    }
    async fn list_events(
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
        .fetch_all(&self.readers)
        .await
        .map_err(db)?;
        rows.into_iter()
            .map(|row| {
                let mut event: EventRecord = decode_row(&row, "data")?;
                event.cursor = Some(EventCursor(
                    u64::try_from(row.try_get::<i64, _>("seq").map_err(db)?)
                        .map_err(|_| StoreError::Validation("invalid event cursor".into()))?,
                ));
                Ok(event)
            })
            .collect()
    }
    async fn list_unfinished_runs(&self) -> Result<Vec<Run>, StoreError> {
        sqlx::query(
            "SELECT data FROM runs WHERE status IN ('pending','running','paused') ORDER BY seq",
        )
        .fetch_all(&self.readers)
        .await
        .map_err(db)?
        .into_iter()
        .map(|row| decode_row(&row, "data"))
        .collect()
    }
    async fn list_unfinished_tool_calls(
        &self,
        run: &RunId,
    ) -> Result<Vec<ToolCallRecord>, StoreError> {
        sqlx::query(
            "SELECT data FROM tool_calls WHERE run_id=$1 AND status IN \
                ('pending','running') ORDER BY id",
        )
        .bind(&run.0)
        .fetch_all(&self.readers)
        .await
        .map_err(db)?
        .into_iter()
        .map(|row| decode_row(&row, "data"))
        .collect()
    }
    async fn admission_execution_state(
        &self,
        run: &RunId,
    ) -> Result<Option<crate::AdmissionExecutionState>, StoreError> {
        let mut tx = self.readers.begin().await.map_err(db)?;
        let result = async {
            let state = admission_execution::load_record(&mut tx, run)
                .await?
                .map(|record| record.state);
            Ok(state)
        }
        .await;
        finish(tx, result, std::convert::identity).await
    }

    async fn claim_expired_run(&self, id: &RunId, owner: &str) -> Result<RunFence, StoreError> {
        let mut tx = self.begin_write().await?;
        let result = async {
            let mut run = load_run(&mut tx, id).await?;
            if admission_execution::load_record(&mut tx, id)
                .await?
                .is_some_and(|r| r.state == crate::AdmissionExecutionState::Unstarted)
            {
                return Err(StoreError::AdmissionRecoveryRequired);
            }
            let now = self.clock.now();
            if run.status.is_terminal() || run.lease_until > now {
                return Err(StoreError::Conflict);
            }
            run.owner = owner.into();
            run.claim_token = uuid::Uuid::new_v4().to_string();
            run.lease_until = now + time::Duration::seconds(30);
            run.updated_at = now;
            save_run(&mut tx, &run).await?;
            Ok(RunFence {
                run_id: id.clone(),
                claim_token: run.claim_token,
            })
        }
        .await;
        finish(tx, result, std::convert::identity).await
    }
    async fn get_extension_state(
        &self,
        extension_id: &str,
        session: &SessionId,
    ) -> Result<BTreeMap<String, String>, StoreError> {
        let rows = sqlx::query(
            "SELECT key,value FROM extension_state WHERE session_id=$1 AND extension_id=$2",
        )
        .bind(&session.0)
        .bind(extension_id)
        .fetch_all(&self.readers)
        .await
        .map_err(db)?;
        rows.into_iter()
            .map(|row| {
                Ok((
                    row.try_get("key").map_err(db)?,
                    row.try_get("value").map_err(db)?,
                ))
            })
            .collect()
    }
    async fn enqueue_inbox(
        &self,
        session: &SessionId,
        kind: InboxKind,
        message: Message,
    ) -> Result<(), StoreError> {
        if message.session_id != *session {
            return Err(StoreError::Validation(
                "inbox message has wrong session".into(),
            ));
        }
        let mut tx = self.begin_write().await?;
        let result = async {
            sqlx::query("SELECT 1 FROM sessions WHERE id=$1")
                .bind(&session.0)
                .fetch_optional(&mut *tx)
                .await
                .map_err(db)?
                .ok_or(StoreError::NotFound)?;
            sqlx::query("INSERT INTO inbox(session_id,kind,data) VALUES($1,$2,$3)")
                .bind(&session.0)
                .bind(inbox_kind(kind))
                .bind(text(&message)?)
                .execute(&mut *tx)
                .await
                .map_err(db)?;
            super::records::bump_public_revision(&mut tx, session).await?;
            Ok(())
        }
        .await;
        finish(tx, result, std::convert::identity).await
    }
}
