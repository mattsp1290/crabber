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
    async fn admit_run(&self, request: AdmitRequest) -> Result<AdmitOutcome, StoreError> {
        let mut tx = self.begin_write().await?;
        let result = async {
            let admitted = self.admit_transaction(&mut tx, request, false).await?;
            Ok(admitted)
        }
        .await;
        finish(tx, result, std::convert::identity).await
    }

    async fn admit_keyed_run(
        &self,
        keyed: KeyedAdmitRequest,
    ) -> Result<KeyedAdmitOutcome, StoreError> {
        let request = &keyed.request;
        let session = request.session_id.as_ref().ok_or_else(|| {
            StoreError::Validation("keyed admission requires a stable session ID".into())
        })?;
        if request.user_message.session_id != *session
            || request.user_message.run_id.is_some()
            || request
                .user_message
                .parts
                .iter()
                .any(|part| part.message_id != request.user_message.id)
        {
            return Err(StoreError::Validation(
                "invalid admission message identity".into(),
            ));
        }
        let digest = keyed.semantic_digest()?;
        if let Some(capsule) = &keyed.execution {
            capsule.validate(&keyed)?;
        }
        let mut tx = self.begin_write().await?;
        let result = async {
            let request = &keyed.request;
            let session = request.session_id.as_ref().ok_or_else(|| {
                StoreError::Validation("keyed admission requires a stable session ID".into())
            })?;
            if let Some(row) = sqlx::query("SELECT data FROM sessions WHERE id=$1")
                .bind(&session.0)
                .fetch_optional(&mut *tx)
                .await
                .map_err(db)?
            {
                let existing: Session = decode_row(&row, "data")?;
                existing.ensure_identity(&request.workspace_id, &request.directory)?;
            }
            if let Some(receipt) = lookup_receipt(&mut tx, session, &keyed.options.key).await? {
                if receipt.fingerprint != keyed.options.fingerprint
                    || receipt.semantic_digest != digest
                {
                    return Err(StoreError::AdmissionConflict);
                }
                return Ok(KeyedAdmitOutcome::Replayed(receipt));
            }
            let session_id = session.clone();
            let user_message_id = request.user_message.id.clone();
            let admitted = self.admit_transaction(&mut tx, keyed.request, true).await?;
            let receipt = AdmissionReceipt {
                session_id,
                run_id: admitted.run.id.clone(),
                user_message_id,
                fingerprint: keyed.options.fingerprint,
                semantic_digest_version: 1,
                semantic_digest: digest,
            };
            sqlx::query(
                "INSERT INTO \
                    admission_receipts(session_id,admission_key,run_id,user_message_id,data) \
                    VALUES($1,$2,$3,$4,$5)",
            )
            .bind(&receipt.session_id.0)
            .bind(keyed.options.key.as_str())
            .bind(&receipt.run_id.0)
            .bind(&receipt.user_message_id.0)
            .bind(text(&receipt)?)
            .execute(&mut *tx)
            .await
            .map_err(db)?;
            if let Some(capsule) = keyed.execution {
                let record = crate::AdmissionExecutionRecord {
                    receipt: receipt.clone(),
                    key: keyed.options.key.clone(),
                    capsule,
                    state: crate::AdmissionExecutionState::Unstarted,
                };
                sqlx::query(
                    "INSERT INTO \
                        admission_executions(run_id,session_id,admission_key,capsule_version,start_state,data) \
                        VALUES($1,$2,$3,1,'Unstarted',$4)",
                )
                .bind(&receipt.run_id.0)
                .bind(&receipt.session_id.0)
                .bind(keyed.options.key.as_str())
                .bind(text(&record)?)
                .execute(&mut *tx)
                .await
                .map_err(db)?;
            }
            Ok(KeyedAdmitOutcome::Started {
                receipt,
                admitted: Box::new(admitted),
            })
        }
        .await;
        finish(tx, result, std::convert::identity).await
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
            Ok(())
        }
        .await;
        finish(tx, result, std::convert::identity).await
    }
}
