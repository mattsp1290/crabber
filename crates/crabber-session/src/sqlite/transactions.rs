//! Admission and fencing inside a caller-owned immediate transaction.
use super::records::{insert_message, load_run, micros, status};
use super::{Sqlite, SqliteStore, StoreError, Transaction, admission_execution, db, text};
use crate::{AdmitOutcome, AdmitRequest};
use crabber_core::{
    ContextEpoch, EpochId, MessageId, Run, RunFence, RunId, RunStatus, Session, Usage,
};

// Complete every transaction explicitly before exposing its result to a caller.
// This also closes read transactions promptly on decoding/validation failures.
pub(super) async fn finish<T, E>(
    mut tx: Transaction<'_, Sqlite>,
    result: Result<T, E>,
    map: fn(StoreError) -> E,
) -> Result<T, E> {
    match result {
        Ok(value) => {
            // Keep the guard until COMMIT responds so a failed commit can be
            // rolled back explicitly. At depth zero rollback only closes the
            // guard; SQLx does not issue SQL after a successful outer commit.
            use sqlx::{TransactionManager, sqlite::SqliteTransactionManager};
            let committed = SqliteTransactionManager::commit(&mut tx).await;
            tx.rollback().await.map_err(db).map_err(map)?;
            committed.map_err(db).map_err(map)?;
            Ok(value)
        }
        Err(error) => {
            tx.rollback().await.map_err(db).map_err(map)?;
            Err(error)
        }
    }
}

impl SqliteStore {
    pub(super) async fn admit_transaction_guarded_with_proof(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        request: &AdmitRequest,
        allow_create: bool,
        guard: crate::queue::AdmissionGuard<'_>,
        read_proof: &mut crate::public_session::v2::proof::Proof,
    ) -> Result<AdmitOutcome, StoreError> {
        let captured = self
            .admit_transaction_captured(
                tx,
                request,
                allow_create,
                guard,
                crate::native_enrollment::caller::initial::Dispatch::raw(
                    crate::native_enrollment::caller::Caller::Raw,
                ),
                read_proof,
            )
            .await?;
        self.finish_initial(
            tx,
            crate::native_enrollment::caller::initial::Input::Ordinary(request),
            captured,
            read_proof,
        )
        .await
    }
    #[allow(clippy::too_many_lines)] // Existing atomic admission body, shared with verified queue admission.
    pub(super) async fn admit_transaction_captured(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        request: &AdmitRequest,
        allow_create: bool,
        guard: crate::queue::AdmissionGuard<'_>,
        caller: crate::native_enrollment::caller::initial::Dispatch<'_>,
        read_proof: &mut crate::public_session::v2::proof::Proof,
    ) -> Result<crate::native_enrollment::caller::initial::CapturedAdmission, StoreError> {
        match caller.caller {
            crate::native_enrollment::caller::Caller::Raw => (),
        }
        crate::native_enrollment::caller::initial::input_request(
            request,
            caller.selected(),
            read_proof,
        )
        .map_err(|_| StoreError::Conflict)?;
        guard.verify(request)?;
        if request.input_envelope.is_some() || !request.additional_messages.is_empty() {
            request.validate_messages()?;
        }
        let now = self.clock.now();
        let lease = time::Duration::try_from(request.lease)
            .map_err(|_| StoreError::Validation("lease is too large".into()))?;
        if lease <= time::Duration::ZERO {
            return Err(StoreError::Validation("lease must be positive".into()));
        }
        let session_id = request
            .session_id
            .clone()
            .unwrap_or_else(|| request.user_message.session_id.clone());
        if request.user_message.session_id != session_id {
            return Err(StoreError::Validation(
                "user message has wrong session".into(),
            ));
        }
        super::deletion::ensure_live(tx, &session_id).await?;
        super::session_owner::context_with_proof(
            tx,
            &session_id,
            &request.workspace_id,
            &request.directory,
            read_proof,
        )
        .await
        .map_err(|error| {
            if error == crate::session_owner::SessionOwnerError::IdentityMismatch {
                StoreError::SessionIdentityMismatch
            } else {
                StoreError::Validation("session ownership proof unavailable".into())
            }
        })?;
        let existing = super::admission_reads::session(tx, &session_id, read_proof).await?;
        let had_session = existing.is_some();
        let current_revision = existing.as_ref().map_or(0, |(_, revision)| *revision);
        let session: Session = if let Some((existing, _)) = existing {
            existing.ensure_identity(&request.workspace_id, &request.directory)?;
            existing
        } else if request.session_id.is_some() && !allow_create {
            return Err(StoreError::NotFound);
        } else {
            Session {
                id: session_id.clone(),
                workspace_id: request.workspace_id.clone(),
                directory: request.directory.clone(),
                title: request.title.clone(),
                created_at: now,
                updated_at: now,
            }
        };
        if guard.is_ordinary()
            && let Some(expected) = guard.expected_revision(request.expected_revision)
        {
            let current = u64::try_from(current_revision)
                .map_err(|_| StoreError::Validation("invalid public session revision".into()))?;
            if expected.get() != current {
                return Err(StoreError::AdmissionRevisionConflict);
            }
        }
        if guard.is_ordinary() && sqlx::query_scalar::<_, bool>("SELECT EXISTS(SELECT 1 FROM future_queue WHERE session_id=$1 AND state='Claimed' AND prepared=true)").bind(&session_id.0).fetch_one(&mut **tx).await.map_err(db)? { return Err(StoreError::Busy); }
        if super::management::admission_archived_with_proof(tx, &session_id, read_proof).await? {
            return Err(StoreError::Conflict);
        }
        let barrier = super::admission_reads::pending(tx, &session_id, read_proof).await?;
        if barrier {
            return Err(StoreError::Busy);
        }
        let busy = sqlx::query(
            "SELECT 1 FROM runs WHERE session_id=$1 AND status IN ('pending','running','paused')",
        )
        .bind(&session_id.0)
        .fetch_optional(&mut **tx)
        .await
        .map_err(db)?
        .is_some();
        if busy {
            return Err(StoreError::Busy);
        }
        request.validate_messages()?;
        let prior_history = if had_session {
            super::admission_reads::project(tx, &session_id, read_proof).await?
        } else {
            Vec::new()
        };
        let previous = super::admission_reads::previous_epoch(tx, &session_id, read_proof).await?;
        let run_id = RunId::new();
        let epoch_id = EpochId::new();
        let token = uuid::Uuid::new_v4().to_string();
        let run = Run {
            id: run_id.clone(),
            session_id: session_id.clone(),
            status: RunStatus::Running,
            owner: request.owner.clone(),
            claim_token: token.clone(),
            lease_until: now + lease,
            epoch_id: epoch_id.clone(),
            config_hash: request.config_hash.clone(),
            plan_fingerprint: request.plan_fingerprint.clone(),
            checkpoint: None,
            error: None,
            usage: Usage::default(),
            created_at: now,
            updated_at: now,
        };
        let epoch = ContextEpoch {
            id: epoch_id.clone(),
            session_id: session_id.clone(),
            run_id: run_id.clone(),
            parent: previous.as_ref().map(|value| value.id.clone()),
            summarized_range: previous
                .as_ref()
                .and_then(|value| value.summarized_range.clone()),
            summary_message_id: previous
                .as_ref()
                .and_then(|value| value.summary_message_id.clone()),
            tail_start_message_id: previous
                .as_ref()
                .and_then(|value| value.tail_start_message_id.clone()),
            provider_id: previous
                .as_ref()
                .map_or_else(String::new, |value| value.provider_id.clone()),
            model_id: previous
                .as_ref()
                .map_or_else(String::new, |value| value.model_id.clone()),
            reason: "initial".into(),
            next_policy: previous.and_then(|value| value.next_policy),
        };
        if !had_session {
            sqlx::query("INSERT INTO sessions(id,data,ownership_protocol,owner_scope_protocol,native_run_protocol) VALUES($1,$2,$3,COALESCE((SELECT scope_protocol FROM session_owner_contexts WHERE session_id=$1),0),COALESCE((SELECT native_run_protocol FROM session_owner_contexts WHERE session_id=$1),1))")
                .bind(&session.id.0)
                .bind(text(&session)?)
                .bind(i32::from(guard.has_owned_resources()))
                .execute(&mut **tx)
                .await
                .map_err(db)?;
        }
        sqlx::query(
            "INSERT INTO runs(id,session_id,status,claim_token,lease_until,data,native_source_protocol,execution_enrollment_protocol,execution_enrollment_origin_id) \
                VALUES($1,$2,$3,$4,$5,$6,1,1,$1)",
        )
        .bind(&run.id.0)
        .bind(&session_id.0)
        .bind(status(run.status))
        .bind(&token)
        .bind(micros(run.lease_until))
        .bind(text(&run)?)
        .execute(&mut **tx)
        .await
        .map_err(db)?;
        let raw_first = if guard.has_owned_resources() {
            None
        } else {
            Some(super::native_parent::record_initial_run_with_proof(tx, &run, read_proof).await?)
        };
        sqlx::query("INSERT INTO epochs(id,session_id,run_id,data) VALUES($1,$2,$3,$4)")
            .bind(&epoch.id.0)
            .bind(&session_id.0)
            .bind(&run.id.0)
            .bind(text(&epoch)?)
            .execute(&mut **tx)
            .await
            .map_err(db)?;
        let mut user = request.user_message.clone();
        user.run_id = Some(run_id.clone());
        insert_message(tx, &run, &user).await?;
        for original in &request.additional_messages {
            let mut message = original.clone();
            message.run_id = Some(run_id.clone());
            insert_message(tx, &run, &message).await?;
        }
        let owned_first = super::ownership_origin::record_admission(
            tx,
            &session,
            &run,
            !had_session,
            guard,
            read_proof,
        )
        .await?;
        let ((Some(first), None) | (None, Some(first))) = (raw_first, owned_first) else {
            return Err(StoreError::Conflict);
        };
        Ok(
            crate::native_enrollment::caller::initial::CapturedAdmission {
                first,
                admitted: AdmitOutcome {
                    session,
                    run,
                    fence: RunFence {
                        run_id,
                        claim_token: token,
                    },
                    assistant_placeholder: MessageId::new(),
                    epoch: epoch_id,
                    prior_history,
                },
            },
        )
    }
    pub(super) async fn ownership_fenced(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        fence: &RunFence,
    ) -> Result<Run, StoreError> {
        let run = load_run(tx, &fence.run_id).await?;
        if run.claim_token != fence.claim_token
            || run.status.is_terminal()
            || run.lease_until <= self.clock.now()
        {
            return Err(StoreError::Conflict);
        }
        Ok(run)
    }
    pub(super) async fn fenced(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        fence: &RunFence,
    ) -> Result<Run, StoreError> {
        let run = self.ownership_fenced(tx, fence).await?;
        if admission_execution::load_record(tx, &run.id)
            .await?
            .is_some_and(|r| r.state == crate::AdmissionExecutionState::Unstarted)
        {
            return Err(StoreError::AdmissionRecoveryRequired);
        }
        Ok(run)
    }
}
