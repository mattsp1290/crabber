//! Admission and fencing inside a caller-owned immediate transaction.
use super::records::{decode_row, insert_message, load_run, micros, project, status};
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
    #[allow(clippy::too_many_lines)] // One atomic admission transaction.
    pub(super) async fn admit_transaction(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        request: AdmitRequest,
        allow_create: bool,
    ) -> Result<AdmitOutcome, StoreError> {
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
        let existing = sqlx::query("SELECT data FROM sessions WHERE id=$1")
            .bind(&session_id.0)
            .fetch_optional(&mut **tx)
            .await
            .map_err(db)?;
        let had_session = existing.is_some();
        let session: Session = if let Some(row) = existing {
            let existing: Session = decode_row(&row, "data")?;
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
        let prior_history = if had_session {
            project(tx, &session_id, None).await?
        } else {
            Vec::new()
        };
        let previous_run =
            sqlx::query("SELECT data FROM runs WHERE session_id=$1 ORDER BY seq DESC LIMIT 1")
                .bind(&session_id.0)
                .fetch_optional(&mut **tx)
                .await
                .map_err(db)?;
        let previous: Option<ContextEpoch> = if let Some(row) = previous_run {
            let previous_run: Run = decode_row(&row, "data")?;
            sqlx::query("SELECT data FROM epochs WHERE id=$1")
                .bind(&previous_run.epoch_id.0)
                .fetch_optional(&mut **tx)
                .await
                .map_err(db)?
                .map(|row| decode_row(&row, "data"))
                .transpose()?
        } else {
            None
        };
        let run_id = RunId::new();
        let epoch_id = EpochId::new();
        let token = uuid::Uuid::new_v4().to_string();
        let run = Run {
            id: run_id.clone(),
            session_id: session_id.clone(),
            status: RunStatus::Running,
            owner: request.owner,
            claim_token: token.clone(),
            lease_until: now + lease,
            epoch_id: epoch_id.clone(),
            config_hash: request.config_hash,
            plan_fingerprint: request.plan_fingerprint,
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
            sqlx::query("INSERT INTO sessions(id,data) VALUES($1,$2)")
                .bind(&session.id.0)
                .bind(text(&session)?)
                .execute(&mut **tx)
                .await
                .map_err(db)?;
        }
        sqlx::query(
            "INSERT INTO runs(id,session_id,status,claim_token,lease_until,data) \
                VALUES($1,$2,$3,$4,$5,$6)",
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
        sqlx::query("INSERT INTO epochs(id,session_id,run_id,data) VALUES($1,$2,$3,$4)")
            .bind(&epoch.id.0)
            .bind(&session_id.0)
            .bind(&run.id.0)
            .bind(text(&epoch)?)
            .execute(&mut **tx)
            .await
            .map_err(db)?;
        let mut user = request.user_message;
        user.run_id = Some(run_id.clone());
        insert_message(tx, &run, &user).await?;
        Ok(AdmitOutcome {
            session,
            run,
            fence: RunFence {
                run_id,
                claim_token: token,
            },
            assistant_placeholder: MessageId::new(),
            epoch: epoch_id,
            prior_history,
        })
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
