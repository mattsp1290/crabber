//! Atomic attempt transitions; no provider or tool work occurs inside a transaction.
mod pristine;
mod records;
use super::{SqliteExecution, SqliteStore, admission_execution, db};
use crate::{
    AttemptAuthority, AttemptCommand, AttemptError as Error, AttemptMutation,
    AttemptMutationOutcome as Outcome, AttemptReceipt, VersionedAttempt,
    attempt::{self, sql},
};
use crabber_core::{AttemptId, ControlId, Run, RunId, SessionId};
use sqlx::{Row, Sqlite, Transaction};

async fn finish<T>(tx: Transaction<'_, Sqlite>, result: Result<T, Error>) -> Result<T, Error> {
    super::transactions::finish(tx, result, sql::unknown).await
}
impl SqliteStore {
    pub(super) async fn read_attempt(
        &self,
        run: &RunId,
        id: Option<&AttemptId>,
    ) -> Result<Option<VersionedAttempt>, Error> {
        if let Some(id) = id {
            attempt::validate_identifier(&id.0)?;
        }
        let mut tx = self
            .readers
            .begin()
            .await
            .map_err(db)
            .map_err(sql::unknown)?;
        let result = async {
            if let Some((record, revision)) = records::load(&mut tx, run, id, false).await? {
                return Ok(Some(VersionedAttempt {
                    revision,
                    attempt: record.snapshot,
                }));
            }
            let exists: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM runs WHERE id=$1)")
                .bind(&run.0)
                .fetch_one(&mut *tx)
                .await
                .map_err(db)
                .map_err(sql::unknown)?;
            if !exists {
                return Err(Error::NotFound);
            }
            Ok(None)
        }
        .await;
        finish(tx, result).await
    }
    pub(super) async fn attempt_receipt(
        &self,
        session: &SessionId,
        id: &ControlId,
    ) -> Result<Option<AttemptReceipt>, Error> {
        attempt::validate_identifier(&id.0)?;
        let mut tx = self
            .readers
            .begin()
            .await
            .map_err(db)
            .map_err(sql::unknown)?;
        let result = records::receipt(&mut tx, session, id).await;
        finish(tx, result).await
    }
}
impl SqliteExecution {
    pub(super) async fn change_attempt(
        &self,
        request: AttemptMutation,
        authority: Option<AttemptAuthority>,
    ) -> Result<Outcome, Error> {
        self.change_attempt_inner(request, authority, None).await
    }
    pub(super) async fn conclude_owned_attempt(
        &self,
        conclusion: crate::AttemptConclusion,
        authority: Option<AttemptAuthority>,
    ) -> Result<Outcome, Error> {
        conclusion.validate()?;
        self.change_attempt_inner(conclusion.finish.clone(), authority, Some(conclusion))
            .await
    }
    async fn change_attempt_inner(
        &self,
        request: AttemptMutation,
        authority: Option<AttemptAuthority>,
        conclusion: Option<crate::AttemptConclusion>,
    ) -> Result<Outcome, Error> {
        attempt::validate_request(&request)?;
        let mut tx = self.store.begin_write().await.map_err(sql::unknown)?;
        let result = async {
            let row = sqlx::query("SELECT session_id,data FROM runs WHERE id=$1")
                .bind(&self.fence.run_id.0)
                .fetch_optional(&mut *tx)
                .await
                .map_err(db)
                .map_err(sql::unknown)?
                .ok_or(Error::NotFound)?;
            let run: Run = super::records::decode_row(&row, "data").map_err(sql::unknown)?;
            if run.id != self.fence.run_id || run.session_id.0 != row.get::<String, _>("session_id")
            {
                return Err(Error::UnknownStoreOutcome);
            }
            self.apply_attempt(
                &mut tx,
                &run,
                &request,
                authority.as_ref(),
                conclusion.as_ref(),
            )
            .await
        }
        .await;
        #[cfg(test)]
        let result = if self
            .store
            .attempt_fault
            .load(std::sync::atomic::Ordering::SeqCst)
            == 1
            && matches!(&result, Ok(Outcome::Applied { .. }))
        {
            Err(Error::UnknownStoreOutcome)
        } else {
            result
        };
        let result = finish(tx, result).await;
        #[cfg(test)]
        if self
            .store
            .attempt_fault
            .load(std::sync::atomic::Ordering::SeqCst)
            == 2
            && matches!(&result, Ok(Outcome::Applied { .. }))
        {
            return Err(Error::UnknownStoreOutcome);
        }
        result
    }
    async fn apply_attempt(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        run: &Run,
        request: &AttemptMutation,
        authority: Option<&AttemptAuthority>,
        conclusion: Option<&crate::AttemptConclusion>,
    ) -> Result<Outcome, Error> {
        let digest = if let Some(conclusion) = conclusion {
            conclusion.fingerprint(&run.session_id, &run.id)?
        } else {
            request.fingerprint(&run.session_id, &run.id)?
        };
        if let Some(receipt) = records::receipt(tx, &run.session_id, &request.id).await? {
            if receipt.attempt.run_id != run.id || receipt.semantic_digest != digest {
                return Err(Error::SemanticConflict);
            }
            if let Some(conclusion) = conclusion {
                conclusion.verify_replay(&receipt)?;
            }
            return Ok(Outcome::Replayed(receipt));
        }
        let now = self.store.clock.now();
        attempt::verify_run(run, &self.fence, now)?;
        if admission_execution::load_record(tx, &run.id)
            .await
            .map_err(sql::unknown)?
            .is_some_and(|r| r.state == crate::AdmissionExecutionState::Unstarted)
        {
            return Err(Error::StaleOwner);
        }
        super::approval::guard_attempt(tx, run, request).await?;
        let current = records::revision(tx, &run.session_id).await?;
        if current != request.expected_revision {
            return Err(Error::RevisionConflict);
        }
        if let Some(conclusion) = conclusion {
            check_boundary(tx, run, conclusion).await?;
        }
        if matches!(request.command, AttemptCommand::ReserveBound { .. }) {
            pristine::check(tx, run).await?;
        }
        let id = &request.command.target().id;
        let insert = matches!(
            request.command,
            AttemptCommand::Reserve { .. } | AttemptCommand::ReserveBound { .. }
        );
        let (record, grant) = if insert {
            if records::exists(tx, id).await? {
                return Err(Error::SemanticConflict);
            }
            let latest = records::load(tx, &run.id, None, true).await?;
            let record = attempt::AttemptRecord::reserve(
                run,
                request,
                latest.as_ref().map(|(record, _)| &record.snapshot),
            )?;
            if latest.is_some() && !records::quiet(tx, &run.id).await? {
                return Err(Error::CleanupBlocked);
            }
            (record, true)
        } else {
            let (mut record, _) = records::load(tx, &run.id, Some(id), true)
                .await?
                .ok_or(Error::NotFound)?;
            let quiet = if conclusion.is_some()
                || matches!(request.command, AttemptCommand::RecordCleanup { .. })
            {
                records::quiet(tx, &run.id).await?
            } else {
                true
            };
            let grant = record.change(run, request, authority, now, quiet)?;
            if conclusion.is_some() {
                record.cleanup(crate::AttemptCleanupResult::Complete, quiet)?;
            }
            (record, grant)
        };
        let authority = if grant {
            Some(record.authority()?)
        } else {
            None
        };
        apply_run_transition(tx, run, request, conclusion, now).await?;
        let revision = records::revision(tx, &run.session_id)
            .await?
            .next()
            .map_err(|_| Error::RevisionExhausted)?;
        let event = if let Some(conclusion) = conclusion {
            crate::attempt_conclusion::event(
                run,
                conclusion.validate()?,
                &record.snapshot,
                &request.id,
                digest.clone(),
                now,
            )?
        } else {
            attempt::event(run, request, &record.snapshot, now)?
        };
        records::save(tx, &record, insert).await?;
        let receipt =
            records::publish(tx, run, request, &record.snapshot, digest, revision, event).await?;
        Ok(Outcome::Applied { receipt, authority })
    }
}
pub(super) async fn renew(
    tx: &mut Transaction<'_, Sqlite>,
    run: &Run,
) -> Result<(), crate::StoreError> {
    records::renew(tx, run).await
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod phase_tests;

#[cfg(test)]
mod metadata_tests;

pub(super) async fn load_owned(
    tx: &mut Transaction<'_, Sqlite>,
    run: &RunId,
    id: &AttemptId,
) -> Result<Option<attempt::AttemptRecord>, Error> {
    records::load(tx, run, Some(id), true)
        .await
        .map(|v| v.map(|(r, _)| r))
}

async fn check_boundary(
    tx: &mut Transaction<'_, Sqlite>,
    run: &Run,
    conclusion: &crate::AttemptConclusion,
) -> Result<(), Error> {
    if let crate::RunBoundary::Terminal { status, .. } = &conclusion.boundary {
        if *status == crabber_core::RunStatus::Completed {
            let pending: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM inbox WHERE session_id=$1 AND consumed_by_run IS NULL)").bind(&run.session_id.0).fetch_one(&mut **tx).await.map_err(db).map_err(sql::unknown)?;
            if pending {
                return Err(Error::PendingInput);
            }
        }
        let unfinished: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM tool_calls WHERE run_id=$1 AND status IN ('pending','running'))").bind(&run.id.0).fetch_one(&mut **tx).await.map_err(db).map_err(sql::unknown)?;
        if unfinished {
            return Err(Error::CleanupBlocked);
        }
    }
    Ok(())
}

async fn apply_run_transition(
    tx: &mut Transaction<'_, Sqlite>,
    run: &Run,
    request: &AttemptMutation,
    conclusion: Option<&crate::AttemptConclusion>,
    now: time::OffsetDateTime,
) -> Result<(), Error> {
    if let Some(conclusion) = conclusion {
        let source = conclusion.materialize(run)?;
        super::records::insert_event(tx, run, &source)
            .await
            .map_err(sql::unknown)?;
        let mut updated = run.clone();
        conclusion.apply_run(&mut updated, now)?;
        super::records::save_run(tx, &updated)
            .await
            .map_err(sql::unknown)?;
        if matches!(conclusion.boundary, crate::RunBoundary::Terminal { .. }) {
            super::approval::release_terminal(tx, run, request).await?;
        }
    } else if matches!(
        request.command,
        AttemptCommand::Reserve { .. } | AttemptCommand::ReserveBound { .. }
    ) && run.status == crabber_core::RunStatus::Paused
    {
        let mut activated = run.clone();
        activated.status = crabber_core::RunStatus::Running;
        activated.updated_at = now;
        super::records::save_run(tx, &activated)
            .await
            .map_err(sql::unknown)?;
    }
    Ok(())
}
