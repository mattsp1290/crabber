//! Atomic attempt transitions; no provider or tool work occurs inside a transaction.
mod records;
use super::{PostgresExecution, PostgresStore, admission_execution, db};
use crate::{
    AttemptAuthority, AttemptCommand, AttemptError as Error, AttemptMutation,
    AttemptMutationOutcome as Outcome, AttemptReceipt, VersionedAttempt,
    attempt::{self, sql},
};
use crabber_core::{AttemptId, ControlId, Run, RunId, SessionId};
use sqlx::{Postgres, Transaction};

async fn finish<T>(tx: Transaction<'_, Postgres>, result: Result<T, Error>) -> Result<T, Error> {
    match result {
        Ok(value) => {
            tx.commit().await.map_err(db).map_err(sql::unknown)?;
            Ok(value)
        }
        Err(error) => {
            tx.rollback().await.map_err(db).map_err(sql::unknown)?;
            Err(error)
        }
    }
}
impl PostgresStore {
    pub(super) async fn read_attempt(
        &self,
        run: &RunId,
        id: &AttemptId,
    ) -> Result<VersionedAttempt, Error> {
        attempt::validate_identifier(&id.0)?;
        let mut tx = self.pool.begin().await.map_err(db).map_err(sql::unknown)?;
        let result = records::load(&mut tx, run, Some(id), false)
            .await
            .and_then(|value| {
                let (record, revision) = value.ok_or(Error::NotFound)?;
                Ok(VersionedAttempt {
                    revision,
                    attempt: record.snapshot,
                })
            });
        finish(tx, result).await
    }
    pub(super) async fn attempt_receipt(
        &self,
        session: &SessionId,
        id: &ControlId,
    ) -> Result<Option<AttemptReceipt>, Error> {
        attempt::validate_identifier(&id.0)?;
        let mut tx = self.pool.begin().await.map_err(db).map_err(sql::unknown)?;
        let result = records::receipt(&mut tx, session, id).await;
        finish(tx, result).await
    }
}
impl PostgresExecution {
    pub(super) async fn change_attempt(
        &self,
        request: AttemptMutation,
        authority: Option<AttemptAuthority>,
    ) -> Result<Outcome, Error> {
        attempt::validate_request(&request)?;
        let (tx, run) = self
            .store
            .run_transaction(&self.fence.run_id)
            .await
            .map_err(|error| {
                if error == crate::StoreError::NotFound {
                    Error::NotFound
                } else {
                    sql::unknown(error)
                }
            })?;
        let mut tx = tx;
        let result = self
            .apply_attempt(&mut tx, &run, &request, authority.as_ref())
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
        tx: &mut Transaction<'_, Postgres>,
        run: &Run,
        request: &AttemptMutation,
        authority: Option<&AttemptAuthority>,
    ) -> Result<Outcome, Error> {
        let digest = request.fingerprint(&run.session_id, &run.id)?;
        if let Some(receipt) = records::receipt(tx, &run.session_id, &request.id).await? {
            if receipt.attempt.run_id != run.id || receipt.semantic_digest != digest {
                return Err(Error::SemanticConflict);
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
        let current = records::revision(tx, &run.session_id).await?;
        if current != request.expected_revision {
            return Err(Error::RevisionConflict);
        }
        let revision = current.next().map_err(|_| Error::RevisionExhausted)?;
        let id = &request.command.target().id;
        let insert = matches!(request.command, AttemptCommand::Reserve { .. });
        let (record, grant) = if insert {
            if records::exists(tx, id).await? {
                return Err(Error::SemanticConflict);
            }
            let latest = records::load(tx, &run.id, None, true).await?;
            (
                attempt::AttemptRecord::reserve(
                    run,
                    request,
                    latest.as_ref().map(|(r, _)| &r.snapshot),
                )?,
                true,
            )
        } else {
            let (mut record, _) = records::load(tx, &run.id, Some(id), true)
                .await?
                .ok_or(Error::NotFound)?;
            let quiet = if matches!(request.command, AttemptCommand::RecordCleanup { .. }) {
                records::quiet(tx, &run.id, id).await?
            } else {
                true
            };
            let grant = record.change(run, request, authority, now, quiet)?;
            (record, grant)
        };
        let authority = if grant {
            Some(record.authority()?)
        } else {
            None
        };
        records::save(tx, &record, insert).await?;
        let receipt =
            records::publish(tx, run, request, &record.snapshot, digest, revision, now).await?;
        Ok(Outcome::Applied { receipt, authority })
    }
}
pub(super) async fn renew(
    tx: &mut Transaction<'_, Postgres>,
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
