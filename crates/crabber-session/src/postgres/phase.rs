//! Native phase writes share the session/run gate and operation-key namespace.
mod calls;
mod records;
use super::{PostgresExecution, PostgresStore, admission_execution, db};
use crate::{
    PhaseAuthority, PhaseCommand, PhaseError as Error, PhaseMutation,
    PhaseMutationOutcome as Outcome, PhaseReceipt, PhaseWriteAuthority, VersionedPhase,
    phase::{self, sql},
};
use crabber_core::{ControlId, PhaseId, Run, RunId, SessionId};
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
    pub(super) async fn read_phase(
        &self,
        run: &RunId,
        id: &PhaseId,
    ) -> Result<VersionedPhase, Error> {
        crate::attempt::validate_identifier(&id.0).map_err(|_| Error::InvalidRequest)?;
        let mut tx = self.pool.begin().await.map_err(db).map_err(sql::unknown)?;
        let result = records::metadata(&mut tx, run, id).await.and_then(|value| {
            let (phase, revision) = value.ok_or(Error::NotFound)?;
            Ok(VersionedPhase { revision, phase })
        });
        finish(tx, result).await
    }
    pub(super) async fn phase_receipt(
        &self,
        session: &SessionId,
        id: &ControlId,
    ) -> Result<Option<PhaseReceipt>, Error> {
        crate::attempt::validate_identifier(&id.0).map_err(|_| Error::InvalidRequest)?;
        let mut tx = self.pool.begin().await.map_err(db).map_err(sql::unknown)?;
        let result = records::receipt(&mut tx, session, id).await;
        finish(tx, result).await
    }
}
impl PostgresExecution {
    pub(super) async fn change_phase(
        &self,
        request: PhaseMutation,
        authority: Option<PhaseWriteAuthority>,
    ) -> Result<Outcome, Error> {
        phase::validate_request(&request)?;
        let (mut tx, run) = self
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
        let result = self
            .apply_phase(&mut tx, &run, &request, authority.as_ref())
            .await;
        #[cfg(test)]
        let result = if self
            .store
            .phase_fault
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
            .phase_fault
            .load(std::sync::atomic::Ordering::SeqCst)
            == 2
            && matches!(&result, Ok(Outcome::Applied { .. }))
        {
            return Err(Error::UnknownStoreOutcome);
        }
        result
    }
    async fn apply_phase(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        run: &Run,
        request: &PhaseMutation,
        authority: Option<&PhaseWriteAuthority>,
    ) -> Result<Outcome, Error> {
        let digest = request.fingerprint(&run.session_id, &run.id)?;
        if let Some(receipt) = records::receipt(tx, &run.session_id, &request.id).await? {
            if receipt.phase.run_id != run.id || receipt.semantic_digest != digest {
                return Err(Error::SemanticConflict);
            }
            return Ok(Outcome::Replayed(receipt));
        }
        let now = self.store.clock.now();
        crate::attempt::verify_run(run, &self.fence, now).map_err(|_| Error::StaleOwner)?;
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
        let insert = matches!(request.command, PhaseCommand::Claim { .. });
        let (record, authority) = prepare(tx, run, request, authority, now).await?;
        if matches!(request.command, PhaseCommand::Begin { .. }) {
            calls::begin(tx, run, &record.snapshot.target.call_id).await?;
        }
        records::save(tx, &record, insert).await?;
        let receipt =
            records::publish(tx, run, request, &record.snapshot, digest, revision, now).await?;
        Ok(Outcome::Applied { receipt, authority })
    }
}
async fn prepare(
    tx: &mut Transaction<'_, Postgres>,
    run: &Run,
    request: &PhaseMutation,
    authority: Option<&PhaseWriteAuthority>,
    now: time::OffsetDateTime,
) -> Result<(phase::PhaseRecord, Option<PhaseAuthority>), Error> {
    let target = request.command.target();
    let attempt = super::attempt::load_owned(tx, &run.id, &target.attempt.id)
        .await
        .map_err(|_| Error::UnknownStoreOutcome)?
        .ok_or(Error::NotFound)?;
    let call = calls::load(tx, run, &target.call_id).await?;
    if matches!(request.command, PhaseCommand::Claim { .. }) {
        if records::exists(tx, &target.id).await? {
            return Err(Error::SemanticConflict);
        }
        let Some(PhaseWriteAuthority::Attempt(authority)) = authority else {
            return Err(Error::StaleOwner);
        };
        let previous = records::latest(tx, &run.id, &target.call_id).await?;
        let record = phase::PhaseRecord::claim(
            run,
            &attempt,
            &call,
            request,
            authority,
            now,
            previous.as_ref(),
        )?;
        let grant = record.authority(authority.clone())?;
        Ok((record, Some(grant)))
    } else {
        let mut record = records::load(tx, &run.id, &target.id)
            .await?
            .ok_or(Error::NotFound)?;
        let worker = match authority {
            Some(PhaseWriteAuthority::Phase(authority)) => Some(authority),
            _ => None,
        };
        record.change(run, &attempt, &call, request, worker, now)?;
        Ok((record, None))
    }
}

pub(super) async fn legacy_guard(
    tx: &mut Transaction<'_, Postgres>,
    run: &Run,
    id: &crabber_core::ToolCallId,
) -> Result<(), crate::StoreError> {
    let exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM tool_phases WHERE run_id=$1 AND call_id=$2)",
    )
    .bind(&run.id.0)
    .bind(&id.0)
    .fetch_one(&mut **tx)
    .await
    .map_err(db)?;
    if exists {
        return Err(crate::StoreError::Conflict);
    }
    Ok(())
}

#[cfg(test)]
mod tests;
