//! Native phase writes share the session/run gate and operation-key namespace.
use crate::native_enrollment::caller::{self, Caller};
use crate::public_session::v2::proof::Proof;
pub(super) use records::{latest as latest_host_phase, save as save_host_record};
pub(super) use records::{load_with_proof, receipt_with_proof};
mod calls;
pub(super) use super::tool_result::apply as apply_host_result;
pub(super) use calls::{
    load as load_call, load_with_proof as load_call_with_proof, save as save_host_call,
};
pub(super) use records::load as load_host_record;
mod records;
pub(super) async fn load_owned(
    tx: &mut Transaction<'_, Postgres>,
    run: &RunId,
    id: &PhaseId,
) -> Result<Option<phase::PhaseRecord>, Error> {
    records::load(tx, run, id).await
}
use super::{PostgresExecution, PostgresStore, admission_execution, db};
use crate::postgres::transaction::Transaction;
use crate::{
    PhaseAuthority, PhaseCommand, PhaseError as Error, PhaseMutation,
    PhaseMutationOutcome as Outcome, PhaseReceipt, PhaseWriteAuthority, VersionedPhase,
    phase::{self, sql},
};
use crabber_core::{ControlId, PhaseId, Run, RunId, SessionId};
use sqlx::Postgres;

mod publication;
async fn require_raw_worker(
    tx: &mut Transaction<'_, Postgres>,
    run: &Run,
    proof: &mut Proof,
) -> Result<(), Error> {
    let enrollment = super::native_enrollment::original(tx, &run.session_id, &run.id, proof)
        .await
        .map_err(|_| Error::UnknownStoreOutcome)?;
    crate::native_enrollment::access::allow(enrollment).map_err(|_| Error::Unsupported)
}

#[derive(Clone, Copy)]
struct PhaseContext<'a> {
    authority: Option<&'a PhaseWriteAuthority>,
    effect: phase::PhaseEffect<'a>,
    fence: Option<&'a crabber_core::RunFence>,
    caller: Caller,
}

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
    pub(super) async fn phase_page(
        &self,
        request: &crate::PhasePageRequest,
    ) -> Result<crate::PhasePage, Error> {
        request.validate()?;
        let mut tx = crate::postgres::transaction::begin(&self.pool)
            .await
            .map_err(db)
            .map_err(sql::unknown)?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .execute(&mut *tx)
            .await
            .map_err(db)
            .map_err(sql::unknown)?;
        let result = records::page(&mut tx, request).await;
        finish(tx, result).await
    }

    pub(super) async fn read_phase(
        &self,
        run: &RunId,
        id: &PhaseId,
    ) -> Result<VersionedPhase, Error> {
        crate::attempt::validate_identifier(&id.0).map_err(|_| Error::InvalidRequest)?;
        let mut tx = crate::postgres::transaction::begin(&self.pool)
            .await
            .map_err(db)
            .map_err(sql::unknown)?;
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
        let mut tx = crate::postgres::transaction::begin(&self.pool)
            .await
            .map_err(db)
            .map_err(sql::unknown)?;
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
        self.change_phase_inner(request, authority, phase::PhaseEffect::Metadata)
            .await
    }
    pub(super) async fn settle_phase(
        &self,
        settlement: &crate::PhaseSettlement,
        authority: Option<PhaseAuthority>,
    ) -> Result<Outcome, Error> {
        settlement.output()?;
        self.change_phase_inner(
            settlement.completion.clone(),
            authority.map(PhaseWriteAuthority::Phase),
            phase::PhaseEffect::Settle(settlement),
        )
        .await
    }
    pub(super) async fn start_phase(
        &self,
        start: &crate::PhaseStart,
        authority: Option<PhaseAuthority>,
    ) -> Result<Outcome, Error> {
        start.validate()?;
        self.change_phase_inner(
            start.begin.clone(),
            authority.map(PhaseWriteAuthority::Phase),
            phase::PhaseEffect::Start(start),
        )
        .await
    }
    async fn change_phase_inner(
        &self,
        request: PhaseMutation,
        authority: Option<PhaseWriteAuthority>,
        effect: phase::PhaseEffect<'_>,
    ) -> Result<Outcome, Error> {
        self.store
            .change_phase_transaction(
                &self.fence.run_id,
                request,
                authority,
                effect,
                Some(&self.fence),
            )
            .await
    }
}
impl PostgresStore {
    pub(super) async fn host_phase(
        &self,
        run: &RunId,
        request: PhaseMutation,
        effect: phase::PhaseEffect<'_>,
    ) -> Result<crate::PhaseControlOutcome, Error> {
        match self
            .change_phase_transaction(run, request, None, effect, None)
            .await?
        {
            Outcome::Applied {
                receipt,
                authority: None,
            } => Ok(crate::PhaseControlOutcome::Applied(receipt)),
            Outcome::Replayed(receipt) => Ok(crate::PhaseControlOutcome::Replayed(receipt)),
            Outcome::Applied { .. } => Err(Error::UnknownStoreOutcome),
        }
    }
    pub(super) async fn resolve_phase(
        &self,
        resolution: &crate::PhaseResolution,
    ) -> Result<crate::PhaseResolutionOutcome, Error> {
        resolution.validate()?;
        let outcome = self
            .change_phase_transaction(
                &resolution.run_id,
                resolution.resolution.clone(),
                None,
                phase::PhaseEffect::Resolve(resolution),
                None,
            )
            .await?;
        match outcome {
            Outcome::Applied {
                receipt,
                authority: None,
            } => Ok(crate::PhaseResolutionOutcome::Applied(receipt)),
            Outcome::Replayed(receipt) => Ok(crate::PhaseResolutionOutcome::Replayed(receipt)),
            Outcome::Applied { .. } => Err(Error::UnknownStoreOutcome),
        }
    }
    async fn change_phase_transaction(
        &self,
        run_id: &RunId,
        request: PhaseMutation,
        authority: Option<PhaseWriteAuthority>,
        effect: phase::PhaseEffect<'_>,
        fence: Option<&crabber_core::RunFence>,
    ) -> Result<Outcome, Error> {
        self.phase_transaction_with_proof(
            run_id,
            request,
            authority,
            effect,
            fence,
            &mut Proof::bounded(32 << 20),
        )
        .await
    }
    pub(super) async fn phase_transaction_with_proof(
        &self,
        run_id: &RunId,
        request: PhaseMutation,
        authority: Option<PhaseWriteAuthority>,
        effect: phase::PhaseEffect<'_>,
        fence: Option<&crabber_core::RunFence>,
        proof: &mut Proof,
    ) -> Result<Outcome, Error> {
        phase::validate_request(&request)?;
        proof
            .measure(&(run_id, &request, fence))
            .map_err(|_| Error::UnknownStoreOutcome)?;
        if let Some(start) = effect.start() {
            proof
                .measure(&(&start.begin, &start.turn_id, start.created_at))
                .map_err(|_| Error::UnknownStoreOutcome)?;
        }
        match authority.as_ref() {
            Some(PhaseWriteAuthority::Attempt(a)) => caller::measure_authority(Some(a), proof),
            Some(PhaseWriteAuthority::Phase(a)) => caller::measure_phase_authority(Some(a), proof),
            None => Ok(()),
        }
        .map_err(|_| Error::UnknownStoreOutcome)?;
        let (mut tx, run) = super::attempt::accounting::begin(self, run_id, proof)
            .await
            .map_err(|e| {
                if e == crate::AttemptError::NotFound {
                    Error::NotFound
                } else {
                    Error::UnknownStoreOutcome
                }
            })?;
        let result = self
            .apply_phase(
                &mut tx,
                &run,
                &request,
                PhaseContext {
                    authority: authority.as_ref(),
                    effect,
                    fence,
                    caller: Caller::Raw,
                },
                proof,
            )
            .await;
        #[cfg(test)]
        let result = if self.phase_fault.load(std::sync::atomic::Ordering::SeqCst) == 1
            && matches!(&result, Ok(Outcome::Applied { .. }))
        {
            Err(Error::UnknownStoreOutcome)
        } else {
            result
        };
        let result = finish(tx, result).await;
        #[cfg(test)]
        if self.phase_fault.load(std::sync::atomic::Ordering::SeqCst) == 2
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
        context: PhaseContext<'_>,
        proof: &mut Proof,
    ) -> Result<Outcome, Error> {
        let PhaseContext {
            authority,
            effect,
            fence,
            caller: native_caller,
        } = context;
        if fence.is_some() {
            require_raw_worker(tx, run, proof).await?;
        }
        let digest = effect.fingerprint(request, &run.session_id, &run.id)?;
        if let Some(receipt) = records::receipt_with_proof(tx, &run.session_id, &request.id, proof)
            .await
            .map_err(|e| e.into_domain_or(Error::UnknownStoreOutcome))?
        {
            if receipt.phase.run_id != run.id || receipt.semantic_digest != digest {
                return Err(Error::SemanticConflict);
            }
            if !matches!(effect, phase::PhaseEffect::Metadata) {
                let call =
                    calls::load_with_proof(tx, run, &receipt.phase.target.call_id, proof).await?;
                effect.verify_replay(&receipt, &call)?;
            }
            return Ok(Outcome::Replayed(receipt));
        }
        let now = self.clock.now();
        match (effect.is_host(), fence) {
            (true, None) if authority.is_none() => (),
            (false, Some(fence)) => {
                crate::attempt::verify_run(run, fence, now).map_err(|_| Error::StaleOwner)?;
            }
            _ => return Err(Error::InvalidRequest),
        }
        if admission_execution::load_record_with_proof(tx, &run.id, proof)
            .await
            .map_err(|_| Error::UnknownStoreOutcome)?
            .is_some_and(|r| r.state == crate::AdmissionExecutionState::Unstarted)
        {
            return Err(Error::StaleOwner);
        }
        let current = records::revision(tx, &run.session_id).await?;
        if current != request.expected_revision {
            return Err(Error::RevisionConflict);
        }
        let insert = matches!(request.command, PhaseCommand::Claim { .. });
        let (mut record, authority) =
            prepare(tx, run, request, authority, now, effect.resolution(), proof).await?;
        let running_event = if matches!(request.command, PhaseCommand::Begin { .. }) {
            calls::begin(
                tx,
                run,
                &record.snapshot.target.call_id,
                effect.start(),
                proof,
            )
            .await?
        } else {
            None
        };
        if let Some(settlement) = effect.settlement() {
            calls::settle(tx, run, settlement).await?;
            record.snapshot.quiescence = crate::PhaseQuiescence::Complete;
        }
        if let Some(resolution) = effect.resolution() {
            calls::resolve(tx, run, resolution).await?;
        }
        let revision = records::revision(tx, &run.session_id)
            .await?
            .next()
            .map_err(|_| Error::RevisionExhausted)?;
        caller::measure_phase_record(&record, proof).map_err(|_| Error::UnknownStoreOutcome)?;
        records::save(tx, &record, insert).await?;
        let (receipt, event) =
            records::publish(tx, run, request, &record.snapshot, digest, revision, now).await?;
        proof
            .measure(&event)
            .map_err(|_| Error::UnknownStoreOutcome)?;
        super::native_parent::record_phase(tx, run, &record, &receipt, proof).await?;
        publication::record(
            tx,
            run,
            request,
            (effect, native_caller),
            publication::Published {
                record: &record,
                receipt: &receipt,
                event: &event,
                running_event: running_event.as_ref(),
                issued: authority.as_ref(),
            },
            proof,
            #[cfg(test)]
            &self.native_raw_probe,
        )
        .await?;
        Ok(Outcome::Applied { receipt, authority })
    }
}

async fn prepare(
    tx: &mut Transaction<'_, Postgres>,
    run: &Run,
    request: &PhaseMutation,
    authority: Option<&PhaseWriteAuthority>,
    now: time::OffsetDateTime,
    resolution: Option<&crate::PhaseResolution>,
    proof: &mut Proof,
) -> Result<(phase::PhaseRecord, Option<PhaseAuthority>), Error> {
    let target = request.command.target();
    let attempt =
        super::attempt::load_host_attempt_with_proof(tx, &run.id, Some(&target.attempt.id), proof)
            .await
            .map_err(|_| Error::UnknownStoreOutcome)?
            .ok_or(Error::NotFound)?
            .0;
    let call = calls::load_with_proof(tx, run, &target.call_id, proof).await?;
    if matches!(request.command, PhaseCommand::Claim { .. }) {
        if crate::POSTGRES_SCHEMA_VERSION >= 14
            && super::frontend::origin_with_proof(
                tx,
                &crate::frontend_continuation::FrontendReadScope::new(&run.session_id, &run.id)
                    .map_err(|_| Error::UnknownStoreOutcome)?,
                &target.call_id,
                proof,
            )
            .await
            .map_err(|_| Error::UnknownStoreOutcome)?
            .is_some()
        {
            return Err(Error::InvalidTransition);
        }
        if records::exists(tx, &target.id).await? {
            return Err(Error::SemanticConflict);
        }
        let Some(PhaseWriteAuthority::Attempt(authority)) = authority else {
            return Err(Error::StaleOwner);
        };
        let previous = records::latest_with_proof(tx, &run.id, &target.call_id, proof).await?;
        let record = phase::PhaseRecord::claim(
            run,
            &attempt,
            &call,
            request,
            authority,
            now,
            previous.as_ref(),
        )?;
        caller::measure_phase_record(&record, proof).map_err(|_| Error::UnknownStoreOutcome)?;
        let grant = record.authority(authority.clone())?;
        Ok((record, Some(grant)))
    } else {
        let mut record = records::load_with_proof(tx, &run.id, &target.id, proof)
            .await
            .map_err(|e| e.into_domain_or(Error::UnknownStoreOutcome))?
            .ok_or(Error::NotFound)?;
        let worker = match authority {
            Some(PhaseWriteAuthority::Phase(authority)) => Some(authority),
            _ => None,
        };
        if let Some(resolution) = resolution {
            record.resolve_uncertain(run, &attempt, &call, resolution)?;
        } else {
            record.change(run, &attempt, &call, request, worker, now)?;
        }
        Ok((record, None))
    }
}

pub(super) async fn legacy_guard(
    tx: &mut Transaction<'_, Postgres>,
    run: &Run,
    id: &crabber_core::ToolCallId,
) -> Result<(), crate::StoreError> {
    legacy_guard_with_proof(
        tx,
        run,
        id,
        &mut crate::public_session::v2::proof::Proof::legacy(),
    )
    .await
}

pub(super) async fn legacy_guard_with_proof(
    tx: &mut Transaction<'_, Postgres>,
    run: &Run,
    id: &crabber_core::ToolCallId,
    proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<(), crate::StoreError> {
    if crate::POSTGRES_SCHEMA_VERSION >= 14
        && super::frontend::origin_with_proof(
            tx,
            &crate::frontend_continuation::FrontendReadScope::new(&run.session_id, &run.id)
                .map_err(|_| crate::StoreError::Conflict)?,
            id,
            proof,
        )
        .await
        .map_err(|_| crate::StoreError::Validation("frontend origin integrity failure".into()))?
        .is_some()
    {
        return Err(crate::StoreError::Conflict);
    }
    proof
        .charge(1)
        .map_err(|_| crate::StoreError::Validation("legacy phase guard allowance".into()))?;
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

#[cfg(test)]
mod process_tests;

#[cfg(test)]
mod resolution_tests;

#[path = "phase/terminal_history.rs"]
pub(super) mod terminal_history;

pub(super) use records::latest_metadata_with_proof as latest_host_phase_with_proof;
pub(super) async fn metadata_page_with_proof(
    tx: &mut Transaction<'_, Postgres>,
    request: &crate::PhasePageRequest,
    proof: &mut Proof,
) -> Result<crate::PhasePage, Error> {
    records::page_with_proof(tx, request, proof).await
}
