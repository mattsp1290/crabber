//! Native phase writes share the session/run gate and operation-key namespace.
mod calls;
pub(super) use calls::load as load_call;
mod records;
use super::{SqliteExecution, SqliteStore, admission_execution, db};
use crate::{
    PhaseAuthority, PhaseCommand, PhaseError as Error, PhaseMutation,
    PhaseMutationOutcome as Outcome, PhaseReceipt, PhaseWriteAuthority, VersionedPhase,
    phase::{self, sql},
};
use crabber_core::{ControlId, PhaseId, Run, RunId, SessionId};
use sqlx::{Row, Sqlite, Transaction};

async fn finish<T>(tx: Transaction<'_, Sqlite>, result: Result<T, Error>) -> Result<T, Error> {
    super::transactions::finish(tx, result, sql::unknown).await
}
impl SqliteStore {
    pub(super) async fn phase_page(
        &self,
        request: &crate::PhasePageRequest,
    ) -> Result<crate::PhasePage, Error> {
        request.validate()?;
        let mut tx = self
            .readers
            .begin()
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
        let mut tx = self
            .readers
            .begin()
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
impl SqliteStore {
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
        phase::validate_request(&request)?;
        let mut tx = self.begin_write().await.map_err(sql::unknown)?;
        let result = async {
            let row = sqlx::query("SELECT session_id,data FROM runs WHERE id=$1")
                .bind(&run_id.0)
                .fetch_optional(&mut *tx)
                .await
                .map_err(db)
                .map_err(sql::unknown)?
                .ok_or(Error::NotFound)?;
            let run: Run = super::records::decode_row(&row, "data").map_err(sql::unknown)?;
            if run.id != *run_id || run.session_id.0 != row.get::<String, _>("session_id") {
                return Err(Error::UnknownStoreOutcome);
            }
            self.apply_phase(&mut tx, &run, &request, authority.as_ref(), effect, fence)
                .await
        }
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
        tx: &mut Transaction<'_, Sqlite>,
        run: &Run,
        request: &PhaseMutation,
        authority: Option<&PhaseWriteAuthority>,
        effect: phase::PhaseEffect<'_>,
        fence: Option<&crabber_core::RunFence>,
    ) -> Result<Outcome, Error> {
        let digest = effect.fingerprint(request, &run.session_id, &run.id)?;
        if let Some(receipt) = records::receipt(tx, &run.session_id, &request.id).await? {
            if receipt.phase.run_id != run.id || receipt.semantic_digest != digest {
                return Err(Error::SemanticConflict);
            }
            if !matches!(effect, phase::PhaseEffect::Metadata) {
                let call = calls::load(tx, run, &receipt.phase.target.call_id).await?;
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
        let insert = matches!(request.command, PhaseCommand::Claim { .. });
        let (mut record, authority) =
            prepare(tx, run, request, authority, now, effect.resolution()).await?;
        if matches!(request.command, PhaseCommand::Begin { .. }) {
            calls::begin(tx, run, &record.snapshot.target.call_id, effect.start()).await?;
        }
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
        records::save(tx, &record, insert).await?;
        let receipt =
            records::publish(tx, run, request, &record.snapshot, digest, revision, now).await?;
        Ok(Outcome::Applied { receipt, authority })
    }
}

async fn prepare(
    tx: &mut Transaction<'_, Sqlite>,
    run: &Run,
    request: &PhaseMutation,
    authority: Option<&PhaseWriteAuthority>,
    now: time::OffsetDateTime,
    resolution: Option<&crate::PhaseResolution>,
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
        if let Some(resolution) = resolution {
            record.resolve_uncertain(run, &attempt, &call, resolution)?;
        } else {
            record.change(run, &attempt, &call, request, worker, now)?;
        }
        Ok((record, None))
    }
}

pub(super) async fn legacy_guard(
    tx: &mut Transaction<'_, Sqlite>,
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

#[cfg(test)]
mod process_tests;

#[cfg(test)]
mod resolution_tests;
