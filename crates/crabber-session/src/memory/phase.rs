use super::tool_result::publish_result;
use super::{MemoryExecution, MemoryStore, OperationReceipt, State, bump_public_revision};
use crate::{
    PhaseCommand, PhaseError as Error, PhaseMutation, PhaseMutationOutcome, PhaseReceipt,
    PhaseWriteAuthority, VersionedPhase, phase,
};
use crabber_core::{ControlId, PhaseId, Run, RunId, SessionId, SessionRevision};

#[derive(Clone, Copy)]
enum PhaseCaller<'a> {
    Worker {
        fence: &'a crabber_core::RunFence,
        authority: Option<&'a PhaseWriteAuthority>,
    },
    HostControl,
}

fn revision(state: &State, session: &SessionId) -> SessionRevision {
    state
        .public_revisions
        .get(session)
        .copied()
        .unwrap_or(SessionRevision::INITIAL)
}
fn verify(state: &State, receipt: &PhaseReceipt) -> Result<(), Error> {
    phase::verify_receipt(
        receipt,
        state
            .events
            .get(receipt.event_cursor)
            .ok_or(Error::UnknownStoreOutcome)?,
    )
}
impl MemoryStore {
    pub(super) fn phase_page(
        &self,
        request: &crate::PhasePageRequest,
    ) -> Result<crate::PhasePage, Error> {
        request.validate()?;
        let state = self.state.lock().unwrap();
        let run = state
            .runs
            .get(&request.run_id)
            .filter(|run| run.session_id == request.session_id)
            .ok_or(Error::NotFound)?;
        let mut selected = std::collections::BTreeMap::new();
        for record in state
            .phases
            .values()
            .filter(|record| record.snapshot.run_id == run.id)
        {
            let id = &record.snapshot.target.id.0;
            if request
                .after
                .as_ref()
                .is_some_and(|after| id.as_bytes() <= after.0.as_bytes())
            {
                continue;
            }
            selected.insert(id, &record.snapshot);
            if selected.len() > usize::from(request.limit) + 1 {
                selected.pop_last();
            }
        }
        let phases = selected.into_values().cloned().collect();
        request.finish(revision(&state, &run.session_id), phases)
    }

    pub(super) fn host_phase(
        &self,
        run: &RunId,
        request: PhaseMutation,
        effect: phase::PhaseEffect<'_>,
    ) -> Result<crate::PhaseControlOutcome, Error> {
        match self.apply_phase(run, request, PhaseCaller::HostControl, effect)? {
            PhaseMutationOutcome::Applied {
                receipt,
                authority: None,
            } => Ok(crate::PhaseControlOutcome::Applied(receipt)),
            PhaseMutationOutcome::Replayed(receipt) => {
                Ok(crate::PhaseControlOutcome::Replayed(receipt))
            }
            PhaseMutationOutcome::Applied { .. } => Err(Error::UnknownStoreOutcome),
        }
    }
    pub(super) fn resolve_phase(
        &self,
        resolution: &crate::PhaseResolution,
    ) -> Result<crate::PhaseResolutionOutcome, Error> {
        resolution.validate()?;
        match self.apply_phase(
            &resolution.run_id,
            resolution.resolution.clone(),
            PhaseCaller::HostControl,
            phase::PhaseEffect::Resolve(resolution),
        )? {
            PhaseMutationOutcome::Applied {
                receipt,
                authority: None,
            } => Ok(crate::PhaseResolutionOutcome::Applied(receipt)),
            PhaseMutationOutcome::Replayed(receipt) => {
                Ok(crate::PhaseResolutionOutcome::Replayed(receipt))
            }
            PhaseMutationOutcome::Applied { .. } => Err(Error::UnknownStoreOutcome),
        }
    }
    pub(super) fn read_phase(&self, run_id: &RunId, id: &PhaseId) -> Result<VersionedPhase, Error> {
        let state = self.state.lock().unwrap();
        let run = state.runs.get(run_id).ok_or(Error::NotFound)?;
        let record = state
            .phases
            .get(id)
            .filter(|r| r.snapshot.run_id == *run_id)
            .ok_or(Error::NotFound)?;
        if run.id != *run_id || record.snapshot.session_id != run.session_id {
            return Err(Error::UnknownStoreOutcome);
        }
        record.validate()?;
        Ok(VersionedPhase {
            revision: revision(&state, &run.session_id),
            phase: record.snapshot.clone(),
        })
    }
    pub(super) fn phase_receipt(
        &self,
        session: &SessionId,
        id: &ControlId,
    ) -> Result<Option<PhaseReceipt>, Error> {
        crate::attempt::validate_identifier(&id.0).map_err(|_| Error::InvalidRequest)?;
        let state = self.state.lock().unwrap();
        match state.operation_receipts.get(&(session.clone(), id.clone())) {
            Some(OperationReceipt::Phase(receipt)) => {
                if receipt.phase.session_id != *session || receipt.operation_id != *id {
                    return Err(Error::UnknownStoreOutcome);
                }
                verify(&state, receipt)?;
                Ok(Some(receipt.clone()))
            }
            Some(_) => Err(Error::SemanticConflict),
            None => Ok(None),
        }
    }
}
impl MemoryExecution {
    pub(super) fn change_phase(
        &self,
        request: PhaseMutation,
        authority: Option<&PhaseWriteAuthority>,
    ) -> Result<PhaseMutationOutcome, Error> {
        self.change_phase_inner(request, authority, phase::PhaseEffect::Metadata)
    }
    pub(super) fn settle_phase(
        &self,
        settlement: &crate::PhaseSettlement,
        authority: Option<crate::PhaseAuthority>,
    ) -> Result<PhaseMutationOutcome, Error> {
        settlement.output()?;
        let authority = authority.map(PhaseWriteAuthority::Phase);
        self.change_phase_inner(
            settlement.completion.clone(),
            authority.as_ref(),
            phase::PhaseEffect::Settle(settlement),
        )
    }
    pub(super) fn start_phase(
        &self,
        start: &crate::PhaseStart,
        authority: Option<crate::PhaseAuthority>,
    ) -> Result<PhaseMutationOutcome, Error> {
        start.validate()?;
        let authority = authority.map(PhaseWriteAuthority::Phase);
        self.change_phase_inner(
            start.begin.clone(),
            authority.as_ref(),
            phase::PhaseEffect::Start(start),
        )
    }
    fn change_phase_inner(
        &self,
        request: PhaseMutation,
        authority: Option<&PhaseWriteAuthority>,
        effect: phase::PhaseEffect<'_>,
    ) -> Result<PhaseMutationOutcome, Error> {
        self.store.apply_phase(
            &self.fence.run_id,
            request,
            PhaseCaller::Worker {
                fence: &self.fence,
                authority,
            },
            effect,
        )
    }
}
impl MemoryStore {
    fn apply_phase(
        &self,
        run_id: &RunId,
        request: PhaseMutation,
        caller: PhaseCaller<'_>,
        effect: phase::PhaseEffect<'_>,
    ) -> Result<PhaseMutationOutcome, Error> {
        phase::validate_request(&request)?;
        let result = self.transact(|state| {
            let run = state.runs.get(run_id).cloned().ok_or(Error::NotFound)?;
            let digest = effect.fingerprint(&request, &run.session_id, &run.id)?;
            if let Some(replayed) = replay(state, &run, &request, &digest, effect)? {
                return Ok(replayed);
            }
            let now = self.clock.now();
            let authority = match caller {
                PhaseCaller::Worker { fence, authority } => {
                    crate::attempt::verify_run(&run, fence, now).map_err(|_| Error::StaleOwner)?;
                    authority
                }
                PhaseCaller::HostControl => {
                    if !effect.is_host() {
                        return Err(Error::InvalidRequest);
                    }
                    None
                }
            };
            if state
                .admission_executions
                .get(&run.id)
                .is_some_and(|r| r.state == crate::AdmissionExecutionState::Unstarted)
            {
                return Err(Error::StaleOwner);
            }
            let current = revision(state, &run.session_id);
            if current != request.expected_revision {
                return Err(Error::RevisionConflict);
            }
            let (mut record, authority) =
                prepare_record(state, &run, &request, authority, now, effect.resolution())?;
            if let Some(start) = effect.start() {
                let call = state
                    .calls
                    .get(&record.snapshot.target.call_id)
                    .ok_or(Error::NotFound)?;
                let event = start.materialize(&run, call)?;
                super::insert_event(state, &run, event).map_err(|_| Error::UnknownStoreOutcome)?;
            }
            if let Some(settlement) = effect.settlement() {
                settle_call(state, &run, &mut record, settlement)?;
            }
            if let Some(resolution) = effect.resolution() {
                let call = state
                    .calls
                    .get(&record.snapshot.target.call_id)
                    .ok_or(Error::NotFound)?;
                if let Some(records) = resolution.materialize(&run, call)? {
                    publish_result(state, &run, &record.snapshot.target.call_id, records)?;
                }
            }
            let receipt = publish_phase(state, &run, request, record, digest, now)?;
            #[cfg(test)]
            if self.phase_fault.load(std::sync::atomic::Ordering::SeqCst) == 1 {
                return Err(Error::UnknownStoreOutcome);
            }
            Ok(PhaseMutationOutcome::Applied { receipt, authority })
        });
        #[cfg(test)]
        if self.phase_fault.load(std::sync::atomic::Ordering::SeqCst) == 2
            && matches!(&result, Ok(PhaseMutationOutcome::Applied { .. }))
        {
            return Err(Error::UnknownStoreOutcome);
        }
        result
    }
}
fn publish_phase(
    state: &mut State,
    run: &Run,
    request: PhaseMutation,
    record: phase::PhaseRecord,
    digest: crabber_core::InputFingerprint,
    now: time::OffsetDateTime,
) -> Result<PhaseReceipt, Error> {
    let revision = revision(state, &run.session_id)
        .next()
        .map_err(|_| Error::RevisionExhausted)?;
    let mut event = phase::event(run, &request, &record.snapshot, now, digest.clone())?;
    let cursor = state
        .events
        .next_cursor()
        .map_err(|_| Error::UnknownStoreOutcome)?;
    event.cursor = Some(cursor);
    let key = (run.session_id.clone(), request.id.clone());
    let receipt = PhaseReceipt {
        operation: phase::operation(&request.command),
        resolver: phase::resolver(&request.command),
        operation_id: request.id,
        semantic_digest: digest,
        revision,
        event_cursor: cursor,
        phase: record.snapshot.clone(),
    };
    if matches!(request.command, PhaseCommand::Begin { .. }) {
        state
            .calls
            .get_mut(&record.snapshot.target.call_id)
            .ok_or(Error::NotFound)?
            .status = crabber_core::ToolCallStatus::Running;
        let snapshot_revision = state
            .snapshot_revisions
            .entry(run.session_id.clone())
            .or_default();
        *snapshot_revision = snapshot_revision
            .checked_add(1)
            .ok_or(Error::RevisionExhausted)?;
    }
    state
        .phases
        .insert(record.snapshot.target.id.clone(), record);
    bump_public_revision(state, &run.session_id).map_err(|_| Error::UnknownStoreOutcome)?;
    state
        .events
        .append(event)
        .map_err(|_| Error::UnknownStoreOutcome)?;
    state
        .operation_receipts
        .insert(key, OperationReceipt::Phase(receipt.clone()));
    Ok(receipt)
}

fn replay(
    state: &State,
    run: &Run,
    request: &PhaseMutation,
    digest: &crabber_core::InputFingerprint,
    effect: phase::PhaseEffect<'_>,
) -> Result<Option<PhaseMutationOutcome>, Error> {
    let key = (run.session_id.clone(), request.id.clone());
    if let Some(saved) = state.operation_receipts.get(&key) {
        let OperationReceipt::Phase(receipt) = saved else {
            return Err(Error::SemanticConflict);
        };
        if receipt.phase.session_id != run.session_id || receipt.operation_id != request.id {
            return Err(Error::UnknownStoreOutcome);
        }
        verify(state, receipt)?;
        if receipt.phase.run_id != run.id || &receipt.semantic_digest != digest {
            return Err(Error::SemanticConflict);
        }
        if !matches!(effect, phase::PhaseEffect::Metadata) {
            effect.verify_replay(
                receipt,
                state
                    .calls
                    .get(&receipt.phase.target.call_id)
                    .ok_or(Error::UnknownStoreOutcome)?,
            )?;
        }
        return Ok(Some(PhaseMutationOutcome::Replayed(receipt.clone())));
    }
    Ok(None)
}

fn settle_call(
    state: &mut State,
    run: &Run,
    record: &mut phase::PhaseRecord,
    settlement: &crate::PhaseSettlement,
) -> Result<(), Error> {
    let call = state
        .calls
        .get(&record.snapshot.target.call_id)
        .ok_or(Error::NotFound)?;
    let records = settlement.materialize(run, call)?;
    publish_result(state, run, &record.snapshot.target.call_id, records)?;
    record.snapshot.quiescence = crate::PhaseQuiescence::Complete;
    Ok(())
}

fn prepare_record(
    state: &State,
    run: &Run,
    request: &PhaseMutation,
    authority: Option<&PhaseWriteAuthority>,
    now: time::OffsetDateTime,
    resolution: Option<&crate::PhaseResolution>,
) -> Result<(phase::PhaseRecord, Option<crate::PhaseAuthority>), Error> {
    let target = request.command.target();
    let attempt = state
        .attempts
        .get(&target.attempt.id)
        .filter(|a| a.snapshot.run_id == run.id)
        .ok_or(Error::NotFound)?;
    let call = state
        .calls
        .get(&target.call_id)
        .filter(|c| c.run_id == run.id)
        .ok_or(Error::NotFound)?;
    if matches!(request.command, PhaseCommand::Claim { .. }) {
        if state.phases.contains_key(&target.id) {
            return Err(Error::SemanticConflict);
        }
        let Some(PhaseWriteAuthority::Attempt(authority)) = authority else {
            return Err(Error::StaleOwner);
        };
        let previous = state
            .phases
            .values()
            .find(|r| r.snapshot.run_id == run.id && r.snapshot.target.call_id == call.id)
            .map(|r| &r.snapshot);
        let record =
            phase::PhaseRecord::claim(run, attempt, call, request, authority, now, previous)?;
        let grant = record.authority(authority.clone())?;
        Ok((record, Some(grant)))
    } else {
        let mut record = state
            .phases
            .get(&target.id)
            .filter(|r| r.snapshot.run_id == run.id)
            .cloned()
            .ok_or(Error::NotFound)?;
        let worker = match authority {
            Some(PhaseWriteAuthority::Phase(authority)) => Some(authority),
            _ => None,
        };
        if let Some(resolution) = resolution {
            record.resolve_uncertain(run, attempt, call, resolution)?;
        } else {
            record.change(run, attempt, call, request, worker, now)?;
        }
        Ok((record, None))
    }
}

#[cfg(test)]
#[path = "phase_tests.rs"]
mod tests;
