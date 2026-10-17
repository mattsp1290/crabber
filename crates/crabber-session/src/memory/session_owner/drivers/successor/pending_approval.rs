//! No-grant preflight before factories, with one borrowed selected read allowance.
use super::{Error, MemoryStore, Proof, State, approval, source};
use crate::session_owner::preparation::{
    InspectPendingApprovalPreparation as Request, PendingApprovalPreparationMetadata as Metadata,
    PendingApprovalSource, pending_approval as choice,
};
impl MemoryStore {
    pub(in crate::memory) fn inspect_pending_approval(
        &self,
        request: &Request,
    ) -> Result<Metadata, Error> {
        let metadata = choice::metadata(request)?;
        let state = self.state.lock().map_err(|_| Error::UnknownStoreOutcome)?;
        inspect(
            &state,
            request,
            metadata,
            self.clock.now(),
            &mut Proof::bounded(32 << 20),
        )
    }
}
fn bounded(proof: &mut Proof, value: &impl serde::Serialize, cap: usize) -> Result<(), Error> {
    proof
        .measure(value)
        .map_err(|_| Error::UnknownStoreOutcome)?;
    crate::snapshot::encoded_bytes(value, cap).ok_or(Error::UnknownStoreOutcome)?;
    Ok(())
}
fn inspect(
    state: &State,
    request: &Request,
    metadata: Metadata,
    now: time::OffsetDateTime,
    proof: &mut Proof,
) -> Result<Metadata, Error> {
    let claim = choice::claim(request);
    let session = state
        .sessions
        .get(&claim.session_id)
        .ok_or(Error::NotFound)?;
    bounded(proof, session, 32768)?;
    if session.workspace_id != request.context.workspace_id()
        || session.directory != request.context.directory()
    {
        return Err(Error::IdentityMismatch);
    }
    let current_revision = super::revision(state, &claim.session_id);
    bounded(proof, &current_revision, 32)?;
    if current_revision
        != crate::session_owner::audit::RevisionState::Present(choice::revision(request))
    {
        return Err(Error::RevisionConflict);
    }
    crate::memory::deletion::ensure_live(state, &claim.session_id)
        .map_err(crate::deletion::owner_error)?;
    let run = state.runs.get(&claim.run_id).ok_or(Error::NotFound)?;
    bounded(proof, run, 4 << 20)?;
    if run.session_id != claim.session_id || run.id != claim.run_id {
        return Err(Error::IdentityMismatch);
    }
    crate::memory::native_parent::source_protocol::current(
        state,
        &claim.session_id,
        &claim.run_id,
        proof,
    )
    .map_err(|_| Error::UnknownStoreOutcome)?
    .ok_or(Error::Unsupported)?
    .verify(run)
    .map_err(|_| Error::UnknownStoreOutcome)?;
    approval::protected(state, run, &claim.continuation, proof)?;
    let binding = crate::memory::execution_binding::read_with_proof(
        state,
        &claim.session_id,
        &claim.run_id,
        proof,
    )
    .map_err(|_| Error::UnknownStoreOutcome)?
    .ok_or(Error::Unsupported)?;
    source::approval::input_for(state, &claim.session_id, &claim.run_id, proof)?.verify(
        &claim.session_id,
        &claim.run_id,
        Some(&binding.capsule),
    )?;
    let record = state
        .approvals
        .get(&claim.continuation.id)
        .ok_or(Error::NotFound)?;
    bounded(proof, record, 4 << 20)?;
    record
        .validate(&claim.session_id, &claim.run_id)
        .map_err(Error::ApprovalSource)?;
    if state.approval_barriers.get(&claim.session_id) != Some(&claim.continuation.id)
        || state.tool_barriers.contains_key(&claim.session_id)
    {
        return Err(Error::Busy);
    }
    crate::memory::approval::origin::verify_with_proof(state, run, record, proof)
        .map_err(|_| Error::UnknownStoreOutcome)?;
    crate::memory::approval::current::verify_with_proof(state, run, record, false, proof)
        .map_err(|_| Error::UnknownStoreOutcome)?;
    let original = match &request.source {
        PendingApprovalSource::Claim(_) => None,
        PendingApprovalSource::Recovery(recovery) => Some(
            crate::memory::approval::receipt_with_proof(
                state,
                &claim.session_id,
                &recovery.original.id,
                proof,
            )
            .map_err(|_| Error::UnknownStoreOutcome)?
            .ok_or(Error::NotFound)?,
        ),
    };
    choice::eligible(record, run, &request.source, original.as_ref(), now)?;
    approval::suspended(state, &record.batch, proof)?;
    let current = latest(state, &run.id, proof)?;
    choice::parent(record, run, &request.source, current, now)?;
    if matches!(request.source, PendingApprovalSource::Recovery(_)) {
        approval::unstarted(state, run, &current.snapshot.target, proof)?;
    }
    calls(state, record, proof)?;
    Ok(metadata)
}
fn latest<'a>(
    state: &'a State,
    run: &crabber_core::RunId,
    proof: &mut Proof,
) -> Result<&'a crate::attempt::AttemptRecord, Error> {
    let mut latest = None;
    for (key, record) in state
        .attempts
        .iter()
        .filter(|(_, v)| v.snapshot.run_id == *run)
    {
        bounded(
            proof,
            &(
                &record.snapshot,
                &record.owner,
                &record.token,
                &record.run_token,
                record.lease_until,
            ),
            8192,
        )?;
        bounded(proof, key, 512)?;
        if key != &record.snapshot.target.id {
            return Err(Error::UnknownStoreOutcome);
        }
        record.validate().map_err(|_| Error::UnknownStoreOutcome)?;
        if latest.is_none_or(|old: &crate::attempt::AttemptRecord| {
            old.snapshot.target.generation < record.snapshot.target.generation
        }) {
            latest = Some(record);
        }
    }
    latest.ok_or(Error::NotFound)
}
fn calls(
    state: &State,
    record: &crate::approval::ApprovalRecord,
    proof: &mut Proof,
) -> Result<(), Error> {
    let mut count = 0;
    for (id, call) in state.calls.iter().filter(|(_, c)| {
        c.run_id == record.run_id
            && matches!(
                c.status,
                crabber_core::ToolCallStatus::Pending | crabber_core::ToolCallStatus::Running
            )
    }) {
        bounded(proof, &(id, &call.id, &call.run_id, call.status), 2048)?;
        if *id != call.id {
            return Err(Error::UnknownStoreOutcome);
        }
        count += 1;
        if count > crate::MAX_APPROVAL_CALLS {
            return Err(Error::Capacity);
        }
    }
    if count != record.batch.calls.len() {
        return Err(Error::SemanticConflict);
    }
    for frozen in &record.batch.calls {
        let call = state
            .calls
            .get(&frozen.target.call_id)
            .ok_or(Error::NotFound)?;
        bounded(proof, call, 4 << 20)?;
        if call.id != frozen.target.call_id {
            return Err(Error::UnknownStoreOutcome);
        }
        choice::call(record, call)?;
    }
    for phase in state
        .phases
        .values()
        .filter(|p| p.snapshot.run_id == record.run_id)
    {
        bounded(proof, &phase.snapshot.target, 2048)?;
        if record
            .batch
            .calls
            .iter()
            .any(|f| f.target.call_id == phase.snapshot.target.call_id)
        {
            return Err(Error::Busy);
        }
    }
    Ok(())
}
#[cfg(test)]
#[path = "pending_approval/tests.rs"]
mod tests;
