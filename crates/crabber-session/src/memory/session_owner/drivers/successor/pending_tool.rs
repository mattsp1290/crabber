//! No-grant preflight before factories, with one borrowed selected read allowance.
use super::{Error, MemoryStore, Proof, State, approval, source};
use crate::session_owner::preparation::{
    InspectPendingToolPreparation as Request, PendingToolPreparationMetadata as Metadata,
    pending_tool as choice,
};
impl MemoryStore {
    pub(in crate::memory) fn inspect_pending_tool(
        &self,
        request: &Request,
    ) -> Result<Metadata, Error> {
        let state = self.state.lock().map_err(|_| Error::UnknownStoreOutcome)?;
        read(
            &state,
            request,
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
fn read(
    state: &State,
    request: &Request,
    now: time::OffsetDateTime,
    proof: &mut Proof,
) -> Result<Metadata, Error> {
    let metadata = choice::metadata_with_proof(request, proof)?;
    inspect(state, request, metadata, now, proof)
}
fn inspect(
    state: &State,
    request: &Request,
    metadata: Metadata,
    now: time::OffsetDateTime,
    proof: &mut Proof,
) -> Result<Metadata, Error> {
    let claim = choice::claim(request);
    let run = scoped_run(state, request, proof)?;
    let protected = crate::memory::native_parent::source_protocol::current(
        state,
        &claim.session_id,
        &claim.run_id,
        proof,
    )
    .map_err(|_| Error::UnknownStoreOutcome)?
    .ok_or(Error::Unsupported)?;
    protected
        .verify(run)
        .map_err(|_| Error::UnknownStoreOutcome)?;
    let binding = original_input(state, claim, proof)?;
    let record = crate::memory::tool_continuation::checked_record_with_proof(
        state,
        &claim.continuation.id,
        proof,
    )
    .map_err(|_| Error::UnknownStoreOutcome)?;
    if state.tool_barriers.get(&claim.session_id) != Some(&claim.continuation.id)
        || state.approval_barriers.contains_key(&claim.session_id)
    {
        return Err(Error::Busy);
    }
    for other in state
        .tool_continuations
        .values()
        .filter(|r| r.session_id == run.session_id && r.run_id == run.id)
    {
        bounded(proof, &other.batch.continuation, 1024)?;
        if other.batch.continuation.generation > claim.continuation.generation {
            return Err(Error::Busy);
        }
    }
    let original = if choice::recovery(request).is_some() {
        Some(
            crate::memory::tool_continuation::receipt_with_proof(
                state,
                &claim.session_id,
                &claim.id,
                proof,
            )
            .map_err(|_| Error::UnknownStoreOutcome)?
            .ok_or(Error::NotFound)?,
        )
    } else {
        None
    };
    choice::eligible(&record, run, request, original.as_ref())?;
    let old = state
        .attempts
        .get(&record.batch.attempt.id)
        .ok_or(Error::NotFound)?;
    bounded(
        proof,
        &(
            &old.snapshot,
            &old.owner,
            &old.token,
            &old.run_token,
            old.lease_until,
        ),
        8192,
    )?;
    crate::session_owner::driver::successor::tool_fresh::parent(
        run,
        old,
        &record.batch.attempt,
        false,
        now,
    )?;
    let current = latest(state, &run.id, proof)?;
    choice::parent(&record, run, request, current, now)?;
    if choice::recovery(request).is_some() {
        approval::unstarted(state, run, &current.snapshot.target, proof)?;
    }
    if choice::recovery(request).is_some() {
        provenance::check(state, run, &record, &binding.capsule, protected, proof)?;
    }
    carriers::check(
        state,
        run,
        &record,
        choice::recovery(request).is_some(),
        proof,
    )?;
    Ok(metadata)
}
fn original_input(
    state: &State,
    claim: &crate::ToolContinuationClaim,
    proof: &mut Proof,
) -> Result<crate::RetainedExecutionBinding, Error> {
    let scope = crate::continuation_input::Scope {
        domain: crate::continuation_input::Domain::Tool,
        session: claim.session_id.clone(),
        run: claim.run_id.clone(),
        continuation: claim.continuation.clone(),
    };
    if !state
        .continuation_inputs
        .protocol(&scope, proof)
        .map_err(|_| Error::UnknownStoreOutcome)?
    {
        return Err(Error::Unsupported);
    }
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
    Ok(binding)
}
fn scoped_run<'a>(
    state: &'a State,
    request: &Request,
    proof: &mut Proof,
) -> Result<&'a crabber_core::Run, Error> {
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
    Ok(run)
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

#[path = "pending_tool/carriers.rs"]
mod carriers;

#[cfg(test)]
#[path = "pending_tool/tests.rs"]
mod tests;

#[cfg(test)]
#[path = "pending_tool/coherent_tests.rs"]
mod coherent_tests;

#[path = "pending_tool/provenance.rs"]
mod provenance;

#[cfg(test)]
#[path = "pending_tool/latest_tests.rs"]
mod latest_tests;
