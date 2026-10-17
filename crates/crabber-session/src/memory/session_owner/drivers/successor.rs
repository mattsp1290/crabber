//! One exact noninitial native claim owns a distinct actual worker.
#[path = "successor/approval.rs"]
mod approval;
#[path = "successor/source.rs"]
mod source;
use super::super::{Error, MemoryStore, before, bump_public_revision, fault, revision};
use crate::memory::State;
use crate::public_session::v2::proof::Proof;
use crate::session_owner::audit::RevisionState;
use crate::session_owner::driver::{
    RegisterSuccessorExecutionDriver, SuccessorDriverLink, SuccessorExecutionDriverOutcome,
};

impl MemoryStore {
    pub(in crate::memory) fn register_successor_driver(
        &self,
        request: &RegisterSuccessorExecutionDriver,
    ) -> Result<SuccessorExecutionDriverOutcome, Error> {
        request.fingerprint()?;
        let mut guard = self.state.lock().map_err(|_| Error::UnknownStoreOutcome)?;
        super::super::super::deletion::ensure_live(&guard, request.context.session_id())
            .map_err(crate::deletion::owner_error)?;
        let mut proof = Proof::bounded(crate::deletion::coverage::MAX_PROOF_BYTES);
        if let Some(receipt) = guard
            .session_owners
            .successor_receipt(request, &mut proof)?
        {
            let selected = source::original(&guard, request, &mut proof, None)?;
            let actual = guard
                .session_owners
                .source_audit(
                    request.context.session_id(),
                    &receipt.worker.reserve.operation_id,
                )
                .and_then(|audit| audit.record.successor_driver.as_deref())
                .ok_or(Error::UnknownStoreOutcome)?;
            if actual != &selected.link {
                return Err(Error::SemanticConflict);
            }
            return Ok(SuccessorExecutionDriverOutcome::Replayed(receipt));
        }
        let link = fresh(&guard, request, None, self.clock.now(), &mut proof)?;
        let mut next = guard.clone();
        let current = revision(&next, request.context.session_id());
        let result = next.session_owners.register_successor(
            request,
            &link,
            current,
            self.clock.now(),
            &mut proof,
        )?;
        bump_public_revision(&mut next, request.context.session_id())
            .map_err(|_| Error::UnknownStoreOutcome)?;
        bump_public_revision(&mut next, request.context.session_id())
            .map_err(|_| Error::UnknownStoreOutcome)?;
        let boundary = fault(self);
        before(boundary)?;
        *guard = next;
        drop(guard);
        if boundary == 2 {
            Err(Error::UnknownStoreOutcome)
        } else {
            Ok(result)
        }
    }
}
pub(in crate::memory::session_owner) fn fresh(
    state: &State,
    request: &RegisterSuccessorExecutionDriver,
    expected: Option<RevisionState>,
    now: time::OffsetDateTime,
    proof: &mut Proof,
) -> Result<SuccessorDriverLink, Error> {
    request.fingerprint()?;
    let session = request.context.session_id();
    let id = &request.fence.run_id;
    state
        .session_owners
        .validate_driver_membership(session, proof)?;
    let run = state.runs.get(id).ok_or(Error::NotFound)?;
    super::measure(proof, run)?;
    if crate::snapshot::encoded_bytes(run, 4 << 20).is_none() {
        return Err(Error::UnknownStoreOutcome);
    }
    if run.session_id != *session
        || run.claim_token != request.fence.claim_token
        || run.lease_until <= now
    {
        return Err(Error::StaleOwner);
    }
    let latest = crate::memory::native_parent::source_protocol::current(state, session, id, proof)
        .map_err(|_| Error::UnknownStoreOutcome)?
        .ok_or(Error::UnknownStoreOutcome)?;
    let current = state.sessions.get(session).ok_or(Error::NotFound)?;
    super::measure(proof, current)?;
    if crate::snapshot::encoded_bytes(current, 16384).is_none()
        || current.workspace_id != request.context.workspace_id()
        || current.directory != request.context.directory()
    {
        return Err(Error::UnknownStoreOutcome);
    }
    let binding = crate::memory::execution_binding::read_with_proof(state, session, id, proof)
        .map_err(|_| Error::UnknownStoreOutcome)?
        .ok_or(Error::Unsupported)?;
    if run.config_hash != binding.capsule.config_hash.as_str()
        || run.plan_fingerprint != binding.capsule.plan_fingerprint.as_str()
    {
        return Err(Error::UnknownStoreOutcome);
    }
    let selected = source::original(state, request, proof, Some(&binding.capsule))?;
    if latest.sequence != selected.head
        || crate::native_parent::issuance_digest(latest).map_err(|_| Error::UnknownStoreOutcome)?
            != selected.head_digest
    {
        return Err(Error::StaleOwner);
    }
    if let Some((continuation, parent)) = &selected.approval {
        approval::fresh(state, run, continuation, parent, now, proof)?;
    }
    let root = state
        .session_owners
        .source_audit(session, &request.claim.root_join_id)
        .ok_or(Error::UnknownStoreOutcome)?;
    state
        .session_owners
        .successor_preparation(root, &selected.child_begin, now, proof)?;
    if revision(state, session) != expected.unwrap_or(RevisionState::Present(selected.revision)) {
        return Err(Error::RevisionConflict);
    }
    if latest.completion_source.as_ref().is_some_and(|source| {
        source.native_operation == crate::native_parent::CompletionOperation::AdmissionRecovery
    }) {
        let record = state
            .admission_executions
            .get(id)
            .ok_or(Error::Unsupported)?;
        super::measure(proof, record)?;
        if crate::snapshot::encoded_bytes(record, 16 << 20).is_none() {
            return Err(Error::UnknownStoreOutcome);
        }
        super::journal(state, run, &binding, proof)?;
    }
    Ok(selected.link)
}

#[cfg(test)]
#[path = "successor/tests.rs"]
mod tests;

#[cfg(test)]
#[path = "successor/approval_tests.rs"]
mod approval_tests;

#[cfg(test)]
#[path = "successor/birth_source_tests.rs"]
mod birth_source_tests;
