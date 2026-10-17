//! First-worker registration and native source checks share the actual lock.
#[path = "drivers/history.rs"]
mod history;
#[path = "drivers/queue.rs"]
mod queue;
#[path = "drivers/successor.rs"]
pub(super) mod successor;
pub(in crate::memory) use history::classified;
#[cfg(test)]
#[path = "drivers/tests.rs"]
mod tests;
#[cfg(test)]
#[path = "drivers/unkeyed.rs"]
mod unkeyed;
use super::super::State;
use super::{Error, MemoryStore, before, bump_public_revision, fault, revision};
use crate::native_parent::RunIssuance;
use crate::public_session::v2::proof::{Proof, ReadError};
use crate::session_owner::driver::{DriverLink, ExecutionDriverOutcome, RegisterExecutionDriver};
use crate::session_owner::preparation::admission::{SourceAudits, Witness};

impl MemoryStore {
    pub(in crate::memory) fn register_driver(
        &self,
        request: &RegisterExecutionDriver,
    ) -> Result<ExecutionDriverOutcome, Error> {
        request.fingerprint()?;
        let mut guard = self.state.lock().map_err(|_| Error::UnknownStoreOutcome)?;
        super::super::deletion::ensure_live(&guard, request.context.session_id())
            .map_err(crate::deletion::owner_error)?;
        let mut proof = Proof::bounded(crate::deletion::coverage::MAX_PROOF_BYTES);
        if let Some(receipt) = guard.session_owners.driver_receipt(request, &mut proof)? {
            let audit = guard
                .session_owners
                .source_audit(&receipt.session_id, &receipt.reserve.operation_id)
                .ok_or(Error::UnknownStoreOutcome)?;
            let source = audit
                .record
                .driver
                .as_deref()
                .ok_or(Error::UnknownStoreOutcome)?;
            original(&guard, source, &mut proof)?;
            return Ok(ExecutionDriverOutcome::Replayed(receipt));
        }
        // Borrowed bounded validation precedes even Memory's transaction clone.
        let source = fresh(&guard, request, self.clock.now(), &mut proof, None)?;
        let mut next = guard.clone();
        let current = revision(&next, request.context.session_id());
        let outcome = next.session_owners.register_driver(
            request,
            source,
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
            Ok(outcome)
        }
    }
}

pub(super) fn fresh(
    state: &State,
    request: &RegisterExecutionDriver,
    now: time::OffsetDateTime,
    proof: &mut Proof,
    activation_revision: Option<crate::session_owner::audit::RevisionState>,
) -> Result<DriverLink, Error> {
    request.fingerprint()?;
    state
        .session_owners
        .validate_driver_membership(request.context.session_id(), proof)?;
    let run = state
        .runs
        .get(&request.fence.run_id)
        .ok_or(Error::NotFound)?;
    if crate::snapshot::encoded_bytes(run, 32768).is_none() {
        return Err(Error::UnknownStoreOutcome);
    }
    measure(proof, run)?;
    if run.lease_until <= now {
        return Err(Error::StaleOwner);
    }
    let latest = super::super::native_parent::source_protocol::current(
        state,
        &run.session_id,
        &run.id,
        proof,
    )
    .map_err(|_| Error::UnknownStoreOutcome)?
    .ok_or(Error::UnknownStoreOutcome)?;
    latest.verify(run).map_err(|_| Error::UnknownStoreOutcome)?;
    if latest.sequence != 1 {
        return Err(Error::Unsupported);
    }
    let binding =
        super::super::execution_binding::read_with_proof(state, &run.session_id, &run.id, proof)
            .map_err(|_| Error::UnknownStoreOutcome)?
            .ok_or(Error::Unsupported)?;
    super::super::attempt::pristine_for_driver(state, run, &binding)
        .map_err(|_| Error::InvalidTransition)?;
    let (session, initial, witness, sources) = sources(state, &run.id, proof)?;
    witness.verify_sources(session, initial, &sources)?;
    let declared = sources
        .reserve
        .record
        .preparation
        .as_ref()
        .and_then(|origin| origin.resource_contract.as_ref());
    if declared.is_none()
        || crate::session_owner::preparation::resource_contract(&binding.capsule.runtime_semantics)?
            .as_ref()
            != declared
    {
        return Err(Error::Unsupported);
    }
    state
        .session_owners
        .driver_preparation(sources.child_begin, proof)?;
    if witness.is_unkeyed() {
        verify_unkeyed_eligibility(state, run, witness, activation_revision, proof)?;
        return DriverLink::initial_unkeyed(
            request,
            session,
            run,
            initial,
            witness,
            &sources,
            &binding.capsule,
        );
    }
    journal(state, run, &binding, proof)?;
    let init = initial_journal(state, &run.id, initial, proof)?;
    queue::revision(
        state,
        &crate::session_owner::preparation::queue_admission::AdmissionSources {
            session,
            initial,
            witness,
            audits: sources,
            init,
        },
        activation_revision,
        proof,
    )?;
    DriverLink::initial(request, session, run, initial, witness, &sources, init)
}
fn verify_unkeyed_eligibility(
    state: &State,
    run: &crabber_core::Run,
    witness: &Witness,
    activation_revision: Option<crate::session_owner::audit::RevisionState>,
    proof: &mut Proof,
) -> Result<(), Error> {
    let expected = activation_revision.unwrap_or(
        crate::session_owner::audit::RevisionState::Present(witness.admitted_revision()),
    );
    if revision(state, &run.session_id) != expected {
        return Err(Error::RevisionConflict);
    }
    let incompatible = state.admission_executions.contains_key(&run.id)
        || state.admission_execution_protocols.contains(&run.id)
        || [
            crate::admission_journal::Kind::Init,
            crate::admission_journal::Kind::Begin,
        ]
        .iter()
        .any(|kind| {
            state
                .admission_execution_sources
                .contains_key(&(run.id.clone(), *kind))
        });
    measure(proof, &(&run.id, incompatible))?;
    if incompatible {
        return Err(Error::UnknownStoreOutcome);
    }
    Ok(())
}
fn original(state: &State, source: &DriverLink, proof: &mut Proof) -> Result<(), Error> {
    original_read(state, source, proof)
        .map_err(|error| error.into_domain_or(Error::UnknownStoreOutcome))
}
fn initial_journal<'a>(
    state: &'a State,
    run: &crabber_core::RunId,
    initial: &RunIssuance,
    proof: &mut Proof,
) -> Result<&'a crate::admission_journal::Init, Error> {
    initial_journal_read(state, run, initial, proof)
        .map_err(|error| error.into_domain_or(Error::UnknownStoreOutcome))
}
fn original_read(
    state: &State,
    source: &DriverLink,
    proof: &mut Proof,
) -> Result<(), ReadError<Error>> {
    let (session, initial, witness, sources) = sources_read(state, source.run_id(), proof)?;
    if source.is_unkeyed() {
        return source
            .verify_original_unkeyed(session, initial, witness, &sources)
            .map_err(Into::into);
    }
    let init = initial_journal_read(state, source.run_id(), initial, proof)?;
    source
        .verify_original(session, initial, witness, &sources, init)
        .map_err(Into::into)
}
pub(super) fn initial_journal_read<'a>(
    state: &'a State,
    run: &crabber_core::RunId,
    initial: &RunIssuance,
    proof: &mut Proof,
) -> Result<&'a crate::admission_journal::Init, ReadError<Error>> {
    let protocol = state.admission_execution_protocols.contains(run);
    measure_read(proof, &(run, protocol))?;
    if !protocol {
        return Err(Error::UnknownStoreOutcome.into());
    }
    let source = state
        .admission_execution_sources
        .get(&(run.clone(), crate::admission_journal::Kind::Init))
        .ok_or(Error::UnknownStoreOutcome)?;
    measure_read(proof, source)?;
    if crate::snapshot::encoded_bytes(source, crate::admission_journal::MAX_BYTES).is_none() {
        return Err(Error::UnknownStoreOutcome.into());
    }
    let crate::admission_journal::Source::Init(init) = source else {
        return Err(Error::UnknownStoreOutcome.into());
    };
    let receipt = state
        .receipts
        .get(&(initial.state.session_id.clone(), init.driver_key().clone()))
        .ok_or(Error::UnknownStoreOutcome)?;
    measure_read(proof, receipt)?;
    init.verify_driver_origin(receipt, initial)
        .map_err(|_| Error::UnknownStoreOutcome)?;
    Ok(init)
}
/// First-driver eligibility requires the independently protected keyed journal.
/// Legacy unkeyed admission has no Init source and cannot acquire this capability.
fn journal(
    state: &State,
    run: &crabber_core::Run,
    binding: &crate::RetainedExecutionBinding,
    proof: &mut Proof,
) -> Result<(), Error> {
    use crate::admission_journal::Kind;
    let record = state
        .admission_executions
        .get(&run.id)
        .ok_or(Error::Unsupported)?;
    measure(proof, record)?;
    let protocol = state.admission_execution_protocols.contains(&run.id);
    measure(proof, &(&run.id, protocol))?;
    for kind in [Kind::Init, Kind::Begin] {
        if let Some(source) = state
            .admission_execution_sources
            .get(&(run.id.clone(), kind))
        {
            measure(proof, source)?;
            let issuer = source.issuance();
            let retained = state
                .native_run_issuances
                .get(&(run.session_id.clone(), run.id.clone(), issuer.sequence))
                .ok_or(Error::UnknownStoreOutcome)?;
            measure(proof, retained)?;
        }
    }
    let receipt = state
        .receipts
        .get(&(run.session_id.clone(), record.key.clone()))
        .ok_or(Error::UnknownStoreOutcome)?;
    measure(proof, receipt)?;
    if !protocol
        || receipt != &record.receipt
        || record.capsule.request != binding.capsule.request
        || record.capsule.runtime_semantics != binding.capsule.runtime_semantics
        || record.capsule.input_envelope != binding.capsule.input_envelope
        || record.capsule.config_hash != binding.capsule.config_hash.as_str()
        || record.capsule.plan_fingerprint != binding.capsule.plan_fingerprint.as_str()
        || record.capsule.behavior_fingerprint != binding.capsule.behavior_fingerprint
    {
        return Err(Error::UnknownStoreOutcome);
    }
    super::super::admission_journal::verify(state, &run.id)
        .map_err(|_| Error::UnknownStoreOutcome)?;
    if record.state != crate::AdmissionExecutionState::Unstarted
        || state
            .admission_execution_sources
            .contains_key(&(run.id.clone(), Kind::Begin))
    {
        return Err(Error::InvalidTransition);
    }
    Ok(())
}
pub(super) type Sources<'a> = (
    &'a crabber_core::Session,
    &'a RunIssuance,
    &'a Witness,
    SourceAudits<'a>,
);
fn sources<'a>(
    state: &'a State,
    run: &crabber_core::RunId,
    proof: &mut Proof,
) -> Result<Sources<'a>, Error> {
    sources_read(state, run, proof)
        .map_err(|error| error.into_domain_or(Error::UnknownStoreOutcome))
}
fn measure_read(proof: &mut Proof, value: &impl serde::Serialize) -> Result<(), ReadError<Error>> {
    proof.measure(value).map_err(ReadError::Budget)
}
pub(super) fn sources_read<'a>(
    state: &'a State,
    run: &crabber_core::RunId,
    proof: &mut Proof,
) -> Result<Sources<'a>, ReadError<Error>> {
    let witness = state
        .preparation_admissions
        .get(run)
        .ok_or(Error::Unsupported)?;
    measure_read(proof, witness)?;
    witness.validate()?;
    if witness.run_id() != run {
        return Err(Error::UnknownStoreOutcome.into());
    }
    let session = state
        .sessions
        .get(witness.session_id())
        .ok_or(Error::UnknownStoreOutcome)?;
    let initial = state
        .native_run_issuances
        .get(&(session.id.clone(), run.clone(), 1))
        .ok_or(Error::UnknownStoreOutcome)?;
    measure_read(proof, session)?;
    measure_read(proof, initial)?;
    let join = witness.join_id()?;
    let indexed = state
        .preparation_admission_joins
        .get(&(session.id.clone(), join.clone()))
        .ok_or(Error::UnknownStoreOutcome)?;
    measure_read(proof, &(&session.id, &join, indexed))?;
    if indexed != run {
        return Err(Error::UnknownStoreOutcome.into());
    }
    let ids = witness.audit_ids()?;
    let get = |id| {
        state
            .session_owners
            .source_audit(&session.id, id)
            .ok_or(Error::UnknownStoreOutcome)
    };
    let sources = SourceAudits {
        reserve: get(&ids[0])?,
        begin: get(&ids[1])?,
        join: get(&ids[2])?,
        child_reserve: get(&ids[3])?,
        child_begin: get(&ids[4])?,
    };
    for audit in [
        sources.reserve,
        sources.begin,
        sources.join,
        sources.child_reserve,
        sources.child_begin,
    ] {
        measure_read(proof, audit)?;
    }
    Ok((session, initial, witness, sources))
}
fn measure(proof: &mut Proof, value: &impl serde::Serialize) -> Result<(), Error> {
    proof.measure(value).map_err(|_| Error::UnknownStoreOutcome)
}
