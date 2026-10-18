//! Native-only borrowed historical preparation, binding and exact member sources.
use crate::memory::{MemoryStore, State};
use crate::native_enrollment::{
    initial_consumer as domain,
    pending_initial::{Assignment, prefix::HistoricalInitialSources},
};
use crate::public_session::v2::proof::{Failure, Proof, ReadError, unknown};
use crabber_core::Run;
impl MemoryStore {
    pub(in crate::memory) fn classify_initial_pending(
        state: &State,
        run: &Run,
        assignment: &Assignment,
        proof: &mut Proof,
    ) -> Result<domain::InitialConsumption, Failure> {
        let expected = &assignment.correlation;
        let birth = state
            .pending_initial
            .selected(&expected.key, &expected.identity, proof)?;
        let view = super::selected(state, expected, proof)?;
        if !view.empty() {
            view.verify_birth(birth, proof)?;
            super::native::verify_members(state, &view, proof).map_err(unknown)?;
        }
        state
            .session_owners
            .validate_driver_membership(&run.session_id, proof)
            .map_err(unknown)?;
        let (session, initial, witness, audits) =
            crate::memory::session_owner::drivers::sources_read(state, &run.id, proof)
                .map_err(ReadError::public)?;
        let activation = crate::session_owner::owned_scope::source::activation_key(
            &session.id,
            &audits.child_begin.record.snapshot.target,
        )
        .map_err(unknown)?;
        let activated = state
            .session_owners
            .source_audit(&session.id, &activation)
            .ok_or_else(|| unknown("Initial original child activation"))?;
        let binding = crate::memory::execution_binding::read_with_proof(
            state,
            &run.session_id,
            &run.id,
            proof,
        )
        .map_err(ReadError::public)?
        .ok_or_else(|| unknown("Initial original binding"))?;
        let keyed = keyed(state, run, initial, witness.is_unkeyed(), proof)?;
        HistoricalInitialSources {
            session,
            initial,
            witness,
            audits,
            activated,
            binding: &binding,
            keyed,
        }
        .verify(expected, proof)?;
        domain::classify_closed(&view, birth, run, proof)
    }
}
fn keyed<'a>(
    state: &'a State,
    run: &Run,
    initial: &crate::native_parent::RunIssuance,
    unkeyed: bool,
    proof: &mut Proof,
) -> Result<
    Option<(
        &'a crate::AdmissionExecutionRecord,
        &'a crate::admission_journal::Init,
    )>,
    Failure,
> {
    use crate::admission_journal::Kind;
    proof.measure(&(
        &run.id,
        state.admission_execution_protocols.contains(&run.id),
    ))?;
    let record = state.admission_executions.get(&run.id);
    for kind in [Kind::Init, Kind::Begin] {
        if let Some(source) = state
            .admission_execution_sources
            .get(&(run.id.clone(), kind))
        {
            proof.measure(source)?;
            let issuer = source.issuance();
            let actual = state
                .native_run_issuances
                .get(&(run.session_id.clone(), run.id.clone(), issuer.sequence))
                .ok_or_else(|| unknown("Initial keyed issuer"))?;
            proof.measure(actual)?;
        }
    }
    if let Some(record) = record {
        proof.measure(record)?;
    }
    crate::memory::admission_journal::verify(state, &run.id).map_err(unknown)?;
    if unkeyed {
        if record.is_some() || state.admission_execution_protocols.contains(&run.id) {
            return Err(unknown("unkeyed Initial has keyed journal"));
        }
        return Ok(None);
    }
    let record = record.ok_or_else(|| unknown("keyed Initial missing original execution"))?;
    let init =
        crate::memory::session_owner::drivers::initial_journal_read(state, &run.id, initial, proof)
            .map_err(ReadError::public)?;
    Ok(Some((record, init)))
}
