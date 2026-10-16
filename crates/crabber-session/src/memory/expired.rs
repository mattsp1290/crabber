//! Pure generic-expiry inspection under the actual Store lock.
#[path = "expired/unkeyed.rs"]
pub(super) mod unkeyed;
#[path = "expired/unstarted.rs"]
pub(super) mod unstarted;
use super::{MemoryStore, State};
use crate::session_owner::{
    SessionOwnerError as Error,
    preparation::{ExpiredRunPreparation, InspectExpiredRunPreparation},
};
use crabber_core::{InputFingerprint, Run, RunStatus};
impl MemoryStore {
    pub(super) fn inspect_expired_source(
        &self,
        request: InspectExpiredRunPreparation,
    ) -> Result<ExpiredRunPreparation, Error> {
        request.validate()?;
        let state = self.state.lock().map_err(unknown)?;
        super::deletion::ensure_live(&state, &request.session_id)
            .map_err(crate::deletion::owner_error)?;
        let source = context(&state, request, self.clock.now())?;
        crate::session_owner::preparation::expired::unstarted::validate_capture(&source)?;
        Ok(source)
    }
}
pub(super) fn context(
    state: &State,
    request: InspectExpiredRunPreparation,
    now: time::OffsetDateTime,
) -> Result<ExpiredRunPreparation, Error> {
    request.validate()?;
    let run = state
        .runs
        .get(&request.expected_fence.run_id)
        .ok_or(Error::NotFound)?;
    // A real suspended checkpoint is bounded by the native boundary contract;
    // no private Run is cloned before checking this finite carrier.
    if crate::snapshot::encoded_bytes(run, crate::MAX_RUN_BOUNDARY_BYTES + 32768).is_none()
        || run.error.is_some()
    {
        return Err(Error::UnknownStoreOutcome);
    }
    if run.session_id != request.session_id || run.claim_token != request.expected_fence.claim_token
    {
        return Err(Error::StaleOwner);
    }
    if run.lease_until > now {
        return Err(Error::LiveOwner);
    }
    if super::has_pending_continuation(state, &run.session_id).map_err(unknown)? {
        return Err(Error::Busy);
    }
    let binding = super::execution_binding::read(state, &run.session_id, &run.id)
        .map_err(unknown)?
        .ok_or(Error::Unsupported)?;
    let issuer = super::native_parent::latest_run(state, run)
        .map_err(unknown)?
        .ok_or(Error::Unsupported)?;
    let (native, origin) = match run.status {
        RunStatus::Paused => (
            suspended(state, run)?,
            crate::session_owner::preparation::ExpiredRunOrigin::Suspended(
                state
                    .suspension_proofs
                    .get(&run.id)
                    .ok_or(Error::UnknownStoreOutcome)?
                    .receipt
                    .operation_id
                    .clone(),
            ),
        ),
        RunStatus::Running => initial_source(state, run, &binding, now)?,
        _ => return Err(Error::InvalidTransition),
    };
    let crate::session_owner::audit::RevisionState::Present(revision) =
        super::session_owner::revision(state, &run.session_id)
    else {
        return Err(Error::NotFound);
    };
    let target = crate::session_owner::preparation::ExpiredRunTarget {
        issuance: issuer.sequence,
        fence_digest: issuer.state.token.clone(),
        issuer_digest: crate::native_parent::issuance_digest(issuer).map_err(unknown)?,
        binding_digest: ExpiredRunPreparation::fingerprint_binding(&binding.capsule)?,
        native_source: native,
        origin,
        owner: request.owner.clone(),
        lease: request.lease,
    };
    ExpiredRunPreparation::from_verified_source(request, revision, target)
}
fn initial_source(
    state: &State,
    run: &Run,
    binding: &crate::RetainedExecutionBinding,
    now: time::OffsetDateTime,
) -> Result<
    (
        InputFingerprint,
        crate::session_owner::preparation::ExpiredRunOrigin,
    ),
    Error,
> {
    let initial = initial_provenance(state, run, binding)?;
    unstarted::guard(state, run, binding, now, initial)
}
fn initial_provenance(
    state: &State,
    run: &Run,
    binding: &crate::RetainedExecutionBinding,
) -> Result<
    (
        InputFingerprint,
        crate::session_owner::preparation::ExpiredRunOrigin,
    ),
    Error,
> {
    if let Some(original) = unkeyed::read(state, &run.session_id, &run.id)? {
        original.verify_binding(&binding.capsule)?;
        return Ok((original.digest()?, original.origin()?));
    }
    let digest = initial(state, run, binding)?;
    let key = state
        .admission_executions
        .get(&run.id)
        .ok_or(Error::Unsupported)?
        .key
        .clone();
    Ok((
        digest,
        crate::session_owner::preparation::ExpiredRunOrigin::KeyedInitial(key),
    ))
}
fn suspended(state: &State, run: &Run) -> Result<InputFingerprint, Error> {
    if state
        .phases
        .values()
        .filter(|phase| phase.snapshot.run_id == run.id)
        .take(65)
        .count()
        > 64
        || state
            .calls
            .values()
            .filter(|call| call.run_id == run.id)
            .take(65)
            .count()
            > 64
    {
        return Err(Error::Capacity);
    }
    super::run_control::suspension::verify(state, run).map_err(unknown)?;
    let proof = state
        .suspension_proofs
        .get(&run.id)
        .ok_or(Error::UnknownStoreOutcome)?;
    crate::session_owner::preparation::expired::suspension_digest(proof, run)
}
fn initial(
    state: &State,
    run: &Run,
    binding: &crate::RetainedExecutionBinding,
) -> Result<InputFingerprint, Error> {
    use crate::admission_journal::{Kind, Source};
    if state
        .admission_executions
        .get(&run.id)
        .is_some_and(|record| crate::snapshot::encoded_bytes(record, 16_777_216).is_none())
    {
        return Err(Error::UnknownStoreOutcome);
    }
    super::admission_journal::verify(state, &run.id).map_err(unknown)?;
    if !state.admission_execution_protocols.contains(&run.id) {
        return Err(Error::Unsupported);
    }
    let record = state
        .admission_executions
        .get(&run.id)
        .ok_or(Error::Unsupported)?;
    if record.state != crate::AdmissionExecutionState::Started {
        return Err(Error::InvalidTransition);
    }
    if state
        .receipts
        .get(&(run.session_id.clone(), record.key.clone()))
        != Some(&record.receipt)
    {
        return Err(Error::UnknownStoreOutcome);
    }
    let Some(Source::Init(init)) = state
        .admission_execution_sources
        .get(&(run.id.clone(), Kind::Init))
    else {
        return Err(Error::UnknownStoreOutcome);
    };
    let frozen = &record.capsule;
    if frozen.request != binding.capsule.request
        || frozen.runtime_semantics != binding.capsule.runtime_semantics
        || frozen.input_envelope != binding.capsule.input_envelope
        || frozen.config_hash != binding.capsule.config_hash.as_str()
        || frozen.plan_fingerprint != binding.capsule.plan_fingerprint.as_str()
        || frozen.behavior_fingerprint != binding.capsule.behavior_fingerprint
    {
        return Err(Error::UnknownStoreOutcome);
    }
    crate::session_owner::validation::digest(&(
        "crabber.preparation.expired-initial.v1",
        init.digest().map_err(unknown)?,
    ))
}
fn unknown(_: impl std::fmt::Debug) -> Error {
    Error::UnknownStoreOutcome
}

impl MemoryStore {
    pub(in crate::memory) fn claim_prepared_expired(
        &self,
        state: &mut State,
        prepared: &crate::session_owner::preparation::PreparedExpiredRunResume,
        proof: &crate::session_owner::preparation::proof::VerifiedPreparation<'_>,
    ) -> Result<
        crate::session_owner::preparation::ExpiredRunOutcome,
        crate::session_owner::preparation::PreparationCommitError,
    > {
        use crate::session_owner::preparation::expired;
        prepared.validate()?;
        let source = &prepared.source;
        let current = context(state, source.request().clone(), self.clock.now())?;
        source.verify_source(&current)?;
        let completion = expired::completion_source(source, proof, current.expected_revision())?;
        let original = state
            .runs
            .get(&source.request().expected_fence.run_id)
            .ok_or(Error::NotFound)?
            .clone();
        let next = expired::replacement(&original, source, self.clock.now())?;
        super::native_parent::record_run_completed(state, &original, &next, Some(&completion))
            .map_err(unknown)?;
        let outcome = expired::claimed(source, &completion, current.expected_revision(), &next);
        state.runs.insert(next.id.clone(), next);
        Ok(outcome)
    }
}

#[cfg(test)]
#[path = "expired/tests.rs"]
mod tests;
