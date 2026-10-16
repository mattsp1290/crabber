//! Coherent pure recovery preflight; no owner, grant or revision publication.
use super::{Error, MemoryStore};
use crate::{
    ClaimUnstartedAdmissionRequest, session_owner::preparation::AdmissionRecoveryPreparation,
};
impl MemoryStore {
    pub(in crate::memory) fn inspect_admission_source(
        &self,
        request: ClaimUnstartedAdmissionRequest,
    ) -> Result<AdmissionRecoveryPreparation, Error> {
        AdmissionRecoveryPreparation::validate_request(&request)?;
        let state = self.state.lock().expect("memory store poisoned");
        let (revision, sequence, binding) = context(&state, &request, self.clock.now())?;
        let digest = AdmissionRecoveryPreparation::fingerprint_binding(&binding)
            .map_err(|_| Error::SemanticConflict)?;
        AdmissionRecoveryPreparation::from_verified_source(request, revision, sequence, digest)
    }
}
pub(in crate::memory) fn validate_source(
    state: &super::super::State,
    source: &AdmissionRecoveryPreparation,
    now: time::OffsetDateTime,
) -> Result<(), Error> {
    source.fingerprint()?;
    let (_, sequence, binding) = context(state, source.request(), now)?;
    source.verify_binding(&binding)?;
    if sequence != source.expected_issuance() {
        return Err(Error::StaleOwner);
    }
    Ok(())
}
fn context(
    state: &super::super::State,
    request: &ClaimUnstartedAdmissionRequest,
    now: time::OffsetDateTime,
) -> Result<
    (
        crabber_core::SessionRevision,
        u64,
        crate::ExecutionBindingCapsule,
    ),
    Error,
> {
    let run = state
        .runs
        .get(&request.expected_fence.run_id)
        .ok_or(Error::MissingEvidence)?;
    if run.checkpoint.is_some()
        || run.error.is_some()
        || crate::snapshot::encoded_bytes(run, 32768).is_none()
    {
        return Err(Error::SemanticConflict);
    }
    let record = state
        .admission_executions
        .get(&run.id)
        .ok_or(Error::MissingEvidence)?;
    super::super::admission_journal::verify(state, &run.id).map_err(|_| Error::SemanticConflict)?;
    let count = record
        .capsule
        .input_envelope
        .as_ref()
        .map_or(1, |input| input.messages().len());
    let messages: Vec<_> = state
        .messages
        .iter()
        .filter(|message| {
            message.session_id == run.session_id && message.run_id.as_ref() == Some(&run.id)
        })
        .take(count)
        .cloned()
        .collect();
    record.validate_messages(run, &messages)?;
    if state
        .receipts
        .get(&(request.session_id.clone(), request.key.clone()))
        != Some(&record.receipt)
    {
        return Err(Error::SemanticConflict);
    }
    record.verify_claim(request, run)?;
    if run.status != crabber_core::RunStatus::Running {
        return Err(Error::SemanticConflict);
    }
    if run.lease_until > now {
        return Err(Error::LiveLease);
    }
    let sequence = super::super::native_parent::latest_run(state, run)
        .map_err(|_| Error::SemanticConflict)?
        .ok_or(Error::MissingEvidence)?
        .sequence;
    let crate::session_owner::audit::RevisionState::Present(revision) =
        super::super::session_owner::revision(state, &request.session_id)
    else {
        return Err(Error::MissingEvidence);
    };
    let binding = super::super::execution_binding::read(state, &request.session_id, &run.id)
        .map_err(|_| Error::SemanticConflict)?
        .ok_or(Error::MissingEvidence)?;
    Ok((revision, sequence, binding.capsule))
}
