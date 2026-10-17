//! Actual expired keyed-admission claim and its same-transaction Raw witness.
use super::{Error, MemoryStore};
use crate::native_enrollment::caller::{Caller, Fresh};
use crate::native_enrollment::caller::{initial::Capture, recovery::AdmissionRecovery};
use crate::public_session::v2::proof::Proof;
use crate::{ClaimUnstartedAdmissionRequest, ClaimedAdmission};
use crabber_core::RunFence;
fn unknown(_: impl std::fmt::Debug) -> Error {
    Error::UnknownStoreFailure
}
impl MemoryStore {
    pub(in crate::memory) fn claim_admission(
        &self,
        request: ClaimUnstartedAdmissionRequest,
    ) -> Result<ClaimedAdmission, Error> {
        self.claim_admission_with_proof(request, &mut Proof::bounded(32 << 20))
    }
    pub(in crate::memory) fn claim_admission_with_proof(
        &self,
        request: ClaimUnstartedAdmissionRequest,
        proof: &mut Proof,
    ) -> Result<ClaimedAdmission, Error> {
        crate::native_enrollment::caller::recovery::measure_request(&request, proof)
            .map_err(unknown)?;
        #[cfg(test)]
        let run_id = request.expected_fence.run_id.clone();
        let result = self.transact_selected(
            proof,
            |state, proof| {
                if state.session_owners.has_preparation(&request.session_id) {
                    return Err(Error::PreparationBusy);
                }
                selected(state, &request, proof)?;
                Ok(None)
            },
            |state, proof| self.claim_admission_transaction(state, &request, None, proof),
        );
        #[cfg(test)]
        if result.is_ok() && self.native_raw_probe.after_admission_recovery(&run_id) {
            return Err(Error::UnknownStoreFailure);
        }
        result
    }
    pub(in crate::memory) fn claim_admission_transaction(
        &self,
        state: &mut super::super::State,
        request: &ClaimUnstartedAdmissionRequest,
        completion: Option<crate::native_parent::CompletionSource>,
        proof: &mut Proof,
    ) -> Result<ClaimedAdmission, Error> {
        crate::native_enrollment::caller::recovery::measure_request(request, proof)
            .map_err(unknown)?;
        proof.measure(&completion).map_err(unknown)?;
        selected(state, request, proof)?;
        let run = state
            .runs
            .get(&request.expected_fence.run_id)
            .ok_or(Error::MissingEvidence)?;
        // Charged by selected before either original-result clone.
        proof.measure(run).map_err(unknown)?;
        let previous = run.clone();
        proof.measure(run).map_err(unknown)?;
        let mut run = run.clone();
        let record = state
            .admission_executions
            .get(&run.id)
            .ok_or(Error::MissingEvidence)?;
        record.verify_claim(request, &run)?;
        let now = self.clock.now();
        if run.lease_until > now {
            return Err(Error::LiveLease);
        }
        let lease = time::Duration::try_from(request.lease).map_err(|_| Error::SemanticConflict)?;
        if lease <= time::Duration::ZERO {
            return Err(Error::SemanticConflict);
        }
        run.owner.clone_from(&request.owner);
        run.claim_token = uuid::Uuid::new_v4().to_string();
        run.lease_until = now.checked_add(lease).ok_or(Error::SemanticConflict)?;
        run.updated_at = now;
        crate::ensure_storable(&run).map_err(unknown)?;
        let outcome = ClaimedAdmission {
            record: record.clone(),
            fence: RunFence {
                run_id: run.id.clone(),
                claim_token: run.claim_token.clone(),
            },
            run,
        };
        let published = super::super::native_parent::record_run_completed_publication_with_proof(
            state,
            &previous,
            &outcome.run,
            completion.as_ref(),
            proof,
            Capture::Original,
        )
        .map_err(unknown)?;
        let publication = published
            .updated
            .as_ref()
            .ok_or(Error::UnknownStoreFailure)?;
        // Reserve the actual retained output before the final refusal boundary.
        proof.measure(&outcome.run).map_err(unknown)?;
        super::super::native_enrollment::record_raw_native(
            state,
            Caller::Raw,
            &Fresh::AdmissionRecovery(AdmissionRecovery {
                request,
                previous: &previous,
                result: &outcome,
                completion: completion.as_ref(),
                publication,
            }),
            proof,
            #[cfg(test)]
            &self.native_raw_probe,
        )
        .map_err(unknown)?;
        state
            .runs
            .insert(outcome.run.id.clone(), outcome.run.clone());
        Ok(outcome)
    }
}
fn selected(
    state: &super::super::State,
    request: &ClaimUnstartedAdmissionRequest,
    proof: &mut Proof,
) -> Result<(), Error> {
    super::super::admission_journal::verify_with_proof(
        state,
        &request.expected_fence.run_id,
        proof,
    )
    .map_err(unknown)?;
    let run = state
        .runs
        .get(&request.expected_fence.run_id)
        .ok_or(Error::MissingEvidence)?;
    proof.measure(run).map_err(unknown)?;
    let record = state
        .admission_executions
        .get(&run.id)
        .ok_or(Error::MissingEvidence)?;
    proof.measure(record).map_err(unknown)?;
    let count = record
        .capsule
        .input_envelope
        .as_ref()
        .map_or(1, |input| input.messages().len());
    let messages = state
        .messages
        .iter()
        .filter(|message| {
            message.run_id.as_ref() == Some(&run.id) && message.session_id == run.session_id
        })
        .take(count)
        .map(|message| {
            proof.measure(message).map_err(unknown)?;
            Ok(message.clone())
        })
        .collect::<Result<Vec<_>, Error>>()?;
    record.validate_messages(run, &messages)?;
    let receipt = state
        .receipts
        .get(&(request.session_id.clone(), request.key.clone()));
    proof.measure(&receipt).map_err(unknown)?;
    if receipt != Some(&record.receipt) {
        return Err(Error::SemanticConflict);
    }
    Ok(())
}
