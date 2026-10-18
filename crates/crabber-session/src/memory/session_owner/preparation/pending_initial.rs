//! The protected caller reuses the original actual prepared Initial publication.
use crate::memory::{MemoryStore, State};
use crate::native_enrollment::{
    caller::initial::{self, Dispatch, SelectedOriginal},
    pending_initial::{
        self as protocol, ActualRawMetadata, Assignment, Correlation, EnrolledInitialData,
        RawInitialReceipt, VerifiedInitialCall,
    },
};
use crate::public_session::v2::proof::{Failure, Proof};
use crate::session_owner::{
    SessionOwnerError as Error,
    preparation::{
        JoinedPreparation, PreparationCommitError, PreparationCompletion, PreparedOperation,
    },
};
fn failure(error: &Failure) -> PreparationCommitError {
    match error {
        Failure::Limited(_) => Error::Capacity.into(),
        Failure::Error(_) => Error::UnknownStoreOutcome.into(),
    }
}
impl MemoryStore {
    pub(in crate::memory) fn pending_initial_preflight(
        &self,
        state: &State,
        original: &JoinedPreparation,
        proof: &mut Proof,
    ) -> Result<(), PreparationCommitError> {
        state
            .session_owners
            .preflight_preparation(original.observed_grant(), proof)?;
        match original.observed_operation() {
            PreparedOperation::Admit(bound) => {
                crate::memory::admission::selected(state, &bound.request, None, proof)?;
            }
            PreparedOperation::AdmitKeyed(bound) => crate::memory::admission::selected(
                state,
                &bound.keyed.request,
                Some(&bound.keyed),
                proof,
            )?,
            _ => return Err(Error::Unsupported.into()),
        }
        let mut verified = state.session_owners.verify_preparation_with_proof(
            original.observed_grant(),
            crate::memory::session_owner::revision(
                state,
                &original
                    .observed_grant()
                    .original_reservation()
                    .owner
                    .binding
                    .session_id,
            ),
            self.clock.now(),
            proof,
        )?;
        verified.bind_observed_operation(original)?;
        Ok(())
    }
    fn join_pending_initial<'a>(
        &self,
        state: &mut State,
        original: &'a JoinedPreparation,
        proof: &mut Proof,
    ) -> Result<
        crate::session_owner::preparation::proof::VerifiedPreparation<'a>,
        PreparationCommitError,
    > {
        let session = &original
            .observed_grant()
            .original_reservation()
            .owner
            .binding
            .session_id;
        crate::memory::deletion::ensure_live(state, session)?;
        let before = crate::memory::session_owner::revision(state, session);
        let mut verified = state.session_owners.verify_preparation_with_proof(
            original.observed_grant(),
            before,
            self.clock.now(),
            proof,
        )?;
        verified.bind_observed_operation(original)?;
        state
            .session_owners
            .mutate_preparation_join(&verified, self.clock.now(), before)?;
        if state.sessions.contains_key(session) {
            crate::memory::bump_public_revision(state, session)?;
        }
        Ok(verified)
    }
    pub(in crate::memory) fn complete_pending_initial(
        &self,
        state: &mut State,
        original: &JoinedPreparation,
        call: &VerifiedInitialCall<'_>,
        proof: &mut Proof,
    ) -> Result<EnrolledInitialData, PreparationCommitError> {
        call.selected()
            .verify(original, proof)
            .map_err(|error| failure(&error))?;
        let verified = self.join_pending_initial(state, original, proof)?;
        let guard = crate::queue::AdmissionGuard::Preparation(&verified);
        let (completion, first) = match original.observed_operation() {
            PreparedOperation::Admit(bound) => {
                bound.binding.validate(&bound.request)?;
                let captured = crate::memory::admission::admit_transaction_captured(
                    state,
                    &bound.request,
                    self.clock.now(),
                    false,
                    guard,
                    Dispatch::pending(call),
                    proof,
                )?;
                retain(
                    state,
                    &captured.admitted,
                    &bound.request.user_message,
                    &bound.binding,
                    proof,
                )?;
                (
                    PreparationCompletion::Admitted(captured.admitted),
                    captured.first,
                )
            }
            PreparedOperation::AdmitKeyed(bound) => {
                bound.binding.validate_keyed(&bound.keyed)?;
                let captured = crate::memory::admission::admit_keyed_captured(
                    state,
                    &bound.keyed,
                    self.clock.now(),
                    guard,
                    Dispatch::pending(call),
                    proof,
                )?;
                let initial::CapturedKeyedAdmission::Started { receipt, admitted } = captured
                else {
                    return Err(Error::UnknownStoreOutcome.into());
                };
                let captured = *admitted;
                retain(
                    state,
                    &captured.admitted,
                    &bound.keyed.request.user_message,
                    &bound.binding,
                    proof,
                )?;
                (
                    PreparationCompletion::Keyed(crate::KeyedAdmitOutcome::Started {
                        receipt,
                        admitted: Box::new(captured.admitted),
                    }),
                    captured.first,
                )
            }
            _ => return Err(Error::Unsupported.into()),
        };
        let assignment = Assignment::from_actual(call, &completion, &first, proof)?;
        let view = state
            .pending_initial
            .selected(&call.correlation().key, &call.correlation().identity, proof)
            .map_err(|error| failure(&error))?;
        let delta = protocol::prepare_assignment(call, &assignment, view, proof)?;
        state
            .pending_initial
            .publish(delta, proof)
            .map_err(|error| failure(&error))?;
        crate::memory::native_enrollment::publish_pending_birth(state, &assignment, &first, proof)
            .map_err(|error| failure(&error))?;
        proof
            .charge(std::mem::size_of::<EnrolledInitialData>())
            .map_err(|error| failure(&error))?;
        Ok(EnrolledInitialData {
            completion,
            first,
            assignment,
        })
    }
    pub(in crate::memory) fn pending_initial_raw_completed(
        state: &State,
        original: &JoinedPreparation,
        expected: &Correlation,
        selected: &SelectedOriginal<'_>,
        proof: &mut Proof,
    ) -> Result<Option<ActualRawMetadata>, PreparationCommitError> {
        selected
            .verify(original, proof)
            .map_err(|error| failure(&error))?;
        let grant = original.observed_grant();
        let metadata = grant.reconciliation()?;
        let key = (metadata.session_id, metadata.join_id);
        proof.measure(&key).map_err(|error| failure(&error))?;
        let Some(run) = state.preparation_admission_joins.get(&key) else {
            if state.session_owners.source_audit(&key.0, &key.1).is_some() {
                return Err(Error::UnknownStoreOutcome.into());
            }
            if let PreparedOperation::AdmitKeyed(bound) = original.observed_operation()
                && state
                    .receipts
                    .contains_key(&(key.0.clone(), bound.keyed.options.key.clone()))
            {
                return Err(Error::Unsupported.into());
            }
            return Ok(None);
        };
        proof.measure(run).map_err(|error| failure(&error))?;
        match crate::memory::native_enrollment::original(state, &key.0, run, proof)
            .map_err(|error| failure(&error))?
        {
            crate::native_enrollment::Status::Pending => return Ok(None),
            crate::native_enrollment::Status::Legacy => return Err(Error::Unsupported.into()),
            crate::native_enrollment::Status::Unclassified => (),
        }
        let (session, issuer, witness, sources) =
            super::super::drivers::sources_read(state, run, proof)
                .map_err(|e| failure(&e.public()))?;
        witness.verify_sources(session, issuer, &sources)?;
        state.session_owners.verify_preparation_join(grant)?;
        let source = state
            .native_run_sources
            .get(&(key.0.clone(), run.clone(), 1))
            .ok_or(Error::UnknownStoreOutcome)?;
        proof.measure(source).map_err(|error| failure(&error))?;
        source
            .verify(issuer)
            .map_err(|_| Error::UnknownStoreOutcome)?;
        let first = initial::FirstPublication::capture(issuer, source, proof)
            .map_err(|error| failure(&error))?;
        let raw = match original.observed_operation() {
            PreparedOperation::Admit(bound) => {
                bound.binding.validate(&bound.request)?;
                let receipt = witness.replay(grant, session, issuer, &sources, &bound.binding)?;
                ActualRawMetadata::from_original(
                    expected,
                    &first,
                    RawInitialReceipt::Unkeyed(&receipt),
                    proof,
                )
                .map_err(|error| failure(&error))?
            }
            PreparedOperation::AdmitKeyed(bound) => {
                bound.binding.validate_keyed(&bound.keyed)?;
                let receipt = state
                    .receipts
                    .get(&(key.0.clone(), bound.keyed.options.key.clone()))
                    .ok_or(Error::UnknownStoreOutcome)?;
                proof.measure(receipt).map_err(|error| failure(&error))?;
                if receipt.run_id != *run
                    || receipt.semantic_digest != bound.keyed.validated_digest()?
                    || receipt.fingerprint != bound.keyed.options.fingerprint
                {
                    return Err(Error::SemanticConflict.into());
                }
                let init = super::super::drivers::initial_journal_read(state, run, issuer, proof)
                    .map_err(|e| failure(&e.public()))?;
                let execution = bound.keyed.execution.as_ref().ok_or(Error::Unsupported)?;
                init.verify_capsule(receipt, &bound.keyed.options.key, execution)?;
                ActualRawMetadata::from_original(
                    expected,
                    &first,
                    RawInitialReceipt::Keyed(receipt),
                    proof,
                )
                .map_err(|error| failure(&error))?
            }
            _ => return Err(Error::Unsupported.into()),
        };
        Ok(Some(raw))
    }
}
fn retain(
    state: &mut State,
    admitted: &crate::AdmitOutcome,
    user: &crabber_core::Message,
    binding: &crate::ExecutionBindingCapsule,
    proof: &mut Proof,
) -> Result<(), PreparationCommitError> {
    proof
        .measure(&(
            binding,
            &admitted.run.id,
            &admitted.run.session_id,
            &user.id,
        ))
        .map_err(|error| failure(&error))?;
    let retained = crate::RetainedExecutionBinding::new(&admitted.run, user, binding.clone());
    proof.measure(&retained).map_err(|error| failure(&error))?;
    let encoded = retained.encode()?;
    proof
        .measure(&admitted.run.id)
        .map_err(|error| failure(&error))?;
    state
        .execution_bindings
        .insert(admitted.run.id.clone(), encoded);
    Ok(())
}
