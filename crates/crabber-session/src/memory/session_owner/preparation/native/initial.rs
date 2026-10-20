//! Actual initial completion bodies share the original preparation transaction.
use crate::memory::{MemoryStore, State};
use crate::native_enrollment::caller::{Caller, initial::Dispatch};
use crate::public_session::v2::proof::Proof;
use crate::session_owner::preparation::{
    PreparationCommitError, PreparationCompletion, proof::VerifiedPreparation,
};
use crate::{KeyedAdmitOutcome, RetainedExecutionBinding};
impl MemoryStore {
    pub(super) fn admit_prepared_bound(
        &self,
        state: &mut State,
        bound: &crate::BoundAdmitRequest,
        proof: &VerifiedPreparation<'_>,
        read_proof: &mut Proof,
    ) -> Result<PreparationCompletion, PreparationCommitError> {
        let guard = crate::queue::AdmissionGuard::Preparation(proof);

        read_proof
            .measure(&(&bound.request, &bound.binding))
            .map_err(|_| crate::StoreError::Conflict)?;
        bound.binding.validate(&bound.request)?;
        let captured = crate::memory::admission::admit_transaction_captured(
            state,
            &bound.request,
            self.clock.now(),
            false,
            guard,
            Dispatch::raw(Caller::Raw),
            read_proof,
        )?;
        let retained = RetainedExecutionBinding::new(
            &captured.admitted.run,
            &bound.request.user_message,
            bound.binding.clone(),
        );
        state
            .execution_bindings
            .insert(captured.admitted.run.id.clone(), retained.encode()?);
        let admitted = crate::memory::admission::finish_initial(
            state,
            crate::native_enrollment::caller::initial::Input::Bound {
                request: &bound.request,
                binding: &bound.binding,
            },
            captured,
            read_proof,
            #[cfg(test)]
            &self.native_raw_probe,
        )?;
        Ok(PreparationCompletion::Admitted(admitted))
    }
    pub(super) fn admit_prepared_keyed(
        &self,
        state: &mut State,
        bound: &crate::BoundKeyedAdmitRequest,
        proof: &VerifiedPreparation<'_>,
        read_proof: &mut Proof,
    ) -> Result<PreparationCompletion, PreparationCommitError> {
        let guard = crate::queue::AdmissionGuard::Preparation(proof);

        read_proof
            .measure(&(&bound.keyed, &bound.binding))
            .map_err(|_| crate::StoreError::Conflict)?;
        let outcome = match crate::memory::admission::admit_keyed_captured(
            state,
            &bound.keyed,
            self.clock.now(),
            guard,
            Dispatch::raw(Caller::Raw),
            read_proof,
        )? {
            crate::native_enrollment::caller::initial::CapturedKeyedAdmission::Replayed(
                receipt,
            ) => KeyedAdmitOutcome::Replayed(receipt),
            crate::native_enrollment::caller::initial::CapturedKeyedAdmission::Started {
                receipt,
                admitted,
            } => {
                let retained = RetainedExecutionBinding::new(
                    &admitted.admitted.run,
                    &bound.keyed.request.user_message,
                    bound.binding.clone(),
                );
                state
                    .execution_bindings
                    .insert(admitted.admitted.run.id.clone(), retained.encode()?);
                let admitted = crate::memory::admission::finish_initial(
                    state,
                    crate::native_enrollment::caller::initial::Input::BoundKeyed {
                        request: &bound.keyed,
                        binding: &bound.binding,
                        receipt: &receipt,
                    },
                    *admitted,
                    read_proof,
                    #[cfg(test)]
                    &self.native_raw_probe,
                )?;
                KeyedAdmitOutcome::Started {
                    receipt,
                    admitted: Box::new(admitted),
                }
            }
        };
        Ok(PreparationCompletion::Keyed(outcome))
    }
}
