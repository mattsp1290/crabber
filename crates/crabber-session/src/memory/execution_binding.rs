use super::{MemoryStore, State};
use crate::public_session::v2::proof::{Proof, ReadError};
use crate::{
    AdmitOutcome, BoundAdmitRequest, BoundKeyedAdmitRequest, KeyedAdmitOutcome,
    RetainedExecutionBinding, StoreError,
};
use crabber_core::{RunId, SessionId};

pub(super) fn read(
    state: &State,
    session: &SessionId,
    id: &RunId,
) -> Result<Option<RetainedExecutionBinding>, StoreError> {
    read_with_proof(state, session, id, &mut Proof::legacy())
        .map_err(|error| error.into_domain_or(StoreError::AdmissionConflict))
}
pub(super) fn read_with_proof(
    state: &State,
    session: &SessionId,
    id: &RunId,
    proof: &mut Proof,
) -> Result<Option<RetainedExecutionBinding>, ReadError<StoreError>> {
    let Some(run) = state.runs.get(id).filter(|run| run.session_id == *session) else {
        return Ok(None);
    };
    let Some(encoded) = state.execution_bindings.get(id) else {
        return Ok(None);
    };
    proof.charge(encoded.len()).map_err(ReadError::Budget)?;
    proof
        .measure(&(&run.config_hash, &run.plan_fingerprint))
        .map_err(ReadError::Budget)?;
    let binding = RetainedExecutionBinding::decode(encoded)?;
    let count = binding
        .capsule
        .input_envelope
        .as_ref()
        .map_or(1, |input| input.messages().len());
    for message in state
        .messages
        .iter()
        .filter(|m| m.run_id.as_ref() == Some(id) && m.session_id == *session)
        .take(count)
    {
        proof.measure(message).map_err(ReadError::Budget)?;
    }
    let messages: Vec<_> = state
        .messages
        .iter()
        .filter(|message| message.run_id.as_ref() == Some(id) && message.session_id == *session)
        .take(count)
        .cloned()
        .collect();
    binding.validate_messages(
        id,
        session,
        &run.config_hash,
        &run.plan_fingerprint,
        &messages,
    )?;
    state
        .sessions
        .get(session)
        .ok_or(StoreError::AdmissionConflict)?
        .ensure_identity(
            &binding.capsule.request.workspace_id,
            &binding.capsule.request.directory,
        )?;
    Ok(Some(binding))
}
impl MemoryStore {
    #[cfg(test)]
    pub(crate) fn arm_binding_fault(&self, stage: u8) {
        self.binding_fault
            .store(stage, std::sync::atomic::Ordering::SeqCst);
    }
    pub(super) fn read_execution_binding(
        &self,
        session: &SessionId,
        run: &RunId,
    ) -> Result<Option<RetainedExecutionBinding>, StoreError> {
        read(&self.state.lock().unwrap(), session, run)
    }
    pub(super) fn admit_with_binding(
        &self,
        request: &BoundAdmitRequest,
    ) -> Result<AdmitOutcome, StoreError> {
        let mut proof = Proof::bounded(32 << 20);
        proof
            .measure(&(&request.request, &request.binding))
            .map_err(|_| StoreError::Conflict)?;
        request.binding.validate(&request.request)?;
        let result = self.transact_selected(
            &mut proof,
            |state, proof| {
                super::admission::selected(state, &request.request, None, proof)?;
                Ok(None)
            },
            |state, proof| {
                let captured = super::admission::admit_transaction_captured(
                    state,
                    &request.request,
                    self.clock.now(),
                    false,
                    crate::queue::AdmissionGuard::Ordinary,
                    crate::native_enrollment::caller::initial::Dispatch::raw(
                        crate::native_enrollment::caller::Caller::Raw,
                    ),
                    proof,
                )?;
                let retained = RetainedExecutionBinding::new(
                    &captured.admitted.run,
                    &request.request.user_message,
                    request.binding.clone(),
                );
                state
                    .execution_bindings
                    .insert(captured.admitted.run.id.clone(), retained.encode()?);
                #[cfg(test)]
                crate::binding_fault::trip(&self.binding_fault, 1)?;
                super::admission::finish_initial(
                    state,
                    crate::native_enrollment::caller::initial::Input::Bound {
                        request: &request.request,
                        binding: &request.binding,
                    },
                    captured,
                    proof,
                    #[cfg(test)]
                    &self.native_raw_probe,
                )
            },
        )?;
        #[cfg(test)]
        crate::binding_fault::trip(&self.binding_fault, 2)?;
        Ok(result)
    }
    pub(super) fn admit_keyed_with_binding(
        &self,
        request: &BoundKeyedAdmitRequest,
    ) -> Result<KeyedAdmitOutcome, StoreError> {
        let mut proof = Proof::bounded(32 << 20);
        proof
            .measure(&(&request.keyed, &request.binding))
            .map_err(|_| StoreError::Conflict)?;
        request.binding.validate_keyed(&request.keyed)?;
        let result = self.transact_selected(
            &mut proof,
            |state, proof| {
                super::admission::selected(
                    state,
                    &request.keyed.request,
                    Some(&request.keyed),
                    proof,
                )?;
                Ok(None)
            },
            |state, proof| match super::admission::admit_keyed_captured(
                state,
                &request.keyed,
                self.clock.now(),
                crate::queue::AdmissionGuard::Ordinary,
                crate::native_enrollment::caller::initial::Dispatch::raw(
                    crate::native_enrollment::caller::Caller::Raw,
                ),
                proof,
            )? {
                crate::native_enrollment::caller::initial::CapturedKeyedAdmission::Replayed(
                    receipt,
                ) => {
                    if read_with_proof(state, &receipt.session_id, &receipt.run_id, proof)
                        .map_err(|e| e.into_domain_or(StoreError::AdmissionConflict))?
                        .is_none_or(|original| original.capsule != request.binding)
                    {
                        return Err(StoreError::AdmissionConflict);
                    }
                    Ok(KeyedAdmitOutcome::Replayed(receipt))
                }
                crate::native_enrollment::caller::initial::CapturedKeyedAdmission::Started {
                    receipt,
                    admitted,
                } => {
                    let retained = RetainedExecutionBinding::new(
                        &admitted.admitted.run,
                        &request.keyed.request.user_message,
                        request.binding.clone(),
                    );
                    state
                        .execution_bindings
                        .insert(admitted.admitted.run.id.clone(), retained.encode()?);
                    #[cfg(test)]
                    crate::binding_fault::trip(&self.binding_fault, 1)?;
                    let admitted = super::admission::finish_initial(
                        state,
                        crate::native_enrollment::caller::initial::Input::BoundKeyed {
                            request: &request.keyed,
                            binding: &request.binding,
                            receipt: &receipt,
                        },
                        *admitted,
                        proof,
                        #[cfg(test)]
                        &self.native_raw_probe,
                    )?;
                    Ok(KeyedAdmitOutcome::Started {
                        receipt,
                        admitted: Box::new(admitted),
                    })
                }
            },
        )?;
        #[cfg(test)]
        if matches!(&result, KeyedAdmitOutcome::Started { .. }) {
            crate::binding_fault::trip(&self.binding_fault, 2)?;
        }
        Ok(result)
    }
}
