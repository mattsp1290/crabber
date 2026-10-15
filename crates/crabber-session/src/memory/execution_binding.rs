use super::{MemoryStore, State, admit_keyed_transaction, admit_transaction};
use crate::{
    AdmitOutcome, BoundAdmitRequest, BoundKeyedAdmitRequest, KeyedAdmitOutcome,
    RetainedExecutionBinding, StoreError,
};
use crabber_core::{RunId, SessionId};

fn read(
    state: &State,
    session: &SessionId,
    id: &RunId,
) -> Result<Option<RetainedExecutionBinding>, StoreError> {
    let Some(run) = state.runs.get(id).filter(|run| run.session_id == *session) else {
        return Ok(None);
    };
    let Some(encoded) = state.execution_bindings.get(id) else {
        return Ok(None);
    };
    let binding = RetainedExecutionBinding::decode(encoded)?;
    let user = state
        .messages
        .iter()
        .find(|message| message.id == binding.user_message_id)
        .ok_or(StoreError::AdmissionConflict)?;
    binding.validate(id, session, &run.config_hash, &run.plan_fingerprint, user)?;
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
        request: BoundAdmitRequest,
    ) -> Result<AdmitOutcome, StoreError> {
        request.binding.validate(&request.request)?;
        let result = self.transact(|state| {
            let admitted = admit_transaction(state, &request.request, self.clock.now(), false)?;
            let retained = RetainedExecutionBinding::new(
                &admitted.run,
                &request.request.user_message,
                request.binding,
            );
            state
                .execution_bindings
                .insert(admitted.run.id.clone(), retained.encode()?);
            #[cfg(test)]
            crate::binding_fault::trip(&self.binding_fault, 1)?;
            Ok(admitted)
        })?;
        #[cfg(test)]
        crate::binding_fault::trip(&self.binding_fault, 2)?;
        Ok(result)
    }
    pub(super) fn admit_keyed_with_binding(
        &self,
        request: BoundKeyedAdmitRequest,
    ) -> Result<KeyedAdmitOutcome, StoreError> {
        request.binding.validate_keyed(&request.keyed)?;
        let result = self.transact(|state| {
            let outcome = admit_keyed_transaction(state, &request.keyed, self.clock.now())?;
            match &outcome {
                KeyedAdmitOutcome::Started { admitted, .. } => {
                    let retained = RetainedExecutionBinding::new(
                        &admitted.run,
                        &request.keyed.request.user_message,
                        request.binding,
                    );
                    state
                        .execution_bindings
                        .insert(admitted.run.id.clone(), retained.encode()?);
                    #[cfg(test)]
                    crate::binding_fault::trip(&self.binding_fault, 1)?;
                }
                KeyedAdmitOutcome::Replayed(receipt) => {
                    if read(state, &receipt.session_id, &receipt.run_id)?
                        .is_none_or(|original| original.capsule != request.binding)
                    {
                        return Err(StoreError::AdmissionConflict);
                    }
                }
            }
            Ok(outcome)
        })?;
        #[cfg(test)]
        if matches!(&result, KeyedAdmitOutcome::Started { .. }) {
            crate::binding_fault::trip(&self.binding_fault, 2)?;
        }
        Ok(result)
    }
}
