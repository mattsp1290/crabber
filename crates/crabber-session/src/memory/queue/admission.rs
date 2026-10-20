//! One actual Queue-selected input remains scoped through its Initial result.
use super::{
    AdmissionGuard, Change, MemoryStore, QueueMutation, QueueOutcome, State, StoreError, complete,
    corrupt,
};
impl MemoryStore {
    pub(super) fn queue_admit_and_publish(
        &self,
        state: &mut State,
        operation: (
            QueueMutation,
            crabber_core::InputFingerprint,
            Option<crate::queue::source::Source>,
            time::OffsetDateTime,
        ),
        mut change: Change<'_>,
        preparation: Option<&crate::session_owner::preparation::proof::VerifiedPreparation<'_>>,
        read_proof: &mut crate::public_session::v2::proof::Proof,
    ) -> Result<QueueOutcome, StoreError> {
        let (request, digest, previous, now) = operation;
        let Some(admission) = change.admission.take() else {
            let (saved, authority) = super::publish_queue_change(
                state,
                (request, digest, previous, now),
                change,
                read_proof,
            )?;
            return Ok(QueueOutcome::Applied {
                receipt: saved,
                authority,
                admitted: None,
            });
        };
        crate::native_enrollment::caller::initial::with_queue_input(
            &admission,
            read_proof,
            |selected, read_proof| {
                let captured = self.queue_admission(
                    state,
                    &mut change,
                    (&admission, selected),
                    preparation,
                    read_proof,
                )?;
                let (saved, authority) = super::publish_queue_change(
                    state,
                    (request, digest, previous, now),
                    change,
                    read_proof,
                )?;
                let actual = &state
                    .queue_audits
                    .get(&(saved.session_id.clone(), saved.operation_id.clone()))
                    .ok_or_else(corrupt)?
                    .request;
                let input = crate::native_enrollment::caller::initial::Input::Queue {
                    request: actual,
                    prepared: admission.bound_ref(),
                    receipt: &captured.receipt,
                    queue_receipt: &saved,
                    selected,
                };
                let admitted = crate::memory::admission::finish_initial(
                    state,
                    input,
                    captured.admitted,
                    read_proof,
                    #[cfg(test)]
                    &self.native_raw_probe,
                )?;
                Ok(QueueOutcome::Applied {
                    receipt: saved,
                    authority,
                    admitted: Some(Box::new(admitted)),
                })
            },
        )
    }
    fn queue_admission(
        &self,
        state: &mut State,
        change: &mut Change<'_>,
        input: (
            &crate::queue::VerifiedAdmission<'_>,
            &crate::native_enrollment::caller::initial::SelectedInput<'_>,
        ),
        preparation: Option<&crate::session_owner::preparation::proof::VerifiedPreparation<'_>>,
        read_proof: &mut crate::public_session::v2::proof::Proof,
    ) -> Result<crate::native_enrollment::caller::initial::CapturedQueueAdmission, StoreError> {
        let (proof, selected) = input;
        let bound = proof.bound_ref();
        let outcome = crate::memory::admission::admit_keyed_captured(
            state,
            &bound.keyed,
            self.clock.now(),
            preparation.map_or(AdmissionGuard::Queue(proof), |preparation| {
                AdmissionGuard::PreparedQueue {
                    queue: proof,
                    preparation,
                }
            }),
            selected.dispatch(crate::native_enrollment::caller::Caller::Raw),
            read_proof,
        )?;
        let crate::native_enrollment::caller::initial::CapturedKeyedAdmission::Started {
            receipt,
            admitted,
        } = outcome
        else {
            return Err(StoreError::AdmissionConflict);
        };
        let binding = crate::RetainedExecutionBinding::new(
            &admitted.admitted.run,
            &bound.keyed.request.user_message,
            bound.binding.clone(),
        );
        state
            .execution_bindings
            .insert(admitted.admitted.run.id.clone(), binding.encode()?);
        complete(change, &admitted.admitted, receipt.clone());
        Ok(
            crate::native_enrollment::caller::initial::CapturedQueueAdmission {
                admitted: *admitted,
                receipt,
            },
        )
    }
}
