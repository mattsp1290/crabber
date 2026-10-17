use super::{AdmissionGuard, PostgresStore, complete};
use crate::StoreError;
use sqlx::{Postgres, Transaction};
impl PostgresStore {
    pub(super) async fn queue_admit_and_publish(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        operation: (
            crate::QueueMutation,
            crabber_core::InputFingerprint,
            Option<crate::queue::source::Source>,
            time::OffsetDateTime,
        ),
        mut change: crate::queue::Change,
        preparation: Option<&crate::session_owner::preparation::proof::VerifiedPreparation<'_>>,
        read_proof: &mut crate::public_session::v2::proof::Proof,
    ) -> Result<crate::QueueOutcome, StoreError> {
        let Some(admission) = change.admission.take() else {
            let (audit, authority) = publish(tx, operation, change, read_proof).await?;
            return Ok(crate::QueueOutcome::Applied {
                receipt: audit.receipt,
                authority,
                admitted: None,
            });
        };
        let admission_ref = &admission;
        crate::native_enrollment::caller::initial::with_queue_input_async(
            &admission,
            read_proof,
            |selected, read_proof| {
                Box::pin(async move {
                    let captured = self
                        .queue_admission(
                            tx,
                            &mut change,
                            (admission_ref, selected),
                            preparation,
                            read_proof,
                        )
                        .await?;
                    let (audit, authority) = publish(tx, operation, change, read_proof).await?;
                    let input = crate::native_enrollment::caller::initial::Input::Queue {
                        request: &audit.request,
                        prepared: admission_ref.bound_ref(),
                        receipt: &captured.receipt,
                        queue_receipt: &audit.receipt,
                        selected,
                    };
                    let admitted = self
                        .finish_initial(tx, input, captured.admitted, read_proof)
                        .await?;
                    Ok(crate::QueueOutcome::Applied {
                        receipt: audit.receipt,
                        authority,
                        admitted: Some(Box::new(admitted)),
                    })
                })
            },
        )
        .await
    }
    async fn queue_admission(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        change: &mut crate::queue::Change,
        input: (
            &crate::queue::VerifiedAdmission,
            &crate::native_enrollment::caller::initial::SelectedInput<'_>,
        ),
        preparation: Option<&crate::session_owner::preparation::proof::VerifiedPreparation<'_>>,
        read_proof: &mut crate::public_session::v2::proof::Proof,
    ) -> Result<crate::native_enrollment::caller::initial::CapturedQueueAdmission, StoreError> {
        let (proof, selected) = input;
        let bound = proof.bound_ref();
        let outcome = self
            .admit_keyed_captured(
                tx,
                &bound.keyed,
                preparation.map_or(AdmissionGuard::Queue(proof), |preparation| {
                    AdmissionGuard::PreparedQueue {
                        queue: proof,
                        preparation,
                    }
                }),
                selected.dispatch(crate::native_enrollment::caller::Caller::Raw),
                read_proof,
            )
            .await?;
        let crate::native_enrollment::caller::initial::CapturedKeyedAdmission::Started {
            receipt,
            admitted,
        } = outcome
        else {
            return Err(StoreError::AdmissionConflict);
        };
        super::super::execution_binding::save(
            tx,
            &crate::RetainedExecutionBinding::new(
                &admitted.admitted.run,
                &bound.keyed.request.user_message,
                bound.binding.clone(),
            ),
        )
        .await?;
        complete(change, &admitted.admitted, receipt.clone());
        Ok(
            crate::native_enrollment::caller::initial::CapturedQueueAdmission {
                admitted: *admitted,
                receipt,
            },
        )
    }
}

async fn publish(
    tx: &mut Transaction<'_, Postgres>,
    operation: (
        crate::QueueMutation,
        crabber_core::InputFingerprint,
        Option<crate::queue::source::Source>,
        time::OffsetDateTime,
    ),
    change: crate::queue::Change,
    proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<(crate::queue::Audit, Option<crate::QueueAuthority>), StoreError> {
    let (request, digest, previous, now) = operation;
    let saved =
        super::publish_queue_revision(tx, &request, digest, change.record.summary.clone()).await?;
    let audit = crate::queue::Audit {
        request,
        receipt: saved,
        authority: change.authority.clone(),
        created_at: now,
    };
    super::save(tx, &audit, &change.record, previous, proof).await?;
    Ok((audit, change.authority))
}
