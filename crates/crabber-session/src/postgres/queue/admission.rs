use super::{AdmissionGuard, PostgresStore, complete};
use crate::StoreError;
use sqlx::{Postgres, Transaction};
impl PostgresStore {
    pub(super) async fn finish_queue_initial(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        request: &crate::QueueMutation,
        saved: &crate::QueueReceipt,
        pending_initial: Option<crate::native_enrollment::caller::initial::CapturedQueueAdmission>,
        read_proof: &mut crate::public_session::v2::proof::Proof,
    ) -> Result<Option<Box<crate::AdmitOutcome>>, StoreError> {
        Ok(if let Some(captured) = pending_initial {
            let input = crate::native_enrollment::caller::initial::Input::Queue {
                request,
                prepared: &captured.prepared,
                receipt: &captured.receipt,
                queue_receipt: saved,
            };
            Some(Box::new(
                self.finish_initial(tx, input, captured.admitted, read_proof)
                    .await?,
            ))
        } else {
            None
        })
    }
    pub(super) async fn queue_admission(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        change: &mut crate::queue::Change,
        preparation: Option<&crate::session_owner::preparation::proof::VerifiedPreparation<'_>>,
        read_proof: &mut crate::public_session::v2::proof::Proof,
    ) -> Result<Option<crate::native_enrollment::caller::initial::CapturedQueueAdmission>, StoreError>
    {
        Ok(if let Some(proof) = &change.admission {
            read_proof
                .measure(proof.bound_ref())
                .map_err(|_| StoreError::Conflict)?;
            let bound = proof.bound();
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
                    crate::native_enrollment::caller::Caller::Raw,
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
            Some(
                crate::native_enrollment::caller::initial::CapturedQueueAdmission {
                    admitted: *admitted,
                    prepared: bound,
                    receipt,
                },
            )
        } else {
            None
        })
    }
}
