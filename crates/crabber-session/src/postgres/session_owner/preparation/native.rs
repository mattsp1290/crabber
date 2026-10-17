//! Apply the exact task-returned native intent under the preparation gate.
#[path = "native/continuation.rs"]
mod continuation;
#[path = "native/control.rs"]
mod control;
use crate::session_owner::preparation::{PreparationCompletion, PreparedOperation};
use crate::{KeyedAdmitOutcome, RetainedExecutionBinding};
impl super::super::PostgresStore {
    pub(super) async fn apply_prepared_native(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        operation: PreparedOperation,
        proof: &crate::session_owner::preparation::proof::VerifiedPreparation<'_>,
        read_proof: &mut crate::public_session::v2::proof::Proof,
    ) -> Result<PreparationCompletion, crate::session_owner::preparation::PreparationCommitError>
    {
        let guard = crate::queue::AdmissionGuard::Preparation(proof);
        let completed = match operation {
            PreparedOperation::ExpiredRunResume(prepared) => PreparationCompletion::ExpiredRun(
                self.claim_prepared_expired(tx, &prepared, proof, read_proof)
                    .await?,
            ),
            PreparedOperation::AdmissionRecovery(recovery) => {
                PreparationCompletion::AdmissionRecovery(
                    crate::session_owner::preparation::AdmissionRecoveryOutcome::Claimed(Box::new(
                        super::super::super::admission_execution::claim_prepared(
                            self, tx, &recovery, proof, read_proof,
                        )
                        .await?,
                    )),
                )
            }
            operation @ (PreparedOperation::FrontendClaim(_)
            | PreparedOperation::FrontendRecovery(_)
            | PreparedOperation::ApprovalClaim(_)
            | PreparedOperation::ToolClaim(_)
            | PreparedOperation::ApprovalRecovery(_)
            | PreparedOperation::ToolRecovery(_)) => {
                Box::pin(self.apply_prepared_continuation(tx, operation, proof, read_proof)).await?
            }
            PreparedOperation::Queue { request, .. } => {
                let digest = request.fingerprint()?;
                PreparationCompletion::Queue(
                    Box::pin(self.queue_apply_guarded(
                        tx,
                        request,
                        digest,
                        Some(proof),
                        read_proof,
                    ))
                    .await?,
                )
            }
            PreparedOperation::Admit(bound) => {
                self.admit_prepared_bound(tx, *bound, proof, read_proof)
                    .await?
            }
            PreparedOperation::AdmitKeyed(bound) => {
                read_proof
                    .measure(&(&bound.keyed, &bound.binding))
                    .map_err(|_| crate::StoreError::Conflict)?;
                let outcome=match self.admit_keyed_captured(tx,&bound.keyed,guard,crate::native_enrollment::caller::Caller::Raw,read_proof).await?{
                    crate::native_enrollment::caller::initial::CapturedKeyedAdmission::Replayed(receipt)=>KeyedAdmitOutcome::Replayed(receipt),
                    crate::native_enrollment::caller::initial::CapturedKeyedAdmission::Started{receipt,admitted}=>{
                        super::super::super::execution_binding::save(tx,&RetainedExecutionBinding::new(&admitted.admitted.run,&bound.keyed.request.user_message,bound.binding.clone())).await?;
                        let admitted=self.finish_initial(tx,crate::native_enrollment::caller::initial::Input::BoundKeyed{request:&bound.keyed,binding:&bound.binding,receipt:&receipt},*admitted,read_proof).await?;
                        KeyedAdmitOutcome::Started{receipt,admitted:Box::new(admitted)}
                    }
                };
                PreparationCompletion::Keyed(outcome)
            }
        };
        control::verify(tx, proof.grant, &completed, read_proof).await?;
        Ok(completed)
    }
}

impl super::super::PostgresStore {
    async fn admit_prepared_bound(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        bound: crate::BoundAdmitRequest,
        proof: &crate::session_owner::preparation::proof::VerifiedPreparation<'_>,
        read_proof: &mut crate::public_session::v2::proof::Proof,
    ) -> Result<PreparationCompletion, crate::session_owner::preparation::PreparationCommitError>
    {
        let guard = crate::queue::AdmissionGuard::Preparation(proof);
        read_proof
            .measure(&(&bound.request, &bound.binding))
            .map_err(|_| crate::StoreError::Conflict)?;
        bound.binding.validate(&bound.request)?;
        let captured = self
            .admit_transaction_captured(
                tx,
                &bound.request,
                false,
                guard,
                crate::native_enrollment::caller::Caller::Raw,
                read_proof,
            )
            .await?;
        super::super::super::execution_binding::save(
            tx,
            &RetainedExecutionBinding::new(
                &captured.admitted.run,
                &bound.request.user_message,
                bound.binding.clone(),
            ),
        )
        .await?;
        let admitted = self
            .finish_initial(
                tx,
                crate::native_enrollment::caller::initial::Input::Bound {
                    request: &bound.request,
                    binding: &bound.binding,
                },
                captured,
                read_proof,
            )
            .await?;
        Ok(PreparationCompletion::Admitted(admitted))
    }
}
