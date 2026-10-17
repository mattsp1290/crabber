use super::{AdmissionGuard, PostgresStore, complete};
use crate::StoreError;
use crate::postgres::transaction::Transaction;
use sqlx::Postgres;
impl PostgresStore {
    pub(super) async fn queue_admission(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        change: &mut crate::queue::Change,
        preparation: Option<&crate::session_owner::preparation::proof::VerifiedPreparation<'_>>,
        read_proof: &mut crate::public_session::v2::proof::Proof,
    ) -> Result<Option<Box<crate::AdmitOutcome>>, StoreError> {
        Ok(if let Some(proof) = &change.admission {
            let bound = proof.bound();
            let outcome = self
                .admit_keyed_guarded_with_proof(
                    tx,
                    bound.keyed.clone(),
                    preparation.map_or(AdmissionGuard::Queue(proof), |preparation| {
                        AdmissionGuard::PreparedQueue {
                            queue: proof,
                            preparation,
                        }
                    }),
                    read_proof,
                )
                .await?;
            let crate::KeyedAdmitOutcome::Started { receipt, admitted } = outcome else {
                return Err(StoreError::AdmissionConflict);
            };
            super::super::execution_binding::save(
                tx,
                &crate::RetainedExecutionBinding::new(
                    &admitted.run,
                    &bound.keyed.request.user_message,
                    bound.binding,
                ),
            )
            .await?;
            complete(change, &admitted, receipt);
            Some(admitted)
        } else {
            None
        })
    }
}
