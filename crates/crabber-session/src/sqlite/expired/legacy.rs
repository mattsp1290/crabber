//! Original compatibility claim transaction; actual publisher and returned fence.
use super::super::SqliteStore;
use crate::native_enrollment::caller::{Caller, Fresh, initial::Capture, recovery::LegacyExpired};
use crate::public_session::v2::proof::Proof;
use crate::{AdmissionExecutionState, StoreError};
use crabber_core::{RunFence, RunId};
fn unknown(_: impl std::fmt::Debug) -> StoreError {
    StoreError::Validation("native expired claim evidence unavailable".into())
}
impl SqliteStore {
    pub(in crate::sqlite) async fn claim_legacy_expired_with_proof(
        &self,
        id: &RunId,
        owner: &str,
        proof: &mut Proof,
    ) -> Result<RunFence, StoreError> {
        proof.measure(&(id, owner)).map_err(unknown)?;
        let mut tx = self.begin_write().await?;
        let mut run = super::super::records::load_run_with_proof(&mut tx, id, proof).await?;
        let result = async {
            proof.charge(3).map_err(unknown)?;
            super::super::session_owner::guard_native_claim(&mut tx, &run.session_id)
                .await
                .map_err(|error| match error {
                    crate::session_owner::SessionOwnerError::Busy => {
                        StoreError::AdmissionRecoveryRequired
                    }
                    _ => StoreError::Validation("preparation evidence unavailable".into()),
                })?;
            if super::super::admission_execution::load_record_with_proof(&mut tx, id, proof)
                .await
                .map_err(|error| error.into_domain_or(unknown("native claim proof")))?
                .is_some_and(|record| record.state == AdmissionExecutionState::Unstarted)
            {
                return Err(StoreError::AdmissionRecoveryRequired);
            }
            if super::super::tool_continuation::pending_in_session_with_proof(
                &mut tx,
                &run.session_id,
                proof,
            )
            .await
            .map_err(|_| StoreError::Validation("invalid tool continuation evidence".into()))?
            {
                return Err(StoreError::AdmissionRecoveryRequired);
            }
            let now = self.clock.now();
            if run.status.is_terminal() || run.lease_until > now {
                return Err(StoreError::Conflict);
            }
            proof.measure(&run).map_err(unknown)?;
            let previous = run.clone();
            owner.clone_into(&mut run.owner);
            run.claim_token = uuid::Uuid::new_v4().to_string();
            run.lease_until = now + time::Duration::seconds(30);
            run.updated_at = now;
            let fence = RunFence {
                run_id: id.clone(),
                claim_token: run.claim_token.clone(),
            };
            let published = super::super::records::save_run_publication_with_proof(
                &mut tx,
                &run,
                None,
                proof,
                Capture::Original,
            )
            .await?;
            let publication = published
                .updated
                .as_ref()
                .ok_or_else(|| unknown("missing actual issuer"))?;
            super::super::native_enrollment::record_raw_native(
                &mut tx,
                Caller::Raw,
                &Fresh::LegacyExpired(LegacyExpired {
                    id,
                    owner,
                    previous: &previous,
                    updated: &run,
                    fence: &fence,
                    publication,
                }),
                proof,
                #[cfg(test)]
                &self.native_raw_probe,
            )
            .await
            .map_err(unknown)?;
            Ok(fence)
        }
        .await;
        let fence = match result {
            Ok(fence) => {
                tx.commit().await.map_err(super::super::db)?;
                fence
            }
            Err(error) => {
                tx.rollback().await.map_err(super::super::db)?;
                return Err(error);
            }
        };
        #[cfg(test)]
        if self.native_raw_probe.after_legacy_expired(id) {
            return Err(unknown("lost original compatibility claim ACK"));
        }
        Ok(fence)
    }
}
