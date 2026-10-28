//! Bounded original metadata reconciliation, never replacement dispatch authority.
#[path = "replay/admission.rs"]
mod admission;
#[path = "replay/expired.rs"]
mod expired;
#[path = "replay/recovery.rs"]
mod recovery;
use crate::session_owner::preparation::{
    JoinedPreparation, PreparationCompletion, PreparedOperation,
};
use crate::{KeyedAdmitOutcome, StoreError};
impl super::super::PostgresStore {
    pub(super) async fn replay_prepared(
        &self,
        tx: &mut crate::postgres::transaction::Transaction<'_, sqlx::Postgres>,
        joined: &JoinedPreparation,
        proof: &mut crate::public_session::v2::proof::Proof,
    ) -> Result<
        Option<PreparationCompletion>,
        crate::session_owner::preparation::PreparationCommitError,
    > {
        if let Some(outcome) = Self::replay_expired(tx, joined, proof).await? {
            return Ok(Some(outcome));
        }
        if let Some(outcome) = Self::replay_recovery(tx, joined, proof).await? {
            return Ok(Some(outcome));
        }
        if let Some(outcome) = Self::replay_admission(tx, joined, proof).await? {
            return Ok(Some(outcome));
        }
        if let PreparedOperation::AdmitKeyed(bound) = joined.observed_operation()
            && let Some(outcome) = self.replay_keyed(tx, bound, proof).await?
        {
            super::verify_replay(tx, joined.observed_grant(), proof).await?;
            return Ok(Some(outcome));
        }
        if let PreparedOperation::Queue { request, .. } = joined.observed_operation()
            && let Some(saved) = self.queue_replay(tx, request, proof).await?
        {
            super::verify_replay(tx, joined.observed_grant(), proof).await?;
            return Ok(Some(PreparationCompletion::Queue(saved)));
        }
        if let Some(outcome) = Self::replay_frontend(tx, joined, proof).await? {
            return Ok(Some(outcome));
        }
        match joined.observed_operation() {
            PreparedOperation::ApprovalClaim(claim) => {
                if let Some(saved) = super::super::super::approval::replay_prepared(
                    tx,
                    &claim.session_id,
                    &claim.run_id,
                    &claim.id,
                    &claim.fingerprint()?,
                    proof,
                )
                .await?
                {
                    super::verify_replay(tx, joined.observed_grant(), proof).await?;
                    return Ok(Some(PreparationCompletion::Approval(
                        crate::ApprovalClaimOutcome::Replayed(saved),
                    )));
                }
            }
            PreparedOperation::ToolClaim(claim) => {
                if let Some(saved) = super::super::super::tool_continuation::replay_prepared(
                    tx,
                    &claim.session_id,
                    &claim.run_id,
                    &claim.id,
                    &claim.fingerprint()?,
                    proof,
                )
                .await?
                {
                    super::verify_replay(tx, joined.observed_grant(), proof).await?;
                    return Ok(Some(PreparationCompletion::ToolContinuation(
                        crate::ToolContinuationClaimOutcome::Replayed(saved),
                    )));
                }
            }
            PreparedOperation::ApprovalRecovery(recovery) => {
                if let Some(saved) = super::super::super::approval::replay_prepared(
                    tx,
                    &recovery.original.session_id,
                    &recovery.original.run_id,
                    &recovery.id,
                    &recovery.fingerprint()?,
                    proof,
                )
                .await?
                {
                    recovery.verify_receipt(&saved)?;
                    super::verify_replay(tx, joined.observed_grant(), proof).await?;
                    return Ok(Some(PreparationCompletion::Approval(
                        crate::ApprovalClaimOutcome::Replayed(saved),
                    )));
                }
            }
            PreparedOperation::ToolRecovery(recovery) => {
                if let Some(saved) = super::super::super::tool_continuation::replay_prepared(
                    tx,
                    &recovery.original.session_id,
                    &recovery.original.run_id,
                    &recovery.id,
                    &recovery.fingerprint()?,
                    proof,
                )
                .await?
                {
                    recovery.verify_receipt(&saved)?;
                    super::verify_replay(tx, joined.observed_grant(), proof).await?;
                    return Ok(Some(PreparationCompletion::ToolContinuation(
                        crate::ToolContinuationClaimOutcome::Replayed(saved),
                    )));
                }
            }
            _ => (),
        }

        Ok(None)
    }
}

impl super::super::PostgresStore {
    async fn replay_keyed(
        &self,
        tx: &mut crate::postgres::transaction::Transaction<'_, sqlx::Postgres>,
        bound: &crate::BoundKeyedAdmitRequest,
        proof: &mut crate::public_session::v2::proof::Proof,
    ) -> Result<
        Option<PreparationCompletion>,
        crate::session_owner::preparation::PreparationCommitError,
    > {
        let session = bound.binding.request.session_id.clone();

        bound.binding.validate_keyed(&bound.keyed)?;
        if super::super::super::lookup_receipt_with_proof(
            tx,
            &session,
            &bound.keyed.options.key,
            proof,
        )
        .await?
        .is_some()
        {
            proof.measure(&bound.keyed).map_err(|error| {
                proof.admission_failure(
                    &error,
                    crate::session_owner::preparation::PreparationCommitError::Owner(
                        crate::session_owner::SessionOwnerError::Capacity,
                    ),
                )
            })?;
            let outcome = self
                .admit_keyed_guarded_with_proof(
                    tx,
                    bound.keyed.clone(),
                    crate::queue::AdmissionGuard::Ordinary,
                    proof,
                )
                .await?;
            if let KeyedAdmitOutcome::Replayed(receipt) = &outcome
                && super::super::super::execution_binding::read_with_proof(
                    tx,
                    &session,
                    &receipt.run_id,
                    proof,
                )
                .await
                .map_err(|error| proof.admission_read(error, |error| error, StoreError::Conflict))?
                .is_none_or(|saved| saved.capsule != bound.binding)
            {
                return Err(StoreError::AdmissionConflict.into());
            }

            return Ok(Some(PreparationCompletion::Keyed(outcome)));
        }

        Ok(None)
    }
}

impl super::super::PostgresStore {
    async fn replay_frontend(
        tx: &mut crate::postgres::transaction::Transaction<'_, sqlx::Postgres>,
        joined: &JoinedPreparation,
        proof: &mut crate::public_session::v2::proof::Proof,
    ) -> Result<
        Option<PreparationCompletion>,
        crate::session_owner::preparation::PreparationCommitError,
    > {
        match joined.observed_operation() {
            PreparedOperation::FrontendClaim(claim) => {
                if let Some(saved) = super::super::super::tool_continuation::replay_prepared(
                    tx,
                    &claim.claim.session_id,
                    &claim.claim.run_id,
                    &claim.claim.id,
                    &claim.fingerprint()?,
                    proof,
                )
                .await?
                {
                    super::verify_replay(tx, joined.observed_grant(), proof).await?;
                    return Ok(Some(PreparationCompletion::ToolContinuation(
                        crate::ToolContinuationClaimOutcome::Replayed(saved),
                    )));
                }
            }
            PreparedOperation::FrontendRecovery(recovery) => {
                if let Some(saved) = super::super::super::tool_continuation::replay_prepared(
                    tx,
                    &recovery.original.claim.session_id,
                    &recovery.original.claim.run_id,
                    &recovery.id,
                    &recovery.fingerprint()?,
                    proof,
                )
                .await?
                {
                    recovery.verify_receipt(&saved)?;
                    super::verify_replay(tx, joined.observed_grant(), proof).await?;
                    return Ok(Some(PreparationCompletion::ToolContinuation(
                        crate::ToolContinuationClaimOutcome::Replayed(saved),
                    )));
                }
            }
            _ => (),
        }
        Ok(None)
    }
}

#[cfg(test)]
mod proof_limit_tests {
    use super::*;
    use crate::session_owner::preparation_contract::resources;
    use crate::{Store, public_session::v2::proof::Proof};

    #[tokio::test]
    async fn keyed_admission_replay_preserves_carrier_and_binding_read_limits() {
        use crabber_test_support::postgres::{DatabaseMode, TestDatabase};
        TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {
            crate::PostgresStore::migrate(&url).await.unwrap();
            let store = crate::PostgresStore::connect(&url).await.unwrap();
            let (root, bound) = resources::declaration::setup(&store).await;
            let original = resources::initial_driver_observation(
                &store,
                root,
                PreparedOperation::AdmitKeyed(Box::new(bound)),
            )
            .await;
            let result = Store::complete_preparation(&store, original.clone())
                .await
                .unwrap();
            assert!(!result.is_replayed());
            let PreparedOperation::AdmitKeyed(bound) = original.observed_operation() else {
                unreachable!()
            };
            let session = bound.keyed.request.session_id.as_ref().unwrap();
            let revision = store.session_revision(session).await.unwrap();
            let mut tx = crate::postgres::transaction::begin(&store.pool)
                .await
                .unwrap();
            let mut receipt = Proof::bounded(32 << 20);
            crate::postgres::lookup_receipt_with_proof(
                &mut tx,
                session,
                &bound.keyed.options.key,
                &mut receipt,
            )
            .await
            .unwrap()
            .unwrap();
            let mut full = Proof::bounded(32 << 20);
            assert!(
                store
                    .replay_keyed(&mut tx, bound, &mut full)
                    .await
                    .unwrap()
                    .unwrap()
                    .is_replayed()
            );
            for cap in [receipt.used(), full.used() - 1] {
                let error = store
                    .replay_keyed(&mut tx, bound, &mut Proof::bounded(cap))
                    .await
                    .err()
                    .unwrap();
                let crate::session_owner::preparation::PreparationCommitError::Store(error) = error
                else {
                    panic!("typed keyed replay proof limit");
                };
                assert_eq!(
                    error.admission_limit().unwrap().limit_bytes(),
                    Some(u64::try_from(cap).unwrap())
                );
            }
            tx.rollback().await.unwrap();
            assert_eq!(store.session_revision(session).await.unwrap(), revision);
        })
        .await
        .unwrap();
    }
}
