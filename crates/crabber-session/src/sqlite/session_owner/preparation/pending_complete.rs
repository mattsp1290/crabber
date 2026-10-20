//! Borrow the exact original Initial body through its actual prepared publisher.
use crate::native_enrollment::{
    caller::initial::{CapturedKeyedAdmission, Dispatch, FirstPublication},
    pending_initial::VerifiedInitialCall,
};
use crate::public_session::v2::proof::Proof;
use crate::session_owner::{
    SessionOwnerError as Error,
    preparation::{
        JoinedPreparation, PreparationCommitError, PreparationCompletion, PreparedOperation,
    },
};
use crate::transaction::Transaction;
use sqlx::Sqlite;
impl crate::sqlite::SqliteStore {
    pub(in crate::sqlite) async fn pending_initial_fresh(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        original: &JoinedPreparation,
        read_proof: &mut Proof,
    ) -> Result<(), PreparationCommitError> {
        super::super::reads::scope(
            tx,
            &original
                .observed_grant()
                .original_reservation()
                .owner
                .binding
                .session_id,
        )
        .await
        .map_err(super::storage_error)?;
        let mut verified = Box::pin(self.verify_service_preparation(
            tx,
            original.observed_grant(),
            None,
            true,
            read_proof,
        ))
        .await
        .map_err(super::storage_error)?;
        verified.bind_observed_operation(original)?;
        verified.require_resource_activation()?;
        Ok(())
    }
    pub(in crate::sqlite) async fn complete_pending_initial(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        original: &JoinedPreparation,
        call: &VerifiedInitialCall<'_>,
        read_proof: &mut Proof,
    ) -> Result<(PreparationCompletion, FirstPublication), PreparationCommitError> {
        let grant = original.observed_grant();
        let session = &grant.original_reservation().owner.binding.session_id;
        super::super::reads::scope(tx, session)
            .await
            .map_err(super::storage_error)?;
        let mut record = super::super::reads::current_with_proof(
            tx,
            session,
            &grant.original_reservation().owner.binding.id,
            read_proof,
        )
        .await
        .map_err(super::storage_error)?;
        let mut verified =
            Box::pin(self.verify_service_preparation(tx, grant, None, true, read_proof))
                .await
                .map_err(super::storage_error)?;
        verified.bind_observed_operation(original)?;
        verified.require_resource_activation()?;
        let join = crate::session_owner::preparation::proof::join_request(grant)
            .map_err(super::storage_error)?;
        record
            .mutate(&join, Some(grant.owner_authority()), self.clock.now())
            .map_err(super::storage_error)?;
        crate::session_owner::preparation::proof::record_handoff(&mut record, &verified)
            .map_err(super::storage_error)?;
        super::publish_join(tx, session, record, &join, read_proof)
            .await
            .map_err(super::storage_error)?;
        let guard = crate::queue::AdmissionGuard::Preparation(&verified);
        let (completion, first) =
            Box::pin(self.apply_pending_initial_body(tx, original, call, guard, read_proof))
                .await?;
        super::super::before(self).map_err(super::storage_error)?;
        Ok((completion, first))
    }
    async fn apply_pending_initial_body(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        original: &JoinedPreparation,
        call: &VerifiedInitialCall<'_>,
        guard: crate::queue::AdmissionGuard<'_>,
        read_proof: &mut Proof,
    ) -> Result<(PreparationCompletion, FirstPublication), PreparationCommitError> {
        Ok(match original.observed_operation() {
            PreparedOperation::Admit(bound) => {
                bound.binding.validate(&bound.request)?;
                let captured = self
                    .admit_transaction_captured(
                        tx,
                        &bound.request,
                        false,
                        guard,
                        Dispatch::pending(call),
                        read_proof,
                    )
                    .await?;
                read_proof
                    .measure(&bound.binding)
                    .map_err(|_| Error::Capacity)?;
                super::super::super::execution_binding::save(
                    tx,
                    &crate::RetainedExecutionBinding::new(
                        &captured.admitted.run,
                        &bound.request.user_message,
                        bound.binding.clone(),
                    ),
                )
                .await?;
                (
                    PreparationCompletion::Admitted(captured.admitted),
                    captured.first,
                )
            }
            PreparedOperation::AdmitKeyed(bound) => {
                bound.binding.validate_keyed(&bound.keyed)?;
                let CapturedKeyedAdmission::Started { receipt, admitted } = self
                    .admit_keyed_captured(
                        tx,
                        &bound.keyed,
                        guard,
                        Dispatch::pending(call),
                        read_proof,
                    )
                    .await?
                else {
                    return Err(Error::InvalidTransition.into());
                };
                read_proof
                    .measure(&bound.binding)
                    .map_err(|_| Error::Capacity)?;
                super::super::super::execution_binding::save(
                    tx,
                    &crate::RetainedExecutionBinding::new(
                        &admitted.admitted.run,
                        &bound.keyed.request.user_message,
                        bound.binding.clone(),
                    ),
                )
                .await?;
                let crate::native_enrollment::caller::initial::CapturedAdmission {
                    admitted,
                    first,
                } = *admitted;
                (
                    PreparationCompletion::Keyed(crate::KeyedAdmitOutcome::Started {
                        receipt,
                        admitted: Box::new(admitted),
                    }),
                    first,
                )
            }
            _ => return Err(Error::Unsupported.into()),
        })
    }
}
