//! Physical preparation join and native successor publication share one transaction.
mod inspection;
mod native;
mod queue_inspection;
mod replay;
use super::{Audit, Error, SessionOwnerOperation, publish_revision, reads};
use crate::StoreError;
use crate::session_owner::preparation::{
    JoinedPreparation, PreparationCompletion, PreparedDriverCompletion,
};
impl super::SqliteStore {
    pub(in crate::sqlite) async fn complete_preparation(
        &self,
        joined: JoinedPreparation,
    ) -> Result<PreparationCompletion, crate::session_owner::preparation::PreparationCommitError>
    {
        Box::pin(self.complete_preparation_captured(joined, false))
            .await
            .map(|v| v.completion)
    }
    pub(in crate::sqlite) async fn complete_preparation_captured(
        &self,
        joined: JoinedPreparation,
        require_approval_birth: bool,
    ) -> Result<PreparedDriverCompletion, crate::session_owner::preparation::PreparationCommitError>
    {
        let mut read_proof = crate::public_session::v2::proof::Proof::bounded(32 << 20);
        crate::native_enrollment::caller::initial::preparation_input(&joined, &mut read_proof)
            .map_err(|_| Error::Capacity)?;
        joined.verify_original_intent()?;
        let session = joined
            .observed_grant()
            .original_reservation()
            .owner
            .binding
            .session_id
            .clone();
        let mut tx = self
            .writer
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(super::super::db)?;
        let result = async {
            if let Some(outcome) = self.replay_prepared(&mut tx, &joined).await? {
                return Ok(crate::session_owner::preparation::driver_completion::replayed(outcome));
            }
            read_proof.charge(2).map_err(|_| Error::Capacity)?;
            reads::scope(&mut tx, &session)
                .await
                .map_err(storage_error)?;
            let mut record = reads::current_with_proof(
                &mut tx,
                &session,
                &joined
                    .observed_grant()
                    .original_reservation()
                    .owner
                    .binding
                    .id,
                &mut read_proof,
            )
            .await
            .map_err(storage_error)?;
            require_birth(&mut tx, &joined, require_approval_birth, &mut read_proof).await?;
            let mut proof = Box::pin(self.verify_service_preparation(
                &mut tx,
                joined.observed_grant(),
                None,
                false,
                &mut read_proof,
            ))
            .await
            .map_err(storage_error)?;
            proof.bind_observed_operation(&joined)?;
            let join =
                crate::session_owner::preparation::proof::join_request(joined.observed_grant())
                    .map_err(storage_error)?;
            record
                .mutate(
                    &join,
                    Some(joined.observed_grant().owner_authority()),
                    self.clock.now(),
                )
                .map_err(storage_error)?;
            crate::session_owner::preparation::proof::record_handoff(&mut record, &proof)
                .map_err(storage_error)?;
            publish_join(&mut tx, &session, record, &join, &mut read_proof)
                .await
                .map_err(storage_error)?;
            let result = Box::pin(self.apply_prepared_native(
                &mut tx,
                joined.observed_operation().clone(),
                &proof,
                &mut read_proof,
            ))
            .await?;
            let result =
                crate::session_owner::preparation::driver_completion::native(result, &proof)?;
            super::before(self).map_err(storage_error)?;
            #[cfg(test)]
            crate::binding_fault::trip(&self.binding_fault, 1)?;
            #[cfg(test)]
            if matches!(result.completion, PreparationCompletion::ExpiredRun(_)) {
                crate::run_control_contract::process::pause("expired-before");
            }
            Ok(result)
        }
        .await;
        match result {
            Ok(value) => {
                tx.commit().await.map_err(super::super::db)?;
                #[cfg(test)]
                after_commit(self, &value.completion)?;
                #[cfg(test)]
                if matches!(value.completion, PreparationCompletion::ExpiredRun(_)) {
                    crate::run_control_contract::process::pause("expired-after");
                }
                Ok(value)
            }
            Err(error) => {
                tx.rollback().await.map_err(super::super::db)?;
                Err(error)
            }
        }
    }
}
fn storage_error(error: Error) -> StoreError {
    match error {
        Error::RevisionConflict => StoreError::AdmissionRevisionConflict,
        _ => StoreError::Validation("preparation ownership proof unavailable".into()),
    }
}

async fn verify_replay(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    grant: &crate::session_owner::preparation::PreparationGrant,
) -> Result<(), Error> {
    let join = crate::session_owner::preparation::proof::join_request(grant)?;
    let source = reads::audit(tx, &join.session_id, &join.id).await?;
    crate::session_owner::preparation::proof::verify_join_audit(grant, source.as_ref())
}

#[cfg(test)]
fn after_commit(
    store: &super::SqliteStore,
    value: &PreparationCompletion,
) -> Result<(), crate::session_owner::preparation::PreparationCommitError> {
    use std::sync::atomic::Ordering;
    if value.is_replayed() {
        return Ok(());
    }
    if store.session_owner_fault.load(Ordering::SeqCst) == 2 {
        return Err(crate::session_owner::SessionOwnerError::UnknownStoreOutcome.into());
    }
    match value {
        PreparationCompletion::Approval(_) if store.approval_fault.load(Ordering::SeqCst) == 2 => {
            return Err(crate::ApprovalError::UnknownStoreOutcome.into());
        }
        PreparationCompletion::ToolContinuation(_)
            if store.tool_continuation_fault.load(Ordering::SeqCst) == 2 =>
        {
            return Err(crate::ToolContinuationError::UnknownStoreOutcome.into());
        }
        PreparationCompletion::Queue(_) => crate::binding_fault::trip(&store.queue_fault, 2)?,
        _ => (),
    }
    crate::binding_fault::trip(&store.binding_fault, 2)?;
    Ok(())
}

async fn publish_join(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    session: &crabber_core::SessionId,
    record: crate::session_owner::record::Record,
    join: &crate::session_owner::MutateSessionOwner,
    proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<(), Error> {
    let receipt = record.receipt(
        join.id.clone(),
        crate::session_owner::preparation::handoff::record_fingerprint(&record, &join.id)?,
        SessionOwnerOperation::Join,
    );
    let publication = publish_revision(tx, session, true).await?;
    reads::publish_with_proof(
        tx,
        &Audit {
            version: 2,
            receipt,
            record,
            publication: Some(publication),
        },
        false,
        proof,
    )
    .await?;

    Ok(())
}

async fn require_birth(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    joined: &JoinedPreparation,
    required: bool,
    proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<(), crate::session_owner::preparation::PreparationCommitError> {
    if required {
        let scope = crate::session_owner::preparation::driver_completion::protected_scope(joined)?;
        if !crate::sqlite::continuation_input::protocol(tx, &scope, proof)
            .await
            .map_err(|_| Error::UnknownStoreOutcome)?
        {
            return Err(Error::Unsupported.into());
        }
    }
    Ok(())
}
