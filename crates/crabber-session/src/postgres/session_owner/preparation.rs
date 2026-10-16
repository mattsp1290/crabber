//! Physical preparation join and native successor publication share one transaction.
mod native;
mod replay;
use super::{Audit, Error, SessionOwnerOperation, publish_revision, reads, revision};
use crate::StoreError;
use crate::session_owner::preparation::{JoinedPreparation, PreparationCompletion};
impl super::PostgresStore {
    pub(in crate::postgres) async fn complete_preparation(
        &self,
        joined: JoinedPreparation,
    ) -> Result<PreparationCompletion, crate::session_owner::preparation::PreparationCommitError>
    {
        joined.verify_original_intent()?;
        let session = joined
            .observed_grant()
            .original_reservation()
            .owner
            .binding
            .session_id
            .clone();
        let mut tx = self.session_transaction(&session).await?;
        let result = async {
            if let Some(outcome) = self.replay_prepared(&mut tx, &joined).await? {
                return Ok(outcome);
            }
            reads::scope(&mut tx, &session)
                .await
                .map_err(storage_error)?;
            let mut record = reads::current(
                &mut tx,
                &session,
                &joined
                    .observed_grant()
                    .original_reservation()
                    .owner
                    .binding
                    .id,
            )
            .await
            .map_err(storage_error)?;
            let child = resource_child(&mut tx, joined.observed_grant())
                .await
                .map_err(storage_error)?;
            reads::preparation_order(&mut tx, joined.observed_grant())
                .await
                .map_err(storage_error)?;
            let audits = reads::preparation_audits(
                &mut tx,
                &session,
                &record.snapshot.target.id,
                child.as_ref().map(|record| &record.snapshot.target.id),
            )
            .await
            .map_err(storage_error)?;
            let before = revision(&mut tx, &session).await.map_err(storage_error)?;
            let proof = crate::session_owner::preparation::proof::verify_with_resources(
                joined.observed_grant(),
                &record,
                child.as_ref(),
                &audits,
                before,
                self.clock.now(),
            )
            .map_err(storage_error)?;
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
            publish_join(&mut tx, &session, record, &join)
                .await
                .map_err(storage_error)?;
            let result = Box::pin(self.apply_prepared_native(
                &mut tx,
                joined.observed_operation().clone(),
                &proof,
            ))
            .await?;
            super::before(self).map_err(storage_error)?;
            #[cfg(test)]
            crate::binding_fault::trip(&self.binding_fault, 1)?;
            Ok(result)
        }
        .await;
        match result {
            Ok(value) => {
                tx.commit().await.map_err(super::super::db)?;
                #[cfg(test)]
                after_commit(self, &value)?;
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
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    grant: &crate::session_owner::preparation::PreparationGrant,
) -> Result<(), Error> {
    let join = crate::session_owner::preparation::proof::join_request(grant)?;
    let source = reads::audit(tx, &join.session_id, &join.id).await?;
    crate::session_owner::preparation::proof::verify_join_audit(grant, source.as_ref())
}

#[cfg(test)]
fn after_commit(
    store: &super::PostgresStore,
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
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    session: &crabber_core::SessionId,
    record: crate::session_owner::record::Record,
    join: &crate::session_owner::MutateSessionOwner,
) -> Result<(), Error> {
    let receipt = record.receipt(
        join.id.clone(),
        crate::session_owner::preparation::handoff::record_fingerprint(&record, &join.id)?,
        SessionOwnerOperation::Join,
    );
    let publication = publish_revision(tx, session, true).await?;
    reads::publish(
        tx,
        &Audit {
            version: 2,
            receipt,
            record,
            publication: Some(publication),
        },
        false,
    )
    .await?;

    Ok(())
}

async fn resource_child(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    grant: &crate::session_owner::preparation::PreparationGrant,
) -> Result<Option<crate::session_owner::record::Record>, Error> {
    let Some(authority) = grant.retained_resources_authority() else {
        return Ok(None);
    };
    reads::current(tx, authority.session_id(), &authority.target().id)
        .await
        .map(Some)
}
