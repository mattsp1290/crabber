//! Atomic first-driver Reserve/Begin and fresh gated dispatch. No coverage claim.
pub(in crate::postgres) mod activation;
pub(in crate::postgres) mod fresh;
mod history;
pub(in crate::postgres) use history::classified;
mod proof;
pub(in crate::postgres) use proof::read_audit as selected_consumer_audit;
mod publish;
mod raw;

pub(in crate::postgres) mod source;
mod successor;
use super::{
    Audit, Error, PostgresStore, Record, SessionOwnerOperation, SessionOwnerOutcome, before, fault,
    finish, publish_revision, reads, unknown,
};
use crate::postgres::transaction::Transaction;
use crate::public_session::v2::proof::Proof;
use crate::session_owner::driver::{
    DriverLink, ExecutionDriverOutcome, ExecutionDriverReceipt, RegisterExecutionDriver,
};
use crate::session_owner::owned_scope::{OwnedScopeActivation, OwnedScopeParentAuthority};
use sqlx::Postgres;
fn budget() -> Proof {
    Proof::bounded(crate::deletion::coverage::MAX_PROOF_BYTES)
}
impl PostgresStore {
    pub(in crate::postgres) async fn register_driver(
        &self,
        request: RegisterExecutionDriver,
    ) -> Result<ExecutionDriverOutcome, Error> {
        request.fingerprint()?;
        let mut tx = self
            .session_transaction(request.context.session_id())
            .await
            .map_err(unknown)?;
        let result = Box::pin(async {
            super::super::deletion::ensure_live(&mut tx, request.context.session_id())
                .await
                .map_err(crate::deletion::owner_error)?;
            let mut proof = budget();
            raw::run(
                &mut tx,
                request.context.session_id(),
                &request.fence.run_id,
                &mut proof,
            )
            .await?;
            if let Some(receipt) = Box::pin(metadata(&mut tx, &request, &mut proof)).await? {
                return Ok(ExecutionDriverOutcome::Replayed(receipt));
            }
            let link = Box::pin(fresh::fresh(
                &mut tx,
                &request,
                None,
                self.clock.now(),
                &mut proof,
            ))
            .await?;
            let published =
                Box::pin(self.publish_initial_driver(&mut tx, &request, link, &mut proof)).await?;
            before(self)?;
            Ok(published.outcome)
        })
        .await;
        let result = finish(tx, result).await?;
        if matches!(result, ExecutionDriverOutcome::Applied { .. }) && fault(self) == 2 {
            return Err(Error::UnknownStoreOutcome);
        }
        Ok(result)
    }
    pub(in crate::postgres) async fn activate_driver(
        &self,
        request: OwnedScopeActivation,
        authority: crate::session_owner::SessionOwnerAuthority,
        parent: Option<OwnedScopeParentAuthority>,
    ) -> Result<SessionOwnerOutcome, Error> {
        let digest = request.fingerprint()?;
        let mut tx = self
            .session_transaction(request.session_id())
            .await
            .map_err(unknown)?;
        let result = Box::pin(async {
            super::super::deletion::ensure_live(&mut tx, request.session_id())
                .await
                .map_err(crate::deletion::owner_error)?;
            let mut proof = budget();
            raw::activation(&mut tx, &request, &mut proof).await?;
            if let Some(audit) = proof::audit(
                &mut tx,
                request.session_id(),
                request.operation_id(),
                &mut proof,
            )
            .await?
            {
                if audit.receipt.operation != SessionOwnerOperation::ActivateScope
                    || audit.receipt.semantic_digest != digest
                {
                    return Err(Error::SemanticConflict);
                }
                return Ok(SessionOwnerOutcome::Replayed(audit.receipt));
            }
            let record = Box::pin(activation::record(
                &mut tx,
                &request,
                &authority,
                parent.as_ref(),
                self.clock.now(),
                &mut proof,
            ))
            .await?;
            let published =
                Box::pin(self.publish_driver_activation(&mut tx, &request, record, &mut proof))
                    .await?;
            before(self)?;
            Ok(SessionOwnerOutcome::Applied {
                receipt: published.audit.receipt,
                authority: None,
            })
        })
        .await;
        let result = finish(tx, result).await?;
        if matches!(result, SessionOwnerOutcome::Applied { .. }) && fault(self) == 2 {
            return Err(Error::UnknownStoreOutcome);
        }
        Ok(result)
    }
}
async fn metadata(
    tx: &mut Transaction<'_, Postgres>,
    request: &RegisterExecutionDriver,
    proof: &mut Proof,
) -> Result<Option<ExecutionDriverReceipt>, Error> {
    let ids = request.identities()?;
    let session = request.context.session_id();
    let Some(reserve) = proof::audit(tx, session, &ids.reserve, proof).await? else {
        return Ok(None);
    };
    let link: &DriverLink = reserve
        .record
        .driver
        .as_deref()
        .ok_or(Error::SemanticConflict)?;
    link.matches_request(request)?;
    let begin = proof::audit(tx, session, &ids.begin, proof)
        .await?
        .ok_or(Error::UnknownStoreOutcome)?;
    crate::session_owner::audit::verify_current(&begin.record, &reserve, &begin)?;
    let original = source::original(tx, session, &request.fence.run_id, proof).await?;
    original.verify_link(link)?;
    let receipt = ExecutionDriverReceipt {
        session_id: session.clone(),
        run_id: request.fence.run_id.clone(),
        request_digest: request.fingerprint()?,
        reserve: reserve.receipt,
        begin: begin.receipt,
    };
    crate::session_owner::owned_scope::OwnedScopeSource::from_receipts(
        &receipt.reserve,
        &receipt.begin,
    )?;
    Ok(Some(receipt))
}

#[cfg(test)]
mod tests;

#[cfg(test)]
use crate::session_owner::driver::ExecutionDriverGrant;
