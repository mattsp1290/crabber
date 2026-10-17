//! Atomic first-driver Reserve/Begin and fresh gated dispatch. No coverage claim.
mod activation;
mod fresh;
mod history;
pub(in crate::sqlite) use history::classified;
mod proof;
pub(in crate::sqlite) mod source;
mod successor;
use super::{
    Audit, Error, Record, SessionOwnerOperation, SessionOwnerOutcome, SqliteStore, before, fault,
    finish, publish_revision, reads, unknown,
};
use crate::public_session::v2::proof::Proof;
use crate::session_owner::driver::{
    DriverLink, ExecutionDriverGrant, ExecutionDriverOutcome, ExecutionDriverReceipt,
    RegisterExecutionDriver,
};
use crate::session_owner::owned_scope::{OwnedScopeActivation, OwnedScopeParentAuthority};
use sqlx::{Sqlite, Transaction};
fn budget() -> Proof {
    Proof::bounded(crate::deletion::coverage::MAX_PROOF_BYTES)
}
impl SqliteStore {
    pub(in crate::sqlite) async fn register_driver(
        &self,
        request: RegisterExecutionDriver,
    ) -> Result<ExecutionDriverOutcome, Error> {
        request.fingerprint()?;
        let mut tx = self.begin_write().await.map_err(unknown)?;
        let result = Box::pin(async {
            super::super::deletion::ensure_live(&mut tx, request.context.session_id()).await.map_err(crate::deletion::owner_error)?;
            let mut proof = budget();
            if let Some(receipt) = Box::pin(metadata(&mut tx,&request,&mut proof)).await? {
                return Ok(ExecutionDriverOutcome::Replayed(receipt));
            }
            let link = Box::pin(fresh::fresh(&mut tx,&request,None,self.clock.now(),&mut proof))
            .await?;
            let reservation = link.reservation(&request)?;
            // Fresh source validation already authenticated the current child context.
            proof.charge(9).map_err(unknown)?; // Capacity count and target-existence scalar.
            reads::capacity(&mut tx,request.context.session_id()).await?;
            let exists: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM session_owner_current WHERE session_id=$1 AND owner_id=$2)").bind(&reservation.binding.session_id.0).bind(&reservation.binding.id.0).fetch_one(&mut *tx).await.map_err(unknown)?;
            if exists { return Err(Error::SemanticConflict); }
            let mut record = Record::reserve(&reservation,None,self.clock.now())?;
            record.driver = Some(Box::new(link.clone()));
            let authority = record.grant()?;
            let reserve = record.receipt(reservation.id.clone(),link.fingerprint(&reservation)?,SessionOwnerOperation::Reserve);
            let publication = publish_revision(&mut tx,request.context.session_id(),true).await?;
            reads::publish_with_proof(&mut tx,&Audit{version:2,receipt:reserve.clone(),record:record.clone(),publication:Some(publication)},true,&mut proof).await?;
            let command = crate::session_owner::MutateSessionOwner { id:request.identities()?.begin, session_id:request.context.session_id().clone(),target:authority.target().clone(),command:crate::session_owner::SessionOwnerCommand::Begin };
            record.mutate(&command,Some(&authority),self.clock.now())?;
            let begin = record.receipt(command.id.clone(),command.fingerprint()?,SessionOwnerOperation::Begin);
            let publication = publish_revision(&mut tx,request.context.session_id(),true).await?;
            reads::publish_with_proof(&mut tx,&Audit{version:2,receipt:begin.clone(),record,publication:Some(publication)},false,&mut proof).await?;
            let receipt = ExecutionDriverReceipt { session_id:request.context.session_id().clone(),run_id:request.fence.run_id.clone(),request_digest:request.fingerprint()?,reserve,begin };
            let grant = ExecutionDriverGrant::from_fresh_begin(receipt.clone(),authority,request.clone(),link.identity()?)?;
            before(self)?;
            Ok(ExecutionDriverOutcome::Applied{receipt,grant:Box::new(grant)})
        }).await;
        let result = finish(tx, result).await?;
        if matches!(result, ExecutionDriverOutcome::Applied { .. }) && fault(self) == 2 {
            return Err(Error::UnknownStoreOutcome);
        }
        Ok(result)
    }
    pub(in crate::sqlite) async fn activate_driver(
        &self,
        request: OwnedScopeActivation,
        authority: crate::session_owner::SessionOwnerAuthority,
        parent: Option<OwnedScopeParentAuthority>,
    ) -> Result<SessionOwnerOutcome, Error> {
        let digest = request.fingerprint()?;
        let mut tx = self.begin_write().await.map_err(unknown)?;
        let result = Box::pin(async {
            super::super::deletion::ensure_live(&mut tx, request.session_id())
                .await
                .map_err(crate::deletion::owner_error)?;
            let mut proof = budget();
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
            let receipt = record.receipt(
                request.id.clone(),
                digest,
                SessionOwnerOperation::ActivateScope,
            );
            let publication = publish_revision(&mut tx, request.session_id(), true).await?;
            reads::publish_with_proof(
                &mut tx,
                &Audit {
                    version: 2,
                    receipt: receipt.clone(),
                    record,
                    publication: Some(publication),
                },
                false,
                &mut proof,
            )
            .await?;
            before(self)?;
            Ok(SessionOwnerOutcome::Applied {
                receipt,
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
    tx: &mut Transaction<'_, Sqlite>,
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
