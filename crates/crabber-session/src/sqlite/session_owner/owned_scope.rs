//! Exact original source and native-parent proof precede fresh activation.
mod context;
use super::{
    Audit, Error, SessionOwnerAuthority, SessionOwnerOperation, SqliteStore, before, fault, finish,
    publish_revision, reads, unknown,
};
use crate::session_owner::owned_scope::{OwnedScopeActivation, source};
impl SqliteStore {
    pub(in crate::sqlite) async fn publish_owned_scope_activation(
        &self,
        request: OwnedScopeActivation,
        authority: SessionOwnerAuthority,
        parent_authority: Option<crate::session_owner::owned_scope::OwnedScopeParentAuthority>,
    ) -> Result<crate::session_owner::SessionOwnerOutcome, Error> {
        let digest = request.fingerprint()?;
        let mut tx = self.begin_write().await.map_err(unknown)?;
        let result = Box::pin(async {
            if let Some(audit) =
                reads::audit(&mut tx, request.session_id(), request.operation_id()).await?
            {
                if audit.receipt.semantic_digest != digest
                    || audit.receipt.operation != SessionOwnerOperation::ActivateScope
                {
                    return Err(Error::SemanticConflict);
                }
                return Ok(crate::session_owner::SessionOwnerOutcome::Replayed(
                    audit.receipt,
                ));
            }
            reads::scope(&mut tx, request.session_id()).await?;
            let mut record =
                reads::current(&mut tx, request.session_id(), &request.target().id).await?;
            let reserve = reads::audit(&mut tx, request.session_id(), &request.source.reserve)
                .await?
                .ok_or(Error::UnknownStoreOutcome)?;
            let begin = reads::audit(&mut tx, request.session_id(), &request.source.begin)
                .await?
                .ok_or(Error::UnknownStoreOutcome)?;
            source::verify(&request.source, &record, &reserve, &begin)?;
            Box::pin(self.owned_scope_context(&mut tx, &record, parent_authority.as_ref())).await?;
            source::activate(&mut record, &request, &authority, self.clock.now())?;
            let receipt = record.receipt(
                request.id.clone(),
                digest,
                SessionOwnerOperation::ActivateScope,
            );
            let publication = publish_revision(&mut tx, request.session_id(), true).await?;
            reads::publish(
                &mut tx,
                &Audit {
                    version: 2,
                    receipt: receipt.clone(),
                    record,
                    publication: Some(publication),
                },
                false,
            )
            .await?;
            #[cfg(test)]
            crate::session_owner_contract::process::pause("scope-before");
            before(self)?;
            Ok(crate::session_owner::SessionOwnerOutcome::Applied {
                receipt,
                authority: None,
            })
        })
        .await;
        let outcome = finish(tx, result).await?;
        #[cfg(test)]
        if matches!(
            outcome,
            crate::session_owner::SessionOwnerOutcome::Applied { .. }
        ) {
            crate::session_owner_contract::process::pause("scope-after");
        }
        if matches!(
            outcome,
            crate::session_owner::SessionOwnerOutcome::Applied { .. }
        ) && fault(self) == 2
        {
            return Err(Error::UnknownStoreOutcome);
        }
        Ok(outcome)
    }
    pub(in crate::sqlite) async fn join_owned_scope(
        &self,
        proof: crate::session_owner::owned_scope::JoinedOwnedScope,
        authority: SessionOwnerAuthority,
    ) -> Result<crate::session_owner::SessionOwnerOutcome, Error> {
        let request = proof.request()?;
        let digest = proof.fingerprint()?;
        let mut tx = self.begin_write().await.map_err(unknown)?;
        let result = Box::pin(async {
            if let Some(audit) = reads::audit(&mut tx, &request.session_id, &request.id).await? {
                if audit.receipt.operation != SessionOwnerOperation::Join
                    || audit.receipt.semantic_digest != digest
                {
                    return Err(Error::SemanticConflict);
                }
                return Ok(crate::session_owner::SessionOwnerOutcome::Replayed(
                    audit.receipt,
                ));
            }
            reads::scope(&mut tx, &request.session_id).await?;
            let mut record =
                reads::current(&mut tx, &request.session_id, &request.target.id).await?;
            let activation = proof.activation();
            let reserve = reads::audit(&mut tx, &request.session_id, &activation.source.reserve)
                .await?
                .ok_or(Error::UnknownStoreOutcome)?;
            let begin = reads::audit(&mut tx, &request.session_id, &activation.source.begin)
                .await?
                .ok_or(Error::UnknownStoreOutcome)?;
            let activated = reads::audit(&mut tx, &request.session_id, &activation.id).await?;
            source::join_candidate(&record, &proof, &reserve, &begin, activated.as_ref())?;
            record.mutate(&request, Some(&authority), self.clock.now())?;
            record.owned_join = Some(proof.inspection().clone());
            let receipt = record.receipt(request.id.clone(), digest, SessionOwnerOperation::Join);
            let publication = publish_revision(&mut tx, &request.session_id, true).await?;
            reads::publish(
                &mut tx,
                &Audit {
                    version: 2,
                    receipt: receipt.clone(),
                    record,
                    publication: Some(publication),
                },
                false,
            )
            .await?;
            #[cfg(test)]
            crate::session_owner_contract::process::pause("scope-join-before");
            before(self)?;
            Ok(crate::session_owner::SessionOwnerOutcome::Applied {
                receipt,
                authority: None,
            })
        })
        .await;
        let outcome = finish(tx, result).await?;
        #[cfg(test)]
        if matches!(
            outcome,
            crate::session_owner::SessionOwnerOutcome::Applied { .. }
        ) {
            crate::session_owner_contract::process::pause("scope-join-after");
        }
        if matches!(
            outcome,
            crate::session_owner::SessionOwnerOutcome::Applied { .. }
        ) && fault(self) == 2
        {
            return Err(Error::UnknownStoreOutcome);
        }
        Ok(outcome)
    }
}
