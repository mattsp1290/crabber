//! Exact original source and native-parent proof precede fresh activation.
mod initial_retirement;
pub(in crate::sqlite) use initial_retirement::driver_roster as initial_driver_roster;
mod context;
use super::{
    Audit, Error, SessionOwnerAuthority, SessionOwnerOperation, SessionOwnerOutcome, SqliteStore,
    before, fault, finish, publish_revision, reads, unknown,
};
use crate::session_owner::owned_scope::{OwnedScopeActivation, source};
impl SqliteStore {
    pub(in crate::sqlite) async fn publish_owned_scope_activation(
        &self,
        request: OwnedScopeActivation,
        authority: SessionOwnerAuthority,
        parent_authority: Option<crate::session_owner::owned_scope::OwnedScopeParentAuthority>,
    ) -> Result<SessionOwnerOutcome, Error> {
        if is_driver_activation(&request, parent_authority.as_ref()) {
            return self
                .activate_driver(request, authority, parent_authority)
                .await;
        }
        let digest = request.fingerprint()?;
        let mut tx = self.begin_write().await.map_err(unknown)?;
        let result = Box::pin(async {
            let mut proof = crate::public_session::v2::proof::Proof::bounded(32 * 1024 * 1024);
            if let Some(audit) = reads::audit_with_proof(
                &mut tx,
                request.session_id(),
                request.operation_id(),
                &mut proof,
            )
            .await
            .map_err(crate::public_session::v2::proof::ReadError::owner)?
            {
                if audit.receipt.semantic_digest != digest
                    || audit.receipt.operation != SessionOwnerOperation::ActivateScope
                {
                    return Err(Error::SemanticConflict);
                }
                return Ok(SessionOwnerOutcome::Replayed(audit.receipt));
            }
            reads::scope(&mut tx, request.session_id()).await?;
            context::preflight(&mut tx, &request, &mut proof).await?;
            let mut record =
                reads::current(&mut tx, request.session_id(), &request.target().id).await?;
            let reserve = reads::audit_with_proof(
                &mut tx,
                request.session_id(),
                &request.source.reserve,
                &mut proof,
            )
            .await
            .map_err(crate::public_session::v2::proof::ReadError::owner)?
            .ok_or(Error::UnknownStoreOutcome)?;
            let begin = reads::audit_with_proof(
                &mut tx,
                request.session_id(),
                &request.source.begin,
                &mut proof,
            )
            .await
            .map_err(crate::public_session::v2::proof::ReadError::owner)?
            .ok_or(Error::UnknownStoreOutcome)?;
            source::verify(&request.source, &record, &reserve, &begin)?;
            Box::pin(self.owned_scope_context(
                &mut tx,
                &record,
                parent_authority.as_ref(),
                &mut proof,
            ))
            .await?;
            source::activate(&mut record, &request, &authority, self.clock.now())?;
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
            .await
            .map_err(crate::public_session::v2::proof::ReadError::owner)?;
            #[cfg(test)]
            crate::session_owner_contract::process::pause("scope-before");
            before(self)?;
            Ok(SessionOwnerOutcome::Applied {
                receipt,
                authority: None,
            })
        })
        .await;
        let outcome = finish(tx, result).await?;
        #[cfg(test)]
        if matches!(outcome, SessionOwnerOutcome::Applied { .. }) {
            crate::session_owner_contract::process::pause("scope-after");
        }
        if matches!(outcome, SessionOwnerOutcome::Applied { .. }) && fault(self) == 2 {
            return Err(Error::UnknownStoreOutcome);
        }
        Ok(outcome)
    }
    pub(in crate::sqlite) async fn join_owned_scope_in(
        &self,
        tx: &mut crate::transaction::Transaction<'_, sqlx::Sqlite>,
        proof: &crate::session_owner::owned_scope::JoinedOwnedScope,
        authority: &SessionOwnerAuthority,
        budget: &mut crate::public_session::v2::proof::Proof,
    ) -> Result<(SessionOwnerOutcome, Audit), Error> {
        budget
            .measure(proof.inspection())
            .map_err(|_| Error::Capacity)?;
        crate::native_enrollment::caller::measure_owner_authority(authority, budget)
            .map_err(|_| Error::Capacity)?;
        let request = proof.request()?;
        let digest = proof.fingerprint()?;
        super::super::deletion::ensure_live(tx, &request.session_id)
            .await
            .map_err(crate::deletion::owner_error)?;

        if let Some(audit) =
            super::drivers::selected_consumer_audit(tx, &request.session_id, &request.id, budget)
                .await
                .map_err(|error| error.into_domain_or(Error::Capacity))?
        {
            if audit.receipt.operation != SessionOwnerOperation::Join
                || audit.receipt.semantic_digest != digest
            {
                return Err(Error::SemanticConflict);
            }
            budget
                .measure(&audit.receipt)
                .map_err(|_| Error::Capacity)?;
            let outcome = SessionOwnerOutcome::Replayed(audit.receipt.clone());
            return Ok((outcome, audit));
        }
        reads::scope(tx, &request.session_id).await?;
        let mut record =
            reads::current_with_proof(tx, &request.session_id, &request.target.id, budget)
                .await
                .map_err(crate::public_session::v2::proof::ReadError::owner)?;
        let activation = proof.activation();
        let reserve = super::drivers::selected_consumer_audit(
            tx,
            &request.session_id,
            &activation.source.reserve,
            budget,
        )
        .await
        .map_err(|error| error.into_domain_or(Error::Capacity))?
        .ok_or(Error::UnknownStoreOutcome)?;
        let begin = super::drivers::selected_consumer_audit(
            tx,
            &request.session_id,
            &activation.source.begin,
            budget,
        )
        .await
        .map_err(|error| error.into_domain_or(Error::Capacity))?
        .ok_or(Error::UnknownStoreOutcome)?;
        let activated = super::drivers::selected_consumer_audit(
            tx,
            &request.session_id,
            &activation.id,
            budget,
        )
        .await
        .map_err(|error| error.into_domain_or(Error::Capacity))?;
        source::join_candidate(&record, proof, &reserve, &begin, activated.as_ref())?;
        record.mutate(&request, Some(authority), self.clock.now())?;
        budget
            .measure(proof.inspection())
            .map_err(|_| Error::Capacity)?;
        record.owned_join = Some(proof.inspection().clone());
        budget
            .measure(&(&request.id, &digest, &record.snapshot))
            .map_err(|_| Error::Capacity)?;
        let receipt = record.receipt(request.id.clone(), digest, SessionOwnerOperation::Join);
        let publication = publish_revision(tx, &request.session_id, true).await?;
        budget.measure(&receipt).map_err(|_| Error::Capacity)?;
        let audit = Audit {
            version: 2,
            receipt: receipt.clone(),
            record,
            publication: Some(publication),
        };
        budget.measure(&audit).map_err(|_| Error::Capacity)?;
        let actual = Box::pin(reads::publish_captured_with_proof(
            tx, &audit, false, budget,
        ))
        .await
        .map_err(crate::public_session::v2::proof::ReadError::owner)?;
        Ok((
            SessionOwnerOutcome::Applied {
                receipt,
                authority: None,
            },
            actual,
        ))
    }
    pub(in crate::sqlite) async fn join_owned_scope(
        &self,
        proof: crate::session_owner::owned_scope::JoinedOwnedScope,
        authority: SessionOwnerAuthority,
    ) -> Result<SessionOwnerOutcome, Error> {
        proof.request()?;
        let mut tx = self.begin_write().await.map_err(unknown)?;
        let result = Box::pin(async {
            let mut budget = crate::public_session::v2::proof::Proof::bounded(32 << 20);
            let (outcome, _audit) =
                Box::pin(self.join_owned_scope_in(&mut tx, &proof, &authority, &mut budget))
                    .await?;
            if matches!(outcome, SessionOwnerOutcome::Applied { .. }) {
                #[cfg(test)]
                crate::session_owner_contract::process::pause("scope-join-before");
                before(self)?;
            }
            Ok(outcome)
        })
        .await;
        let outcome = finish(tx, result).await?;
        #[cfg(test)]
        if matches!(outcome, SessionOwnerOutcome::Applied { .. }) {
            crate::session_owner_contract::process::pause("scope-join-after");
        }
        if matches!(outcome, SessionOwnerOutcome::Applied { .. }) && fault(self) == 2 {
            return Err(Error::UnknownStoreOutcome);
        }
        Ok(outcome)
    }
}

fn is_driver_activation(
    request: &OwnedScopeActivation,
    parent: Option<&crate::session_owner::owned_scope::OwnedScopeParentAuthority>,
) -> bool {
    request.target().id.0.starts_with("driver-owner-")
        || matches!(
            parent,
            Some(crate::session_owner::owned_scope::OwnedScopeParentAuthority::ExecutionDriver(_) | crate::session_owner::owned_scope::OwnedScopeParentAuthority::SuccessorExecutionDriver(_))
        )
}
