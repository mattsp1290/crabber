//! Acknowledgement alone mints a permit; replay and uncertain commit never do.
use super::{Error, MemoryStore, before, bump_public_revision, fault, revision};
use crate::session_owner::SessionOwnerAuthority;
use crate::session_owner::owned_scope::OwnedScopeActivation;
impl MemoryStore {
    pub(in crate::memory) fn publish_owned_scope_activation(
        &self,
        request: &OwnedScopeActivation,
        authority: &SessionOwnerAuthority,
        parent: Option<&crate::session_owner::owned_scope::OwnedScopeParentAuthority>,
    ) -> Result<crate::session_owner::SessionOwnerOutcome, Error> {
        // The stable namespace only selects stricter validation. It never grants
        // authority, including when a mutable source link has been erased.
        if request.target().id.0.starts_with("driver-owner-") || request.target().id.0.starts_with("successor-owner-")
            || matches!(
                parent,
                Some(
                    crate::session_owner::owned_scope::OwnedScopeParentAuthority::ExecutionDriver(_) | crate::session_owner::owned_scope::OwnedScopeParentAuthority::SuccessorExecutionDriver(_)
                )
            )
        {
            return self.publish_driver_activation(request, authority, parent);
        }
        let outcome = self.transact(|state| {
            super::super::deletion::ensure_live(state, request.session_id())
                .map_err(crate::deletion::owner_error)?;
            if let Some(receipt) = state.session_owners.scope_activation_receipt(request)? {
                return Ok(crate::session_owner::SessionOwnerOutcome::Replayed(receipt));
            }
            let record = state.session_owners.scope_activation_record(request)?;
            if record.parent.is_some() && !state.sessions.contains_key(request.session_id()) {
                return Err(Error::UnknownStoreOutcome);
            }
            scope_context(state, &record, parent, self.clock.now())?;
            let current = revision(state, request.session_id());
            let receipt = state.session_owners.activate_scope(
                record,
                request,
                authority,
                current,
                self.clock.now(),
            )?;
            if state.sessions.contains_key(request.session_id()) {
                bump_public_revision(state, request.session_id())
                    .map_err(|_| Error::UnknownStoreOutcome)?;
            }
            before(fault(self))?;
            Ok(crate::session_owner::SessionOwnerOutcome::Applied {
                receipt,
                authority: None,
            })
        })?;
        if matches!(
            outcome,
            crate::session_owner::SessionOwnerOutcome::Applied { .. }
        ) && fault(self) == 2
        {
            return Err(Error::UnknownStoreOutcome);
        }
        Ok(outcome)
    }
    fn publish_driver_activation(
        &self,
        request: &OwnedScopeActivation,
        authority: &SessionOwnerAuthority,
        parent: Option<&crate::session_owner::owned_scope::OwnedScopeParentAuthority>,
    ) -> Result<crate::session_owner::SessionOwnerOutcome, Error> {
        use crate::session_owner::owned_scope::OwnedScopeParentAuthority;
        let mut guard = self.state.lock().map_err(|_| Error::UnknownStoreOutcome)?;
        super::super::deletion::ensure_live(&guard, request.session_id())
            .map_err(crate::deletion::owner_error)?;
        let mut proof = crate::public_session::v2::proof::Proof::bounded(
            crate::deletion::coverage::MAX_PROOF_BYTES,
        );
        if let Some(receipt) = guard
            .session_owners
            .driver_activation_receipt_bounded(request, &mut proof)?
        {
            return Ok(crate::session_owner::SessionOwnerOutcome::Replayed(receipt));
        }
        guard
            .session_owners
            .validate_driver_membership(request.session_id(), &mut proof)?;
        let record = guard
            .session_owners
            .driver_activation_record_bounded(request, &mut proof)?;
        if record.cancellation.is_some() {
            return Err(Error::CancellationPending);
        }
        let activation_revision = guard
            .session_owners
            .source_audit(request.session_id(), &request.source.begin)
            .and_then(|audit| audit.publication)
            .map(|publication| publication.after)
            .ok_or(Error::UnknownStoreOutcome)?;
        match parent {
            Some(OwnedScopeParentAuthority::ExecutionDriver(grant)) => {
                let source = record.driver.as_deref().ok_or(Error::UnknownStoreOutcome)?;
                if &source.identity()? != grant.source_identity()
                    || grant.authority().target() != &record.snapshot.target
                {
                    return Err(Error::SemanticConflict);
                }
                if &super::drivers::fresh(
                    &guard,
                    grant.original_request(),
                    self.clock.now(),
                    &mut proof,
                    Some(activation_revision),
                )? != source
                {
                    return Err(Error::StaleOwner);
                }
            }
            Some(OwnedScopeParentAuthority::SuccessorExecutionDriver(grant)) => {
                let source = record
                    .successor_driver
                    .as_deref()
                    .ok_or(Error::UnknownStoreOutcome)?;
                if &source.identity()? != grant.source_identity()
                    || grant.authority().target() != &record.snapshot.target
                {
                    return Err(Error::SemanticConflict);
                }
                if &super::drivers::successor::fresh(
                    &guard,
                    grant.original_request(),
                    Some(activation_revision),
                    self.clock.now(),
                    &mut proof,
                )? != source
                {
                    return Err(Error::StaleOwner);
                }
            }
            _ => return Err(Error::InvalidRequest),
        }
        // All borrowed source checks precede the inherited Memory transaction
        // clone; unrelated stored history is not covered by this proof budget.
        crate::session_owner::owned_scope::source::validate_activation(
            record,
            request,
            authority,
            self.clock.now(),
        )?;
        let original = record.clone();
        let mut next = guard.clone();
        let current = revision(&next, request.session_id());
        let receipt = next.session_owners.activate_scope(
            original,
            request,
            authority,
            current,
            self.clock.now(),
        )?;
        bump_public_revision(&mut next, request.session_id())
            .map_err(|_| Error::UnknownStoreOutcome)?;
        let boundary = fault(self);
        before(boundary)?;
        *guard = next;
        if boundary == 2 {
            return Err(Error::UnknownStoreOutcome);
        }
        Ok(crate::session_owner::SessionOwnerOutcome::Applied {
            receipt,
            authority: None,
        })
    }
    pub(in crate::memory) fn join_owned_scope(
        &self,
        proof: &crate::session_owner::owned_scope::JoinedOwnedScope,
        authority: &SessionOwnerAuthority,
    ) -> Result<crate::session_owner::SessionOwnerOutcome, Error> {
        let request = proof.request()?;
        let outcome = self.transact(|state| {
            super::super::deletion::ensure_live(state, &request.session_id)
                .map_err(crate::deletion::owner_error)?;
            if let Some(receipt) = state.session_owners.joined_scope_receipt(proof)? {
                return Ok(crate::session_owner::SessionOwnerOutcome::Replayed(receipt));
            }
            let current = revision(state, &request.session_id);
            let receipt =
                state
                    .session_owners
                    .join_scope(proof, authority, current, self.clock.now())?;
            if state.sessions.contains_key(&request.session_id) {
                bump_public_revision(state, &request.session_id)
                    .map_err(|_| Error::UnknownStoreOutcome)?;
            }
            before(fault(self))?;
            Ok(crate::session_owner::SessionOwnerOutcome::Applied {
                receipt,
                authority: None,
            })
        })?;
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

fn scope_context(
    state: &super::super::State,
    record: &crate::session_owner::record::Record,
    authority: Option<&crate::session_owner::owned_scope::OwnedScopeParentAuthority>,
    now: time::OffsetDateTime,
) -> Result<(), Error> {
    use crate::session_owner::owned_scope::OwnedScopeParentAuthority;
    if record.driver.is_some() {
        return Err(Error::InvalidRequest);
    }
    if record.service.is_some() {
        return service_context(state, record, authority, now);
    }
    if record.resource.is_some() {
        let Some(OwnedScopeParentAuthority::Preparation(grant)) = authority else {
            return Err(Error::InvalidRequest);
        };
        if grant
            .retained_resources_authority()
            .map(crate::session_owner::SessionOwnerAuthority::target)
            != Some(&record.snapshot.target)
            || grant.original_reservation().owner.binding.session_id
                != record.snapshot.binding.session_id
        {
            return Err(Error::InvalidRequest);
        }
        super::validate_preparation_source(state, grant.original_reservation(), now)?;
        state.session_owners.verify_preparation(
            grant,
            revision(state, &record.snapshot.binding.session_id),
            now,
        )?;
        return Ok(());
    }
    if record.preparation.is_some() {
        return Err(Error::InvalidRequest);
    }
    let native = match authority {
        Some(OwnedScopeParentAuthority::Native(value)) => Some(value),
        None => None,
        _ => return Err(Error::InvalidRequest),
    };
    let request = crate::session_owner::ReserveSessionOwner {
        id: record.origin.clone(),
        binding: record.snapshot.binding.clone(),
        owner: record.owner.clone(),
        lease_ms: 1,
    };
    if super::parent_proof(state, &request, native, now)? != record.parent {
        return Err(Error::StaleOwner);
    }
    Ok(())
}

fn service_context(
    state: &super::super::State,
    record: &crate::session_owner::record::Record,
    authority: Option<&crate::session_owner::owned_scope::OwnedScopeParentAuthority>,
    now: time::OffsetDateTime,
) -> Result<(), Error> {
    let Some(crate::session_owner::owned_scope::OwnedScopeParentAuthority::ServicePreparation(
        grant,
    )) = authority
    else {
        return Err(Error::InvalidRequest);
    };
    let root = crate::session_owner::preparation::PreparationResourceLink::for_grant(grant)?;
    if record
        .service
        .as_ref()
        .is_none_or(|source| source.root != root)
    {
        return Err(Error::InvalidRequest);
    }
    if record.cancellation.is_some() {
        return Err(Error::CancellationPending);
    }
    super::validate_preparation_source(state, grant.original_reservation(), now)?;
    state
        .session_owners
        .verify_service_activation(
            grant,
            revision(state, &record.snapshot.binding.session_id),
            now,
            &record.snapshot.target,
        )?
        .require_resource_activation()
}
