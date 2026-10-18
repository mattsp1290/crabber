//! Borrowed original ownership, never a source marker or serializable permission.
use super::super::enrolled_initial::InitialResourceAssociation;
use super::{
    Error, FixedPreparationOwner, JoinedPreparation, MemoryStore, OwnedScopeActivation,
    PreparationCommitError, PreparationGrant, Proof, SessionOwnerAuthority, Weak,
};
use crate::native_enrollment::pending_initial::Correlation;
use crate::session_owner::owned_scope::JoinedOwnedScope;

pub(crate) struct OriginalPolicyView<'a> {
    owner: &'a FixedPreparationOwner,
    association: &'a InitialResourceAssociation<'a>,
}
impl<'a> OriginalPolicyView<'a> {
    pub(super) fn new(
        owner: &'a FixedPreparationOwner,
        association: &'a InitialResourceAssociation<'a>,
    ) -> Self {
        Self { owner, association }
    }
    pub(crate) fn verify_open(
        &self,
        actual_store: &MemoryStore,
        association: &InitialResourceAssociation<'_>,
        original: &JoinedPreparation,
        expected: &Correlation,
        proof: &mut Proof,
    ) -> Result<&OwnedScopeActivation, PreparationCommitError> {
        if !std::ptr::eq(actual_store, self.owner.core.actual_store())
            || !self.association.same_holder(association)
            || !association.belongs_to(self.owner.holder.get().and_then(Weak::upgrade).as_deref())
            || self.owner.inner_task.get().is_none()
        {
            return Err(Error::IdentityMismatch.into());
        }
        let grant = self.owner.grant.get().ok_or(Error::InvalidTransition)?;
        let child = grant
            .retained_resources_authority()
            .ok_or(Error::InvalidTransition)?;
        let observed = original.observed_grant();
        crate::native_enrollment::caller::measure_owner_authority(child, proof)
            .map_err(|_| Error::Capacity)?;
        crate::native_enrollment::caller::measure_owner_authority(grant.owner_authority(), proof)
            .map_err(|_| Error::Capacity)?;
        let observed_child = observed
            .retained_resources_authority()
            .ok_or(Error::InvalidTransition)?;
        let activation = self
            .owner
            .activation
            .get()
            .ok_or(Error::InvalidTransition)?;
        proof
            .measure(&(activation, &expected.key.session_id))
            .map_err(|_| Error::Capacity)?;
        if !observed_child.authenticates(child.session_id(), child.target(), &child.token)
            || !observed.owner_authority().authenticates(
                grant.owner_authority().session_id(),
                grant.owner_authority().target(),
                &grant.owner_authority().token,
            )
            || original.observed_registration() != Some(activation)
            || &expected.key.session_id != child.session_id()
            || self.owner.lock().failed
        {
            return Err(Error::IdentityMismatch.into());
        }
        Ok(activation)
    }
    pub(in crate::session_owner::owned_scope::task::pending) fn retirement(
        &'a self,
    ) -> OriginalPolicyRetirementView<'a> {
        OriginalPolicyRetirementView {
            owner: self.owner,
            policy: Some(self),
        }
    }
}

pub(crate) struct OriginalPolicyRetirementView<'a> {
    pub(super) owner: &'a FixedPreparationOwner,
    policy: Option<&'a OriginalPolicyView<'a>>,
}
impl<'a> OriginalPolicyRetirementView<'a> {
    pub(super) fn partial(owner: &'a FixedPreparationOwner) -> Self {
        Self {
            owner,
            policy: None,
        }
    }
    pub(crate) fn verify_close(
        &self,
        store: &MemoryStore,
        association: &InitialResourceAssociation<'_>,
        original: &JoinedPreparation,
        expected: &Correlation,
        proof: &mut Proof,
    ) -> Result<&OriginalPolicyView<'_>, PreparationCommitError> {
        let policy = self.policy.ok_or(Error::InvalidTransition)?;
        policy.verify_open(store, association, original, expected, proof)?;
        let (_, disposition) = self.owner.observed_join(proof)?;
        if disposition != super::super::OwnedMainDisposition::Returned {
            return Err(Error::InvalidTransition.into());
        }
        association.verify_policy_close(original, expected, proof)?;
        Ok(policy)
    }
    pub(crate) fn joined_scope(
        &self,
        proof: &mut Proof,
    ) -> Result<JoinedOwnedScope, PreparationCommitError> {
        JoinedOwnedScope::from_fixed_preparation(self, proof)
    }
    pub(crate) fn authority(&self) -> &SessionOwnerAuthority {
        // The view exists only for a loan returned after a genuine retained
        // resource grant. Absence cannot be replaced with a constructed token.
        self.owner
            .grant
            .get()
            .and_then(PreparationGrant::retained_resources_authority)
            .expect("fixed preparation loan retains original child authority")
    }
    pub(crate) fn observed_join(
        &self,
        proof: &mut Proof,
    ) -> Result<(&OwnedScopeActivation, super::super::OwnedMainDisposition), PreparationCommitError>
    {
        self.owner.observed_join(proof)
    }
}
