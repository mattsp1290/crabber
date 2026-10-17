//! Pure validation of independently selected, bounded preparation publications.
use super::PreparationGrant;
use crate::session_owner::{
    MutateSessionOwner, SessionOwnerCommand, SessionOwnerError as Error, SessionOwnerOperation,
    SessionOwnerState, SessionOwnerTermination,
    audit::{Audit, RevisionState},
    record::Record,
};
use crabber_core::SessionRevision;
use time::OffsetDateTime;

pub(crate) const MAX_PUBLICATIONS: usize = 64;

/// Fresh service activation permits only its exact target to await dispatch;
/// normal preparation completion requires every service already activated.
#[derive(Clone, Copy)]
pub(crate) enum ServiceRequirement<'a> {
    Ready(&'a [Record]),
    Activate {
        records: &'a [Record],
        target: &'a crate::session_owner::SessionOwnerTarget,
    },
}
impl ServiceRequirement<'_> {
    pub(crate) fn records(&self) -> &[Record] {
        match self {
            Self::Ready(records) | Self::Activate { records, .. } => records,
        }
    }
    pub(crate) fn activating(&self) -> Option<&crate::session_owner::SessionOwnerTarget> {
        match self {
            Self::Ready(_) => None,
            Self::Activate { target, .. } => Some(target),
        }
    }
}

/// This proof never escapes the transaction that authenticated it.
#[derive(Clone)]
pub(crate) struct VerifiedPreparation<'a> {
    pub grant: &'a PreparationGrant,
    pub(super) claim_capture: super::capture::ClaimCapture,
    pub effective_revision: SessionRevision,
    pub(super) resource_handoff: Option<VerifiedResourceHandoff>,
    pub(crate) unkeyed_input: Option<crabber_core::InputFingerprint>,
}

/// Transaction-local source evidence, minted only after current root and child
/// authority, immutable origins and the exact publication chain all agree.
/// This establishes retained capacity, not physical cleanup or whole-session coverage.
#[derive(Clone)]
pub(crate) struct VerifiedResourceHandoff {
    pub(super) link: super::PreparationResourceLink,
    pub(super) child: crate::session_owner::SessionOwnerTarget,
    pub(super) activation: Option<crabber_core::InputFingerprint>,
}
impl VerifiedResourceHandoff {
    pub(super) fn from_verified_child(
        link: super::PreparationResourceLink,
        child: &Record,
    ) -> Result<Self, Error> {
        Ok(Self {
            link,
            child: child.snapshot.target.clone(),
            activation: child
                .owned_scope
                .as_ref()
                .map(crate::session_owner::owned_scope::OwnedScopeActivation::fingerprint)
                .transpose()?,
        })
    }
    pub(crate) fn source(&self) -> &super::PreparationResourceLink {
        &self.link
    }
    pub(crate) fn child(&self) -> &crate::session_owner::SessionOwnerTarget {
        &self.child
    }
}

impl VerifiedPreparation<'_> {
    /// Called only after authenticating this transaction's actual joined task.
    pub(crate) fn bind_observed_operation(
        &mut self,
        joined: &super::JoinedPreparation,
    ) -> Result<(), Error> {
        self.verify_observation(joined)?;
        if self.resource_handoff.is_some()
            && let super::PreparedOperation::Admit(bound) = joined.observed_operation()
        {
            if !matches!(
                self.grant.original_reservation().key,
                super::PreparationKey::Unkeyed(_)
            ) {
                return Err(Error::SemanticConflict);
            }
            self.unkeyed_input = Some(super::binding_digest::unkeyed_binding(&bound.binding)?);
        }
        Ok(())
    }
    pub(crate) fn require_resource_activation(&self) -> Result<(), Error> {
        if self
            .grant
            .original_reservation()
            .resource_contract
            .is_none()
            || self
                .resource_handoff()
                .is_none_or(|handoff| handoff.activation.is_none())
        {
            return Err(Error::StaleOwner);
        }
        Ok(())
    }
    pub(crate) fn verify_observation(
        &self,
        joined: &super::JoinedPreparation,
    ) -> Result<(), Error> {
        if self
            .grant
            .original_reservation()
            .resource_contract
            .is_some()
        {
            let observed = joined
                .observed_registration()
                .ok_or(Error::InvalidTransition)?
                .fingerprint()?;
            if self
                .resource_handoff()
                .and_then(|handoff| handoff.activation.as_ref())
                != Some(&observed)
            {
                return Err(Error::SemanticConflict);
            }
        }
        Ok(())
    }
    /// Native transaction consumers may pair this exact proof with their actual
    /// newly admitted `RunID`. Legacy preparation has no positive handoff proof.
    pub(crate) fn resource_handoff(&self) -> Option<&VerifiedResourceHandoff> {
        self.resource_handoff.as_ref()
    }
    pub fn control_revision(
        &self,
        key: &super::PreparationKey,
        session: &crabber_core::SessionId,
        run: &crabber_core::RunId,
        original_revision: SessionRevision,
    ) -> Result<SessionRevision, Error> {
        if self.grant.original_reservation().key != *key
            || self.grant.original_reservation().owner.binding.session_id != *session
            || self.grant.original_reservation().expected_revision != original_revision
            || !matches!(&self.grant.original_reservation().purpose, super::PreparationPurpose::ExistingRun { run_id, .. } if run_id == run)
        {
            return Err(Error::SemanticConflict);
        }
        Ok(self.effective_revision)
    }
    pub fn verify_queue(&self, request: &crate::QueueMutation) -> Result<(), crate::StoreError> {
        let super::PreparationPurpose::Queue { authority } =
            &self.grant.original_reservation().purpose
        else {
            return Err(crate::StoreError::AdmissionConflict);
        };
        let (crate::QueueAction::Prepare {
            authority: actual, ..
        }
        | crate::QueueAction::Admit {
            authority: actual, ..
        }) = &request.action
        else {
            return Err(crate::StoreError::AdmissionConflict);
        };
        if actual != authority
            || request.session_id != self.grant.original_reservation().owner.binding.session_id
            || request.expected_revision != self.grant.original_reservation().expected_revision
        {
            return Err(crate::StoreError::AdmissionConflict);
        }
        Ok(())
    }
    pub fn verify_admission(&self, request: &crate::AdmitRequest) -> Result<(), crate::StoreError> {
        if let Some(proof) = self.resource_handoff()
            && (proof.source().root != *self.grant.owner_authority().target()
                || self
                    .grant
                    .retained_resources_authority()
                    .map(crate::session_owner::SessionOwnerAuthority::target)
                    != Some(proof.child()))
        {
            return Err(crate::StoreError::AdmissionConflict);
        }
        if request
            .session_id
            .as_ref()
            .unwrap_or(&request.user_message.session_id)
            != &self.grant.original_reservation().owner.binding.session_id
            || request.workspace_id != self.grant.original_reservation().owner.binding.workspace_id
            || request.directory != self.grant.original_reservation().owner.binding.directory
            || request.expected_revision
                != Some(self.grant.original_reservation().expected_revision)
        {
            return Err(crate::StoreError::AdmissionConflict);
        }
        Ok(())
    }
}

pub(crate) fn authenticate_root(
    grant: &PreparationGrant,
    record: &Record,
    now: OffsetDateTime,
) -> Result<(), Error> {
    let request = &grant.original_reservation();
    if record.snapshot.state != SessionOwnerState::Active
        || record.lease_until <= now
        || !grant.owner_authority().authenticates(
            &record.snapshot.binding.session_id,
            &record.snapshot.target,
            &record.token,
        )
        || record.preparation.as_ref() != Some(&crate::session_owner::preparation::origin(request))
        || record.snapshot.binding != request.owner.binding
    {
        return Err(Error::StaleOwner);
    }
    Ok(())
}
#[cfg(any(feature = "postgres", feature = "sqlite"))]
pub(crate) fn verify_with_resources<'a>(
    grant: &'a PreparationGrant,
    record: &Record,
    child: Option<&Record>,
    audits: &[Audit],
    revision: RevisionState,
    now: OffsetDateTime,
) -> Result<VerifiedPreparation<'a>, Error> {
    verify_with_services(
        grant,
        record,
        child,
        audits,
        revision,
        now,
        ServiceRequirement::Ready(&[]),
    )
}

pub(crate) fn verify_with_services<'a>(
    grant: &'a PreparationGrant,
    record: &Record,
    child: Option<&Record>,
    audits: &[Audit],
    revision: RevisionState,
    now: OffsetDateTime,
    services: ServiceRequirement<'_>,
) -> Result<VerifiedPreparation<'a>, Error> {
    if let Some(child) = child {
        return super::resource_proof::verify(
            grant, record, child, audits, revision, now, services,
        );
    }
    if !services.records().is_empty()
        || services.activating().is_some()
        || grant.retained_resources_authority().is_some()
        || record.snapshot.binding.component_id == super::OWNED_PREPARATION_COMPONENT
    {
        return Err(Error::StaleOwner);
    }
    verify_root_for_link(grant, record, audits, revision, now)?;
    let effective_revision = match revision {
        RevisionState::Absent(_) => SessionRevision::INITIAL,
        RevisionState::Present(value) => value.next().map_err(|_| Error::UnknownStoreOutcome)?,
    };
    Ok(VerifiedPreparation {
        grant,
        claim_capture: super::capture::ClaimCapture::default(),
        effective_revision,
        resource_handoff: None,
        unkeyed_input: None,
    })
}
pub(crate) fn join_fingerprint(
    proof: &VerifiedPreparation<'_>,
    command: &MutateSessionOwner,
) -> Result<crabber_core::InputFingerprint, Error> {
    match proof.resource_handoff() {
        Some(handoff) => super::handoff::fingerprint_with_input(
            command,
            handoff.source(),
            handoff.child(),
            proof.unkeyed_input.as_ref(),
        ),
        None => command.fingerprint(),
    }
}
pub(crate) fn record_handoff(
    record: &mut Record,
    proof: &VerifiedPreparation<'_>,
) -> Result<(), Error> {
    if proof
        .grant
        .original_reservation()
        .resource_contract
        .is_some()
        && proof
            .resource_handoff()
            .is_none_or(|handoff| handoff.activation.is_none())
    {
        return Err(Error::StaleOwner);
    }
    if let Some(handoff) = proof.resource_handoff() {
        record.retained_resources = Some(handoff.child().clone());
        record.unkeyed_input.clone_from(&proof.unkeyed_input);
    } else if record.snapshot.binding.component_id == super::OWNED_PREPARATION_COMPONENT {
        return Err(Error::StaleOwner);
    }
    Ok(())
}

pub(crate) fn verify_root_for_link(
    grant: &PreparationGrant,
    record: &Record,
    audits: &[Audit],
    revision: RevisionState,
    now: OffsetDateTime,
) -> Result<(), Error> {
    authenticate_root(grant, record, now)?;
    let request = &grant.original_reservation();
    if audits.is_empty() || audits.len() > MAX_PUBLICATIONS {
        return Err(Error::Capacity);
    }
    let mut expected = None;
    let mut sequence = 0;
    let mut saw_begin = false;
    for audit in audits {
        audit.validate()?;
        if audit.record.sequence <= sequence
            || audit.record.snapshot.target.id != record.snapshot.target.id
            || audit.record.snapshot.binding != record.snapshot.binding
            || audit.record.preparation != record.preparation
        {
            return Err(Error::UnknownStoreOutcome);
        }
        let publication = audit.publication.ok_or(Error::Unsupported)?;
        if !publication.public_change {
            return Err(Error::UnknownStoreOutcome);
        }
        if let Some(previous) = expected {
            if publication.before != previous {
                return Err(Error::RevisionConflict);
            }
        } else {
            let original = match publication.before {
                RevisionState::Absent(_) => SessionRevision::INITIAL,
                RevisionState::Present(value) => value,
            };
            if audit.receipt.operation != SessionOwnerOperation::Reserve
                || audit.receipt.operation_id != request.owner.id
                || audit.receipt.semantic_digest != request.fingerprint()?
                || original != request.expected_revision
            {
                return Err(Error::UnknownStoreOutcome);
            }
        }
        match audit.receipt.operation {
            SessionOwnerOperation::Reserve if sequence == 0 => (),
            SessionOwnerOperation::ReclaimUnstarted if !saw_begin => (),
            SessionOwnerOperation::Begin if !saw_begin => {
                if audit.receipt.operation_id
                    != crate::session_owner::preparation::identity::ids(
                        &request.key,
                        &request.owner.binding.session_id,
                    )?
                    .begin
                {
                    return Err(Error::UnknownStoreOutcome);
                }
                saw_begin = true;
            }
            _ => return Err(Error::InvalidTransition),
        }
        expected = Some(publication.after);
        sequence = audit.record.sequence;
    }
    if !saw_begin || expected != Some(revision) {
        return Err(Error::RevisionConflict);
    }
    Ok(())
}

pub(crate) fn join_request(grant: &PreparationGrant) -> Result<MutateSessionOwner, Error> {
    let ids = super::identity::ids(
        &grant.original_reservation().key,
        &grant.original_reservation().owner.binding.session_id,
    )?;
    Ok(MutateSessionOwner {
        id: ids.join,
        session_id: grant
            .original_reservation()
            .owner
            .binding
            .session_id
            .clone(),
        target: grant.owner_authority().target().clone(),
        command: SessionOwnerCommand::Join {
            termination: SessionOwnerTermination::Returned,
        },
    })
}

/// Replayed native metadata cannot conceal an unretired preparation. Read only
/// the original Join receipt; current owner corruption cannot invalidate an
/// already committed exact retry or turn its metadata into fresh authority.
fn verify_join_receipt(
    grant: &PreparationGrant,
    receipt: Option<&crate::session_owner::SessionOwnerReceipt>,
    input: Option<&crabber_core::InputFingerprint>,
) -> Result<(), Error> {
    let receipt = receipt.ok_or(Error::InvalidTransition)?;
    let expected = join_request(grant)?;
    if receipt.session_id != expected.session_id
        || receipt.operation_id != expected.id
        || receipt.semantic_digest
            != match grant.retained_resources_authority() {
                Some(child) => super::handoff::fingerprint_with_input(
                    &expected,
                    &super::PreparationResourceLink::for_grant(grant)?,
                    child.target(),
                    input,
                )?,
                None => expected.fingerprint()?,
            }
        || receipt.operation != SessionOwnerOperation::Join
        || receipt.owner.binding != grant.original_reservation().owner.binding
        || receipt.owner.target != expected.target
        || receipt.owner.state != SessionOwnerState::Joined
    {
        return Err(Error::SemanticConflict);
    }
    Ok(())
}

pub(crate) fn verify_join_audit(
    grant: &PreparationGrant,
    audit: Option<&Audit>,
) -> Result<(), Error> {
    let audit = audit.ok_or(Error::InvalidTransition)?;
    audit.validate()?;
    if audit.record.preparation.as_ref()
        != Some(&crate::session_owner::preparation::origin(
            grant.original_reservation(),
        ))
        || audit.record.origin != grant.original_reservation().owner.id
    {
        return Err(Error::SemanticConflict);
    }
    if grant.original_reservation().owner.binding.component_id == super::OWNED_PREPARATION_COMPONENT
        && audit.record.retained_resources.as_ref()
            != grant
                .retained_resources_authority()
                .map(crate::session_owner::SessionOwnerAuthority::target)
    {
        return Err(Error::InvalidTransition);
    }
    if grant.original_reservation().owner.binding.component_id == super::OWNED_PREPARATION_COMPONENT
        && audit.record.retained_resources.is_none()
    {
        return Err(Error::InvalidTransition);
    }
    verify_join_receipt(
        grant,
        Some(&audit.receipt),
        audit.record.unkeyed_input.as_ref(),
    )
}
