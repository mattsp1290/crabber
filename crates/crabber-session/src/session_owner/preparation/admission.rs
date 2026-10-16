//! Exact association from a sealed preparation to an actual new native Run.
//! This is structural ownership evidence; physical resource coverage is proved
//! separately by the typed dispatch/join sources, never an empty owner roster.
use super::{PreparationKey, PreparationPurpose, handoff, identity, proof};
use crate::native_parent::{RunIssuance, RunState};
use crate::session_owner::{
    MutateSessionOwner, SessionOwnerCommand, SessionOwnerError as Error, SessionOwnerTarget,
    SessionOwnerTermination, audit::Audit,
};
use crabber_core::{InputFingerprint, Run, RunId, Session, SessionId, SessionRevision};
use serde::{Deserialize, Serialize};

pub(crate) const MAX_WITNESS_BYTES: usize = 8192;

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SessionOrigin {
    workspace_id: String,
    directory: String,
    created_at: time::OffsetDateTime,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Witness {
    format_version: u8,
    initial: RunState,
    session: SessionOrigin,
    admitted_revision: SessionRevision,
    key: PreparationKey,
    source: super::PreparationResourceLink,
    child: SessionOwnerTarget,
    join_digest: InputFingerprint,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    unkeyed_input: Option<InputFingerprint>,
}

pub(crate) struct SourceAudits<'a> {
    pub reserve: &'a Audit,
    pub begin: &'a Audit,
    pub join: &'a Audit,
    pub child_reserve: &'a Audit,
    pub child_begin: &'a Audit,
}

impl Witness {
    pub(crate) fn from_preparation(
        preparation: &proof::VerifiedPreparation<'_>,
        session: &Session,
        run: &Run,
        admitted_revision: SessionRevision,
    ) -> Result<Option<Self>, Error> {
        let Some(resources) = preparation.resource_handoff() else {
            return Ok(None);
        };
        let original = preparation.grant.original_reservation();
        if !matches!(
            original.purpose,
            PreparationPurpose::OrdinaryAdmission | PreparationPurpose::Queue { .. }
        ) || run.session_id != session.id
            || original.owner.binding.session_id != session.id
            || original.owner.binding.workspace_id != session.workspace_id
            || original.owner.binding.directory != session.directory
        {
            return Err(Error::SemanticConflict);
        }
        let join = proof::join_request(preparation.grant)?;
        let value = Self {
            format_version: if preparation.unkeyed_input.is_some() {
                2
            } else {
                1
            },
            initial: RunState::from_run(run).map_err(|_| Error::UnknownStoreOutcome)?,
            session: SessionOrigin {
                workspace_id: session.workspace_id.clone(),
                directory: session.directory.clone(),
                created_at: session.created_at,
            },
            admitted_revision,
            key: original.key.clone(),
            source: resources.source().clone(),
            child: resources.child().clone(),
            join_digest: proof::join_fingerprint(preparation, &join)?,
            unkeyed_input: preparation.unkeyed_input.clone(),
        };
        value.validate()?;
        Ok(Some(value))
    }
    pub(crate) fn session_id(&self) -> &SessionId {
        &self.initial.session_id
    }
    pub(crate) fn run_id(&self) -> &RunId {
        &self.initial.run_id
    }
    pub(crate) fn is_unkeyed(&self) -> bool {
        matches!(self.key, PreparationKey::Unkeyed(_))
    }
    pub(crate) fn unkeyed_binding_digest(&self) -> Result<&InputFingerprint, Error> {
        self.validate()?;
        if !self.is_unkeyed() || self.format_version != 2 {
            return Err(Error::Unsupported);
        }
        self.unkeyed_input
            .as_ref()
            .ok_or(Error::UnknownStoreOutcome)
    }
    pub(crate) fn verify_unkeyed_binding(
        &self,
        binding: &crate::ExecutionBindingCapsule,
    ) -> Result<&InputFingerprint, Error> {
        let original = self.unkeyed_binding_digest()?;
        if super::binding_digest::unkeyed_binding(binding)? != *original {
            return Err(Error::UnknownStoreOutcome);
        }
        Ok(original)
    }
    pub(crate) fn admitted_revision(&self) -> SessionRevision {
        self.admitted_revision
    }
    pub(crate) fn join_id(&self) -> Result<crabber_core::SessionOwnerOperationId, Error> {
        Ok(identity::ids(&self.key, self.session_id())?.join)
    }
    pub(crate) fn audit_ids(&self) -> Result<[crabber_core::SessionOwnerOperationId; 5], Error> {
        let child = self.source.identities()?;
        Ok([
            self.source.root_reserve.clone(),
            self.source.root_begin.clone(),
            self.join_id()?,
            child.reserve,
            child.begin,
        ])
    }
    pub(crate) fn fingerprint(&self) -> Result<InputFingerprint, Error> {
        self.validate()?;
        crate::session_owner::digest(&("crabber.preparation.admission-witness.v1", self))
    }
    pub(crate) fn initial_issuance(&self, run: &Run) -> Result<RunIssuance, Error> {
        if self.initial != RunState::from_run(run).map_err(|_| Error::UnknownStoreOutcome)? {
            return Err(Error::SemanticConflict);
        }
        let issuance = RunIssuance {
            version: 2,
            sequence: 1,
            state: self.initial.clone(),
            ownership_handoff: Some(self.fingerprint()?),
            completion_source: None,
        };
        issuance
            .validate()
            .map_err(|_| Error::UnknownStoreOutcome)?;
        Ok(issuance)
    }
    #[cfg(any(feature = "postgres", feature = "sqlite"))]
    pub(crate) fn encoded(&self) -> Result<String, Error> {
        self.validate()?;
        serde_json::to_string(self).map_err(|_| Error::UnknownStoreOutcome)
    }
    pub(crate) fn validate(&self) -> Result<(), Error> {
        if !matches!(
            (self.format_version, self.unkeyed_input.is_some()),
            (1, false) | (2, true)
        ) || (self.unkeyed_input.is_some() && !matches!(self.key, PreparationKey::Unkeyed(_)))
            || self.initial.status != crabber_core::RunStatus::Running
            || self.admitted_revision == SessionRevision::INITIAL
            || self.source.session_id != *self.session_id()
            || crate::snapshot::encoded_bytes(self, MAX_WITNESS_BYTES).is_none()
        {
            return Err(Error::UnknownStoreOutcome);
        }
        let ids = identity::ids(&self.key, self.session_id())?;
        let child = self.source.identities()?;
        let join = MutateSessionOwner {
            id: ids.join,
            session_id: self.session_id().clone(),
            target: self.source.root.clone(),
            command: SessionOwnerCommand::Join {
                termination: SessionOwnerTermination::Returned,
            },
        };
        if self.source.root.id != ids.owner
            || self.source.root_reserve != ids.reserve
            || self.source.root_begin != ids.begin
            || self.child.id != child.owner
            || handoff::fingerprint_with_input(
                &join,
                &self.source,
                &self.child,
                self.unkeyed_input.as_ref(),
            )? != self.join_digest
        {
            return Err(Error::UnknownStoreOutcome);
        }
        // Also validate the narrow native scope/token and first-issuance format.
        RunIssuance {
            version: 1,
            sequence: 1,
            state: self.initial.clone(),
            ownership_handoff: None,
            completion_source: None,
        }
        .validate()
        .map_err(|_| Error::UnknownStoreOutcome)
    }

    pub(crate) fn replay(
        &self,
        grant: &super::PreparationGrant,
        session: &Session,
        initial: &RunIssuance,
        audits: &SourceAudits<'_>,
        binding: &crate::ExecutionBindingCapsule,
    ) -> Result<super::PreparationAdmissionReceipt, Error> {
        self.verify_sources(session, initial, audits)?;
        if let Some(input) = &self.unkeyed_input
            && super::binding_digest::unkeyed_binding(binding)? != *input
        {
            return Err(Error::SemanticConflict);
        }
        super::proof::verify_join_audit(grant, Some(audits.join))?;
        let original = grant.original_reservation();
        let super::PreparationKey::Unkeyed(id) = &original.key else {
            return Err(Error::SemanticConflict);
        };
        if self.key != original.key
            || self.source != super::PreparationResourceLink::for_grant(grant)?
            || grant
                .retained_resources_authority()
                .map(crate::session_owner::SessionOwnerAuthority::target)
                != Some(&self.child)
        {
            return Err(Error::SemanticConflict);
        }
        Ok(super::PreparationAdmissionReceipt {
            session_id: self.session_id().clone(),
            run_id: self.run_id().clone(),
            operation_id: id.clone(),
        })
    }

    pub(crate) fn verify_sources(
        &self,
        session: &Session,
        initial: &RunIssuance,
        audits: &SourceAudits<'_>,
    ) -> Result<(), Error> {
        self.validate()?;
        audits.verify_scope(self.session_id())?;
        initial.validate().map_err(|_| Error::UnknownStoreOutcome)?;
        if self.session_id() != &session.id
            || self.session.workspace_id != session.workspace_id
            || self.session.directory != session.directory
            || self.session.created_at != session.created_at
            || initial.version != 2
            || initial.sequence != 1
            || initial.state != self.initial
            || initial.ownership_handoff.as_ref() != Some(&self.fingerprint()?)
        {
            return Err(Error::UnknownStoreOutcome);
        }
        let child = self.source.identities()?;
        crate::session_owner::audit::verify_current(
            &audits.join.record,
            audits.reserve,
            audits.join,
        )?;
        crate::session_owner::audit::verify_current(
            &audits.begin.record,
            audits.reserve,
            audits.begin,
        )?;
        crate::session_owner::audit::verify_current(
            &audits.child_begin.record,
            audits.child_reserve,
            audits.child_begin,
        )?;
        super::resource_source::verify(&audits.child_begin.record, audits.reserve, audits.begin)?;
        if audits.reserve.receipt.operation_id != self.source.root_reserve
            || audits.reserve.receipt.semantic_digest != self.source.root_digest
            || audits.begin.receipt.operation_id != self.source.root_begin
            || audits.begin.receipt.operation != crate::session_owner::SessionOwnerOperation::Begin
            || audits.begin.record.snapshot.target != self.source.root
            || audits.join.receipt.operation_id != self.join_id()?
            || audits.join.receipt.semantic_digest != self.join_digest
            || audits.join.receipt.operation != crate::session_owner::SessionOwnerOperation::Join
            || audits.join.record.retained_resources.as_ref() != Some(&self.child)
            || audits.join.record.unkeyed_input != self.unkeyed_input
            || audits.child_reserve.receipt.operation_id != child.reserve
            || audits.child_begin.receipt.operation_id != child.begin
            || audits.child_begin.receipt.operation
                != crate::session_owner::SessionOwnerOperation::Begin
            || audits.child_begin.record.snapshot.target != self.child
            || audits.child_begin.record.resource.as_ref() != Some(&self.source)
        {
            return Err(Error::UnknownStoreOutcome);
        }
        let original = audits
            .reserve
            .record
            .preparation
            .as_ref()
            .ok_or(Error::UnknownStoreOutcome)?;
        if original.key != self.key
            || !matches!(
                original.purpose,
                PreparationPurpose::OrdinaryAdmission | PreparationPurpose::Queue { .. }
            )
            || audits.reserve.record.snapshot.binding.workspace_id != self.session.workspace_id
            || audits.reserve.record.snapshot.binding.directory != self.session.directory
        {
            return Err(Error::UnknownStoreOutcome);
        }
        Ok(())
    }
}

impl SourceAudits<'_> {
    fn verify_scope(&self, session: &SessionId) -> Result<(), Error> {
        for audit in [
            self.reserve,
            self.begin,
            self.join,
            self.child_reserve,
            self.child_begin,
        ] {
            audit.validate()?;
            if audit.receipt.session_id != *session
                || audit.record.snapshot.binding.session_id != *session
            {
                return Err(Error::UnknownStoreOutcome);
            }
        }
        Ok(())
    }
}

impl<'a> SourceAudits<'a> {
    pub(crate) fn from_array(audits: &'a [Audit; 5]) -> Self {
        Self {
            reserve: &audits[0],
            begin: &audits[1],
            join: &audits[2],
            child_reserve: &audits[3],
            child_begin: &audits[4],
        }
    }
}
