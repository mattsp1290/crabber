//! Exact preparation lineage. Numeric revision refresh is never authorization.
use super::SessionOwnerTarget;
use super::{ReserveSessionOwner, SessionOwnerError as Error, SessionOwnerKind};
use crabber_core::{
    AdmissionKey, ControlId, InputFingerprint, RunId, SessionId, SessionOwnerGeneration,
    SessionOwnerOperationId, SessionRevision,
};
use serde::{Deserialize, Serialize};

/// The work whose frozen input a preparation is restoring. This declaration is
/// corroborated against native storage before any fresh preparation grant.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum PreparationPurpose {
    OrdinaryAdmission,
    ExistingRun {
        run_id: RunId,
        work: ExistingWorkTarget,
    },
    Queue {
        authority: crate::QueueAuthority,
    },
}

/// A preparation reservation preserves the caller's original CAS and intent.
/// The intent is a digest of the original request, never credentials or a plan.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReservePreparation {
    pub owner: ReserveSessionOwner,
    pub key: PreparationKey,
    #[serde(skip)]
    pub source: Option<PreparationSource>,
    pub expected_revision: SessionRevision,
    pub purpose: PreparationPurpose,
    pub intent: InputFingerprint,
}
impl ReservePreparation {
    /// # Errors
    /// Refuses non-preparation bindings and invalid routing identities.
    pub fn fingerprint(&self) -> Result<InputFingerprint, Error> {
        self.owner.fingerprint()?;
        if self.owner.binding.kind != SessionOwnerKind::Preparation
            || self.owner.binding.parent.is_some()
        {
            return Err(Error::InvalidRequest);
        }
        if let Some(source) = &self.source {
            source_validation::validate(source, self)?;
        }
        origin(self).fingerprint(&self.owner)
    }
}

mod origin;
pub(crate) use origin::{Origin, origin};

mod completion;
pub use completion::{
    JoinedPreparation, PreparationCompletion, PreparationGrant, PreparationOperationKind,
    PreparedOperation,
};

pub(crate) mod admission;
pub(crate) mod handoff;
pub(crate) mod proof;

pub(crate) mod identity;

mod error;
pub use error::PreparationCommitError;

pub(crate) mod intent;
pub use intent::{admission_intent, queue_admission_intent};

mod source;
pub use source::{ExistingWorkTarget, PreparationSource};

/// Original public operation namespace. Changing semantics reuses these IDs and
/// conflicts with the original reservation instead of creating new capacity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum PreparationKey {
    Unkeyed(ControlId),
    Admission(AdmissionKey),
    Queue(ControlId),
    Approval(ControlId),
    ToolContinuation(ControlId),
}

/// Safe correlation for explicit reconciliation. Possession grants no right to
/// Join, renew, dispatch, cancel or resolve an owner.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreparationReconciliation {
    pub session_id: SessionId,
    pub workspace_id: String,
    pub directory: String,
    pub owner: SessionOwnerTarget,
    pub reserve_id: SessionOwnerOperationId,
    pub begin_id: SessionOwnerOperationId,
    pub join_id: SessionOwnerOperationId,
    pub expected_revision: crabber_core::SessionRevision,
}

impl ReservePreparation {
    /// Bind a new reservation to its stable original operation identity. Owner
    /// and lease bookkeeping remain excluded from reconciliation semantics.
    /// # Errors
    /// Rejects invalid scope or original operation IDs.
    pub fn bind_key(mut self, key: PreparationKey) -> Result<Self, Error> {
        let ids = identity::ids(&key, &self.owner.binding.session_id)?;
        self.owner.id = ids.reserve;
        self.owner.binding.id = ids.owner;
        self.key = key;
        self.fingerprint()?;
        Ok(self)
    }
    /// Correlation metadata only, usable before any fresh dispatch authority.
    /// # Errors
    /// Rejects inconsistent identity or malformed routing metadata.
    pub fn reconciliation(&self) -> Result<PreparationReconciliation, Error> {
        self.fingerprint()?;
        let ids = identity::ids(&self.key, &self.owner.binding.session_id)?;
        Ok(PreparationReconciliation {
            session_id: self.owner.binding.session_id.clone(),
            workspace_id: self.owner.binding.workspace_id.clone(),
            directory: self.owner.binding.directory.clone(),
            owner: SessionOwnerTarget {
                id: ids.owner,
                generation: SessionOwnerGeneration::INITIAL,
            },
            reserve_id: ids.reserve,
            begin_id: ids.begin,
            join_id: ids.join,
            expected_revision: self.expected_revision,
        })
    }
}

pub(crate) mod source_validation;

mod resources;
pub use resources::{
    OWNED_PREPARATION_COMPONENT, PreparationResourceIds, PreparationResourceLink,
    PreparationResourceOutcome, PreparationResourceReceipt,
};

#[path = "preparation/resources/proof.rs"]
pub(crate) mod resource_proof;
#[path = "preparation/resources/source.rs"]
pub(crate) mod resource_source;
