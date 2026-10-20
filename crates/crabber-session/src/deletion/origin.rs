//! Explicit creation origin for the ownership protocol. Existing sessions never
//! gain coverage through rename, defaults, archive or an empty owner roster.
use crabber_core::{ControlId, InputFingerprint, SessionId, SessionRevision};
use serde::{Deserialize, Serialize};

pub(crate) const MAX_ORIGIN_BYTES: usize = 8192;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum OriginKind {
    ManagedCreate,
    Fork,
    PreparedAdmission,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct OwnershipOrigin {
    pub format_version: u8,
    pub session_id: SessionId,
    pub created_revision: SessionRevision,
    pub operation_id: ControlId,
    pub semantic_digest: InputFingerprint,
    pub kind: OriginKind,
}

impl OwnershipOrigin {
    #[cfg(any(feature = "postgres", feature = "sqlite"))]
    pub(crate) fn encoded(&self) -> Result<String, crate::StoreError> {
        self.validate(&self.session_id, self.created_revision)
            .map_err(|_| crate::StoreError::Validation("invalid ownership origin".into()))?;
        serde_json::to_string(self)
            .map_err(|_| crate::StoreError::Validation("ownership origin encoding failed".into()))
    }
    pub(crate) fn managed(receipt: &crate::ManagementReceipt) -> Self {
        Self {
            format_version: 1,
            session_id: receipt.session_id.clone(),
            created_revision: receipt.revision,
            operation_id: receipt.operation_id.clone(),
            semantic_digest: receipt.semantic_digest.clone(),
            kind: OriginKind::ManagedCreate,
        }
    }
    pub(crate) fn fork(receipt: &crate::ForkReceipt) -> Self {
        Self {
            format_version: 1,
            session_id: receipt.target.session_id.clone(),
            created_revision: receipt.target.revision,
            operation_id: receipt.operation_id.clone(),
            semantic_digest: receipt.semantic_digest.clone(),
            kind: OriginKind::Fork,
        }
    }
    pub(crate) fn prepared(
        witness: &crate::session_owner::preparation::admission::Witness,
    ) -> Result<Self, crate::session_owner::SessionOwnerError> {
        Ok(Self {
            format_version: 1,
            session_id: witness.session_id().clone(),
            created_revision: witness.admitted_revision(),
            operation_id: ControlId(witness.run_id().0.clone()),
            semantic_digest: witness.fingerprint()?,
            kind: OriginKind::PreparedAdmission,
        })
    }
    pub(crate) fn validate(
        &self,
        session: &SessionId,
        revision: SessionRevision,
    ) -> Result<(), crate::DeletionError> {
        crate::management::validate_scope(&self.session_id, &self.operation_id)
            .map_err(|_| crate::DeletionError::UnknownStoreOutcome)?;
        if self.format_version != 1
            || self.session_id != *session
            || self.created_revision == SessionRevision::INITIAL
            || (self.kind != OriginKind::PreparedAdmission && self.created_revision.get() != 1)
            || revision < self.created_revision
            || crate::snapshot::encoded_bytes(self, MAX_ORIGIN_BYTES).is_none()
        {
            return Err(crate::DeletionError::UnknownStoreOutcome);
        }
        Ok(())
    }
}
