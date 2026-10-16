//! Exact preparation Join associated with an actual noninitial native claim.
//! This source is structural evidence, never a dispatch or cleanup grant.
use super::Error;
use crabber_core::{InputFingerprint, SessionOwnerOperationId};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CompletionOperation {
    AdmissionRecovery,
    ExpiredRunResume,
    ApprovalClaim,
    ApprovalRecovery,
    ToolClaim,
    ToolRecovery,
    FrontendClaim,
    FrontendRecovery,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CompletionSource {
    pub root_join_id: SessionOwnerOperationId,
    pub root_join_digest: InputFingerprint,
    pub native_operation: CompletionOperation,
    pub native_semantic_digest: InputFingerprint,
}
impl CompletionSource {
    pub(crate) fn validate(&self) -> Result<(), Error> {
        crate::attempt::validate_identifier(&self.root_join_id.0).map_err(|_| Error)?;
        for digest in [&self.root_join_digest, &self.native_semantic_digest] {
            InputFingerprint::new(digest.as_str().to_owned()).map_err(|_| Error)?;
        }
        if crate::snapshot::encoded_bytes(self, 1024).is_none() {
            return Err(Error);
        }
        Ok(())
    }
}

impl CompletionSource {
    pub(crate) fn from_preparation(
        proof: &crate::session_owner::preparation::proof::VerifiedPreparation<'_>,
        native_operation: CompletionOperation,
        native_semantic_digest: InputFingerprint,
    ) -> Result<Self, crate::session_owner::SessionOwnerError> {
        use crate::session_owner::preparation::proof::{join_fingerprint, join_request};
        if proof.grant.original_reservation().intent != native_semantic_digest {
            return Err(crate::session_owner::SessionOwnerError::SemanticConflict);
        }
        let join = join_request(proof.grant)?;
        Ok(Self {
            root_join_id: join.id.clone(),
            root_join_digest: join_fingerprint(proof, &join)?,
            native_operation,
            native_semantic_digest,
        })
    }
}

impl CompletionSource {
    pub(crate) fn accepts_status(&self, status: crabber_core::RunStatus) -> bool {
        match self.native_operation {
            CompletionOperation::ExpiredRunResume => matches!(
                status,
                crabber_core::RunStatus::Running | crabber_core::RunStatus::Paused
            ),
            CompletionOperation::ApprovalClaim => status == crabber_core::RunStatus::Paused,
            _ => status == crabber_core::RunStatus::Running,
        }
    }
}

impl CompletionSource {
    pub(crate) fn optional(
        preparation: Option<&crate::session_owner::preparation::proof::VerifiedPreparation<'_>>,
        operation: CompletionOperation,
        digest: &InputFingerprint,
    ) -> Result<Option<Self>, crate::session_owner::SessionOwnerError> {
        preparation
            .map(|proof| Self::from_preparation(proof, operation, digest.clone()))
            .transpose()
    }
}

impl CompletionSource {
    pub(crate) fn control(
        preparation: Option<&crate::session_owner::preparation::proof::VerifiedPreparation<'_>>,
        operation: CompletionOperation,
        key: &crate::session_owner::preparation::PreparationKey,
        scope: (&crabber_core::SessionId, &crabber_core::RunId),
        revision: crabber_core::SessionRevision,
        digest: &InputFingerprint,
    ) -> Result<
        (crabber_core::SessionRevision, Option<Self>),
        crate::session_owner::SessionOwnerError,
    > {
        let revision = preparation.map_or(Ok(revision), |proof| {
            proof.control_revision(key, scope.0, scope.1, revision)
        })?;
        Ok((revision, Self::optional(preparation, operation, digest)?))
    }
}
