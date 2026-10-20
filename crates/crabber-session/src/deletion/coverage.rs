//! Opaque scope for positive ownership coverage. It is separate from native
//! cleanup/readiness and never follows from an empty owner roster.
use crate::deletion::origin::{OriginKind, OwnershipOrigin};
use crate::{DeletionError, ForkReceipt, ManagementReceipt};
use crabber_core::{SessionId, SessionRevision};

pub(crate) const MAX_PROOF_BYTES: usize = 32 * 1024 * 1024;

#[cfg(any(feature = "postgres", feature = "sqlite"))]
#[path = "coverage/sql.rs"]
pub(crate) mod sql;

pub(crate) enum CoverageRead {
    Unknown,
    Limited,
    Verified(VerifiedDeletionCoverage),
}

pub(crate) struct VerifiedDeletionCoverage {
    session_id: SessionId,
    revision: SessionRevision,
    proof_bytes: usize,
}

impl VerifiedDeletionCoverage {
    pub(crate) fn session_id(&self) -> &SessionId {
        &self.session_id
    }
    pub(crate) fn revision(&self) -> SessionRevision {
        self.revision
    }
    /// Already charged original coverage carriers. Native readiness must debit
    /// these from the same whole-operation proof allowance before more reads.
    pub(crate) fn proof_bytes(&self) -> usize {
        self.proof_bytes
    }
    /// Only isolated storage-purge tests can assert this fixture scope. There
    /// is no production constructor from caller claims or public summaries.
    #[cfg(test)]
    pub(crate) fn fixture(session_id: SessionId, revision: SessionRevision) -> Self {
        Self {
            session_id,
            revision,
            proof_bytes: 0,
        }
    }
}

pub(crate) fn managed(
    origin: &OwnershipOrigin,
    session: &crabber_core::Session,
    revision: SessionRevision,
    audit: &crate::management::Audit,
    receipt: &ManagementReceipt,
    proof_bytes: usize,
) -> Result<VerifiedDeletionCoverage, DeletionError> {
    origin.validate(&session.id, revision)?;
    crate::management::verify_audit(audit, receipt)
        .map_err(|_| DeletionError::UnknownStoreOutcome)?;
    if origin.kind != OriginKind::ManagedCreate
        || !matches!(audit.request.change, crate::SessionChange::Create { .. })
        || receipt.session_id != session.id
        || receipt.operation_id != origin.operation_id
        || receipt.semantic_digest != origin.semantic_digest
        || receipt.revision != origin.created_revision
        || !same_session(session, &receipt.summary)
        || proof_bytes > MAX_PROOF_BYTES
    {
        return Err(DeletionError::UnknownStoreOutcome);
    }
    Ok(VerifiedDeletionCoverage {
        session_id: session.id.clone(),
        revision,
        proof_bytes,
    })
}

pub(crate) fn fork(
    origin: &OwnershipOrigin,
    session: &crabber_core::Session,
    revision: SessionRevision,
    audit: &crate::fork::Audit,
    receipt: &ForkReceipt,
    proof_bytes: usize,
) -> Result<VerifiedDeletionCoverage, DeletionError> {
    origin.validate(&session.id, revision)?;
    audit
        .validate()
        .map_err(|_| DeletionError::UnknownStoreOutcome)?;
    if origin.kind != OriginKind::Fork
        || audit.receipt != *receipt
        || receipt.target.session_id != session.id
        || receipt.operation_id != origin.operation_id
        || receipt.semantic_digest != origin.semantic_digest
        || receipt.target.revision != origin.created_revision
        || !same_session(session, &receipt.target)
        || proof_bytes > MAX_PROOF_BYTES
    {
        return Err(DeletionError::UnknownStoreOutcome);
    }
    Ok(VerifiedDeletionCoverage {
        session_id: session.id.clone(),
        revision,
        proof_bytes,
    })
}

fn same_session(session: &crabber_core::Session, original: &crate::SessionSummary) -> bool {
    session.id == original.session_id
        && session.workspace_id == original.workspace_id
        && session.directory == original.directory
        && session.created_at == original.created_at
}

/// Creation association only; live resources and every retained Run still need
/// independent readiness proofs before physical deletion is possible.
pub(crate) fn prepared(
    origin: &OwnershipOrigin,
    session: &crabber_core::Session,
    revision: SessionRevision,
    witness: &crate::session_owner::preparation::admission::Witness,
    initial: &crate::native_parent::RunIssuance,
    sources: &crate::session_owner::preparation::admission::SourceAudits<'_>,
    proof_bytes: usize,
) -> Result<VerifiedDeletionCoverage, DeletionError> {
    origin.validate(&session.id, revision)?;
    witness
        .verify_sources(session, initial, sources)
        .map_err(|_| DeletionError::UnknownStoreOutcome)?;
    if origin.kind != OriginKind::PreparedAdmission
        || origin.operation_id.0 != witness.run_id().0
        || origin.semantic_digest
            != witness
                .fingerprint()
                .map_err(|_| DeletionError::UnknownStoreOutcome)?
        || origin.created_revision != witness.admitted_revision()
        || proof_bytes > MAX_PROOF_BYTES
    {
        return Err(DeletionError::UnknownStoreOutcome);
    }
    Ok(VerifiedDeletionCoverage {
        session_id: session.id.clone(),
        revision,
        proof_bytes,
    })
}
