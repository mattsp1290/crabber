//! A distinct worker for one actual prepared native claim, never the initial worker.
pub(crate) mod source;
pub(crate) mod tool_fresh;
use super::{Error, ExecutionDriverIds, ExecutionDriverReceipt};
use crabber_core::{InputFingerprint, SessionOwnerId, SessionOwnerOperationId};
use serde::{Deserialize, Serialize};
pub(crate) use source::SuccessorDriverLink;

/// Bounded original-source selection metadata. This grants no native authority.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeClaimLocator {
    pub issuance: u64,
    pub root_join_id: SessionOwnerOperationId,
}
impl NativeClaimLocator {
    /// Validate bounded locator representation without authenticating a source.
    /// # Errors
    /// Rejects malformed operation IDs and absent or overflowing issuances.
    pub fn validate(&self) -> Result<(), Error> {
        super::super::identifier(&self.root_join_id.0, 128, false)?;
        if self.issuance <= 1 || self.issuance > i64::MAX as u64 {
            return Err(Error::InvalidRequest);
        }
        Ok(())
    }
}
/// The exact current native fence is private authority, never replay metadata.
#[derive(Clone)]
pub struct RegisterSuccessorExecutionDriver {
    pub context: crabber_core::SessionContext,
    pub fence: crabber_core::RunFence,
    pub claim: NativeClaimLocator,
}
impl std::fmt::Debug for RegisterSuccessorExecutionDriver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("RegisterSuccessorExecutionDriver([redacted])")
    }
}
impl RegisterSuccessorExecutionDriver {
    /// Stable identities bind the original claim, independently of a retry's fence.
    /// # Errors
    /// Rejects malformed context, native fence, or original-source identifiers.
    pub fn identities(&self) -> Result<ExecutionDriverIds, Error> {
        super::super::identifier(&self.context.session_id().0, 512, false)?;
        super::super::identifier(&self.fence.run_id.0, 128, false)?;
        super::super::identifier(&self.fence.claim_token, 512, false)?;
        if self.context.workspace_id().len() > 512
            || self.context.directory().len() > 16384
            || self.context.workspace_id().contains('\0')
            || self.context.directory().contains('\0')
        {
            return Err(Error::InvalidRequest);
        }
        self.claim.validate()?;
        let hash = super::super::digest(&(
            "crabber.execution-driver.successor.identity.v1",
            self.context.session_id(),
            &self.fence.run_id,
            &self.claim,
        ))?;
        Ok(ExecutionDriverIds {
            owner: SessionOwnerId(format!("successor-owner-{}", hash.as_str())),
            reserve: SessionOwnerOperationId(format!("successor-reserve-{}", hash.as_str())),
            begin: SessionOwnerOperationId(format!("successor-begin-{}", hash.as_str())),
            join: SessionOwnerOperationId(format!("successor-join-{}", hash.as_str())),
        })
    }
    /// Exact retry semantics preserve the original context, source and fence.
    /// # Errors
    /// Rejects malformed original request metadata before hashing.
    pub fn fingerprint(&self) -> Result<InputFingerprint, Error> {
        self.identities()?;
        super::super::digest(&(
            "crabber.execution-driver.successor.request.v1",
            self.context.session_id(),
            self.context.workspace_id(),
            self.context.directory(),
            &self.fence.run_id,
            &self.fence.claim_token,
            &self.claim,
        ))
    }
}
/// Original metadata only. Replaying this receipt never supplies a dispatch right.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SuccessorExecutionDriverReceipt {
    pub claim: NativeClaimLocator,
    pub worker: ExecutionDriverReceipt,
}
/// Supplied only by an acknowledged fresh native successor Reserve/Begin.
#[derive(Clone)]
pub struct SuccessorExecutionDriverGrant {
    receipt: SuccessorExecutionDriverReceipt,
    authority: super::super::SessionOwnerAuthority,
    request: RegisterSuccessorExecutionDriver,
    source_identity: InputFingerprint,
}
impl std::fmt::Debug for SuccessorExecutionDriverGrant {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SuccessorExecutionDriverGrant([redacted])")
    }
}
impl SuccessorExecutionDriverGrant {
    /// Trusted adapter SPI after an acknowledged fresh native Reserve/Begin.
    /// Native activation independently authenticates the original claim and source.
    /// # Errors
    /// Rejects mismatched context, source selection, receipt or authority.
    pub fn from_fresh_begin(
        receipt: SuccessorExecutionDriverReceipt,
        authority: super::super::SessionOwnerAuthority,
        request: RegisterSuccessorExecutionDriver,
        source_identity: InputFingerprint,
    ) -> Result<Self, Error> {
        let ids = request.identities()?;
        let worker = &receipt.worker;
        if receipt.claim != request.claim
            || worker.request_digest != request.fingerprint()?
            || worker.run_id != request.fence.run_id
            || worker.session_id != *request.context.session_id()
            || worker.reserve.operation_id != ids.reserve
            || worker.begin.operation_id != ids.begin
            || authority.target() != &worker.begin.owner.target
            || authority.session_id() != request.context.session_id()
        {
            return Err(Error::UnknownStoreOutcome);
        }
        let result = Self {
            receipt,
            authority,
            request,
            source_identity,
        };
        result.scope_source()?;
        Ok(result)
    }
    #[must_use]
    pub fn receipt(&self) -> &SuccessorExecutionDriverReceipt {
        &self.receipt
    }
    #[must_use]
    pub fn authority(&self) -> &super::super::SessionOwnerAuthority {
        &self.authority
    }
    #[must_use]
    pub fn original_request(&self) -> &RegisterSuccessorExecutionDriver {
        &self.request
    }
    #[must_use]
    pub fn source_identity(&self) -> &InputFingerprint {
        &self.source_identity
    }
    /// Register the actual successor task before requesting dispatch activation.
    /// # Errors
    /// Rejects inconsistent original Reserve/Begin metadata.
    pub fn scope_source(&self) -> Result<super::super::owned_scope::OwnedScopeSource, Error> {
        super::super::owned_scope::OwnedScopeSource::from_receipts(
            &self.receipt.worker.reserve,
            &self.receipt.worker.begin,
        )
    }
}
#[derive(Debug, Clone)]
pub enum SuccessorExecutionDriverOutcome {
    Applied {
        receipt: SuccessorExecutionDriverReceipt,
        grant: Box<SuccessorExecutionDriverGrant>,
    },
    Replayed(SuccessorExecutionDriverReceipt),
}
