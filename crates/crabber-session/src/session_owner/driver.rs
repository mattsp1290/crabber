//! Actual outer worker ownership, separate from terminal Run publications.
//! Native sources cover a genuine first prepared keyed admission with its
//! protected execution journal, or an explicitly anchored unkeyed admission. It
//! describes main-task/resource joins, never complete provider/service coverage.
#[cfg(test)]
pub(crate) mod history_fixture;
mod source;
pub(crate) mod successor;
use super::{SessionOwnerError as Error, identifier};
use crabber_core::{
    InputFingerprint, RunId, SessionContext, SessionId, SessionOwnerId, SessionOwnerOperationId,
};
use serde::{Deserialize, Serialize};
pub(crate) use source::DriverLink;
pub(crate) use successor::SuccessorDriverLink;
pub use successor::{
    NativeClaimLocator, RegisterSuccessorExecutionDriver, SuccessorExecutionDriverGrant,
    SuccessorExecutionDriverOutcome, SuccessorExecutionDriverReceipt,
};

pub const RUNTIME_DRIVER_COMPONENT: &str = "crabber.runtime.outer-worker.v1";
/// Pure capability selection. A supported origin never supplies authority.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SuccessorExecutionDriverOrigin {
    AdmissionRecovery,
    ExpiredRun,
    ApprovalClaim,
    ApprovalRecovery,
    ToolClaim,
    ToolRecovery,
    FrontendClaim,
    FrontendRecovery,
}
/// Fixed main-task and sealed-cleanup declaration. This marker proves no join.
/// # Errors
/// Returns a sanitized serialization error.
pub fn resource_contract() -> Result<InputFingerprint, Error> {
    super::digest(&(
        RUNTIME_DRIVER_COMPONENT,
        "actual-main-and-sealed-registered-cleanup",
        1u32,
    ))
}

/// The private original Run fence is authority, not replay metadata.
#[derive(Clone)]
pub struct RegisterExecutionDriver {
    pub context: SessionContext,
    pub fence: crabber_core::RunFence,
}
impl std::fmt::Debug for RegisterExecutionDriver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("RegisterExecutionDriver([redacted])")
    }
}
impl RegisterExecutionDriver {
    /// Stable first-driver identities exclude a replacement fence or choice.
    /// # Errors
    /// Refuses malformed routing/fences before cloning supplied values.
    pub fn identities(&self) -> Result<ExecutionDriverIds, Error> {
        identifier(&self.context.session_id().0, 512, false)?;
        identifier(&self.fence.run_id.0, 128, false)?;
        identifier(&self.fence.claim_token, 512, false)?;
        if self.context.workspace_id().len() > 512
            || self.context.directory().len() > 16384
            || self.context.workspace_id().contains('\0')
            || self.context.directory().contains('\0')
        {
            return Err(Error::InvalidRequest);
        }
        let hash = super::digest(&(
            "crabber.execution-driver.initial.identity.v1",
            self.context.session_id(),
            &self.fence.run_id,
        ))?;
        Ok(ExecutionDriverIds {
            owner: SessionOwnerId(format!("driver-owner-{}", hash.as_str())),
            reserve: SessionOwnerOperationId(format!("driver-reserve-{}", hash.as_str())),
            begin: SessionOwnerOperationId(format!("driver-begin-{}", hash.as_str())),
            join: SessionOwnerOperationId(format!("driver-join-{}", hash.as_str())),
        })
    }
    /// Original retry semantics include the exact fence and immutable context.
    /// # Errors
    /// Refuses malformed routing/fences before hashing any carrier.
    pub fn fingerprint(&self) -> Result<InputFingerprint, Error> {
        self.identities()?;
        super::digest(&(
            "crabber.execution-driver.initial.request.v1",
            self.context.session_id(),
            self.context.workspace_id(),
            self.context.directory(),
            &self.fence.run_id,
            &self.fence.claim_token,
        ))
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionDriverIds {
    pub owner: SessionOwnerId,
    pub reserve: SessionOwnerOperationId,
    pub begin: SessionOwnerOperationId,
    pub join: SessionOwnerOperationId,
}

/// Original metadata only; replay never supplies a dispatch authority.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionDriverReceipt {
    pub session_id: SessionId,
    pub run_id: RunId,
    pub request_digest: InputFingerprint,
    pub reserve: super::SessionOwnerReceipt,
    pub begin: super::SessionOwnerReceipt,
}
/// First acknowledgement only. The source remains private to native adapters.
#[derive(Clone)]
pub struct ExecutionDriverGrant {
    receipt: ExecutionDriverReceipt,
    authority: super::SessionOwnerAuthority,
    source_identity: InputFingerprint,
    request: RegisterExecutionDriver,
}
impl std::fmt::Debug for ExecutionDriverGrant {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ExecutionDriverGrant([redacted])")
    }
}
impl ExecutionDriverGrant {
    /// Trusted adapter bridge after a freshly acknowledged native Reserve/Begin.
    /// The fingerprint is metadata: native activation independently authenticates
    /// the actual source and authority. Receipt replay must never mint a grant.
    /// # Errors
    /// Refuses mismatched operation identities, context, or authority.
    pub fn from_fresh_begin(
        receipt: ExecutionDriverReceipt,
        authority: super::SessionOwnerAuthority,
        request: RegisterExecutionDriver,
        source_identity: InputFingerprint,
    ) -> Result<Self, Error> {
        let ids = request.identities()?;
        if receipt.request_digest != request.fingerprint()?
            || receipt.run_id != request.fence.run_id
            || receipt.session_id != *request.context.session_id()
            || receipt.reserve.operation_id != ids.reserve
            || receipt.begin.operation_id != ids.begin
            || authority.target() != &receipt.begin.owner.target
            || authority.session_id() != request.context.session_id()
        {
            return Err(Error::UnknownStoreOutcome);
        }
        let result = Self {
            receipt,
            authority,
            source_identity,
            request,
        };
        result.scope_source()?;
        Ok(result)
    }
    /// Original bounded metadata, independently rechecked by native activation.
    #[must_use]
    pub fn source_identity(&self) -> &InputFingerprint {
        &self.source_identity
    }
    /// Exact original request held by this freshly granted capability.
    #[must_use]
    pub fn original_request(&self) -> &RegisterExecutionDriver {
        &self.request
    }

    #[must_use]
    pub fn receipt(&self) -> &ExecutionDriverReceipt {
        &self.receipt
    }
    #[must_use]
    pub fn authority(&self) -> &super::SessionOwnerAuthority {
        &self.authority
    }
    /// Register this exact original main task before requesting activation.
    /// # Errors
    /// Refuses malformed or mismatched original Reserve/Begin metadata.
    pub fn scope_source(&self) -> Result<super::owned_scope::OwnedScopeSource, Error> {
        super::owned_scope::OwnedScopeSource::from_receipts(
            &self.receipt.reserve,
            &self.receipt.begin,
        )
    }
}
#[derive(Debug, Clone)]
pub enum ExecutionDriverOutcome {
    Applied {
        receipt: ExecutionDriverReceipt,
        grant: Box<ExecutionDriverGrant>,
    },
    Replayed(ExecutionDriverReceipt),
}
