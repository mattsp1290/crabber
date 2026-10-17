//! Immutable association to one actual native claim. No current state or grant.
#[path = "source/approval.rs"]
pub(crate) mod approval;
#[path = "source/claim.rs"]
pub(crate) mod claim;
#[path = "source/input.rs"]
pub(crate) mod input;
#[path = "source/tool.rs"]
pub(crate) mod tool;
use super::{Error, RegisterSuccessorExecutionDriver};
pub(crate) use claim::AuthenticatedClaim;
use crabber_core::{InputFingerprint, RunId, SessionId};
use serde::{Deserialize, Serialize};

pub(crate) struct SelectedClaim {
    pub link: SuccessorDriverLink,
    pub revision: crabber_core::SessionRevision,
    pub child_begin: crate::session_owner::audit::Audit,
    pub head: u64,
    pub head_digest: InputFingerprint,
    pub approval: Option<(crate::ContinuationTarget, crate::AttemptTarget)>,
    pub approval_receipt: Option<crate::ApprovalReceipt>,
    pub tool: Option<(crate::ContinuationTarget, crate::AttemptTarget)>,
    pub tool_receipt: Option<crate::ToolContinuationReceipt>,
    pub tool_payload_digest: Option<InputFingerprint>,
}
pub(crate) struct ContinuationFacts {
    pub revision: crabber_core::SessionRevision,
    pub head: u64,
    pub head_digest: InputFingerprint,
    pub continuation: crate::ContinuationTarget,
    pub attempt: crate::AttemptTarget,
}
pub(crate) fn selected(
    request: &RegisterSuccessorExecutionDriver,
    previous: &crate::native_parent::RunIssuance,
    next: &crate::native_parent::RunIssuance,
    audits: &[crate::session_owner::audit::Audit],
    native: claim::Native<'_>,
    binding: Option<&crate::ExecutionBindingCapsule>,
) -> Result<SelectedClaim, Error> {
    if let claim::Native::Approval { input, .. } | claim::Native::Tool { input, .. } = &native {
        input.verify(&next.state.session_id, &next.state.run_id, binding)?;
    }
    let approval_receipt = match &native {
        claim::Native::Approval { evidence, .. } => Some(evidence.receipt.clone()),
        _ => None,
    };
    let tool_receipt = match &native {
        claim::Native::Tool { evidence, .. } => Some(evidence.receipt.clone()),
        _ => None,
    };
    let verified = AuthenticatedClaim::verify(previous, next, audits, native)?;
    if let Some(binding) = binding {
        verified.verify_binding(binding)?;
    }
    Ok(SelectedClaim {
        link: SuccessorDriverLink::original(request, &verified)?,
        revision: verified.revision,
        child_begin: audits[4].clone(),
        head: verified.head,
        head_digest: verified.head_digest,
        approval: verified.approval,
        approval_receipt,
        tool: verified.tool,
        tool_receipt,
        tool_payload_digest: None,
    })
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SuccessorDriverLink {
    version: u8,
    session_id: SessionId,
    run_id: RunId,
    claim: super::NativeClaimLocator,
    issuer_digest: InputFingerprint,
    join_digest: InputFingerprint,
    binding_behavior: InputFingerprint,
    native_intent: InputFingerprint,
    resource_contract: InputFingerprint,
    request_digest: InputFingerprint,
}
impl SuccessorDriverLink {
    pub(crate) fn original(
        request: &RegisterSuccessorExecutionDriver,
        source: &AuthenticatedClaim<'_>,
    ) -> Result<Self, Error> {
        source.matches_request(request)?;
        let origin = source.audits[0]
            .record
            .preparation
            .as_ref()
            .ok_or(Error::UnknownStoreOutcome)?;
        let declaration = origin.resource_contract.clone().ok_or(Error::Unsupported)?;
        let binding = &source.audits[0].record.snapshot.binding;
        let result = Self {
            version: 1,
            session_id: request.context.session_id().clone(),
            run_id: request.fence.run_id.clone(),
            claim: request.claim.clone(),
            issuer_digest: crate::native_parent::issuance_digest(source.next).map_err(unknown)?,
            join_digest: source.audits[2].receipt.semantic_digest.clone(),
            binding_behavior: binding.behavior_fingerprint.clone(),
            native_intent: source
                .next
                .completion_source
                .as_ref()
                .ok_or(Error::Unsupported)?
                .native_semantic_digest
                .clone(),
            resource_contract: declaration,
            request_digest: request.fingerprint()?,
        };
        result.validate()?;
        Ok(result)
    }
    pub(crate) fn validate(&self) -> Result<(), Error> {
        crate::session_owner::identifier(&self.session_id.0, 512, false)?;
        crate::attempt::validate_identifier(&self.run_id.0).map_err(unknown)?;
        self.claim.validate()?;
        if self.version != 1 || crate::snapshot::encoded_bytes(self, 4096).is_none() {
            return Err(Error::UnknownStoreOutcome);
        }
        Ok(())
    }
    pub(crate) fn run_id(&self) -> &RunId {
        &self.run_id
    }
    pub(crate) fn matches_request(
        &self,
        request: &RegisterSuccessorExecutionDriver,
    ) -> Result<(), Error> {
        self.validate()?;
        if self.session_id != *request.context.session_id()
            || self.run_id != request.fence.run_id
            || self.claim != request.claim
            || self.request_digest != request.fingerprint()?
        {
            return Err(Error::SemanticConflict);
        }
        Ok(())
    }
    pub(crate) fn identity(&self) -> Result<InputFingerprint, Error> {
        self.validate()?;
        crate::session_owner::digest(&("crabber.execution-driver.successor.source.v1", self))
    }
    pub(crate) fn reservation(
        &self,
        request: &RegisterSuccessorExecutionDriver,
    ) -> Result<crate::session_owner::ReserveSessionOwner, Error> {
        self.matches_request(request)?;
        let ids = request.identities()?;
        Ok(crate::session_owner::ReserveSessionOwner {
            id: ids.reserve,
            binding: crate::session_owner::SessionOwnerBinding {
                session_id: self.session_id.clone(),
                workspace_id: request.context.workspace_id().into(),
                directory: request.context.directory().into(),
                id: ids.owner,
                kind: crate::session_owner::SessionOwnerKind::RetainedCapacity,
                component_id: super::super::RUNTIME_DRIVER_COMPONENT.into(),
                behavior_fingerprint: crate::session_owner::digest(&(
                    "crabber.execution-driver.successor.binding.v1",
                    &self.binding_behavior,
                    &self.native_intent,
                ))?,
                resource_contract: Some(super::super::resource_contract()?),
                parent: Some(crate::session_owner::SessionOwnerParent {
                    run_id: self.run_id.clone(),
                    call_id: None,
                    phase: None,
                }),
            },
            owner: super::super::RUNTIME_DRIVER_COMPONENT.into(),
            lease_ms: 30000,
        })
    }
    pub(crate) fn fingerprint(
        &self,
        request: &crate::session_owner::ReserveSessionOwner,
    ) -> Result<InputFingerprint, Error> {
        self.validate()?;
        let identity = crate::session_owner::digest(&(
            "crabber.execution-driver.successor.identity.v1",
            &self.session_id,
            &self.run_id,
            &self.claim,
        ))?;
        let binding = &request.binding;
        if binding.session_id != self.session_id
            || binding.id.0 != format!("successor-owner-{}", identity.as_str())
            || request.id.0 != format!("successor-reserve-{}", identity.as_str())
            || binding.kind != crate::session_owner::SessionOwnerKind::RetainedCapacity
            || binding.component_id != super::super::RUNTIME_DRIVER_COMPONENT
            || binding.resource_contract.as_ref() != Some(&super::super::resource_contract()?)
            || binding.behavior_fingerprint
                != crate::session_owner::digest(&(
                    "crabber.execution-driver.successor.binding.v1",
                    &self.binding_behavior,
                    &self.native_intent,
                ))?
            || binding.parent.as_ref().is_none_or(|parent| {
                parent.run_id != self.run_id || parent.call_id.is_some() || parent.phase.is_some()
            })
        {
            return Err(Error::UnknownStoreOutcome);
        }
        crate::session_owner::digest(&(
            "crabber.execution-driver.successor.reserve.v1",
            self,
            binding,
            &request.owner,
        ))
    }
}
fn unknown(_: impl std::fmt::Debug) -> Error {
    Error::UnknownStoreOutcome
}

#[path = "source/scope.rs"]
pub(crate) mod scope;
