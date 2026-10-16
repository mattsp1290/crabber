//! An immutable initial issuer association; no caller constructor is exported.
use super::{Error, RegisterExecutionDriver};
use crate::native_parent::RunIssuance;
use crabber_core::{InputFingerprint, Run, RunId, Session, SessionId};
use serde::{Deserialize, Serialize};

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DriverLink {
    version: u8,
    session_id: SessionId,
    run_id: RunId,
    issuer_digest: InputFingerprint,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    init_digest: Option<InputFingerprint>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    unkeyed_input: Option<InputFingerprint>,
    admission_handoff: InputFingerprint,
    config_hash: InputFingerprint,
    plan_fingerprint: InputFingerprint,
    request_digest: InputFingerprint,
}
#[path = "source/origin.rs"]
mod origin;
impl DriverLink {
    pub(crate) fn reservation(
        &self,
        request: &RegisterExecutionDriver,
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
                component_id: super::RUNTIME_DRIVER_COMPONENT.into(),
                behavior_fingerprint: crate::session_owner::digest(&(
                    "crabber.execution-driver.binding.v1",
                    &self.config_hash,
                    &self.plan_fingerprint,
                ))?,
                resource_contract: Some(super::resource_contract()?),
                parent: Some(crate::session_owner::SessionOwnerParent {
                    run_id: self.run_id.clone(),
                    call_id: None,
                    phase: None,
                }),
            },
            owner: super::RUNTIME_DRIVER_COMPONENT.into(),
            lease_ms: 30000,
        })
    }
    pub(crate) fn fingerprint(
        &self,
        request: &crate::session_owner::ReserveSessionOwner,
    ) -> Result<InputFingerprint, Error> {
        self.validate()?;
        let binding = &request.binding;
        let identity = crate::session_owner::digest(&(
            "crabber.execution-driver.initial.identity.v1",
            &self.session_id,
            &self.run_id,
        ))?;
        if binding.session_id != self.session_id
            || binding.id.0 != format!("driver-owner-{}", identity.as_str())
            || request.id.0 != format!("driver-reserve-{}", identity.as_str())
            || binding.kind != crate::session_owner::SessionOwnerKind::RetainedCapacity
            || binding.component_id != super::RUNTIME_DRIVER_COMPONENT
            || binding.resource_contract.as_ref() != Some(&super::resource_contract()?)
            || binding.behavior_fingerprint
                != crate::session_owner::digest(&(
                    "crabber.execution-driver.binding.v1",
                    &self.config_hash,
                    &self.plan_fingerprint,
                ))?
            || binding.parent.as_ref().is_none_or(|parent| {
                parent.run_id != self.run_id || parent.call_id.is_some() || parent.phase.is_some()
            })
        {
            return Err(Error::UnknownStoreOutcome);
        }
        crate::session_owner::digest(&(
            "crabber.execution-driver.initial.reserve.v1",
            self,
            binding,
            &request.owner,
        ))
    }
    pub(crate) fn identity(&self) -> Result<InputFingerprint, Error> {
        self.validate()?;
        crate::session_owner::digest(&("crabber.execution-driver.source-identity.v1", self))
    }
    pub(crate) fn run_id(&self) -> &RunId {
        &self.run_id
    }
    pub(crate) fn matches_request(&self, request: &RegisterExecutionDriver) -> Result<(), Error> {
        self.validate()?;
        if self.request_digest != request.fingerprint()?
            || self.session_id != *request.context.session_id()
            || self.run_id != request.fence.run_id
        {
            return Err(Error::SemanticConflict);
        }
        Ok(())
    }
}
fn bounded(value: &impl Serialize, cap: usize) -> Result<(), Error> {
    crate::snapshot::encoded_bytes(value, cap)
        .map(|_| ())
        .ok_or(Error::UnknownStoreOutcome)
}
