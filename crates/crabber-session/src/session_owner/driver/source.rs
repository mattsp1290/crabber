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
    init_digest: InputFingerprint,
    admission_handoff: InputFingerprint,
    config_hash: InputFingerprint,
    plan_fingerprint: InputFingerprint,
    request_digest: InputFingerprint,
}
impl DriverLink {
    pub(crate) fn initial(
        request: &RegisterExecutionDriver,
        session: &Session,
        run: &Run,
        issuance: &RunIssuance,
        witness: &crate::session_owner::preparation::admission::Witness,
        sources: &crate::session_owner::preparation::admission::SourceAudits<'_>,
        init: &crate::admission_journal::Init,
    ) -> Result<Self, Error> {
        let request_digest = request.fingerprint()?;
        bounded(session, 16384)?;
        bounded(run, 32768)?;
        bounded(issuance, 2048)?;
        bounded(witness, 8192)?;
        bounded(init, crate::admission_journal::MAX_BYTES)?;
        for source in [
            sources.reserve,
            sources.begin,
            sources.join,
            sources.child_reserve,
            sources.child_begin,
        ] {
            bounded(source, 32768)?;
        }
        issuance
            .verify(run)
            .map_err(|_| Error::UnknownStoreOutcome)?;
        witness.verify_sources(session, issuance, sources)?;
        let declared = sources
            .reserve
            .record
            .preparation
            .as_ref()
            .and_then(|origin| origin.resource_contract.as_ref());
        if declared.is_none()
            || sources.reserve.record.snapshot.binding.component_id
                != crate::session_owner::preparation::OWNED_PREPARATION_COMPONENT
        {
            return Err(Error::Unsupported);
        }
        if issuance.sequence != 1
            || issuance.version != 2
            || run.status != crabber_core::RunStatus::Running
            || run.id != request.fence.run_id
            || run.claim_token != request.fence.claim_token
            || run.session_id != *request.context.session_id()
            || session.id != run.session_id
            || session.workspace_id != request.context.workspace_id()
            || session.directory != request.context.directory()
            || run.checkpoint.is_some()
            || run.error.is_some()
        {
            return Err(Error::StaleOwner);
        }
        let source = Self {
            version: 1,
            session_id: session.id.clone(),
            run_id: run.id.clone(),
            init_digest: init.digest().map_err(|_| Error::UnknownStoreOutcome)?,
            issuer_digest: crate::native_parent::issuance_digest(issuance)
                .map_err(|_| Error::UnknownStoreOutcome)?,
            admission_handoff: issuance
                .ownership_handoff
                .clone()
                .ok_or(Error::Unsupported)?,
            config_hash: InputFingerprint::new(run.config_hash.clone())
                .map_err(|_| Error::UnknownStoreOutcome)?,
            plan_fingerprint: InputFingerprint::new(run.plan_fingerprint.clone())
                .map_err(|_| Error::UnknownStoreOutcome)?,
            request_digest,
        };
        source.validate()?;
        Ok(source)
    }
    pub(crate) fn validate(&self) -> Result<(), Error> {
        crate::session_owner::identifier(&self.session_id.0, 512, false)?;
        crate::session_owner::identifier(&self.run_id.0, 128, false)?;
        if self.version != 1 {
            return Err(Error::UnknownStoreOutcome);
        }
        bounded(self, 2048)
    }

    pub(crate) fn verify_original(
        &self,
        session: &Session,
        issuer: &RunIssuance,
        witness: &crate::session_owner::preparation::admission::Witness,
        sources: &crate::session_owner::preparation::admission::SourceAudits<'_>,
        init: &crate::admission_journal::Init,
    ) -> Result<(), Error> {
        self.validate()?;
        bounded(init, crate::admission_journal::MAX_BYTES)?;
        witness.verify_sources(session, issuer, sources)?;
        if init.digest().map_err(|_| Error::UnknownStoreOutcome)? != self.init_digest
            || issuer.sequence != 1
            || issuer.version != 2
            || issuer.state.session_id != self.session_id
            || issuer.state.run_id != self.run_id
            || issuer.ownership_handoff.as_ref() != Some(&self.admission_handoff)
            || crate::native_parent::issuance_digest(issuer)
                .map_err(|_| Error::UnknownStoreOutcome)?
                != self.issuer_digest
        {
            return Err(Error::UnknownStoreOutcome);
        }
        Ok(())
    }
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
