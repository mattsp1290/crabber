//! Generic keyed recovery owns preparation before any plan/provider restoration.
use super::super::{Admission, InitialEntry, Orchestrator, Request, RuntimeError, admission};
use super::PreparationSpec;
use crabber_core::{AdmissionOptions, TraceContext};
use crabber_session::session_owner::preparation::{
    AdmissionRecoveryOutcome, AdmissionRecoveryPreparation, ExistingWorkTarget,
    PreparationCompletion, PreparationKey, PreparationPurpose, PreparationSource,
    PreparedAdmissionRecovery, PreparedOperation,
};
use crabber_session::{ClaimUnstartedAdmissionRequest, StoreError};
use sha2::{Digest, Sha256};
use std::time::Duration;

impl Orchestrator {
    pub(in crate::orchestrator) async fn recover_with_preparation(
        &self,
        request: Request,
        options: AdmissionOptions,
        context: Option<TraceContext>,
        retained: admission::Retained,
    ) -> Result<Admission, RuntimeError> {
        self.verify_declared_provider(&retained.binding)?;
        self.require_successor_driver(&retained.binding)?;
        let source = self.inspect_initial_recovery(&retained).await?;
        let session = source.request().session_id.clone();
        let key = PreparationKey::AdmissionRecovery {
            key: source.request().key.clone(),
            issuance: source.expected_issuance(),
        };
        self.reject_pending_preparation(&session, &key).await?;
        let mut captured = Self::original_default_request(&request, &retained.binding)?;
        captured
            .input_envelope
            .clone_from(&retained.binding.input_envelope);
        let spec = PreparationSpec {
            purpose: PreparationPurpose::ExistingRun {
                run_id: retained.run.id.clone(),
                work: ExistingWorkTarget::AdmissionRecovery {
                    key: source.request().key.clone(),
                    issuance: source.expected_issuance(),
                },
            },
            key,
            revision: Some(source.expected_revision()),
            options: None,
            intent: Some(source.fingerprint()?),
            source: Some(PreparationSource::AdmissionRecovery(source.clone())),
        };
        let grant = self
            .begin_control_preparation(&mut captured, &session, spec)
            .await?;
        let locator = crabber_session::session_owner::driver::NativeClaimLocator {
            issuance: source
                .expected_issuance()
                .checked_add(1)
                .ok_or(StoreError::AdmissionConflict)?,
            root_join_id: grant.grant.reconciliation()?.join_id,
        };
        let runtime = self.clone();
        let original_binding = retained.binding;
        let ((plan, bound), completed) = self
            .finish_durable_preparation(grant, move |scope| async move {
                runtime
                    .with_preparation_scope(scope)
                    .restore_initial_recovery(captured, options, original_binding, source)
                    .await
            })
            .await?;
        let PreparationCompletion::AdmissionRecovery(outcome) = completed else {
            return Err(StoreError::AdmissionConflict.into());
        };
        Ok(match outcome {
            AdmissionRecoveryOutcome::Replayed(receipt) => Admission::Replayed(receipt),
            AdmissionRecoveryOutcome::Claimed(claimed) => Admission::Started {
                receipt: claimed.record.receipt,
                handle: self.with_context(context).spawn_admitted(
                    Request::from_execution(&claimed.record.capsule),
                    plan,
                    claimed.fence,
                    &retained.session,
                    InitialEntry::RecoveredKeyed(Some(locator)),
                    Some(bound),
                ),
            },
        })
    }
    pub(in crate::orchestrator) fn require_successor_driver(
        &self,
        binding: &crabber_session::ExecutionBindingCapsule,
    ) -> Result<(), RuntimeError> {
        if crabber_session::session_owner::preparation::resource_contract(
            &binding.runtime_semantics,
        )?
        .is_some()
            && (!self.store.supports_successor_execution_drivers()
                || !self.store.supports_owned_scopes()
                || !self.store.supports_preparation_protocol())
        {
            return Err(StoreError::HostUnsupported.into());
        }
        Ok(())
    }
    async fn inspect_initial_recovery(
        &self,
        retained: &admission::Retained,
    ) -> Result<AdmissionRecoveryPreparation, RuntimeError> {
        let capsule = &retained.record.capsule;
        let identity = serde_json::to_vec(&(
            "crabber.runtime.admission-recovery-owner.v1",
            &retained.run.session_id,
            &retained.record.key,
            &retained.run.id,
            &retained.run.claim_token,
        ))
        .map_err(|_| StoreError::AdmissionConflict)?;
        let request = ClaimUnstartedAdmissionRequest {
            session_id: retained.run.session_id.clone(),
            key: retained.record.key.clone(),
            expected_fence: crabber_core::RunFence {
                run_id: retained.run.id.clone(),
                claim_token: retained.run.claim_token.clone(),
            },
            expected_owner: retained.run.owner.clone(),
            fingerprint: capsule.fingerprint.clone(),
            behavior_fingerprint: capsule.behavior_fingerprint.clone(),
            semantic_digest: capsule.semantic_digest.clone(),
            capsule_digest: capsule.digest()?,
            owner: format!("recover-{:x}", Sha256::digest(identity)),
            lease: Duration::from_secs(30),
        };
        Ok(self.store.inspect_admission_recovery(request).await?)
    }
    async fn restore_initial_recovery(
        &self,
        captured: Request,
        options: AdmissionOptions,
        original: crabber_session::ExecutionBindingCapsule,
        source: AdmissionRecoveryPreparation,
    ) -> Result<
        (
            (crabber_extension::RunPlan, crabber_providers::BoundProvider),
            PreparedOperation,
        ),
        RuntimeError,
    > {
        let (plan, mut prepared) = self.prepare_keyed_admission(&captured, options).await?;
        if prepared.binding != original {
            return Err(StoreError::AdmissionConflict.into());
        }
        // Native lease ownership was chosen before factories and is excluded
        // from the original immutable input/capsule semantics.
        prepared
            .keyed
            .request
            .owner
            .clone_from(&source.request().owner);
        prepared.keyed.request.lease = source.request().lease;
        let bound = self.restore_provider(&plan, &original.provider).await?;
        let recovery = PreparedAdmissionRecovery {
            source,
            binding: Box::new(prepared),
        };
        recovery.validate()?;
        Ok((
            (plan, bound),
            PreparedOperation::AdmissionRecovery(Box::new(recovery)),
        ))
    }
}
