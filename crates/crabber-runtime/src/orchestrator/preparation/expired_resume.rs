//! Generic owned resumption commits its exact expired source before worker dispatch.
use super::super::{
    Orchestrator, Request, RunHandle, RunSession, RuntimeError, continuation::ResumeAuthority,
    driver,
};
use super::PreparationSpec;
use crabber_core::Run;
use crabber_extension::RunPlan;
use crabber_providers::BoundProvider;
use crabber_session::{
    ExecutionBindingCapsule, StoreError,
    session_owner::{
        driver::{NativeClaimLocator, RegisterSuccessorExecutionDriver},
        preparation::{
            ExistingWorkTarget, ExpiredRunOrigin, ExpiredRunOutcome, InspectExpiredRunPreparation,
            PreparationCompletion, PreparationKey, PreparationPurpose, PreparationSource,
            PreparedExpiredRunResume, PreparedOperation,
        },
    },
};
use sha2::{Digest, Sha256};

pub(in crate::orchestrator) struct ExpiredResume {
    pub plan: RunPlan,
    pub bound: BoundProvider,
    pub authority: ResumeAuthority,
}
impl Orchestrator {
    pub(in crate::orchestrator) async fn spawn_prepared_expired(
        &self,
        run: Run,
        validated: (RunSession, Option<Request>),
        binding: ExecutionBindingCapsule,
    ) -> Result<RunHandle, RuntimeError> {
        self.require_successor_driver(&binding)?;
        if self.store.admission_execution_state(&run.id).await?
            == Some(crabber_session::AdmissionExecutionState::Unstarted)
        {
            return Err(StoreError::AdmissionRecoveryRequired.into());
        }
        let context = crabber_core::SessionContext::new(
            run.session_id.clone(),
            binding.request.workspace_id.clone(),
            binding.request.directory.clone(),
        );
        if crabber_session::session_owner::preparation::resource_contract(
            &binding.runtime_semantics,
        )?
        .is_some()
        {
            return Box::pin(self.spawn_pending_expired(run, validated, binding, context)).await;
        }
        let (prepared, claim) = Box::pin(self.prepare_expired_resources(&run, binding)).await?;
        let request = RegisterSuccessorExecutionDriver {
            context,
            fence: prepared.authority.fence.clone(),
            claim,
        };
        self.spawn_worker_inner(
            run.session_id.clone(),
            run.id.clone(),
            Some(driver::Request::Successor(request)),
            move |runtime, cancellation| async move {
                Box::pin(runtime.resume_prepared(
                    run,
                    validated,
                    cancellation,
                    prepared,
                    super::super::recovery::PreparedResumeKind::Expired,
                ))
                .await
            },
        )
    }
    async fn spawn_pending_expired(
        &self,
        run: Run,
        validated: (RunSession, Option<Request>),
        binding: ExecutionBindingCapsule,
        context: crabber_core::SessionContext,
    ) -> Result<RunHandle, RuntimeError> {
        let source = self.inspect_expired_resources(&run, &binding).await?;
        let mut request = Request::from_binding(&binding);
        let key = PreparationKey::ExpiredRun {
            run_id: run.id.clone(),
            issuance: source.expected_issuance(),
        };
        self.reject_pending_preparation(&run.session_id, &key)
            .await?;
        let spec = PreparationSpec {
            key,
            purpose: PreparationPurpose::ExistingRun {
                run_id: run.id.clone(),
                work: ExistingWorkTarget::ExpiredRun(Box::new(source.target().clone())),
            },
            revision: Some(source.expected_revision()),
            options: None,
            intent: Some(source.fingerprint()?),
            source: Some(PreparationSource::ExpiredRun(source.clone())),
        };
        let grant = self
            .begin_control_preparation(&mut request, &run.session_id, spec)
            .await?;
        let native_unstarted = !matches!(source.target().origin, ExpiredRunOrigin::Suspended(_));
        let runtime = self.clone();
        let original = run.clone();
        let outcome = Box::pin(self.finish_existing_preparation(
            grant,
            context,
            move |scope| async move {
                let value = runtime
                    .with_preparation_scope(scope)
                    .restore_resume_resources(&original, &binding)
                    .await?;
                Ok((
                    value,
                    PreparedOperation::ExpiredRunResume(Box::new(PreparedExpiredRunResume {
                        source,
                        binding: Box::new(binding),
                    })),
                ))
            },
            move |(plan, bound), runtime, cancellation, completed| async move {
                let PreparationCompletion::ExpiredRun(ExpiredRunOutcome::Claimed { fence, .. }) =
                    completed
                else {
                    return Err(RuntimeError::PreparationRecoveryRequired);
                };
                let prepared = ExpiredResume {
                    plan,
                    bound,
                    authority: ResumeAuthority {
                        fence,
                        native_unstarted,
                        continuation: None,
                        granted: None,
                        approved: None,
                        tool_phases: Vec::new(),
                        frontend_phases: Vec::new(),
                    },
                };
                Box::pin(runtime.resume_prepared(
                    run,
                    validated,
                    cancellation,
                    prepared,
                    super::super::recovery::PreparedResumeKind::Expired,
                ))
                .await
            },
        ))
        .await?;
        match outcome {
            super::pending::ExistingOutcome::Started {
                handle,
                receipt: None,
            } => Ok(handle),
            _ => Err(RuntimeError::PreparationRecoveryRequired),
        }
    }
    async fn prepare_expired_resources(
        &self,
        run: &Run,
        binding: ExecutionBindingCapsule,
    ) -> Result<(ExpiredResume, NativeClaimLocator), RuntimeError> {
        let source = self.inspect_expired_resources(run, &binding).await?;
        let mut request = Request::from_binding(&binding);
        let key = PreparationKey::ExpiredRun {
            run_id: run.id.clone(),
            issuance: source.expected_issuance(),
        };
        self.reject_pending_preparation(&run.session_id, &key)
            .await?;
        let spec = PreparationSpec {
            key,
            purpose: PreparationPurpose::ExistingRun {
                run_id: run.id.clone(),
                work: ExistingWorkTarget::ExpiredRun(Box::new(source.target().clone())),
            },
            revision: Some(source.expected_revision()),
            options: None,
            intent: Some(source.fingerprint()?),
            source: Some(PreparationSource::ExpiredRun(source.clone())),
        };
        let grant = self
            .begin_control_preparation(&mut request, &run.session_id, spec)
            .await?;
        let locator = NativeClaimLocator {
            issuance: source
                .expected_issuance()
                .checked_add(1)
                .ok_or(StoreError::Conflict)?,
            root_join_id: grant.grant.reconciliation()?.join_id,
        };
        let native_unstarted = !matches!(source.target().origin, ExpiredRunOrigin::Suspended(_));
        let runtime = self.clone();
        let original_run = run.clone();
        let ((plan, bound), completed) = self
            .finish_durable_preparation(grant, move |scope| async move {
                let retained = runtime
                    .with_preparation_scope(scope)
                    .restore_resume_resources(&original_run, &binding)
                    .await?;
                Ok((
                    retained,
                    PreparedOperation::ExpiredRunResume(Box::new(PreparedExpiredRunResume {
                        source,
                        binding: Box::new(binding),
                    })),
                ))
            })
            .await?;
        let PreparationCompletion::ExpiredRun(ExpiredRunOutcome::Claimed { receipt, fence }) =
            completed
        else {
            return Err(RuntimeError::PreparationRecoveryRequired);
        };
        if receipt.session_id != run.session_id
            || receipt.run_id != run.id
            || receipt.operation_id != locator.root_join_id
            || receipt.issuance != locator.issuance
            || fence.run_id != run.id
        {
            return Err(RuntimeError::PreparationRecoveryRequired);
        }
        Ok((
            ExpiredResume {
                plan,
                bound,
                authority: ResumeAuthority {
                    fence,
                    native_unstarted,
                    continuation: None,
                    granted: None,
                    approved: None,
                    tool_phases: Vec::new(),
                    frontend_phases: Vec::new(),
                },
            },
            locator,
        ))
    }
    async fn inspect_expired_resources(
        &self,
        run: &Run,
        binding: &ExecutionBindingCapsule,
    ) -> Result<crabber_session::session_owner::preparation::ExpiredRunPreparation, RuntimeError>
    {
        let request = Request::from_binding(binding);
        self.verify_binding_request(&request, &run.session_id, binding)?;
        let context = crabber_core::SessionContext::new(
            run.session_id.clone(),
            binding.request.workspace_id.clone(),
            binding.request.directory.clone(),
        );
        self.verify_configuration_semantics(
            &binding.runtime_semantics,
            binding.plan_fingerprint.as_str(),
            &context,
        )?;
        self.verify_declared_provider(binding)?;
        let owner = serde_json::to_vec(&(
            "crabber.runtime.expired-resume-owner.v1",
            &run.session_id,
            &run.id,
            &run.claim_token,
        ))
        .map_err(|_| StoreError::Conflict)?;
        let source = self
            .store
            .inspect_expired_run_preparation(InspectExpiredRunPreparation {
                session_id: run.session_id.clone(),
                expected_fence: crabber_core::RunFence {
                    run_id: run.id.clone(),
                    claim_token: run.claim_token.clone(),
                },
                owner: format!("resume-{:x}", Sha256::digest(owner)),
                lease: std::time::Duration::from_secs(30),
            })
            .await?;
        source.verify_binding(binding)?;
        Ok(source)
    }
}
