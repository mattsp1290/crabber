//! Fresh continuation factories join before the exact native claim transaction.
use super::super::continuation::{ApprovalControl, ApprovalResume};
use super::super::tool_continuation::ToolResumeControl;
use super::{Orchestrator, PreparationSpec, Request, RuntimeError};
use crabber_core::Run;
use crabber_extension::RunPlan;
use crabber_providers::BoundProvider;
use crabber_session::session_owner::preparation::{
    ExistingWorkTarget, PreparationKey, PreparationPurpose, PreparationSource, PreparedOperation,
};
use crabber_session::{ExecutionBindingCapsule, StoreError};
fn operation(
    control: &ApprovalControl,
) -> Result<(PreparationSpec, PreparedOperation), RuntimeError> {
    let (source, key, run, work, revision, intent, operation) = match control {
        ApprovalControl::Tool {
            control: ToolResumeControl::FrontendClaim(frontend),
            ..
        } => (
            PreparationSource::FrontendClaim(frontend.clone()),
            PreparationKey::ToolContinuation(frontend.claim.id.clone()),
            frontend.claim.run_id.clone(),
            ExistingWorkTarget::FrontendClaim(frontend.claim.continuation.clone()),
            frontend.claim.expected_revision,
            frontend.fingerprint()?,
            PreparedOperation::FrontendClaim(frontend.clone()),
        ),
        ApprovalControl::Tool {
            control: ToolResumeControl::FrontendRecover(recovery),
            ..
        } => (
            PreparationSource::FrontendRecovery(recovery.clone()),
            PreparationKey::ToolContinuation(recovery.id.clone()),
            recovery.original.claim.run_id.clone(),
            ExistingWorkTarget::FrontendRecovery {
                continuation: recovery.original.claim.continuation.clone(),
                attempt: recovery.attempt.clone(),
            },
            recovery.expected_revision,
            recovery.fingerprint()?,
            PreparedOperation::FrontendRecovery(recovery.clone()),
        ),
        ApprovalControl::Claim(claim) => (
            PreparationSource::ApprovalClaim(claim.clone()),
            PreparationKey::Approval(claim.id.clone()),
            claim.run_id.clone(),
            ExistingWorkTarget::ApprovalClaim(claim.continuation.clone()),
            claim.expected_revision,
            claim.fingerprint()?,
            PreparedOperation::ApprovalClaim(claim.clone()),
        ),
        ApprovalControl::Tool {
            control: ToolResumeControl::Claim(claim),
            ..
        } => (
            PreparationSource::ToolClaim(claim.clone()),
            PreparationKey::ToolContinuation(claim.id.clone()),
            claim.run_id.clone(),
            ExistingWorkTarget::ToolClaim(claim.continuation.clone()),
            claim.expected_revision,
            claim.fingerprint()?,
            PreparedOperation::ToolClaim(claim.clone()),
        ),
        ApprovalControl::Recover(recovery) => (
            PreparationSource::ApprovalRecovery(recovery.clone()),
            PreparationKey::Approval(recovery.id.clone()),
            recovery.original.run_id.clone(),
            ExistingWorkTarget::ApprovalRecovery {
                continuation: recovery.original.continuation.clone(),
                attempt: recovery.attempt.clone(),
            },
            recovery.expected_revision,
            recovery.fingerprint()?,
            PreparedOperation::ApprovalRecovery(recovery.clone()),
        ),
        ApprovalControl::Tool {
            control: ToolResumeControl::Recover(recovery),
            ..
        } => (
            PreparationSource::ToolRecovery(recovery.clone()),
            PreparationKey::ToolContinuation(recovery.id.clone()),
            recovery.original.run_id.clone(),
            ExistingWorkTarget::ToolRecovery {
                continuation: recovery.original.continuation.clone(),
                attempt: recovery.attempt.clone(),
            },
            recovery.expected_revision,
            recovery.fingerprint()?,
            PreparedOperation::ToolRecovery(recovery.clone()),
        ),
    };
    Ok((
        PreparationSpec {
            purpose: PreparationPurpose::ExistingRun { run_id: run, work },
            key,
            revision: Some(revision),
            options: None,
            intent: Some(intent),
            source: Some(source),
        },
        operation,
    ))
}
impl Orchestrator {
    pub(in crate::orchestrator) async fn prepare_resume_resources(
        &self,
        run: &Run,
        binding: &ExecutionBindingCapsule,
        approval: &mut Option<ApprovalResume>,
    ) -> Result<(RunPlan, BoundProvider), RuntimeError> {
        let native = approval
            .as_ref()
            .map(|control| operation(&control.control))
            .transpose()?;
        let Some((spec, operation)) = native else {
            return self.restore_resume_resources(run, binding).await;
        };
        let mut request = Request::from_binding(binding);
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
        self.reject_pending_preparation(&run.session_id, &spec.key)
            .await?;
        let grant = self
            .begin_control_preparation(&mut request, &run.session_id, spec)
            .await?;
        let runtime = self.clone();
        let original_run = run.clone();
        let original_binding = binding.clone();
        let (resources, completed) = self
            .finish_durable_preparation(grant, move |scope| async move {
                let runtime = runtime.with_preparation_scope(scope);
                let retained = runtime
                    .restore_resume_resources(&original_run, &original_binding)
                    .await?;
                Ok((retained, operation))
            })
            .await?;
        approval
            .as_mut()
            .ok_or(StoreError::AdmissionConflict)?
            .prepared = Some(completed);
        Ok(resources)
    }
    async fn restore_resume_resources(
        &self,
        run: &Run,
        binding: &ExecutionBindingCapsule,
    ) -> Result<(RunPlan, BoundProvider), RuntimeError> {
        let original = Request::from_binding(binding);
        let (plan, prepared, _) = self.prepare_admission(&original).await?;
        if prepared.binding.provider != binding.provider {
            return Err(crabber_providers::ProviderBindingError::VersionConflict.into());
        }
        if prepared.binding != *binding || plan.fingerprint().to_string() != run.plan_fingerprint {
            plan.release();
            return Err(RuntimeError::PlanChanged);
        }
        let bound = self.restore_provider(&plan, &binding.provider).await?;
        Ok((plan, bound))
    }
}
