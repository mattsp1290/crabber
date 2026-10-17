//! Owned Tool/Frontend continuations use their one original parked main.
use super::super::{
    RunSession,
    continuation::{ApprovalControl, ApprovalResume},
    preparation::pending::{PendingOutcome, main::Metadata},
    tool_continuation::ToolResumeControl,
};
use super::{Orchestrator, Request, RuntimeError};
use crabber_core::{Run, SessionContext};
use crabber_session::{
    ExecutionBindingCapsule, ToolContinuationClaimOutcome,
    session_owner::{
        driver::SuccessorExecutionDriverOrigin as Origin,
        owned_scope::PendingSuccessorReplay,
        preparation::{InspectPendingToolPreparation, PendingToolSource, PreparationCompletion},
    },
};
impl Orchestrator {
    pub(in crate::orchestrator) async fn spawn_pending_tool(
        &self,
        run: Run,
        validated: (RunSession, Option<Request>),
        binding: ExecutionBindingCapsule,
        control: ToolResumeControl,
    ) -> Result<super::super::tool_continuation::ToolContinuation, RuntimeError> {
        let (mut request, context, origin) = self
            .checked_tool_preflight(&run, &binding, &control)
            .await?;
        let (sender, _unused) = tokio::sync::oneshot::channel();
        let control = ApprovalControl::Tool { control, sender };
        let (spec, operation) = super::resume::operation(&control)?;
        self.reject_pending_preparation(&run.session_id, &spec.key)
            .await?;
        let grant = self
            .begin_control_preparation(&mut request, &run.session_id, spec)
            .await?;
        let runtime = self.clone();
        let original = run.clone();
        let outcome = Box::pin(self.finish_pending_preparation(
            grant,
            context,
            move |scope| async move {
                let value = runtime
                    .with_preparation_scope(scope)
                    .restore_resume_resources(&original, &binding)
                    .await?;
                Ok((value, operation))
            },
            move |(plan, bound), runtime, cancellation, completed| async move {
                let PreparationCompletion::ToolContinuation(
                    ToolContinuationClaimOutcome::Applied { receipt, grant },
                ) = completed
                else {
                    return Err(RuntimeError::PreparationRecoveryRequired);
                };
                let authority = super::super::tool_continuation::prepared_tool_authority(
                    &run, &receipt, *grant,
                )?;
                let prepared = super::expired_resume::ExpiredResume {
                    plan,
                    bound,
                    authority,
                };
                // Preserve the Tool discriminator while the real fresh native
                // grant skips acquisition. This sender supplies no completion ACK.
                let (sender, _unused) = tokio::sync::oneshot::channel();
                let approval = ApprovalResume {
                    prepared: None,
                    control,
                    sender,
                };
                Box::pin(runtime.resume_loaded_inner(
                    run,
                    validated,
                    cancellation,
                    Some(approval),
                    Some(prepared),
                ))
                .await
            },
        ))
        .await?;
        map(outcome, origin)
    }
    async fn checked_tool_preflight(
        &self,
        run: &Run,
        binding: &ExecutionBindingCapsule,
        control: &ToolResumeControl,
    ) -> Result<(Request, SessionContext, Origin), RuntimeError> {
        let (source, origin) = source(control)?;
        if !self.store.supports_pending_execution_driver_origin(origin)
            || !self.store.supports_owned_attempts()
            || !self.store.supports_owned_tool_phases()
            || !self.store.supports_pre_effect_approvals()
            || !self.store.supports_tool_continuations()
            || (matches!(origin, Origin::FrontendClaim | Origin::FrontendRecovery)
                && !self.store.supports_frontend_tools())
        {
            return Err(crabber_session::StoreError::HostUnsupported.into());
        }
        let context = SessionContext::new(
            run.session_id.clone(),
            binding.request.workspace_id.clone(),
            binding.request.directory.clone(),
        );
        let request = Request::from_binding(binding);
        self.verify_binding_request(&request, &run.session_id, binding)?;
        self.verify_configuration_semantics(
            &binding.runtime_semantics,
            binding.plan_fingerprint.as_str(),
            &context,
        )?;
        self.verify_declared_provider(binding)?;
        let preflight = InspectPendingToolPreparation {
            context: context.clone(),
            source,
        };
        let metadata = self
            .store
            .inspect_pending_tool_preparation(preflight.clone())
            .await?;
        preflight.verify_metadata(&metadata)?;
        Ok((request, context, origin))
    }
}
fn source(control: &ToolResumeControl) -> Result<(PendingToolSource, Origin), RuntimeError> {
    Ok(match control {
        ToolResumeControl::Claim(v) => {
            v.fingerprint()?;
            (
                PendingToolSource::Claim(Box::new(v.clone())),
                Origin::ToolClaim,
            )
        }
        ToolResumeControl::Recover(v) => {
            v.fingerprint()?;
            (
                PendingToolSource::Recovery(Box::new(v.clone())),
                Origin::ToolRecovery,
            )
        }
        ToolResumeControl::FrontendClaim(v) => {
            v.fingerprint()?;
            (
                PendingToolSource::FrontendClaim(Box::new(v.clone())),
                Origin::FrontendClaim,
            )
        }
        ToolResumeControl::FrontendRecover(v) => {
            v.fingerprint()?;
            (
                PendingToolSource::FrontendRecovery(Box::new(v.clone())),
                Origin::FrontendRecovery,
            )
        }
    })
}
fn receipt(
    metadata: Metadata,
    origin: Origin,
) -> Result<crabber_session::ToolContinuationReceipt, RuntimeError> {
    match (origin, metadata) {
        (Origin::ToolClaim, Metadata::Successor(PendingSuccessorReplay::ToolClaim(r)))
        | (Origin::ToolRecovery, Metadata::Successor(PendingSuccessorReplay::ToolRecovery(r)))
        | (Origin::FrontendClaim, Metadata::Successor(PendingSuccessorReplay::FrontendClaim(r)))
        | (
            Origin::FrontendRecovery,
            Metadata::Successor(PendingSuccessorReplay::FrontendRecovery(r)),
        ) => Ok(*r),
        _ => Err(RuntimeError::PreparationRecoveryRequired),
    }
}
fn map(
    outcome: PendingOutcome,
    origin: Origin,
) -> Result<super::super::tool_continuation::ToolContinuation, RuntimeError> {
    use super::super::tool_continuation::ToolContinuation;
    match outcome {
        PendingOutcome::Started { handle, metadata } => Ok(ToolContinuation::Started {
            receipt: receipt(metadata, origin)?,
            handle,
        }),
        PendingOutcome::Replayed(metadata) => {
            Ok(ToolContinuation::Replayed(receipt(metadata, origin)?))
        }
    }
}
