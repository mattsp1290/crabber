//! Owned Approval sources enter their actual parked main before native completion.
use super::super::{
    Continuation, RunSession,
    continuation::{ApprovalControl, ApprovalResume},
    preparation::pending::{PendingOutcome, main::Metadata},
};
use super::{Orchestrator, Request, RuntimeError};
use crabber_core::Run;
use crabber_session::{
    ApprovalClaimOutcome, ExecutionBindingCapsule,
    session_owner::{
        driver::SuccessorExecutionDriverOrigin as Origin,
        owned_scope::PendingSuccessorReplay,
        preparation::{
            InspectPendingApprovalPreparation, PendingApprovalSource, PreparationCompletion,
        },
    },
};
impl Orchestrator {
    pub(in crate::orchestrator) async fn spawn_pending_approval(
        &self,
        run: Run,
        validated: (RunSession, Option<Request>),
        binding: ExecutionBindingCapsule,
        control: ApprovalControl,
    ) -> Result<Continuation, RuntimeError> {
        let source = source(&control)?;
        let origin = match source {
            PendingApprovalSource::Claim(_) => Origin::ApprovalClaim,
            PendingApprovalSource::Recovery(_) => Origin::ApprovalRecovery,
        };
        if !self.store.supports_pending_execution_driver_origin(origin)
            || !self.store.supports_owned_attempts()
            || !self.store.supports_owned_tool_phases()
            || !self.store.supports_pre_effect_approvals()
        {
            return Err(crabber_session::StoreError::HostUnsupported.into());
        }
        let context = crabber_core::SessionContext::new(
            run.session_id.clone(),
            binding.request.workspace_id.clone(),
            binding.request.directory.clone(),
        );
        let mut request = Request::from_binding(&binding);
        self.verify_binding_request(&request, &run.session_id, &binding)?;
        self.verify_configuration_semantics(
            &binding.runtime_semantics,
            binding.plan_fingerprint.as_str(),
            &context,
        )?;
        self.verify_declared_provider(&binding)?;
        let preflight = InspectPendingApprovalPreparation {
            context: context.clone(),
            source,
        };
        let metadata = self
            .store
            .inspect_pending_approval_preparation(preflight.clone())
            .await?;
        preflight.verify_metadata(&metadata)?;
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
                let PreparationCompletion::Approval(ApprovalClaimOutcome::Applied {
                    receipt,
                    grant,
                }) = completed
                else {
                    return Err(RuntimeError::PreparationRecoveryRequired);
                };
                let authority = super::super::continuation::prepared_approval_authority(
                    &run, &receipt, *grant,
                )?;
                let prepared = super::expired_resume::ExpiredResume {
                    plan,
                    bound,
                    authority,
                };
                // Preserve the Approval discriminator. The prepared authority
                // skips native acquisition; this sender is never an ACK source.
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
}
fn source(control: &ApprovalControl) -> Result<PendingApprovalSource, RuntimeError> {
    match control {
        ApprovalControl::Claim(claim) => Ok(PendingApprovalSource::Claim(Box::new(claim.clone()))),
        ApprovalControl::Recover(recovery) => {
            Ok(PendingApprovalSource::Recovery(Box::new(recovery.clone())))
        }
        ApprovalControl::Tool { .. } => Err(RuntimeError::PreparationRecoveryRequired),
    }
}
fn receipt(
    metadata: Metadata,
    origin: Origin,
) -> Result<crabber_session::ApprovalReceipt, RuntimeError> {
    match (origin, metadata) {
        (
            Origin::ApprovalClaim,
            Metadata::Successor(PendingSuccessorReplay::ApprovalClaim(receipt)),
        )
        | (
            Origin::ApprovalRecovery,
            Metadata::Successor(PendingSuccessorReplay::ApprovalRecovery(receipt)),
        ) => Ok(*receipt),
        _ => Err(RuntimeError::PreparationRecoveryRequired),
    }
}
fn map(outcome: PendingOutcome, origin: Origin) -> Result<Continuation, RuntimeError> {
    match outcome {
        PendingOutcome::Started { handle, metadata } => Ok(Continuation::Started {
            receipt: receipt(metadata, origin)?,
            handle,
        }),
        PendingOutcome::Replayed(metadata) => {
            Ok(Continuation::Replayed(receipt(metadata, origin)?))
        }
    }
}
