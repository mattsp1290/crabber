//! Frontend declarations enter model metadata and immutable plan identity only.
//! They never create a `ToolDefinition` or backend executor/permission capability.
use super::RuntimeError;
use crabber_core::{FrontendTool, ToolInfo};
use crabber_extension::{ComponentIdentity, RunPlan, compute_fingerprint};
use crabber_session::{ToolContinuationError, frontend_descriptor_fingerprint};
use std::collections::BTreeSet;
const COMPONENT: &str = "contract:crabber/frontend-tools/v1";
pub(super) fn bind(mut plan: RunPlan, tools: &[FrontendTool]) -> Result<RunPlan, RuntimeError> {
    if plan
        .components
        .iter()
        .any(|component| component.id == COMPONENT)
    {
        return Err(ToolContinuationError::SemanticConflict.into());
    }
    if tools.is_empty() {
        return Ok(plan);
    }
    validate(&plan, tools)?;
    let mut value =
        serde_json::to_value(tools).map_err(|_| ToolContinuationError::InvalidRequest)?;
    value.sort_all_objects();
    plan.components.push(ComponentIdentity {
        id: COMPONENT.into(),
        version: value.to_string(),
    });
    plan.fingerprint = compute_fingerprint(&plan.components);
    Ok(plan)
}
fn validate(plan: &RunPlan, tools: &[FrontendTool]) -> Result<(), RuntimeError> {
    crabber_session::validate_frontend_declarations(tools)?;
    let mut names = plan
        .tools
        .iter()
        .map(|tool| tool.info.name.as_str())
        .collect::<BTreeSet<_>>();
    for tool in tools {
        if !names.insert(&tool.name) {
            return Err(crabber_extension::ExtensionError::ToolCollision(tool.name.clone()).into());
        }
    }
    Ok(())
}
pub(super) fn declarations(plan: &RunPlan) -> Result<Vec<FrontendTool>, RuntimeError> {
    let mut components = plan
        .components
        .iter()
        .filter(|component| component.id == COMPONENT);
    let Some(component) = components.next() else {
        return Ok(Vec::new());
    };
    if components.next().is_some() || component.version.len() > 4 * 1024 * 1024 {
        return Err(ToolContinuationError::SemanticConflict.into());
    }
    let tools: Vec<FrontendTool> = serde_json::from_str(&component.version)
        .map_err(|_| ToolContinuationError::SemanticConflict)?;
    validate(plan, &tools)?;
    Ok(tools)
}
pub(super) fn model_tools(plan: &RunPlan) -> Result<Vec<ToolInfo>, RuntimeError> {
    let mut tools = plan
        .tools
        .iter()
        .map(|tool| tool.info.clone())
        .collect::<Vec<_>>();
    tools.extend(declarations(plan)?.into_iter().map(|tool| ToolInfo {
        name: tool.name,
        description: tool.description,
        parameters: tool.input_parameters,
        retry_safe: false,
        required_permissions: Vec::new(),
    }));
    Ok(tools)
}
impl super::Orchestrator {
    pub(super) async fn freeze_frontend_call(
        &self,
        scope: &super::tool_batch::Batch<'_>,
        call: &crabber_core::ToolCallRecord,
        target: crabber_session::PhaseTarget,
    ) -> Result<crabber_session::FrozenFrontendCall, RuntimeError> {
        let origin = self
            .store
            .frontend_call_origin(&scope.session.id, scope.run, &call.id)
            .await?
            .ok_or(ToolContinuationError::UnknownStoreOutcome)?;
        Ok(crabber_session::FrozenFrontendCall {
            target,
            call_binding: origin.call_binding,
            descriptor: origin.descriptor,
            descriptor_fingerprint: origin.descriptor_fingerprint,
            arguments: call.arguments.clone(),
        })
    }
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn stage_frontend(
        &self,
        execution: &dyn super::ExecutionStore,
        session: &crabber_core::SessionId,
        run: &crabber_core::RunId,
        plan: &RunPlan,
        call: super::PendingCall,
        descriptor: FrontendTool,
        lease_lost: &super::AtomicBool,
    ) -> Result<crabber_core::ToolCallRecord, RuntimeError> {
        super::ensure_lease(lease_lost)?;
        let parent = self
            .owned_attempt
            .as_ref()
            .ok_or(crabber_session::AttemptError::InvalidTransition)?;
        let record = crabber_core::ToolCallRecord {
            id: call.id,
            run_id: run.clone(),
            name: call.name,
            arguments: call
                .arguments
                .unwrap_or(serde_json::Value::String(call.raw)),
            status: crabber_core::ToolCallStatus::Pending,
            retry_safe: false,
            result: None,
        };
        let mut request = crabber_session::FrontendCallStaging {
            id: crabber_core::ControlId::new(),
            expected_revision: crabber_core::SessionRevision::INITIAL,
            session_id: session.clone(),
            run_id: run.clone(),
            parent: parent.target.clone(),
            call: record,
            descriptor_fingerprint: frontend_descriptor_fingerprint(&descriptor)?,
            descriptor,
            message_id: call
                .message_id
                .clone()
                .ok_or(ToolContinuationError::InvalidRequest)?,
            source_turn_id: call.turn_id.clone(),
            created_at: self.clock.now(),
        };
        // Provider arguments are owned here. Validate the original before a
        // retry or Store call can duplicate an oversized identity or payload.
        request.fingerprint()?;
        for _ in 0..128 {
            request.id = crabber_core::ControlId::new();
            request.expected_revision = self.store.session_revision(session).await?;
            let digest = request.fingerprint()?;
            let event = request.materialize()?;
            match execution
                .stage_frontend_call(request.clone(), parent.authority.clone())
                .await
            {
                Ok(receipt)
                    if receipt.operation_id == request.id
                        && receipt.semantic_digest == digest
                        && receipt.session_id == *session
                        && receipt.run_id == *run
                        && receipt.call_id == request.call.id =>
                {
                    self.observer.emit(&event);
                    plan.dispatcher
                        .notify::<crabber_extension::EventPublished>(
                            serde_json::to_value(&event).unwrap_or(serde_json::Value::Null),
                        )
                        .await;
                    return Ok(request.call);
                }
                Err(ToolContinuationError::RevisionConflict) => (),
                // A lost staging acknowledgement may have inserted the logical
                // call. Preserve its original key and do not redispatch anything.
                Ok(_) | Err(ToolContinuationError::UnknownStoreOutcome) => {
                    return Err(RuntimeError::ToolContinuationBoundaryUnknown {
                        run_id: run.clone(),
                        operation_id: request.id,
                        reason: ToolContinuationError::UnknownStoreOutcome,
                    });
                }
                Err(error) => return Err(error.into()),
            }
        }
        Err(ToolContinuationError::RevisionConflict.into())
    }
}
