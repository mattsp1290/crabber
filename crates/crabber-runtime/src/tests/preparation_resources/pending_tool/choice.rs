use super::*;
#[derive(Clone)]
pub(super) enum Choice {
    Tool(crate::ContinueTool),
    Frontend(crate::ContinueFrontend),
    RecoverTool(crate::RecoverTool),
    RecoverFrontend(crate::RecoverFrontend),
}
impl Choice {
    pub(super) fn original(&self) -> &crate::ContinueTool {
        match self {
            Self::Tool(v) => v,
            Self::Frontend(v) => &v.continuation,
            Self::RecoverTool(v) => &v.original,
            Self::RecoverFrontend(v) => &v.original.continuation,
        }
    }
    pub(super) fn id(&self) -> &crabber_core::ControlId {
        match self {
            Self::RecoverTool(v) => &v.id,
            Self::RecoverFrontend(v) => &v.id,
            _ => &self.original().id,
        }
    }
    pub(super) fn changed(&self) -> Self {
        let mut changed = self.clone();
        match &mut changed {
            Self::Tool(v) => v.expected_revision = v.expected_revision.next().unwrap(),
            Self::Frontend(v) => v.resolver.authority_claim.push_str(" changed"),
            Self::RecoverTool(v) => v.expected_revision = v.expected_revision.next().unwrap(),
            Self::RecoverFrontend(v) => v.original.resolver.authority_claim.push_str(" changed"),
        }
        changed
    }
    pub(super) async fn invoke(
        &self,
        runtime: &Orchestrator,
    ) -> Result<crate::ToolContinuation, RuntimeError> {
        match self {
            Self::Tool(v) => Box::pin(runtime.continue_tool(v.clone())).await,
            Self::Frontend(v) => Box::pin(runtime.continue_frontend(v.clone())).await,
            Self::RecoverTool(v) => Box::pin(runtime.recover_tool_continuation(v.clone())).await,
            Self::RecoverFrontend(v) => {
                Box::pin(runtime.recover_frontend_continuation(v.clone())).await
            }
        }
    }
    pub(super) async fn recovery(&self, h: &Harness) -> Self {
        let id = crabber_core::ControlId::new();
        let expected_revision = h
            .memory
            .session_revision(&self.original().session_id)
            .await
            .unwrap();
        match self {
            Self::Tool(v) => Self::RecoverTool(crate::RecoverTool {
                id,
                expected_revision,
                original: v.clone(),
            }),
            Self::Frontend(v) => Self::RecoverFrontend(crate::RecoverFrontend {
                id,
                expected_revision,
                original: v.clone(),
            }),
            _ => panic!("recover the original claim"),
        }
    }
    pub(super) async fn descriptor(&self, h: &Harness) -> PreparationReconciliation {
        h.runtime
            .lookup_preparation_operation(
                &self.original().session_id,
                &PreparationKey::ToolContinuation(self.id().clone()),
            )
            .await
            .unwrap()
            .unwrap()
    }
}
