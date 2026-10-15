//! Invoke execution middleware with the exact previously authorized input.
use super::{
    Arc, AtomicBool, Callback, CancellationToken, ContentBlock, EventKind, EventRecord,
    ExecutionStore, Orchestrator, Ordering, RunId, RunPlan, RunSession, RuntimeError, ToolCallId,
    ToolContext, ToolDefinition, ToolExecute, ToolResultContext, ToolRun, Value, ensure_lease,
};
use futures::FutureExt;
use serde_json::json;

impl Orchestrator {
    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    pub(super) async fn permit_and_execute(
        &self,
        execution: &dyn ExecutionStore,
        session: &RunSession,
        run_id: &RunId,
        call_id: &ToolCallId,
        plan: &RunPlan,
        tool: &ToolDefinition,
        permission: crabber_session::GrantedDecision,
        arguments: Value,
        cancellation: &CancellationToken,
        lease_lost: &AtomicBool,
        pipeline_context: &ToolResultContext,
        resources: &crabber_extension::ToolResources,
    ) -> Result<ToolRun, RuntimeError> {
        let (session_id, workspace) = (&session.id, session.workspace.clone());
        if permission == crabber_session::GrantedDecision::Deny {
            return Ok(ToolRun::PermissionDenied);
        }
        ensure_lease(lease_lost)?;
        execution
            .renew_lease(self.clock.now() + time::Duration::seconds(30))
            .await
            .map_err(|_| RuntimeError::LeaseLost)?;
        ensure_lease(lease_lost)?;
        let observer = Arc::clone(&self.observer);
        let session_for_progress = session_id.clone();
        let run_for_progress = run_id.clone();
        let clock = Arc::clone(&self.clock);
        let progress = Arc::new(move |content: ContentBlock| {
            let event = EventRecord {
                cursor: None,
                session_id: session_for_progress.clone(),
                run_id: run_for_progress.clone(),
                turn_id: None,
                kind: EventKind::Custom {
                    name: "tool_progress".into(),
                },
                payload: json!({"content":content}),
                correlation: None,
                live_only: true,
                created_at: clock.now(),
            };
            observer.emit(&event);
        });
        let context = ToolContext::new(
            session_id.clone(),
            run_id.clone(),
            call_id.clone(),
            cancellation.clone(),
            self.host_services.with_overrides(&plan.host_services),
            workspace,
            progress,
        );
        let context = context.with_resources(resources.clone());
        let executor = Arc::clone(&tool.executor);
        let authorized_arguments = arguments.clone();
        let declared_error = Arc::new(AtomicBool::new(false));
        let terminal_error = declared_error.clone();
        let terminal: Callback = Arc::new(move |input| {
            let executor = Arc::clone(&executor);
            let context = context.clone();
            let authorized_arguments = authorized_arguments.clone();
            let terminal_error = terminal_error.clone();
            Box::pin(async move {
                if input != authorized_arguments {
                    return Err(crabber_extension::ExtensionError::Tool(
                        "around handler changed immutable tool input".into(),
                    ));
                }
                let output = executor
                    .execute_with_context(context, authorized_arguments)
                    .await?;
                if output.is_error() {
                    terminal_error.store(true, Ordering::SeqCst);
                }
                Ok(crabber_extension::tool_output_envelope(output))
            })
        });
        let execution_result = tokio::select! {
            biased;
            () = cancellation.cancelled() => return Err(RuntimeError::Interrupted),
            result = plan.dispatcher.around::<ToolExecute>(arguments, terminal) => result,
        };
        let output = match execution_result {
            Ok(output) => output,
            Err(error) => return Ok(ToolRun::ExecutionFailed(error.to_string())),
        };
        let output = match crabber_extension::parse_tool_output_envelope(output) {
            Ok(output) if declared_error.load(Ordering::SeqCst) => output.with_error(),
            Ok(output) => output,
            Err(error) => return Ok(ToolRun::ExecutionFailed(error.to_string())),
        };
        let output_is_error = output.is_error();
        let pipeline_context = ToolResultContext::for_completion(pipeline_context.clone(), &output);
        ensure_lease(lease_lost)?;
        let protection = std::panic::AssertUnwindSafe(async {
            self.tool_pipeline
                .transform_result(&pipeline_context, &tool.info, output)
                .await
        })
        .catch_unwind();
        let transformed = tokio::select! {
            biased;
            () = cancellation.cancelled() => return Ok(ToolRun::ProtectionInterrupted),
            result = protection => result,
        };
        Ok(match transformed {
            Ok(Ok(mut value)) => {
                value.mark_error |= output_is_error;
                ToolRun::Output(value)
            }
            Ok(Err(_)) | Err(_) => ToolRun::PipelineFailed,
        })
    }
}
