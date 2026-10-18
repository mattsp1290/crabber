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
        owner: &super::tool_phase::OwnedPhase,
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
        let call_for_progress = call_id.clone();
        let progress_live = Arc::new(AtomicBool::new(true));
        let _progress_lifetime = ProgressLifetime(progress_live.clone());
        let progress_open = progress_live;

        let progress = Arc::new(move |content: ContentBlock| {
            if !matches!(&content, ContentBlock::Text { .. })
                || crate::public_source::encoded_len(&content, crabber_core::MAX_PUBLIC_STATE_BYTES)
                    .is_err()
            {
                return;
            }
            if !progress_open.load(Ordering::Acquire) {
                return;
            }

            let event = EventRecord {
                cursor: None,
                session_id: session_for_progress.clone(),
                run_id: run_for_progress.clone(),
                turn_id: None,
                kind: EventKind::ToolProgress,
                payload: json!({"version":1,"call_id":call_for_progress,"content":content}),
                correlation: Some(call_for_progress.0.clone()),
                live_only: true,
                created_at: clock.now(),
            };
            observer.emit(&event);
        });
        let phase_source = crate::public_source::bind(
            self.store.clone(),
            session_id.clone(),
            crabber_session::PublicSourceAuthority::Phase(owner.authority.clone()),
            self.source_delivery.clone(),
            Some(call_id.clone()),
        );
        let context = ToolContext::new(
            session_id.clone(),
            run_id.clone(),
            call_id.clone(),
            cancellation.clone(),
            self.host_services.with_overrides(&plan.host_services),
            workspace,
            progress,
        );
        let context = context
            .with_public_source(phase_source.clone())
            .with_resources(resources.clone())
            .with_session_invocations(
                self.session_invocations.bind(
                    self.store.clone(),
                    crate::session_invocations::Parent {
                        session: session_id.clone(),
                        run: run_id.clone(),
                        workspace: session
                            .workspace
                            .workspace_id()
                            .unwrap_or_default()
                            .to_owned(),
                        directory: session.workspace.directory().unwrap_or_default().to_owned(),
                        phase: owner.target.clone(),
                        authority:
                            crabber_session::session_owner::SessionOwnerParentAuthority::Phase(
                                owner.authority.clone(),
                            ),
                        invocation: None,
                    },
                ),
            );
        let executor = Arc::clone(&tool.executor);
        let resumed = self
            .tool_resumes
            .lock()
            .expect("tool resumes poisoned")
            .get(call_id)
            .cloned();
        let identity = frozen_identity(plan, &tool.info.name)?;
        if executor.resumable_identity() != identity
            || executor
                .resumable()
                .map(crabber_extension::ResumableTool::identity)
                != identity
        {
            return Err(crabber_session::ToolContinuationError::SchemaMismatch.into());
        }
        if let Some(identity) = &identity {
            identity.validate()?;
        }
        if resumed.as_ref().is_some_and(|saved| {
            identity.as_ref() != Some(&saved.identity) || !saved.identity.accepts(&saved.checkpoint)
        }) {
            return Err(crabber_session::ToolContinuationError::SchemaMismatch.into());
        }
        let suspended = Arc::new(std::sync::Mutex::new(None));
        let terminal_suspended = suspended.clone();
        let marker = json!({"crabber_native_suspension":crabber_core::ControlId::new()});
        let terminal_marker = marker.clone();
        let authorized_arguments = arguments.clone();
        let declared_error = Arc::new(AtomicBool::new(false));
        let terminal_error = declared_error.clone();
        let terminal: Callback = Arc::new(move |input| {
            let executor = Arc::clone(&executor);
            let context = context.clone();
            let authorized_arguments = authorized_arguments.clone();
            let terminal_error = terminal_error.clone();
            let suspended = terminal_suspended.clone();
            let marker = terminal_marker.clone();
            let resumed = resumed.clone();
            Box::pin(async move {
                if input != authorized_arguments {
                    return Err(crabber_extension::ExtensionError::Tool(
                        "around handler changed immutable tool input".into(),
                    ));
                }
                let outcome = if let Some(tool) = executor.resumable() {
                    if let Some(saved) = resumed {
                        tool.resume(
                            context,
                            authorized_arguments,
                            saved.checkpoint,
                            saved.responses,
                        )
                        .await?
                    } else {
                        tool.start(context, authorized_arguments).await?
                    }
                } else {
                    crabber_core::ToolPhaseOutcome::Completed(
                        executor
                            .execute_with_context(context, authorized_arguments)
                            .await?,
                    )
                };
                let output = match outcome {
                    crabber_core::ToolPhaseOutcome::Completed(output) => output,
                    crabber_core::ToolPhaseOutcome::Suspended(proposal) => {
                        *suspended.lock().expect("tool suspension poisoned") = Some(proposal);
                        return Ok(marker);
                    }
                };
                if output.is_error() {
                    terminal_error.store(true, Ordering::SeqCst);
                }
                Ok(crabber_extension::tool_output_envelope(output))
            })
        });
        let execution_result = tokio::select! {
            biased;
            () = cancellation.cancelled() => return Err(RuntimeError::Interrupted),
            result = crabber_extension::with_public_source(phase_source,plan.dispatcher.around::<ToolExecute>(arguments, terminal)) => result,
        };
        if let Some(proposal) = suspended.lock().expect("tool suspension poisoned").take() {
            return Ok(
                if execution_result
                    .as_ref()
                    .is_ok_and(|value| value == &marker)
                    && identity
                        .as_ref()
                        .is_some_and(|identity| identity.accepts(proposal.checkpoint()))
                {
                    ToolRun::Suspended(proposal)
                } else {
                    ToolRun::SuspensionInvalid
                },
            );
        }
        let output = match execution_result {
            Ok(output) => output,
            Err(error) => return Ok(ToolRun::ExecutionFailed(error.to_string())),
        };
        let output = match crabber_extension::parse_tool_output_envelope(output) {
            Ok(output) if declared_error.load(Ordering::SeqCst) => output.with_error(),
            Ok(output) => output,
            Err(error) => return Ok(ToolRun::ExecutionFailed(error.to_string())),
        };
        self.protect_tool_output(
            &tool.info,
            output,
            pipeline_context,
            cancellation,
            lease_lost,
        )
        .await
    }
    pub(super) async fn protect_tool_output(
        &self,
        info: &crabber_core::ToolInfo,
        output: crabber_core::ToolOutput,
        pipeline_context: &ToolResultContext,
        cancellation: &CancellationToken,
        lease_lost: &AtomicBool,
    ) -> Result<ToolRun, RuntimeError> {
        let output_is_error = output.is_error();
        let pipeline_context = ToolResultContext::for_completion(pipeline_context.clone(), &output);
        ensure_lease(lease_lost)?;
        let protection = std::panic::AssertUnwindSafe(async {
            self.tool_pipeline
                .transform_result(&pipeline_context, info, output)
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

pub(super) fn frozen_identity(
    plan: &RunPlan,
    name: &str,
) -> Result<Option<crabber_core::ResumableToolIdentity>, RuntimeError> {
    let id = format!("resumable:{name}");
    let mut found = plan
        .components
        .iter()
        .filter(|component| component.id == id);
    let identity = found
        .next()
        .map(|component| {
            serde_json::from_str(&component.version).map_err(|_| {
                RuntimeError::from(crabber_session::ToolContinuationError::SchemaMismatch)
            })
        })
        .transpose()?;
    if found.next().is_some() {
        return Err(crabber_session::ToolContinuationError::SchemaMismatch.into());
    }
    Ok(identity)
}

struct ProgressLifetime(Arc<AtomicBool>);
impl Drop for ProgressLifetime {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}
