use super::*;
use crate::{CleanupOwner, ToolInput, ToolOutcomeClass, ToolResourceOwner};
use crabber_core::{RunId, SessionId, ToolCallId, ToolOutput};
use std::time::Duration;

#[tokio::test]
async fn result_handlers_preserve_native_phase_cleanup_and_standalone_mount_cleanup() {
    for scoped in [true, false] {
        let phase = ToolResourceOwner::new();
        let mount = CleanupOwner::new();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let handlers = [TransformPhase::Ordinary, TransformPhase::FinalRedaction]
            .into_iter()
            .enumerate()
            .map(|(index, transform_phase)| {
                let seen = seen.clone();
                Handler {
                    point: ToolResultTransform::ID,
                    mode: Mode::Transform,
                    order: i32::try_from(index).unwrap(),
                    id: format!("handler-{index}"),
                    scope_rank: 0,
                    mount_seq: 0,
                    registration_seq: 0,
                    mount_id: 1,
                    frame: crate::dispatch::MountFrame::fixture(1),
                    phase: transform_phase,
                    cleanup: mount.tracker(),
                    callback: HandlerFn::ResultTransform(Arc::new(move |context, output| {
                        seen.lock()
                            .unwrap()
                            .push((context.phase(), context.cleanup().is_phase_scoped()));
                        let closing = context.cleanup().closing();
                        context.cleanup().spawn(async move {
                            closing.await;
                        });
                        Box::pin(async move { Ok(TransformOutput::new(output)) })
                    })),
                }
            })
            .collect();
        let mut context = ToolResultContext::new(
            "test".into(),
            true,
            ToolInput::Normalized(Value::Null),
            ToolCallId::new(),
            SessionId::new(),
            RunId::new(),
            ToolOutcomeClass::Succeeded,
        );
        if scoped {
            context = context.with_cleanup(phase.cleanup_tracker());
        }
        let outcome = Dispatcher::new(handlers)
            .transform_tool_result(
                context,
                TransformOutput::new(ToolOutput::from_json(&Value::Null).unwrap()),
            )
            .await;
        assert!(matches!(outcome, ToolResultOutcome::Completed { .. }));
        assert_eq!(
            *seen.lock().unwrap(),
            vec![
                (TransformPhase::Ordinary, scoped),
                (TransformPhase::FinalRedaction, scoped),
            ]
        );
        assert_eq!(phase.snapshot().pending, if scoped { 2 } else { 0 });
        assert_eq!(mount.tracker().pending(), 2);
        if scoped {
            // Mount close retains its own accounting until actual child exit,
            // even though the phase owns the retained native task join.
            assert_eq!(mount.join(Duration::ZERO).await.unwrap_err().pending, 2);
        }
        let report = phase.drain(Duration::from_secs(1)).await;
        assert_eq!(report.pending, 0);
        assert_eq!(report.joined, if scoped { 2 } else { 0 });
        assert_eq!(report.failures, [] as [crate::ResourceFailure; 0]);
        mount.join(Duration::from_secs(1)).await.unwrap();
        assert_eq!(mount.tracker().pending(), 0);
    }
}
