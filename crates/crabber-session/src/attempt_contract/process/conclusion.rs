use super::Fixture;
use crate::{AttemptCleanup, AttemptConclusion, AttemptState, RunBoundary, Store};
use crabber_core::{EventKind, RunStatus, Usage};
use serde_json::json;
pub(super) fn request(fixture: &Fixture) -> AttemptConclusion {
    let boundary = if matches!(fixture.mode, super::Mode::Complete) {
        RunBoundary::Terminal {
            status: RunStatus::Completed,
            error: None,
            usage: Usage {
                input_tokens: 11,
                output_tokens: 3,
            },
        }
    } else {
        RunBoundary::Suspended {
            checkpoint: json!({"private":"PROCESS_CHECKPOINT"}),
            usage: Usage {
                input_tokens: 11,
                output_tokens: 3,
            },
        }
    };
    AttemptConclusion {
        finish: fixture.request.clone(),
        boundary,
        created_at: time::OffsetDateTime::UNIX_EPOCH,
        turn_id: None,
        elapsed: std::time::Duration::from_millis(1),
    }
}
pub(super) async fn verify_loss(store: &dyn Store, fixture: Fixture) {
    let conclusion = request(&fixture);
    let receipt = store
        .lookup_attempt_mutation(&fixture.session, &fixture.request.id)
        .await
        .unwrap()
        .unwrap();
    conclusion.verify_replay(&receipt).unwrap();
    assert_eq!(receipt.attempt.cleanup, AttemptCleanup::Complete);
    let current = store
        .execution_attempt(&fixture.fence.run_id, &receipt.attempt.target.id)
        .await
        .unwrap();
    assert_eq!(current.attempt, receipt.attempt);
    assert_eq!(current.revision, receipt.revision);
    let run = store.get_run(&fixture.fence.run_id).await.unwrap().unwrap();
    match &conclusion.boundary {
        RunBoundary::Terminal { status, usage, .. } => {
            assert_eq!(&run.status, status);
            assert_eq!(&run.usage, usage);
            assert_eq!(receipt.attempt.state, AttemptState::Completed);
        }
        RunBoundary::Suspended { checkpoint, usage } => {
            assert_eq!(run.status, RunStatus::Paused);
            assert_eq!(run.checkpoint.as_ref(), Some(checkpoint));
            assert_eq!(&run.usage, usage);
            assert_eq!(receipt.attempt.state, AttemptState::Suspended);
        }
    }
    // A fresh process cannot recover a worker capability from this receipt.
    assert!(store.execution(fixture.fence.clone()).await.is_err());
    let events = store
        .list_events(&fixture.session, None, 100)
        .await
        .unwrap();
    assert_eq!(events.len(), 4);
    assert!(
        events
            .iter()
            .any(|event| matches!(event.kind, EventKind::RunSettled | EventKind::RunPaused))
    );
    assert!(
        !serde_json::to_string(&events)
            .unwrap()
            .contains("PROCESS_CHECKPOINT")
    );
    assert_eq!(
        store.session_revision(&fixture.session).await.unwrap(),
        receipt.revision
    );
}
