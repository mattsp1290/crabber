//! A coherent current status and fence journal cannot rewrite original Finish.
use super::*;
use crate::attempt_contract::conclusion::{started, terminal};
use crabber_core::{ManualClock, RunStatus};
#[tokio::test]
async fn terminal_source_rejects_coherent_current_status_forgery() {
    let clock = std::sync::Arc::new(ManualClock::new(time::OffsetDateTime::UNIX_EPOCH));
    let store = MemoryStore::with_clock(clock.clone());
    let (admitted, execution, owner, conclusion) =
        started(&store, &clock, terminal(RunStatus::Completed)).await;
    let AttemptMutationOutcome::Applied { receipt, .. } = execution
        .conclude_attempt(conclusion, Some(owner))
        .await
        .unwrap()
    else {
        panic!("actual conclusion")
    };
    let mut state = store.state.lock().unwrap();
    verify(&state, &receipt).unwrap();
    state.runs.get_mut(&admitted.run.id).unwrap().status = RunStatus::Failed;
    state
        .native_run_issuances
        .iter_mut()
        .rev()
        .find(|((_, run, _), _)| run == &admitted.run.id)
        .unwrap()
        .1
        .state
        .status = RunStatus::Failed;
    assert_eq!(
        verify(&state, &receipt).unwrap_err(),
        Error::UnknownStoreOutcome
    );
}

#[tokio::test]
async fn host_retire_terminal_source_rejects_private_result_forgery() {
    let clock = std::sync::Arc::new(ManualClock::new(time::OffsetDateTime::UNIX_EPOCH));
    let store = MemoryStore::with_clock(clock);
    for outcome in [
        crate::AttemptRetirementOutcome::Failed,
        crate::AttemptRetirementOutcome::Interrupted,
    ] {
        let fixture = crate::attempt_control_contract::terminal::fixture(&store, outcome).await;
        let mut state = store.state.lock().unwrap();
        verify(&state, &fixture.receipt).unwrap();
        state.runs.get_mut(&fixture.admitted.run.id).unwrap().error = Some("forged error".into());
        assert_eq!(
            verify(&state, &fixture.receipt).unwrap_err(),
            Error::UnknownStoreOutcome
        );
        let run = state.runs.get_mut(&fixture.admitted.run.id).unwrap();
        run.error = Some(crate::attempt_control::terminal::ERROR.into());
        run.usage.output_tokens += 1;
        assert_eq!(
            verify(&state, &fixture.receipt).unwrap_err(),
            Error::UnknownStoreOutcome
        );
        let run = state.runs.get_mut(&fixture.admitted.run.id).unwrap();
        run.usage.output_tokens -= 1;
        run.status = if run.status == RunStatus::Failed {
            RunStatus::Interrupted
        } else {
            RunStatus::Failed
        };
        let status = run.status;
        state
            .native_run_issuances
            .iter_mut()
            .rev()
            .find(|((_, run, _), _)| run == &fixture.admitted.run.id)
            .unwrap()
            .1
            .state
            .status = status;
        assert_eq!(
            verify(&state, &fixture.receipt).unwrap_err(),
            Error::UnknownStoreOutcome
        );
    }
}
