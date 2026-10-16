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
