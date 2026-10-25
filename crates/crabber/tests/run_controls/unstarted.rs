use super::{contract, options, provider::Gate, store::TestStore};
use crabber::{
    Admission, SessionId,
    core::RunStatus,
    session::{AdmissionExecutionState, RunControlOutcome, RunControlStatus, Store},
};
use std::{
    sync::{Arc, atomic::Ordering},
    time::Duration,
};

struct ReleaseOnDrop<'a>(&'a tokio::sync::Notify);
impl Drop for ReleaseOnDrop<'_> {
    fn drop(&mut self) {
        self.0.notify_one();
    }
}

pub(super) async fn cancel_before_first_attempt(store: Arc<dyn Store>) {
    let gate = Gate::new(1);
    let wrapped = Arc::new(TestStore::new(store.clone()));
    wrapped
        .hold_committed_admission
        .store(true, Ordering::SeqCst);
    let worker = Arc::new(contract::agent(wrapped.clone(), &gate));
    let controller = contract::agent(store.clone(), &gate);
    let session = SessionId::new();
    let started = {
        let worker = worker.clone();
        let session = session.clone();
        tokio::spawn(async move { worker.prompt_keyed(session, "original", options()).await })
    };
    let release = ReleaseOnDrop(&wrapped.release);
    tokio::time::timeout(Duration::from_secs(5), wrapped.entered.notified())
        .await
        .unwrap();
    let receipt = store
        .lookup_admission(&session, &options().key)
        .await
        .unwrap()
        .unwrap();
    assert!(
        store
            .latest_execution_attempt(&receipt.run_id)
            .await
            .unwrap()
            .is_none(),
        "the controller must cancel before the first attempt is reserved"
    );
    let execution = store
        .load_admission_execution(&session, &options().key)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(execution.state, AdmissionExecutionState::Unstarted);
    assert_eq!(gate.fake.requests().len(), 0);
    let binding = controller
        .run_control_binding(&session, &receipt.run_id)
        .await
        .unwrap();
    let request =
        contract::request(store.as_ref(), &binding.target, contract::cancel_action()).await;
    assert!(matches!(
        controller.control_run(request).await.unwrap(),
        RunControlOutcome::Applied(_)
    ));
    drop(release);
    let Admission::Started { handle, .. } = started.await.unwrap().unwrap() else {
        panic!("fresh admission")
    };
    let result = tokio::time::timeout(Duration::from_secs(5), handle.done())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result.status, RunStatus::Interrupted);
    assert!(
        gate.fake.requests().is_empty(),
        "accepted cancellation must prevent the first provider call"
    );
    let page = controller.run_controls(&binding.target).await.unwrap();
    assert_eq!(
        page.controls[0].status,
        RunControlStatus::TerminalCommitted {
            status: RunStatus::Interrupted
        }
    );
    worker.close_extensions().await.unwrap();
    controller.close_extensions().await.unwrap();
}
