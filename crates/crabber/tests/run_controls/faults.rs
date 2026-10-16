use super::{
    contract::{agent, cancel_action, request},
    provider::Gate,
    store::TestStore,
};
use crabber::core::{InputEnvelope, RunStatus};
use crabber::{
    RuntimeError,
    session::{RunControlAction, RunControlError, RunControlOutcome, RunControlStatus, Store},
};
use std::sync::{Arc, atomic::Ordering};
pub(super) async fn run(native: Arc<dyn Store>) {
    for fault in [1, 2] {
        terminal(native.clone(), fault).await;
    }
    for steer in [false, true] {
        acceptance(native.clone(), steer).await;
    }
}
async fn terminal(native: Arc<dyn Store>, fault: usize) {
    let wrapped = Arc::new(TestStore::new(native.clone()));
    wrapped.terminal_fault.store(fault, Ordering::SeqCst);
    let gate = Gate::new(1);
    let worker = agent(wrapped.clone(), &gate);
    let controller = agent(native.clone(), &gate);
    let handle = worker
        .prompt(None, "cancel with unknown terminal ACK")
        .await
        .unwrap();
    gate.entered.notified().await;
    let target = controller
        .run_control_binding(handle.session_id(), handle.run_id())
        .await
        .unwrap()
        .target;
    controller
        .control_run(request(native.as_ref(), &target, cancel_action()).await)
        .await
        .unwrap();
    assert!(matches!(
        handle.done().await,
        Err(RuntimeError::AttemptBoundaryUnknown { .. })
    ));
    assert_eq!(wrapped.terminal_calls.load(Ordering::SeqCst), 1);
    let page = controller.run_controls(&target).await.unwrap();
    assert_eq!(
        page.controls[0].status,
        if fault == 1 {
            RunControlStatus::Observed
        } else {
            RunControlStatus::TerminalCommitted {
                status: RunStatus::Interrupted,
            }
        }
    );
    assert_eq!(
        native
            .get_run(&target.run_id)
            .await
            .unwrap()
            .unwrap()
            .status,
        if fault == 1 {
            RunStatus::Running
        } else {
            RunStatus::Interrupted
        }
    );
    assert_eq!(gate.fake.requests().len(), 1);
}
async fn acceptance(native: Arc<dyn Store>, steer: bool) {
    let wrapped = Arc::new(TestStore::new(native.clone()));
    let gate = Gate::new(if steer { 2 } else { 1 });
    let worker = agent(native.clone(), &gate);
    let controller = agent(wrapped.clone(), &gate);
    let handle = worker.prompt(None, "acceptance ACK loss").await.unwrap();
    gate.entered.notified().await;
    let target = controller
        .run_control_binding(handle.session_id(), handle.run_id())
        .await
        .unwrap()
        .target;
    let revision = native.session_revision(&target.session_id).await.unwrap();
    let action = if steer {
        RunControlAction::Steer {
            input: InputEnvelope::text("once after ACK loss", revision).unwrap(),
        }
    } else {
        cancel_action()
    };
    let request = request(native.as_ref(), &target, action).await;
    wrapped.control_loss.store(true, Ordering::SeqCst);
    assert!(matches!(
        controller.control_run(request.clone()).await,
        Err(RuntimeError::RunControl(
            RunControlError::UnknownStoreOutcome
        ))
    ));
    let restarted = agent(native.clone(), &gate);
    restarted.close_extensions().await.unwrap();
    assert!(matches!(
        restarted.control_run(request).await.unwrap(),
        RunControlOutcome::Replayed(_)
    ));
    if steer {
        gate.release.notify_one();
    }
    assert_eq!(
        handle.done().await.unwrap().status,
        if steer {
            RunStatus::Completed
        } else {
            RunStatus::Interrupted
        }
    );
    assert_eq!(gate.fake.requests().len(), if steer { 2 } else { 1 });
}
