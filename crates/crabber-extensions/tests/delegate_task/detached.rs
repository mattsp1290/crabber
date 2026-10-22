use super::background::Journey;
use super::*;
use crabber::core::{EventKind, SessionOwnerId};
use crabber::session::session_owner::SessionOwnerState;
use sha2::{Digest, Sha256};

async fn owner(j: &Journey) -> (SessionOwnerId, crabber::core::ToolCallRecord) {
    let records = snapshot_tools(&j.store, j.session.as_ref().unwrap()).await;
    let start = records
        .into_iter()
        .find(|r| r.name == crabber_extensions::delegate_task::START_TOOL)
        .unwrap();
    let hash = format!(
        "{:x}",
        Sha256::digest(
            serde_json::to_vec(&(j.session.as_ref().unwrap(), &start.run_id, &start.id)).unwrap()
        )
    );
    (
        SessionOwnerId::from(format!("delegate-task.{}", &hash[..32])),
        start,
    )
}
#[tokio::test]
async fn parent_finishes_while_child_is_active_and_a_cold_controller_does_not_own_its_join() {
    let mut pending = PendingHost::new(false);
    let bounds = Limits {
        max_background_duration: Duration::from_secs(60),
        ..limits()
    };
    let mut j = Journey::new(pending.runner.clone(), bounds.clone());
    let id = j.start().await;
    pending.entered(1).await;
    let session = j.session.clone().unwrap();
    let (owner_id, start) = owner(&j).await;
    assert_eq!(
        j.store
            .get_run(&start.run_id)
            .await
            .unwrap()
            .unwrap()
            .status,
        RunStatus::Completed
    );
    assert_eq!(
        j.store
            .get_session_owner(&session, &owner_id)
            .await
            .unwrap()
            .state,
        SessionOwnerState::Active
    );
    let observed = j
        .agent
        .drain_session_invocations(&session, false, Duration::from_millis(1))
        .await;
    assert_eq!(observed.len(), 1);
    assert!(!observed[0].local_joined);
    assert_eq!(observed[0].id, owner_id);
    assert!(
        j.agent
            .drain_session_invocations(&SessionId::new(), true, Duration::ZERO)
            .await
            .is_empty()
    );
    let cold_store = j.store.clone();
    let cold_ext = Arc::new(configured(pending.runner.clone(), bounds));
    let cold = agent(
        cold_ext,
        cold_store.clone(),
        Arc::new(FakeProvider::scripted(vec![status_call(&id), done()])),
        PermissionDecision::Allow,
    );
    assert!(
        cold.drain_session_invocations(&session, false, Duration::ZERO)
            .await
            .is_empty()
    );
    assert_eq!(
        cold_store
            .get_session_owner(&session, &owner_id)
            .await
            .unwrap()
            .state,
        SessionOwnerState::Active
    );
    cold.prompt(Some(session.clone()), "cold status")
        .await
        .unwrap()
        .done()
        .await
        .unwrap();
    assert_eq!(
        snapshot_tools(&cold_store, &session)
            .await
            .last()
            .unwrap()
            .status,
        crabber::core::ToolCallStatus::Failed
    );
    assert!(
        !pending.seen.lock().unwrap()[0]
            .cancellation()
            .is_cancelled()
    );
    pending.release.notify_one();
    pending.exited(1).await;
    j.wait(&[id]).await;
    j.close().await;
    assert!(cold.close(Duration::from_secs(2)).await.local_drained());
    assert_eq!(
        j.store
            .get_session_owner(&session, &owner_id)
            .await
            .unwrap()
            .state,
        SessionOwnerState::Joined
    );
}
#[tokio::test]
async fn cancelling_a_noncooperative_job_keeps_capacity_and_publishes_before_actual_join() {
    let mut pending = PendingHost::new(false);
    let mut j = Journey::new(pending.runner.clone(), limits());
    let id = j.start().await;
    pending.entered(1).await;
    let session = j.session.clone().unwrap();
    let reports = j
        .agent
        .drain_session_invocations(&session, true, Duration::from_millis(10))
        .await;
    assert_eq!(reports.len(), 1);
    assert!(!reports[0].local_joined);
    assert!(
        pending.seen.lock().unwrap()[0]
            .cancellation()
            .is_cancelled()
    );
    assert_eq!(j.ext.live_runners(), 1);
    assert_eq!(
        result_value(
            &j.call(
                crabber_extensions::delegate_task::STATUS_TOOL,
                json!({"task_id":id})
            )
            .await
        )["status"],
        "cancelled"
    );
    assert_eq!(
        result_value(
            &j.call(crabber_extensions::delegate_task::START_TOOL, arguments())
                .await
        )["status"],
        "unavailable"
    );
    let before = j.store.list_events(&session, None, 1000).await.unwrap();
    let before = super::attribution::publications(&before);
    assert_eq!(
        before
            .iter()
            .map(super::attribution::status)
            .collect::<Vec<_>>(),
        ["running", "cancelling"],
        "failures {}",
        j.ext.owner_report().publication_failures
    );
    pending.release.notify_one();
    pending.exited(1).await;
    let reports = j
        .agent
        .drain_session_invocations(&session, false, Duration::from_secs(2))
        .await;
    assert!(
        reports
            .iter()
            .all(|r| r.local_joined && r.status == crabber::SessionInvocationStatus::Joined)
    );
    let events = j.store.list_events(&session, None, 1000).await.unwrap();
    let publications = super::attribution::publications(&events);
    assert_eq!(
        publications
            .iter()
            .map(super::attribution::status)
            .collect::<Vec<_>>(),
        ["running", "cancelling", "cancelled"]
    );
    assert_eq!(
        events
            .iter()
            .filter(|e| e.kind == EventKind::InvocationFinished)
            .count(),
        1
    );
    assert_eq!(j.ext.live_runners(), 0);
    j.close().await;
}
#[tokio::test]
async fn background_deadline_publishes_timed_out_then_runtime_joins() {
    let mut pending = PendingHost::new(true);
    let mut j = Journey::new(
        pending.runner.clone(),
        Limits {
            max_background_duration: Duration::from_secs(1),
            ..limits()
        },
    );
    let id = j.start().await;
    pending.entered(1).await;
    assert_eq!(j.wait(&[id]).await["tasks"][0]["status"], "timed_out");
    pending.exited(1).await;
    let reports = j
        .agent
        .drain_session_invocations(j.session.as_ref().unwrap(), false, Duration::from_secs(2))
        .await;
    assert!(reports.iter().all(|r| r.local_joined));
    let events = j
        .store
        .list_events(j.session.as_ref().unwrap(), None, 1000)
        .await
        .unwrap();
    let publications = super::attribution::publications(&events);
    assert_eq!(
        publications
            .iter()
            .map(super::attribution::status)
            .collect::<Vec<_>>(),
        ["running", "timed_out"]
    );
    j.close().await;
}
#[path = "detached/disposal.rs"]
mod disposal;
#[path = "detached/faults.rs"]
mod faults;
