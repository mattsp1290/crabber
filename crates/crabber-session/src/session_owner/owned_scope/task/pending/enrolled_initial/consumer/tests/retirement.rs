//! One real joined original reconciles its committed Seal without redispatch.
use super::*;
use crate::session_owner::{SessionOwnerOperation, SessionOwnerState};
use std::time::Duration;

struct ReleaseSeal(Arc<Forward>);
impl Drop for ReleaseSeal {
    fn drop(&mut self) {
        self.0.release.notify_one();
    }
}

#[tokio::test]
async fn committed_seal_lost_ack_replays_same_original_and_retains_outputs() {
    for keyed in [false, true] {
        reconcile(keyed).await;
    }
}

async fn reconcile(keyed: bool) {
    let store = Arc::new(MemoryStore::new());
    let source = source();
    let (context, original) = initial_consumer_fixture::prepared(&*store, keyed, &source).await;
    let session = context.session_id().clone();
    let adapter = Forward::new(store.clone(), Fault::LostSealAckThenHold);
    let _release = ReleaseSeal(adapter.clone());
    let owner = PendingDriverTasks::default();
    let mut producer = PendingInitialProducer::register_scripted(
        &owner,
        adapter.clone(),
        context,
        original,
        source,
    )
    .unwrap();
    let control = producer.control();
    let resources = producer.retained.consumer.as_ref().unwrap().clone();
    producer.start().unwrap();
    let observation = producer.join().await.unwrap();
    assert_eq!(observation.status(), &Status::Unresolved);
    assert_eq!(observation.disposition(), OwnedMainDisposition::Returned);
    assert!(!observation.observation_failed());
    let mut retry = observation
        .take_retry()
        .expect("lost real Seal ACK retains its original");
    assert!(control.original_join_observed());
    let activation = producer.retained.calls.activation.get().unwrap();
    let request = crate::session_owner::owned_scope::JoinSource {
        activation: activation.clone(),
        disposition: OwnedMainDisposition::Returned,
    }
    .request()
    .unwrap();
    let before = store
        .lookup_session_owner(&session, &request.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(before.operation, SessionOwnerOperation::Join);
    assert_eq!(before.owner.state, SessionOwnerState::Joined);
    let revision = store.session_revision(&session).await.unwrap();
    let events = store.list_events(&session, None, 1000).await.unwrap();
    let business_calls = adapter.calls.lock().unwrap().len();
    let grant = Arc::downgrade(producer.retained.calls.driver.get().unwrap());
    let completion = {
        let state = producer.retained.lock();
        Arc::downgrade(state.enrolled.as_ref().unwrap())
    };
    let mut attempt = Box::pin(retry.retry());
    tokio::time::timeout(Duration::from_secs(2), async {
        tokio::select! {
            () = adapter.entered.notified() => {},
            result = &mut attempt => panic!("held original Seal returned early: {}", result.is_ok()),
        }
    }).await.unwrap();
    assert!(futures::poll!(&mut attempt).is_pending());
    assert!(grant.upgrade().is_some() && completion.upgrade().is_some());
    let report = resources.owner.snapshot();
    assert!(report.sealed && report.pending == 0 && report.failures.is_empty());
    // Physical tasks are joined: unknown native ACK alone does not fabricate a
    // live task or an indefinite capacity hold. Same opaque originals remain.
    owner.wait_for_supervision().await;
    assert_eq!(owner.available_capacity(), 64);
    adapter.release.notify_one();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), attempt)
            .await
            .unwrap()
            .unwrap(),
        Status::ClosedAssignment
    );
    assert_eq!(
        store
            .lookup_session_owner(&session, &request.id)
            .await
            .unwrap(),
        Some(before)
    );
    assert_eq!(store.session_revision(&session).await.unwrap(), revision);
    assert_eq!(
        store.list_events(&session, None, 1000).await.unwrap(),
        events
    );
    assert_eq!(adapter.calls.lock().unwrap().len(), business_calls);
    {
        let seals = adapter.seals.lock().unwrap();
        assert_eq!(seals.len(), 2);
        assert!(seals.iter().all(|seal| seal == &seals[0]));
    }
    drop(retry);
    drop(control);
    drop(producer);
    owner.wait_for_supervision().await;
    assert!(grant.upgrade().is_none() && completion.upgrade().is_none());
}
