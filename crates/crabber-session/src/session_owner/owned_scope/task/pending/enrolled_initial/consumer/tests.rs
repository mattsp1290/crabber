//! Actual scripted-task and retained-owner mechanisms, without policy claims.
mod forward;
use super::super::{
    PendingDriverTasks, PendingInitialNativeCall, PendingInitialNativeReply,
    PendingInitialProducer, PendingInitialProducerStatus as Status,
};
use super::*;
use crate::native_enrollment::initial_consumer::CallKind;
use crate::session_owner::owned_scope::{OwnedMainDisposition, initial_consumer_fixture};
use crate::{MemoryStore, Store};
use crabber_core::RunId;
use crabber_extension::LibraryScriptedSource;
use forward::{Fault, Forward};
use std::sync::Mutex;
fn source() -> LibraryScriptedSource {
    LibraryScriptedSource::new(
        crabber::Selection {
            provider_id: "p".into(),
            model_id: "m".into(),
        },
        vec!["same original finite answer".into()],
    )
    .unwrap()
}
#[tokio::test]
async fn every_native_call_remains_in_the_same_original_task() {
    for keyed in [false, true] {
        let store = Arc::new(MemoryStore::new());
        let source = source();
        let (context, original) = initial_consumer_fixture::prepared(&*store, keyed, &source).await;
        let adapter = Forward::new(store.clone(), Fault::Pass);
        let owner = PendingDriverTasks::default();
        let mut producer = PendingInitialProducer::register_scripted(
            &owner,
            adapter.clone(),
            context,
            original,
            source,
        )
        .unwrap();
        assert_eq!(
            producer
                .retained
                .consumer
                .as_ref()
                .unwrap()
                .owner
                .snapshot()
                .pending,
            0,
            "parked construction spawned a callback observer"
        );
        producer.start().unwrap();
        let observed = producer.join().await.unwrap();
        assert_eq!(
            observed.status(),
            &Status::ClosedAssignment,
            "{:?}",
            observed.error()
        );
        assert_eq!(observed.disposition(), OwnedMainDisposition::Returned);
        assert!(observed.error().is_none() && !observed.observation_failed());
        owner.wait_for_supervision().await;
        let calls = adapter.calls.lock().unwrap();
        let first = calls.first().unwrap().1;
        assert!(first.is_some());
        assert!(calls.iter().all(|(_, task)| *task == first));
        assert_ne!(first, tokio::task::try_id());
        assert_eq!(calls.len(), if keyed { 12 } else { 11 });
        assert!(
            calls
                .last()
                .is_some_and(|(kind, _)| *kind == CallKind::Conclude)
        );
        assert_eq!(owner.available_capacity(), 64);
    }
}
#[test]
fn literal_consumer_poll_and_drop_children_are_contained() {
    for kind in ["register", "activate", "begin"] {
        let output=std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact","session_owner::owned_scope::task::pending::enrolled_initial::consumer::tests::literal_child","--nocapture"])
            .env("CRABBER_INITIAL_CONSUMER_LITERAL_CHILD",kind).env("RUST_BACKTRACE","0").output().unwrap();
        assert!(
            output.status.success(),
            "{kind}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
}
#[tokio::test]
async fn literal_child() {
    let Ok(kind) = std::env::var("CRABBER_INITIAL_CONSUMER_LITERAL_CHILD") else {
        return;
    };
    let kind = match kind.as_str() {
        "register" => CallKind::Register,
        "activate" => CallKind::Activate,
        "begin" => CallKind::BeginInitial,
        _ => panic!("unknown child"),
    };
    let store = Arc::new(MemoryStore::new());
    let source = source();
    let (context, original) = initial_consumer_fixture::prepared(&*store, false, &source).await;
    let adapter = Forward::new(store, Fault::Literal(kind));
    let owner = PendingDriverTasks::default();
    let mut producer = PendingInitialProducer::register_scripted(
        &owner,
        adapter.clone(),
        context,
        original,
        source,
    )
    .unwrap();
    producer.start().unwrap();
    let observed = producer.join().await.unwrap();
    assert!(observed.error().is_some() && observed.observation_failed());
    assert_eq!(observed.status(), &Status::Unresolved);
    assert_eq!(observed.disposition(), OwnedMainDisposition::NoDispatch);
    owner.wait_for_supervision().await;
    let calls = adapter.calls.lock().unwrap();
    assert!(calls.last().is_some_and(|(actual, _)| *actual == kind));
    assert!(
        calls
            .iter()
            .all(|(actual, _)| *actual != CallKind::AssistantMessage
                && *actual != CallKind::Conclude)
    );
    assert_eq!(owner.available_capacity(), 64);
}

struct Release(Arc<tokio::sync::Notify>);
impl Drop for Release {
    fn drop(&mut self) {
        self.0.notify_one();
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn caller_timeout_and_drop_keep_the_same_supervisor_resource_join_and_capacity() {
    let store = Arc::new(MemoryStore::new());
    let source = source();
    let (context, original) = initial_consumer_fixture::prepared(&*store, false, &source).await;
    let adapter = Forward::new(store, Fault::HoldConsumer(CallKind::TurnCompleted));
    let owner = PendingDriverTasks::default();
    let mut producer = PendingInitialProducer::register_scripted(
        &owner,
        adapter.clone(),
        context,
        original,
        source,
    )
    .unwrap();
    let resources = producer.retained.consumer.as_ref().unwrap().clone();
    assert_eq!(resources.owner.snapshot().pending, 0);
    producer.start().unwrap();
    tokio::select! {
        () = adapter.entered.notified() => {},
        result = producer.join() => panic!("original returned before its actual final lifecycle ACK: {}", result.is_ok()),
    }
    // Actual TurnCompleted has committed and its SAME reply is held before
    // return/Drop. The finite source work is done, while the original attached
    // owner remains open. This extra task tests retained joins, not a custom
    // source; the following 2s checkpoint covers entry to drain, not the turn.
    assert!(resources.capability().is_ok());
    let release = Arc::new(tokio::sync::Notify::new());
    let _release = Release(release.clone());
    let actual = release.clone();
    resources
        .owner
        .resources()
        .spawn(async move {
            actual.notified().await;
            Ok(())
        })
        .unwrap();
    adapter.release.notify_one();
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while !resources.owner.snapshot().sealed {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(15), producer.join())
            .await
            .is_err()
    );
    let control = producer.control();
    let observed = tokio::time::timeout(std::time::Duration::from_secs(15), producer.join())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(observed.status(), &Status::Unresolved);
    assert!(control.original_join_observed());
    assert!(resources.owner.snapshot().pending > 0);
    assert_eq!(owner.available_capacity(), 63);
    drop(producer);
    drop(control);
    let mut supervision = Box::pin(owner.wait_for_supervision());
    assert!(futures::poll!(&mut supervision).is_pending());
    assert_eq!(owner.available_capacity(), 63);
    release.notify_one();
    tokio::time::timeout(std::time::Duration::from_secs(2), supervision)
        .await
        .unwrap();
    assert_eq!(owner.available_capacity(), 64);
    let actual = resources.owner.snapshot();
    assert!(actual.sealed && actual.pending == 0 && actual.failures.is_empty());
}

#[tokio::test]
async fn native_deadline_after_successful_literal_drop_can_reconcile_paired_close() {
    let store = Arc::new(MemoryStore::new());
    let source = source();
    let (context, original) = initial_consumer_fixture::prepared(&*store, false, &source).await;
    let adapter = Forward::new(store, Fault::Pending(CallKind::Register));
    let owner = PendingDriverTasks::default();
    let mut producer = PendingInitialProducer::register_scripted(
        &owner,
        adapter.clone(),
        context,
        original,
        source,
    )
    .unwrap();
    let resources = producer.retained.consumer.as_ref().unwrap().clone();
    producer.start().unwrap();
    let observed = tokio::time::timeout(std::time::Duration::from_secs(8), producer.join())
        .await
        .unwrap()
        .unwrap();
    assert!(observed.error().is_some());
    assert!(
        !observed.observation_failed(),
        "successful literal disposal was marked uncertain"
    );
    assert_eq!(observed.disposition(), OwnedMainDisposition::NoDispatch);
    assert_eq!(observed.status(), &Status::ClosedAssignment);
    owner.wait_for_supervision().await;
    let report = resources.owner.snapshot();
    assert!(report.sealed && report.pending == 0 && report.failures.is_empty());
    let calls = adapter.calls.lock().unwrap();
    assert_eq!(calls.len(), 1);
    assert!(calls[0].0 == CallKind::Register);
    assert_eq!(owner.available_capacity(), 64);
}
