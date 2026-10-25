use super::*;
use crate::{MemoryStore, session_owner::preparation_contract::resources};
use futures::future::BoxFuture;
use std::{
    future::Future,
    pin::Pin,
    sync::atomic::{AtomicUsize, Ordering},
    task::{Context, Poll},
};

async fn prepared(store: &impl Store, keyed: bool) -> (SessionContext, JoinedPreparation) {
    let original = if keyed {
        let (root, bound) = resources::declaration::setup(store).await;
        resources::initial_driver_observation(
            store,
            root,
            crate::session_owner::preparation::PreparedOperation::AdmitKeyed(Box::new(bound)),
        )
        .await
    } else {
        resources::initial_unkeyed_observation(store).await
    };
    let binding = &original
        .observed_grant()
        .original_reservation()
        .owner
        .binding;
    let context = SessionContext::new(
        binding.session_id.clone(),
        binding.workspace_id.clone(),
        binding.directory.clone(),
    );
    (context, original)
}

async fn genuine<S: Store + 'static>(store: Arc<S>) -> Vec<crabber_core::SessionId> {
    let mut sessions = Vec::new();
    for keyed in [false, true] {
        let owner = PendingDriverTasks::default();
        let (context, original) = prepared(store.as_ref(), keyed).await;
        let metadata_source = original.clone();
        let session = context.session_id().clone();
        let mut producer =
            PendingInitialProducer::register(&owner, store.clone(), context, original).unwrap();
        producer.start().unwrap();
        let observed = producer.join().await.unwrap();
        assert_eq!(
            observed.status(),
            &PendingInitialProducerStatus::ClosedAssignment,
            "error={:?}, observation_failed={}",
            observed.error(),
            observed.observation_failed()
        );
        assert_eq!(observed.disposition(), OwnedMainDisposition::NoDispatch);
        assert!(!observed.observation_failed());
        assert!(observed.error().is_none(), "{:?}", observed.error());
        assert!(observed.take_retry().is_none());
        let own_run = match metadata_source.observed_operation() {
            crate::session_owner::preparation::PreparedOperation::AdmitKeyed(bound) => {
                store
                    .lookup_admission(&session, &bound.keyed.options.key)
                    .await
                    .unwrap()
                    .unwrap()
                    .run_id
            }
            crate::session_owner::preparation::PreparedOperation::Admit(_) => {
                store
                    .inspect_initial_preparation_admission(&metadata_source)
                    .await
                    .unwrap()
                    .unwrap()
                    .run_id
            }
            _ => panic!("initial source"),
        };
        assert!(
            store.get_run(&own_run).await.is_err(),
            "this Pending birth cannot expose Raw credentials"
        );
        assert!(
            store.list_unfinished_runs().await.is_err(),
            "public full credentials remain Raw"
        );
        owner.wait_for_supervision().await;
        assert_eq!(owner.available_capacity(), 64);
        sessions.push(session);
    }
    sessions
}
#[tokio::test]
async fn genuine_birth_open_unbound_join_and_seal_memory() {
    let _ = genuine(Arc::new(MemoryStore::new())).await;
}
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn genuine_birth_open_unbound_join_and_seal_sqlite() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("enrolled-initial.sqlite");
    crate::SqliteStore::migrate(&path).await.unwrap();
    let _ = genuine(Arc::new(crate::SqliteStore::connect(&path).await.unwrap())).await;
}
#[cfg(feature = "postgres")]
#[tokio::test]
async fn genuine_birth_open_unbound_join_and_seal_postgres() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};

    TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {
        crate::PostgresStore::migrate(&url).await.unwrap();
        let store = Arc::new(crate::PostgresStore::connect(&url).await.unwrap());
        let sessions = genuine(store.clone()).await;
        for session in sessions {
            store.purge_pending_initial_fixture(&session).await.unwrap();
        }
    })
    .await
    .unwrap();
}

#[derive(Clone, Copy)]
enum Fault {
    None,
    OpenError,
    OpenLostAck,
    ForeignOpen,
    OpenBoth,
    CompleteBoth,
    SealBoth,
    SealLostAck,
    CaptureOpen,
    SubstituteOpen,
    OpenBlocked,
    CompleteBlocked,
    SealBlocked,
    OpenReadyBlocked,
    CompleteReadyBlocked,
    SealReadyBlocked,
    #[cfg(any(feature = "postgres", feature = "sqlite"))]
    HoldOpenAck,
}
struct Seen {
    calls: Mutex<Vec<(Operation, Option<Id>)>>,
    drops: AtomicUsize,
}
#[derive(Default)]
struct DropGate {
    entered: Arc<tokio::sync::Notify>,
    #[cfg(any(feature = "postgres", feature = "sqlite"))]
    async_release: Arc<tokio::sync::Notify>,
    released: (Mutex<bool>, std::sync::Condvar),
}
impl DropGate {
    fn block(&self, seen: &Seen) {
        self.entered.notify_one();
        let (lock, changed) = &self.released;
        let mut released = lock.lock().unwrap();
        while !*released {
            released = changed.wait(released).unwrap();
        }
        seen.drops.fetch_add(1, Ordering::SeqCst);
    }
}
struct ReleaseDrop {
    gate: Arc<DropGate>,
    watchdog: Option<std::thread::JoinHandle<bool>>,
}
impl ReleaseDrop {
    fn new(gate: Arc<DropGate>) -> Self {
        let retained = gate.clone();
        let watchdog = std::thread::spawn(move || {
            let (lock, changed) = &retained.released;
            let (mut released, _) = changed
                .wait_timeout_while(
                    lock.lock().unwrap(),
                    NATIVE_BOUND + Duration::from_secs(3),
                    |released| !*released,
                )
                .unwrap();
            let expired = !*released;
            *released = true;
            changed.notify_all();
            expired
        });
        Self {
            gate,
            watchdog: Some(watchdog),
        }
    }
    fn release(&mut self) -> bool {
        *self.gate.released.0.lock().unwrap() = true;
        self.gate.released.1.notify_all();
        self.watchdog
            .take()
            .is_some_and(|watchdog| watchdog.join().unwrap())
    }
}
impl Drop for ReleaseDrop {
    fn drop(&mut self) {
        self.release();
    }
}
struct Forward {
    inner: Arc<dyn Store>,
    fault: Fault,
    seen: Arc<Seen>,
    cached: Arc<Mutex<Option<PendingInitialNativeReply>>>,
    gate: Arc<DropGate>,
}
struct Blocked<'a> {
    _call: PendingInitialNativeCall<'a>,
    gate: Arc<DropGate>,
    seen: Arc<Seen>,
}
impl Future for Blocked<'_> {
    type Output = Result<PendingInitialNativeReply, PreparationCommitError>;
    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Pending
    }
}
impl Drop for Blocked<'_> {
    fn drop(&mut self) {
        self.gate.block(&self.seen);
    }
}
struct ReadyBlocked<'a> {
    inner: BoxFuture<'a, Result<PendingInitialNativeReply, PreparationCommitError>>,
    gate: Arc<DropGate>,
    seen: Arc<Seen>,
    ready: bool,
}
impl Future for ReadyBlocked<'_> {
    type Output = Result<PendingInitialNativeReply, PreparationCommitError>;
    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let original = self.get_mut();
        let result = original.inner.as_mut().poll(context);
        original.ready |= result.is_ready();
        result
    }
}
impl Drop for ReadyBlocked<'_> {
    fn drop(&mut self) {
        if self.ready {
            self.gate.block(&self.seen);
        }
    }
}
struct Literal<'a> {
    _call: PendingInitialNativeCall<'a>,
    seen: Arc<Seen>,
    both: bool,
}
impl Future for Literal<'_> {
    type Output = Result<PendingInitialNativeReply, PreparationCommitError>;
    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        assert!(!self.both, "literal Pending native poll failure");
        Poll::Ready(Err(Error::Unsupported.into()))
    }
}
impl Drop for Literal<'_> {
    fn drop(&mut self) {
        self.seen.drops.fetch_add(1, Ordering::SeqCst);
        assert!(!self.both, "literal Pending native Drop failure");
    }
}
// Implement the actual async-trait ABI to return the literal future itself.
#[async_trait::async_trait]
impl Store for Forward {
    async fn admit_run(
        &self,
        request: crate::AdmitRequest,
    ) -> Result<crate::AdmitOutcome, crate::StoreError> {
        self.inner.admit_run(request).await
    }
    async fn execution(
        &self,
        fence: crabber_core::RunFence,
    ) -> Result<Box<dyn crate::ExecutionStore>, crate::StoreError> {
        self.inner.execution(fence).await
    }
    async fn get_session(
        &self,
        id: &crabber_core::SessionId,
    ) -> Result<Option<crabber_core::Session>, crate::StoreError> {
        self.inner.get_session(id).await
    }
    async fn get_run(&self, id: &RunId) -> Result<Option<crabber_core::Run>, crate::StoreError> {
        self.inner.get_run(id).await
    }
    async fn list_messages(
        &self,
        id: &crabber_core::SessionId,
        epoch: Option<crabber_core::EpochId>,
    ) -> Result<Vec<crabber_core::Message>, crate::StoreError> {
        self.inner.list_messages(id, epoch).await
    }
    async fn list_all_messages(
        &self,
        id: &crabber_core::SessionId,
    ) -> Result<Vec<crabber_core::Message>, crate::StoreError> {
        self.inner.list_all_messages(id).await
    }
    async fn list_events(
        &self,
        id: &crabber_core::SessionId,
        after: Option<crabber_core::EventCursor>,
        limit: usize,
    ) -> Result<Vec<crabber_core::EventRecord>, crate::StoreError> {
        self.inner.list_events(id, after, limit).await
    }
    async fn list_unfinished_runs(&self) -> Result<Vec<crabber_core::Run>, crate::StoreError> {
        self.inner.list_unfinished_runs().await
    }
    async fn list_unfinished_tool_calls(
        &self,
        run: &RunId,
    ) -> Result<Vec<crabber_core::ToolCallRecord>, crate::StoreError> {
        self.inner.list_unfinished_tool_calls(run).await
    }
    async fn claim_expired_run(
        &self,
        run: &RunId,
        owner: &str,
    ) -> Result<crabber_core::RunFence, crate::StoreError> {
        self.inner.claim_expired_run(run, owner).await
    }
    async fn get_extension_state(
        &self,
        extension: &str,
        session: &crabber_core::SessionId,
    ) -> Result<std::collections::BTreeMap<String, String>, crate::StoreError> {
        self.inner.get_extension_state(extension, session).await
    }
    async fn enqueue_inbox(
        &self,
        session: &crabber_core::SessionId,
        kind: crate::InboxKind,
        message: crabber_core::Message,
    ) -> Result<(), crate::StoreError> {
        self.inner.enqueue_inbox(session, kind, message).await
    }
    fn pending_initial_native<'life0, 'life1, 'async_trait>(
        &'life0 self,
        call: PendingInitialNativeCall<'life1>,
    ) -> BoxFuture<'async_trait, Result<PendingInitialNativeReply, PreparationCommitError>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        let operation = call.operation;
        let ordinal = {
            let mut calls = self.seen.calls.lock().unwrap();
            calls.push((operation, tokio::task::try_id()));
            calls.iter().filter(|(op, _)| *op == operation).count()
        };
        let both = matches!(
            (self.fault, operation),
            (Fault::OpenBoth, Operation::Open)
                | (Fault::CompleteBoth, Operation::Complete)
                | (Fault::SealBoth, Operation::Seal)
        );
        if both || matches!((self.fault, operation), (Fault::OpenError, Operation::Open)) {
            return Box::pin(Literal {
                _call: call,
                seen: self.seen.clone(),
                both,
            });
        }
        if matches!(
            (self.fault, operation),
            (Fault::OpenBlocked, Operation::Open)
                | (Fault::CompleteBlocked, Operation::Complete)
                | (Fault::SealBlocked, Operation::Seal)
        ) {
            return Box::pin(Blocked {
                _call: call,
                gate: self.gate.clone(),
                seen: self.seen.clone(),
            });
        }
        if matches!(
            (self.fault, operation),
            (Fault::OpenReadyBlocked, Operation::Open)
                | (Fault::CompleteReadyBlocked, Operation::Complete)
                | (Fault::SealReadyBlocked, Operation::Seal)
        ) {
            return Box::pin(ReadyBlocked {
                inner: self.inner.pending_initial_native(call),
                gate: self.gate.clone(),
                seen: self.seen.clone(),
                ready: false,
            });
        }
        if matches!(
            (self.fault, operation),
            (Fault::SubstituteOpen, Operation::Open)
        ) {
            let cached = self.cached.lock().unwrap().take();
            return Box::pin(async move { cached.ok_or(Error::UnknownStoreOutcome.into()) });
        }
        if matches!(
            (self.fault, operation),
            (Fault::ForeignOpen, Operation::Open)
        ) {
            let inner = self.inner.clone();
            return Box::pin(async move {
                std::thread::scope(|scope| {
                    scope
                        .spawn(move || {
                            futures::executor::block_on(inner.pending_initial_native(call))
                        })
                        .join()
                        .unwrap()
                })
            });
        }
        Box::pin(async move {
            let result = self.inner.pending_initial_native(call).await;
            #[cfg(any(feature = "postgres", feature = "sqlite"))]
            if matches!(
                (self.fault, operation),
                (Fault::HoldOpenAck, Operation::Open)
            ) && result.is_ok()
            {
                self.gate.entered.notify_one();
                self.gate.async_release.notified().await;
            }
            if matches!(
                (self.fault, operation),
                (Fault::CaptureOpen, Operation::Open)
            ) {
                *self.cached.lock().unwrap() = Some(result?);
                return Err(Error::UnknownStoreOutcome.into());
            }
            if ordinal == 1
                && matches!(
                    (self.fault, operation),
                    (Fault::OpenLostAck, Operation::Open) | (Fault::SealLostAck, Operation::Seal)
                )
            {
                drop(result);
                Err(Error::UnknownStoreOutcome.into())
            } else {
                result
            }
        })
    }
}
impl PendingInitialProducer {
    /// Holds the actual successful Open reply before returning that same owned
    /// value. Test orchestration only; no transport reconstruction or admission.
    #[cfg(any(feature = "postgres", feature = "sqlite"))]
    pub(crate) fn store_holding_open_ack(
        inner: Arc<dyn Store>,
    ) -> (
        Arc<dyn Store>,
        Arc<tokio::sync::Notify>,
        Arc<tokio::sync::Notify>,
    ) {
        let gate = Arc::new(DropGate::default());
        let adapter = Arc::new(Forward {
            inner,
            fault: Fault::HoldOpenAck,
            seen: Arc::new(Seen {
                calls: Mutex::new(Vec::new()),
                drops: AtomicUsize::new(0),
            }),
            cached: Arc::new(Mutex::new(None)),
            gate: gate.clone(),
        });
        (adapter, gate.entered.clone(), gate.async_release.clone())
    }
}
async fn exercise(fault: Fault) {
    let store = Arc::new(MemoryStore::new());
    let owner = PendingDriverTasks::default();
    let (context, original) = prepared(store.as_ref(), false).await;
    let seen = Arc::new(Seen {
        calls: Mutex::new(Vec::new()),
        drops: AtomicUsize::new(0),
    });
    let adapter = Arc::new(Forward {
        inner: store,
        fault,
        seen: seen.clone(),
        cached: Arc::new(Mutex::new(None)),
        gate: Arc::default(),
    });
    let mut producer =
        PendingInitialProducer::register(&owner, adapter, context, original).unwrap();
    producer.start().unwrap();
    let actual = producer.join().await.unwrap();
    let panic = matches!(
        fault,
        Fault::OpenBoth | Fault::CompleteBoth | Fault::SealBoth
    );
    assert_eq!(actual.observation_failed(), panic);
    assert_eq!(actual.disposition(), OwnedMainDisposition::NoDispatch);
    let unresolved = matches!(fault, Fault::SealBoth | Fault::SealLostAck);
    assert_eq!(
        actual.status(),
        &if unresolved {
            PendingInitialProducerStatus::Unresolved
        } else if matches!(fault, Fault::None) {
            PendingInitialProducerStatus::ClosedAssignment
        } else {
            PendingInitialProducerStatus::ClosedNoPublication
        }
    );
    let retry = actual.take_retry();
    assert_eq!(retry.is_some(), unresolved);
    if matches!(fault, Fault::SealLostAck) {
        let mut retry = retry.unwrap();
        assert_eq!(
            retry.retry().await.unwrap(),
            PendingInitialProducerStatus::ClosedAssignment
        );
    }
    {
        let calls = seen.calls.lock().unwrap();
        assert_eq!(
            calls
                .iter()
                .filter(|(op, _)| *op == Operation::Open)
                .count(),
            1
        );
        assert_eq!(
            calls
                .iter()
                .filter(|(op, _)| *op == Operation::Complete)
                .count(),
            usize::from(matches!(
                fault,
                Fault::None | Fault::CompleteBoth | Fault::SealBoth | Fault::SealLostAck
            ))
        );
        assert_eq!(
            calls
                .iter()
                .filter(|(op, _)| *op == Operation::Seal)
                .count(),
            if matches!(fault, Fault::SealLostAck) {
                2
            } else {
                1
            }
        );
        let open_task = calls
            .iter()
            .find(|(op, _)| *op == Operation::Open)
            .unwrap()
            .1;
        assert!(open_task.is_some());
        for (op, task) in calls.iter().filter(|(op, _)| *op == Operation::Complete) {
            assert_eq!(*task, open_task, "{op:?}");
        }
        assert_ne!(
            calls
                .iter()
                .find(|(op, _)| *op == Operation::Seal)
                .unwrap()
                .1,
            open_task
        );
    }
    owner.wait_for_supervision().await;
    assert_eq!(owner.available_capacity(), 64);
}

#[tokio::test]
async fn original_task_admission_and_unknown_open_never_start_complete() {
    for fault in [
        Fault::None,
        Fault::OpenError,
        Fault::OpenLostAck,
        Fault::ForeignOpen,
        Fault::SealLostAck,
    ] {
        exercise(fault).await;
    }
}
#[tokio::test]
async fn never_started_original_is_joined_without_native_call() {
    let store = Arc::new(MemoryStore::new());
    let (context, original) = prepared(store.as_ref(), false).await;
    let owner = PendingDriverTasks::default();
    let seen = Arc::new(Seen {
        calls: Mutex::new(Vec::new()),
        drops: AtomicUsize::new(0),
    });
    let adapter = Arc::new(Forward {
        inner: store,
        fault: Fault::None,
        seen: seen.clone(),
        cached: Arc::new(Mutex::new(None)),
        gate: Arc::default(),
    });
    let mut producer =
        PendingInitialProducer::register(&owner, adapter, context, original).unwrap();
    let actual = producer.join().await.unwrap();
    assert_eq!(
        actual.status(),
        &PendingInitialProducerStatus::OriginalJoined {
            disposition: OwnedMainDisposition::NoDispatch
        }
    );
    assert!(seen.calls.lock().unwrap().is_empty());
}
#[test]
fn literal_poll_and_drop_children_are_contained() {
    let mut results = Vec::new();
    for operation in ["open", "complete", "seal"] {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "session_owner::owned_scope::task::pending::enrolled_initial::tests::literal_child",
                "--nocapture",
            ])
            .env("CRABBER_PENDING_INITIAL_LITERAL_CHILD", operation)
            .env("RUST_BACKTRACE", "0")
            .output()
            .unwrap();
        results.push((operation, output.status));
    }
    assert!(
        results.iter().all(|(_, status)| status.success()),
        "{results:?}"
    );
}
#[tokio::test]
async fn literal_child() {
    let Ok(operation) = std::env::var("CRABBER_PENDING_INITIAL_LITERAL_CHILD") else {
        return;
    };
    exercise(match operation.as_str() {
        "open" => Fault::OpenBoth,
        "complete" => Fault::CompleteBoth,
        "seal" => Fault::SealBoth,
        _ => panic!("unknown operation"),
    })
    .await;
}

#[tokio::test]
async fn genuine_foreign_open_reply_cannot_start_complete() {
    let store = Arc::new(MemoryStore::new());
    let cached = Arc::new(Mutex::new(None));
    for fault in [Fault::CaptureOpen, Fault::SubstituteOpen] {
        let owner = PendingDriverTasks::default();
        let (context, original) = prepared(store.as_ref(), false).await;
        let seen = Arc::new(Seen {
            calls: Mutex::new(Vec::new()),
            drops: AtomicUsize::new(0),
        });
        let adapter = Arc::new(Forward {
            inner: store.clone(),
            fault,
            seen: seen.clone(),
            cached: cached.clone(),
            gate: Arc::default(),
        });
        let mut producer =
            PendingInitialProducer::register(&owner, adapter, context, original).unwrap();
        producer.start().unwrap();
        let actual = producer.join().await.unwrap();
        assert!(actual.error().is_some());
        assert_eq!(
            actual.status(),
            &PendingInitialProducerStatus::ClosedNoPublication
        );
        assert!(!actual.observation_failed());
        assert_eq!(
            seen.calls
                .lock()
                .unwrap()
                .iter()
                .filter(|(op, _)| *op == Operation::Complete)
                .count(),
            0
        );
    }
    assert!(cached.lock().unwrap().is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_native_deadline_keeps_actual_blocked_drop_visible_and_retained() {
    blocked_drop_cases(false).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_ready_native_reply_stays_private_through_actual_blocked_drop() {
    blocked_drop_cases(true).await;
}
fn count_calls(seen: &Seen, operation: Operation) -> usize {
    seen.calls
        .lock()
        .unwrap()
        .iter()
        .filter(|(actual, _)| *actual == operation)
        .count()
}
async fn blocked_drop_cases(ready: bool) {
    for (index, (fault, pending)) in [
        (
            Fault::OpenBlocked,
            PendingInitialProducerStatus::OpenDisposalPending,
        ),
        (
            Fault::CompleteBlocked,
            PendingInitialProducerStatus::CompleteDisposalPending,
        ),
        (
            Fault::SealBlocked,
            PendingInitialProducerStatus::SealDisposalPending,
        ),
    ]
    .into_iter()
    .enumerate()
    {
        let fault = if ready {
            [
                Fault::OpenReadyBlocked,
                Fault::CompleteReadyBlocked,
                Fault::SealReadyBlocked,
            ][index]
        } else {
            fault
        };
        let store = Arc::new(MemoryStore::new());
        let owner = PendingDriverTasks::default();
        let (context, original) = prepared(store.as_ref(), false).await;
        let seen = Arc::new(Seen {
            calls: Mutex::new(Vec::new()),
            drops: AtomicUsize::new(0),
        });
        let gate = Arc::new(DropGate::default());
        // Independent of Tokio's timer driver, including when the worker
        // currently advancing that driver is inside the literal destructor.
        let mut release = ReleaseDrop::new(gate.clone());
        let adapter = Arc::new(Forward {
            inner: store,
            fault,
            seen: seen.clone(),
            cached: Arc::new(Mutex::new(None)),
            gate: gate.clone(),
        });
        let mut producer =
            PendingInitialProducer::register(&owner, adapter, context, original).unwrap();
        let control = producer.control();
        producer.start().unwrap();
        let entered = tokio::time::timeout(
            NATIVE_BOUND + Duration::from_secs(3),
            gate.entered.notified(),
        )
        .await;
        let pending_status = control.status();
        let joined_while_held = control.original_join_observed();
        let complete_before_drop = count_calls(&seen, Operation::Complete);
        let seal_before_drop = count_calls(&seen, Operation::Seal);
        let retained_capacity = owner.available_capacity();
        let premature_join = {
            let mut join = Box::pin(producer.join());
            futures::poll!(&mut join).is_ready()
        };
        // Always unblock the literal before any assertion can unwind the test.
        let watchdog_expired = release.release();
        let observed = tokio::time::timeout(Duration::from_secs(3), producer.join())
            .await
            .unwrap()
            .unwrap();
        assert!(entered.is_ok());
        assert!(
            !watchdog_expired,
            "independent watchdog had to release literal Drop"
        );
        assert_eq!(pending_status, pending);
        assert_eq!(joined_while_held, index == 2);
        assert_eq!(complete_before_drop, usize::from(index != 0));
        assert_eq!(seal_before_drop, usize::from(index == 2));
        assert_eq!(retained_capacity, 63);
        assert!(!premature_join);
        assert_eq!(observed.observation_failed(), !ready);
        if ready {
            assert_eq!(
                observed.status(),
                &PendingInitialProducerStatus::ClosedAssignment
            );
            assert!(observed.error().is_none(), "{:?}", observed.error());
        }
        assert!(control.original_join_observed());
        assert_eq!(seen.drops.load(Ordering::SeqCst), 1);
        owner.wait_for_supervision().await;
        assert_eq!(owner.available_capacity(), 64);
    }
}
