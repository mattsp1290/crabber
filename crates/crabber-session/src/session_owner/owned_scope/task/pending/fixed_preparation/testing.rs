//! Closed diagnostics for the actual fixed factory/output and real operation caps.
use super::{
    Arc, AtomicBool, Error, FixedPreparationLoan, FixedPreparationOwner, JoinedOwnedTask,
    MemoryStore, Mutex, OnceLock, Ordering, OriginalPolicyRetirementView, PreparationGrant, Weak,
};
use crate::session_owner::owned_scope::task::pending::enrolled_initial::{
    InitialProofUsage, NativeInitialOperation,
};
use std::sync::atomic::AtomicUsize;
#[derive(Default)]
pub(crate) struct FixedInitialTestHooks {
    pub(crate) factory_entered: tokio::sync::Notify,
    pub(crate) factory_release: tokio::sync::Notify,
    hold_factory: AtomicBool,
    pub(crate) loan_returned: tokio::sync::Notify,
    pub(crate) loan_release: tokio::sync::Notify,
    hold_loan: AtomicBool,
    pub(crate) inner_registered: tokio::sync::Notify,
    pub(crate) inner_release: tokio::sync::Notify,
    hold_inner: AtomicBool,
    panic_factory: AtomicBool,
    lose_activation_ack: AtomicBool,
    refuse_seal_ack: AtomicBool,
    owner: OnceLock<Weak<FixedPreparationOwner>>,
    output: Mutex<Option<Arc<OutputGate>>>,
    capture_original: AtomicBool,
    captured_original: Mutex<Option<super::JoinedPreparation>>,
    substitute_original: Mutex<Option<super::JoinedPreparation>>,
    substitute_core: Mutex<Option<Arc<super::FixedMemoryInitialControl>>>,
    substitute_source: Mutex<Option<crabber_extension::LibraryScriptedSource>>,
    seal_cap: AtomicUsize,
    consumer_caps: Mutex<Vec<(crate::native_enrollment::initial_consumer::CallKind, usize)>>,
    fixed_seal: AtomicBool,
    pub(crate) usage: Arc<Mutex<InitialProofUsage>>,
}
impl FixedInitialTestHooks {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            seal_cap: AtomicUsize::new(32 << 20),
            ..Self::default()
        })
    }
    pub(in crate::session_owner::owned_scope::task::pending) fn capture_original(&self) {
        self.capture_original.store(true, Ordering::Release);
    }
    pub(in crate::session_owner::owned_scope::task::pending) fn take_original(
        &self,
    ) -> Option<super::JoinedPreparation> {
        self.captured_original.lock().unwrap().take()
    }
    pub(in crate::session_owner::owned_scope::task::pending) fn substitute_original(
        &self,
        original: super::JoinedPreparation,
    ) {
        *self.substitute_original.lock().unwrap() = Some(original);
    }
    pub(in crate::session_owner::owned_scope::task::pending) fn substitute_core(
        &self,
        core: Arc<super::FixedMemoryInitialControl>,
    ) {
        *self.substitute_core.lock().unwrap() = Some(core);
    }
    pub(in crate::session_owner::owned_scope::task::pending) fn substitute_source(
        &self,
        source: crabber_extension::LibraryScriptedSource,
    ) {
        *self.substitute_source.lock().unwrap() = Some(source);
    }
    pub(in crate::session_owner::owned_scope::task::pending) fn original(
        &self,
        original: super::JoinedPreparation,
    ) -> Option<super::JoinedPreparation> {
        if self.capture_original.load(Ordering::Acquire) {
            *self.captured_original.lock().unwrap() = Some(original);
            None
        } else {
            Some(
                self.substitute_original
                    .lock()
                    .unwrap()
                    .take()
                    .unwrap_or(original),
            )
        }
    }
    pub(in crate::session_owner::owned_scope::task::pending) fn core(
        &self,
    ) -> Option<Arc<super::FixedMemoryInitialControl>> {
        self.substitute_core.lock().unwrap().take()
    }
    pub(in crate::session_owner::owned_scope::task::pending) fn source(
        &self,
        source: crabber_extension::LibraryScriptedSource,
    ) -> crabber_extension::LibraryScriptedSource {
        self.substitute_source
            .lock()
            .unwrap()
            .take()
            .unwrap_or(source)
    }
    pub(crate) fn set_consumer_cap(
        &self,
        kind: crate::native_enrollment::initial_consumer::CallKind,
        cap: usize,
    ) {
        let mut caps = self.consumer_caps.lock().unwrap();
        if let Some((_, value)) = caps.iter_mut().find(|(selected, _)| *selected == kind) {
            *value = cap;
        } else {
            caps.push((kind, cap));
        }
    }
    pub(crate) fn set_seal_cap(&self, cap: usize) {
        self.seal_cap.store(cap, Ordering::Release);
    }
    pub(crate) fn usage(&self) -> Arc<Mutex<InitialProofUsage>> {
        self.usage.clone()
    }
    pub(crate) fn enable_fixed_seal_presentation(&self) {
        self.fixed_seal.store(true, Ordering::Release);
    }
    pub(crate) fn lose_activation_ack(&self) {
        self.lose_activation_ack.store(true, Ordering::Release);
    }
    pub(crate) fn refuse_next_seal_ack(&self) {
        self.refuse_seal_ack.store(true, Ordering::Release);
    }
    pub(crate) fn panic_factory(&self) {
        self.panic_factory.store(true, Ordering::Release);
    }
    pub(in crate::session_owner::owned_scope::task::pending) fn owner(
        &self,
    ) -> Arc<FixedPreparationOwner> {
        self.owner
            .get()
            .and_then(Weak::upgrade)
            .expect("actual installed owner remains retained")
    }
    pub(crate) fn hold_factory(&self) {
        self.hold_factory.store(true, Ordering::Release);
    }
    pub(crate) fn hold_returned_loan(&self) {
        self.hold_loan.store(true, Ordering::Release);
    }
    pub(crate) fn hold_registered_inner(&self) {
        self.hold_inner.store(true, Ordering::Release);
    }
    pub(crate) fn output_gate(&self, panic: bool) -> Arc<OutputGate> {
        let gate = Arc::new(OutputGate {
            state: Mutex::new(false),
            changed: std::sync::Condvar::new(),
            entered: tokio::sync::Notify::new(),
            before_drain: tokio::sync::Notify::new(),
            drain_release: tokio::sync::Notify::new(),
            checkpointed: AtomicBool::new(false),
            panic,
        });
        *self.output.lock().unwrap() = Some(gate.clone());
        gate
    }
    pub(in crate::session_owner::owned_scope::task::pending) async fn after_loan(&self) {
        if self.hold_loan.load(Ordering::Acquire) {
            self.loan_returned.notify_one();
            self.loan_release.notified().await;
        }
    }
    pub(in crate::session_owner::owned_scope::task::pending) async fn after_inner(&self) {
        if self.hold_inner.load(Ordering::Acquire) {
            self.inner_registered.notify_one();
            self.inner_release.notified().await;
        }
    }
    pub(super) async fn factory(&self) {
        if self.hold_factory.load(Ordering::Acquire) {
            self.factory_entered.notify_one();
            self.factory_release.notified().await;
        }
        assert!(
            !self.panic_factory.load(Ordering::Acquire),
            "actual fixed literal factory panicked"
        );
    }
    pub(super) async fn before_output_drain(&self) {
        let gate = self.output.lock().unwrap().clone();
        if let Some(gate) = gate
            && !gate.checkpointed.swap(true, Ordering::AcqRel)
        {
            gate.before_drain.notify_one();
            gate.drain_release.notified().await;
        }
    }
    pub(super) fn output(&self) -> Option<OutputProbe> {
        self.output.lock().unwrap().clone().map(OutputProbe)
    }
}
pub(crate) struct OutputGate {
    state: Mutex<bool>,
    changed: std::sync::Condvar,
    pub(crate) entered: tokio::sync::Notify,
    pub(crate) before_drain: tokio::sync::Notify,
    pub(crate) drain_release: tokio::sync::Notify,
    checkpointed: AtomicBool,
    panic: bool,
}
impl OutputGate {
    pub(crate) fn release(&self) {
        *self.state.lock().unwrap() = true;
        self.changed.notify_all();
        self.drain_release.notify_one();
    }
    pub(crate) fn release_on_drop(self: &Arc<Self>) -> OutputRelease {
        OutputRelease {
            gate: self.clone(),
            watchdog: None,
        }
    }
}
pub(super) struct OutputProbe(Arc<OutputGate>);
impl Drop for OutputProbe {
    fn drop(&mut self) {
        self.0.entered.notify_one();
        let mut released = self.0.state.lock().unwrap();
        while !*released {
            released = self.0.changed.wait(released).unwrap();
        }
        drop(released);
        assert!(
            !self.0.panic,
            "actual fixed preparation output disposal failed"
        );
    }
}
pub(super) struct ActivationFault<'a>(&'a MemoryStore);
impl Drop for ActivationFault<'_> {
    fn drop(&mut self) {
        self.0.fixed_preparation_owner_fault(0);
    }
}
impl FixedPreparationOwner {
    pub(super) fn activation_fault(&self) -> Option<ActivationFault<'_>> {
        if self
            .test
            .get()
            .is_some_and(|test| test.lose_activation_ack.load(Ordering::Acquire))
        {
            self.core.actual_store().fixed_preparation_owner_fault(2);
            Some(ActivationFault(self.core.actual_store()))
        } else {
            None
        }
    }
}
impl FixedPreparationLoan {
    pub(in crate::session_owner::owned_scope::task::pending) fn refuse_seal_ack(&self) -> bool {
        self.owner
            .test
            .get()
            .is_some_and(|test| test.refuse_seal_ack.swap(false, Ordering::AcqRel))
    }

    pub(in crate::session_owner::owned_scope::task::pending) fn test_consumer_caps(
        &self,
    ) -> super::super::enrolled_initial::ConsumerProofCaps {
        super::super::enrolled_initial::ConsumerProofCaps(
            self.owner
                .test
                .get()
                .map(|test| test.consumer_caps.lock().unwrap().clone())
                .unwrap_or_default(),
        )
    }

    pub(in crate::session_owner::owned_scope::task::pending) fn test_harness(
        &self,
    ) -> super::super::enrolled_initial::budget::Harness {
        let mut harness = super::super::enrolled_initial::budget::Harness::default();
        if let Some(test) = self.owner.test.get() {
            harness.usage = test.usage();
        }
        harness
    }
    pub(in crate::session_owner::owned_scope::task::pending) fn test_cap(
        &self,
        operation: NativeInitialOperation,
    ) -> Option<usize> {
        (operation == NativeInitialOperation::Seal)
            .then(|| {
                self.owner
                    .test
                    .get()
                    .map(|test| test.seal_cap.load(Ordering::Acquire))
            })
            .flatten()
    }
    pub(in crate::session_owner::owned_scope::task::pending) fn test_configured(&self) -> bool {
        self.owner.test.get().is_some()
    }
}
impl OriginalPolicyRetirementView<'_> {
    pub(crate) fn presentation_now(&self, actual: time::OffsetDateTime) -> time::OffsetDateTime {
        if self
            .owner
            .test
            .get()
            .is_some_and(|test| test.fixed_seal.load(Ordering::Acquire))
        {
            *self.owner.seal_presentation.get_or_init(|| actual)
        } else {
            actual
        }
    }
}
impl FixedPreparationOwner {
    pub(in crate::session_owner::owned_scope::task::pending) fn install_test_hooks(
        self: &Arc<Self>,
        test: Arc<FixedInitialTestHooks>,
    ) -> Result<(), Error> {
        test.owner
            .set(Arc::downgrade(self))
            .map_err(|_| Error::InvalidTransition)?;
        self.test.set(test).map_err(|_| Error::InvalidTransition)
    }
}

impl FixedPreparationOwner {
    pub(in crate::session_owner::owned_scope::task::pending) fn test_targets(
        &self,
    ) -> (
        Option<crate::session_owner::SessionOwnerTarget>,
        Option<crate::session_owner::SessionOwnerTarget>,
    ) {
        (
            self.root.get().map(|authority| authority.target().clone()),
            self.grant
                .get()
                .and_then(PreparationGrant::retained_resources_authority)
                .map(|authority| authority.target().clone()),
        )
    }
    pub(in crate::session_owner::owned_scope::task::pending) fn test_joined_disposition(
        &self,
    ) -> Option<super::super::OwnedMainDisposition> {
        self.joined.get().map(JoinedOwnedTask::disposition)
    }
}
pub(crate) struct OutputRelease {
    gate: Arc<OutputGate>,
    watchdog: Option<std::thread::JoinHandle<bool>>,
}
impl OutputGate {
    pub(crate) fn watchdog(self: &Arc<Self>) -> OutputRelease {
        let gate = self.clone();
        let watchdog = std::thread::spawn(move || {
            let (mut released, _) = gate
                .changed
                .wait_timeout_while(
                    gate.state.lock().unwrap(),
                    std::time::Duration::from_secs(8),
                    |released| !*released,
                )
                .unwrap();
            let expired = !*released;
            *released = true;
            gate.changed.notify_all();
            expired
        });
        OutputRelease {
            gate: self.clone(),
            watchdog: Some(watchdog),
        }
    }
}
impl OutputRelease {
    pub(crate) fn release(&mut self) -> bool {
        self.gate.release();
        self.watchdog
            .take()
            .is_some_and(|watchdog| watchdog.join().unwrap())
    }
}
impl Drop for OutputRelease {
    fn drop(&mut self) {
        self.release();
    }
}
