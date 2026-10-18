//! Test-only limits at actual `Proof` construction, never replacement or padding.
use super::{
    Arc, JoinedPreparation, MAX_PROOF, Mutex, Operation, PendingDriverTasks,
    PendingInitialProducer, PreparationCommitError, PreparedSource, SessionContext, Store,
};

#[derive(Clone, Copy)]
pub(crate) struct InitialProofCaps {
    pub open: usize,
    pub complete: usize,
    pub seal: usize,
}
impl Default for InitialProofCaps {
    fn default() -> Self {
        Self {
            open: MAX_PROOF,
            complete: MAX_PROOF,
            seal: MAX_PROOF,
        }
    }
}
impl InitialProofCaps {
    pub(super) fn cap(self, operation: Operation) -> usize {
        match operation {
            Operation::Open => self.open,
            Operation::Complete => self.complete,
            Operation::Seal => self.seal,
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct InitialOperationUsage {
    pub used: usize,
    pub acknowledged: bool,
}
#[derive(Default)]
pub(crate) struct InitialProofUsage {
    pub constructor: usize,
    pub open: Option<InitialOperationUsage>,
    pub complete: Option<InitialOperationUsage>,
    pub seal: Vec<InitialOperationUsage>,
}
#[derive(Default)]
pub(super) struct Harness {
    pub(super) caps: InitialProofCaps,
    pub(super) usage: Arc<Mutex<InitialProofUsage>>,
}
impl Harness {
    pub(super) fn constructor(&self, used: usize) {
        self.usage.lock().unwrap().constructor = used;
    }
    pub(super) fn record(&self, operation: Operation, used: usize, acknowledged: bool) {
        let mut usage = self.usage.lock().unwrap();
        let value = InitialOperationUsage { used, acknowledged };
        match operation {
            Operation::Open => usage.open = Some(value),
            Operation::Complete => usage.complete = Some(value),
            Operation::Seal => usage.seal.push(value),
        }
    }
}
impl PendingInitialProducer {
    /// Applies real caps before the constructor's first measurement. Every
    /// selected native carrier and returned ACK uses that operation's `Proof`.
    pub(crate) fn register_with_caps(
        owner: &PendingDriverTasks,
        store: Arc<dyn Store>,
        context: SessionContext,
        original: JoinedPreparation,
        caps: InitialProofCaps,
        usage: Arc<Mutex<InitialProofUsage>>,
    ) -> Result<Self, PreparationCommitError> {
        let budget = Harness { caps, usage };
        let (prepared, used) = PreparedSource::bounded(context, original, caps.open);
        budget.constructor(used);
        Self::register_prepared(owner, store, prepared?, budget)
    }
}

impl PendingInitialProducer {
    /// Real constructor/Open, Complete and Seal caps for the same scripted
    /// registration path. Presentation alone is fixed for numerical tests;
    /// native lease clocks, Proofs, permission and disposal stay unchanged.
    pub(crate) fn register_scripted_with_initial_caps(
        owner: &PendingDriverTasks,
        store: Arc<dyn Store>,
        context: SessionContext,
        original: JoinedPreparation,
        source: crabber_extension::LibraryScriptedSource,
        limits: (InitialProofCaps, Arc<Mutex<InitialProofUsage>>),
    ) -> Result<Self, PreparationCommitError> {
        let (caps, usage) = limits;
        let budget = Harness { caps, usage };
        let (prepared, used) = PreparedSource::bounded(context, original, caps.open);
        budget.constructor(used);
        let mut prepared = prepared?;
        let resources = (|| {
            super::scripted_binding::verify(
                &source,
                &prepared.source.original,
                &mut prepared.proof,
            )?;
            super::consumer::InitialResources::new(
                source,
                prepared.source.clone(),
                &mut prepared.proof,
            )
        })();
        budget.constructor(prepared.proof.used());
        let task = Self::register_with_resources(owner, store, prepared, Some(resources?), budget)?;
        task.retained.calls.fixed_presentation();
        Ok(task)
    }
}
