//! One operation's real Proof, retained through returned-value use and disposal.
use super::super::{Arc, MAX_PROOF, Proof, RetainedInitial};
use crate::native_enrollment::initial_consumer::CallKind;
use std::ops::{Deref, DerefMut};

pub(in crate::session_owner::owned_scope::task::pending::enrolled_initial) struct OperationProof {
    proof: Proof,
    #[cfg(test)]
    observation: (Harness, CallKind, usize, bool),
}
impl OperationProof {
    pub(super) fn new(retained: &Arc<RetainedInitial>, kind: CallKind) -> Self {
        #[cfg(test)]
        {
            let harness = retained.calls.budget.lock().unwrap().clone();
            let cap = harness
                .caps
                .0
                .iter()
                .find_map(|(selected, cap)| (*selected == kind).then_some(*cap))
                .unwrap_or(MAX_PROOF);
            Self {
                proof: Proof::bounded(cap),
                observation: (harness, kind, cap, false),
            }
        }
        #[cfg(not(test))]
        {
            let _ = (retained, kind);
            Self {
                proof: Proof::bounded(MAX_PROOF),
            }
        }
    }
    #[cfg(test)]
    pub(super) fn acknowledged(&mut self) {
        #[cfg(test)]
        {
            self.observation.3 = true;
        }
    }
}
impl Deref for OperationProof {
    type Target = Proof;
    fn deref(&self) -> &Proof {
        &self.proof
    }
}
impl DerefMut for OperationProof {
    fn deref_mut(&mut self) -> &mut Proof {
        &mut self.proof
    }
}
#[cfg(test)]
impl Drop for OperationProof {
    fn drop(&mut self) {
        let (harness, kind, cap, acknowledged) = &self.observation;
        harness.usage.lock().unwrap().push(ConsumerProofUsage {
            kind: *kind,
            used: self.proof.used(),
            acknowledged: *acknowledged && self.proof.used() <= *cap,
        });
    }
}
#[cfg(test)]
#[derive(Clone, Default)]
pub(crate) struct ConsumerProofCaps(pub Vec<(CallKind, usize)>);
#[cfg(test)]
#[derive(Clone, Copy)]
pub(crate) struct ConsumerProofUsage {
    pub kind: CallKind,
    pub used: usize,
    pub acknowledged: bool,
}
#[cfg(test)]
#[derive(Clone, Default)]
pub(super) struct Harness {
    caps: ConsumerProofCaps,
    usage: Arc<std::sync::Mutex<Vec<ConsumerProofUsage>>>,
    fixed_presentation: bool,
}
#[cfg(test)]
impl super::Calls {
    pub(in crate::session_owner::owned_scope::task::pending::enrolled_initial) fn fixed_presentation(
        &self,
    ) {
        self.budget.lock().unwrap().fixed_presentation = true;
    }
}
#[cfg(test)]
impl super::super::PendingInitialProducer {
    /// Applies limits at the actual per-call Proof constructor, never by reset.
    pub(crate) fn register_scripted_with_caps(
        owner: &super::super::PendingDriverTasks,
        store: Arc<dyn crate::Store>,
        context: crabber_core::SessionContext,
        original: crate::session_owner::preparation::JoinedPreparation,
        source: crabber_extension::LibraryScriptedSource,
        caps: ConsumerProofCaps,
        usage: Arc<std::sync::Mutex<Vec<ConsumerProofUsage>>>,
    ) -> Result<Self, super::super::PreparationCommitError> {
        let task = Self::register_scripted(owner, store, context, original, source)?;
        *task.retained.calls.budget.lock().unwrap() = Harness {
            caps,
            usage,
            fixed_presentation: true,
        };
        Ok(task)
    }
}

pub(super) fn created_at(calls: &super::Calls) -> time::OffsetDateTime {
    #[cfg(test)]
    if calls.budget.lock().unwrap().fixed_presentation {
        return time::OffsetDateTime::UNIX_EPOCH;
    }
    #[cfg(not(test))]
    let _ = calls;
    time::OffsetDateTime::now_utc()
}
pub(super) fn elapsed(calls: &super::Calls, started: std::time::Instant) -> std::time::Duration {
    #[cfg(test)]
    if calls.budget.lock().unwrap().fixed_presentation {
        return std::time::Duration::ZERO;
    }
    #[cfg(not(test))]
    let _ = calls;
    started.elapsed()
}
