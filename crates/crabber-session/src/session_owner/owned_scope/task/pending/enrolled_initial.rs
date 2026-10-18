//! Fixed initial birth producer. No Driver registration, activation or business.
#[cfg(test)]
pub(super) mod budget;
mod consumer;
mod lifecycle;
pub(super) mod observe;
mod retirement;
mod scripted_binding;
pub(crate) use scripted_binding::{
    FixedInitialProfile, fixed_initial_profile, fixed_initial_resource_contract,
    verify_fixed_binding,
};
mod source;
mod transport;
use super::super::driver::MainIngress;
use super::{
    Error, OwnedMainDisposition, PendingDriverTasks,
    core::{self, Control},
};
use crate::native_enrollment::{
    caller::initial::with_initial_original_async,
    pending_initial::{self as protocol, Correlation, Identity, OpenRecord},
};
use crate::{
    Store,
    public_session::v2::proof::Proof,
    session_owner::preparation::{JoinedPreparation, PreparationCommitError},
};
#[cfg(test)]
pub(crate) use budget::{InitialOperationUsage, InitialProofCaps, InitialProofUsage};
pub(crate) use consumer::{
    DrainedInitialResources, InitialResourceAssociation, NativeConsumerParts,
};
pub(in crate::session_owner::owned_scope::task::pending) use consumer::{
    FreshBegun, InitialResources,
};
pub use consumer::{PendingInitialConsumerNativeCall, PendingInitialConsumerNativeReply};
use crabber_core::{ControlId, RunId, SessionContext};
pub(crate) use retirement::{InitialRetirementView, InitialRetirementWitness};
use source::{PreparedSource, RetainedInitialSource};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::task::Id;
pub(crate) use transport::{NativeInitialOperation, NativeInitialParts};
use transport::{NativeInitialOperation as Operation, Reply};
pub use transport::{PendingInitialNativeCall, PendingInitialNativeReply};

const MAX_PROOF: usize = 32 << 20;
const NATIVE_BOUND: Duration = Duration::from_secs(5);

/// Safe progress only. These values are neither authority nor `MainJoin` evidence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PendingInitialProducerStatus {
    Parked,
    Armed,
    Opened,
    Published { run_id: RunId },
    SourceAlreadyCompletedRaw { run_id: RunId },
    OriginalJoined { disposition: OwnedMainDisposition },
    SealDisposalPending,
    OpenDisposalPending,
    CompleteDisposalPending,
    ConsumerDisposalPending,
    ClosedNoPublication,
    ClosedAssignment,
    Unresolved,
}

struct State {
    task: Option<Id>,
    ingress: Option<Arc<MainIngress>>,
    control: Option<Control>,
    identity: Option<Arc<Identity>>,
    status: PendingInitialProducerStatus,
    staged: Option<Result<PendingInitialNativeReply, PreparationCommitError>>,
    enrolled: Option<Arc<protocol::EnrolledInitialData>>,
    joined: bool,
    observation_failed: bool,
}
pub(in crate::session_owner::owned_scope::task::pending) struct RetainedInitial {
    source: Arc<RetainedInitialSource>,
    store: Arc<dyn Store>,
    state: Mutex<State>,
    consumer: Option<Arc<consumer::InitialResources>>,
    calls: consumer::Calls,
    fixed_preparation: Option<super::fixed_preparation::FixedPreparationLoan>,
    #[cfg(test)]
    budget: budget::Harness,
}
impl RetainedInitial {
    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
    pub(in crate::session_owner::owned_scope::task::pending) fn attach(
        &self,
        task: Id,
        ingress: Arc<MainIngress>,
        control: Control,
    ) -> Result<(), Error> {
        if let Some(preparation) = &self.fixed_preparation {
            preparation.registered(task)?;
        }
        if let Some(resources) = &self.consumer {
            resources.registered(task)?;
            control.install_resources(&resources.owner)?;
        }
        let mut state = self.lock();
        state.task = Some(task);
        state.ingress = Some(ingress);
        state.control = Some(control);
        Ok(())
    }
    fn status(&self, value: PendingInitialProducerStatus) {
        self.lock().status = value;
    }
    fn new_proof(operation: Operation, #[cfg(test)] retained: &Self) -> Proof {
        #[cfg(test)]
        let cap = retained
            .fixed_preparation
            .as_ref()
            .and_then(|loan| loan.test_cap(operation))
            .unwrap_or_else(|| retained.budget.caps.cap(operation));
        #[cfg(not(test))]
        let cap = {
            let _ = operation;
            MAX_PROOF
        };
        Proof::bounded(cap)
    }
    fn original_lease(self: &Arc<Self>) -> Result<Option<LocalLease>, PreparationCommitError> {
        let control = self
            .lock()
            .control
            .clone()
            .ok_or(Error::InvalidTransition)?;
        Ok(control.enter_call()?.map(|call| LocalLease {
            retained: self.clone(),
            _call: Some(call),
            joined: None,
        }))
    }
    fn identity(&self) -> Result<Arc<Identity>, PreparationCommitError> {
        self.lock()
            .identity
            .clone()
            .ok_or(Error::InvalidTransition.into())
    }
    fn arm(
        self: &Arc<Self>,
        prepared: &protocol::prefix::ExpectedSource,
        proof: &mut Proof,
    ) -> Result<Arc<Correlation>, PreparationCommitError> {
        let state = self.lock();
        if state.task != tokio::task::try_id()
            || state.joined
            || state.identity.is_some()
            || state
                .ingress
                .as_ref()
                .is_none_or(|ingress| ingress.is_closed())
        {
            return Err(Error::InvalidTransition.into());
        }
        drop(state);
        let identity = Identity {
            epoch: ControlId::new(),
            complete: ControlId::new(),
            seal: ControlId::new(),
        };
        let expected = prepared.correlation(&identity, proof)?;
        let mut state = self.lock();
        state.identity = Some(Arc::new(identity));
        state.status = PendingInitialProducerStatus::Armed;
        Ok(Arc::new(expected))
    }
    fn stage(&self, result: Result<PendingInitialNativeReply, PreparationCommitError>) {
        self.lock().staged = Some(result);
    }
    fn take_staged(&self) -> Result<PendingInitialNativeReply, PreparationCommitError> {
        self.lock()
            .staged
            .take()
            .ok_or(Error::UnknownStoreOutcome)?
    }
    fn stopped(&self) -> bool {
        self.lock().control.as_ref().is_none_or(Control::stopped)
    }
    fn fail(&self) {
        let mut state = self.lock();
        state.status = PendingInitialProducerStatus::Unresolved;
        if let Some(control) = &state.control {
            control.request_stop();
        }
    }
    async fn invoke(
        self: &Arc<Self>,
        operation: Operation,
        expected: &Correlation,
        opened: Option<&OpenRecord>,
        proof: &mut Proof,
        lease: LocalLease,
        deadline: Option<tokio::time::Instant>,
    ) -> Result<Reply, PreparationCommitError> {
        let result =
            with_initial_original_async(&self.source.original, proof, |selected, proof| {
                Box::pin(async move {
                    let consumer = self
                        .consumer
                        .as_ref()
                        .map(|resources| resources.association())
                        .transpose()?;
                    let retirement = match (lease.joined, consumer.as_ref()) {
                        (Some(disposition), Some(association)) => Some(
                            InitialRetirementWitness::new(self, disposition, association, expected),
                        ),
                        _ => None,
                    };
                    let policy = self
                        .fixed_preparation
                        .as_ref()
                        .zip(consumer.as_ref())
                        .map(|(loan, association)| loan.view(association));
                    let policy_retirement = if operation == Operation::Seal {
                        policy
                            .as_ref()
                            .map(super::fixed_preparation::OriginalPolicyView::retirement)
                    } else {
                        None
                    };
                    let call = PendingInitialNativeCall {
                        source: &self.source,
                        expected,
                        operation,
                        opened,
                        selected,
                        proof: &mut *proof,
                        lease: &lease,
                        consumer: consumer.as_ref(),
                        retirement: retirement.as_ref(),
                        original_policy: policy.as_ref(),
                        policy_retirement: policy_retirement.as_ref(),
                    };
                    let observed = observe::original(
                        || self.store.pending_initial_native(call),
                        |result| self.stage(result),
                        deadline,
                        || {
                            self.status(match operation {
                                Operation::Open => {
                                    PendingInitialProducerStatus::OpenDisposalPending
                                }
                                Operation::Complete => {
                                    PendingInitialProducerStatus::CompleteDisposalPending
                                }
                                Operation::Seal => {
                                    PendingInitialProducerStatus::SealDisposalPending
                                }
                            });
                        },
                        || {
                            self.lock().observation_failed = true;
                            self.fail();
                        },
                    )
                    .await;
                    if observed.is_err() {
                        self.lock().observation_failed = true;
                        self.fail();
                        return Err(Error::UnknownStoreOutcome.into());
                    }
                    let result = self.take_staged()?.correlated(expected, selected, proof);
                    if result.is_err() {
                        self.fail();
                    }
                    result
                })
            })
            .await;
        #[cfg(test)]
        self.budget.record(operation, proof.used(), result.is_ok());
        result
    }
}

struct LocalLease {
    retained: Arc<RetainedInitial>,
    _call: Option<core::CallAdmission>,
    joined: Option<OwnedMainDisposition>,
}
impl LocalLease {
    fn verify(
        &self,
        operation: Operation,
        source: &RetainedInitialSource,
        expected: &Correlation,
    ) -> Result<(), PreparationCommitError> {
        let state = self.retained.lock();
        if !std::ptr::eq(source, self.retained.source.as_ref())
            || state.identity.as_deref() != Some(&expected.identity)
            || &expected.key.session_id != source.context.session_id()
        {
            return Err(Error::IdentityMismatch.into());
        }
        if self.joined.is_some() {
            if operation != Operation::Seal || !state.joined || state.identity.is_none() {
                return Err(Error::InvalidTransition.into());
            }
        } else if operation == Operation::Seal
            || state.joined
            || state.task != tokio::task::try_id()
            || state.identity.is_none()
            || state
                .ingress
                .as_ref()
                .is_none_or(|ingress| ingress.is_closed())
        {
            return Err(Error::InvalidTransition.into());
        }
        Ok(())
    }
}

async fn run(
    retained: Arc<RetainedInitial>,
    prepared: PreparedSource,
) -> Result<core::TaskOutput<()>, super::PendingSuccessorError> {
    let _resources = retained
        .consumer
        .as_ref()
        .map(|resources| resources.owner.guard());
    if let Some(resources) = &retained.consumer {
        if retained.stopped() {
            return Ok(core::TaskOutput::Stopped);
        }
        resources.attach()?;
        if retained.stopped() {
            return Ok(core::TaskOutput::Stopped);
        }
    }
    if let Some(resources) = &retained.consumer {
        let capability = resources.capability()?;
        crabber_extension::with_callback_invocations(
            capability,
            run_attached(retained.clone(), prepared),
        )
        .await
    } else {
        run_attached(retained.clone(), prepared).await
    }
}

async fn run_attached(
    retained: Arc<RetainedInitial>,
    mut prepared: PreparedSource,
) -> Result<core::TaskOutput<()>, super::PendingSuccessorError> {
    let expected = retained.arm(&prepared.expected, &mut prepared.proof)?;
    let Some(lease) = retained.original_lease()? else {
        return Ok(core::TaskOutput::Stopped);
    };
    let opened = match retained
        .invoke(
            Operation::Open,
            &expected,
            None,
            &mut prepared.proof,
            lease,
            Some(tokio::time::Instant::now() + NATIVE_BOUND),
        )
        .await?
    {
        Reply::Opened(opened) => Arc::new(opened),
        Reply::RawCompleted(raw) => {
            retained.status(PendingInitialProducerStatus::SourceAlreadyCompletedRaw {
                run_id: raw.run_id,
            });
            return Ok(core::TaskOutput::Stopped);
        }
        Reply::AlreadyOpened(_)
        | Reply::AlreadyPublished(_)
        | Reply::Closed(_)
        | Reply::Completed(_) => return Err(Error::UnknownStoreOutcome.into()),
    };
    retained.status(PendingInitialProducerStatus::Opened);
    if retained.stopped() {
        return Ok(core::TaskOutput::Stopped);
    }
    let mut proof = RetainedInitial::new_proof(
        Operation::Complete,
        #[cfg(test)]
        &retained,
    );
    let identity = retained.identity()?;
    let expected = protocol::prefix::expected(&retained.source.original, &identity, &mut proof)?;
    let Some(lease) = retained.original_lease()? else {
        return Ok(core::TaskOutput::Stopped);
    };
    let outcome = retained
        .invoke(
            Operation::Complete,
            &expected,
            Some(&opened),
            &mut proof,
            lease,
            Some(tokio::time::Instant::now() + NATIVE_BOUND),
        )
        .await;
    let accepted = outcome.and_then(|reply| {
        match reply {
            Reply::Completed(data) => {
                proof
                    .measure(&data.assignment.run_id)
                    .map_err(|_| Error::Capacity)?;
                retained.status(PendingInitialProducerStatus::Published {
                    run_id: data.assignment.run_id.clone(),
                });
                retained.lock().enrolled = Some(Arc::from(data));
            }
            Reply::RawCompleted(raw) => {
                retained.status(PendingInitialProducerStatus::SourceAlreadyCompletedRaw {
                    run_id: raw.run_id,
                });
            }
            Reply::AlreadyPublished(_)
            | Reply::AlreadyOpened(_)
            | Reply::Closed(_)
            | Reply::Opened(_) => return Err(Error::UnknownStoreOutcome.into()),
        }
        Ok(())
    });
    #[cfg(test)]
    retained
        .budget
        .record(Operation::Complete, proof.used(), accepted.is_ok());
    accepted?;
    if retained.consumer.is_some() {
        return consumer::run(&retained).await;
    }
    Ok(core::TaskOutput::Stopped)
}

/// Signals and reads safe progress for the same original producer.
#[derive(Clone)]
pub struct PendingInitialProducerControl {
    retained: Arc<RetainedInitial>,
    control: Control,
}
impl PendingInitialProducerControl {
    pub fn request_stop(&self) {
        self.control.request_stop();
    }
    #[must_use]
    pub fn status(&self) -> PendingInitialProducerStatus {
        self.retained.lock().status.clone()
    }
    /// Test diagnostic only: set by the existing retainer after awaiting the
    /// original `JoinHandle`. This neither admits native work nor claims `MainJoin`.
    #[cfg(test)]
    pub(crate) fn original_join_observed(&self) -> bool {
        self.retained.lock().joined
    }
    /// Test diagnostic for the SAME unaccepted opaque reply; exposes no payload.
    #[cfg(test)]
    pub(crate) fn consumer_reply_staged(&self) -> bool {
        self.retained.calls.has_staged_reply()
    }
}

/// Actual original-task observation plus native close metadata. No `MainJoin`.
pub struct PendingInitialProducerObservation {
    pub(super) status: PendingInitialProducerStatus,
    pub(super) disposition: OwnedMainDisposition,
    pub(super) retry: Option<LocallyJoinedInitialSeal>,
    pub(super) observation_failed: bool,
    pub(super) error: Option<super::PendingSuccessorError>,
}
impl PendingInitialProducerObservation {
    #[must_use]
    pub fn status(&self) -> &PendingInitialProducerStatus {
        &self.status
    }
    #[must_use]
    pub fn disposition(&self) -> OwnedMainDisposition {
        self.disposition
    }
    #[must_use]
    pub fn observation_failed(&self) -> bool {
        self.observation_failed
    }
    #[must_use]
    pub fn error(&self) -> Option<&super::PendingSuccessorError> {
        self.error.as_ref()
    }
    pub fn take_retry(&mut self) -> Option<LocallyJoinedInitialSeal> {
        self.retry.take()
    }
}

/// Fixed birth/Open/Seal producer over the existing parked-task supervisor.
pub struct PendingInitialProducer {
    task: core::Task<()>,
    retained: Arc<RetainedInitial>,
    observed: Option<PendingInitialProducerObservation>,
}
impl PendingInitialProducer {
    /// Retain the original source and park before any enrolled native I/O.
    /// # Errors
    /// Refuses unsupported or changed original source and shared capacity.
    pub fn register(
        owner: &PendingDriverTasks,
        store: Arc<dyn Store>,
        context: SessionContext,
        original: JoinedPreparation,
    ) -> Result<Self, PreparationCommitError> {
        let prepared = PreparedSource::new(context, original)?;
        #[cfg(test)]
        {
            let budget = budget::Harness::default();
            budget.constructor(prepared.proof.used());
            Self::register_prepared(owner, store, prepared, budget)
        }
        #[cfg(not(test))]
        {
            Self::register_prepared(owner, store, prepared)
        }
    }
    /// Register the fixed model-only consumer with the same original parked
    /// task. The pristine concrete source is consumed before start; no generic
    /// business closure, public fence bridge or replacement producer is used.
    /// # Errors
    /// Refuses unsupported source/profile, changed preparation or shared capacity.
    pub fn register_scripted(
        owner: &PendingDriverTasks,
        store: Arc<dyn Store>,
        context: SessionContext,
        original: JoinedPreparation,
        source: crabber_extension::LibraryScriptedSource,
    ) -> Result<Self, PreparationCommitError> {
        let mut prepared = PreparedSource::new(context, original)?;
        scripted_binding::verify(&source, &prepared.source.original, &mut prepared.proof)?;
        let resources =
            consumer::InitialResources::new(source, prepared.source.clone(), &mut prepared.proof)?;
        #[cfg(test)]
        {
            let budget = budget::Harness::default();
            budget.constructor(prepared.proof.used());
            Self::register_with_resources(owner, store, prepared, Some(resources), budget)
        }
        #[cfg(not(test))]
        {
            Self::register_with_resources(owner, store, prepared, Some(resources))
        }
    }
    pub(in crate::session_owner::owned_scope::task::pending) fn register_fixed_scripted(
        owner: &PendingDriverTasks,
        core: &Arc<super::fixed_memory::FixedMemoryInitialControl>,
        context: SessionContext,
        original: JoinedPreparation,
        source: crabber_extension::LibraryScriptedSource,
        loan: super::fixed_preparation::FixedPreparationLoan,
    ) -> Result<Self, PreparationCommitError> {
        let mut prepared = PreparedSource::new(context, original)?;
        loan.verify_intake(core, &prepared.source.original, &mut prepared.proof)?;
        scripted_binding::verify_fixed(&source, &prepared.source.original, &mut prepared.proof)?;
        let resources =
            consumer::InitialResources::new(source, prepared.source.clone(), &mut prepared.proof)?;
        loan.bind(&resources)?;
        #[cfg(test)]
        let budget = {
            let budget = loan.test_harness();
            budget.constructor(prepared.proof.used());
            budget
        };
        #[cfg(test)]
        let fixed_presentation = loan.test_configured();
        #[cfg(test)]
        let consumer_caps = loan.test_consumer_caps();
        let task = Self::register_with_preparation(
            owner,
            core.store().clone(),
            prepared,
            Some(resources),
            Some(loan),
            #[cfg(test)]
            budget,
        )?;
        #[cfg(test)]
        if fixed_presentation {
            task.retained.calls.fixed_presentation();
            task.retained.calls.fixed_test_caps(consumer_caps);
        }
        Ok(task)
    }
    pub(in crate::session_owner::owned_scope::task::pending) fn take_fixed_observation(
        &mut self,
    ) -> Option<PendingInitialProducerObservation> {
        self.observed.take()
    }
    fn register_prepared(
        owner: &PendingDriverTasks,
        store: Arc<dyn Store>,
        prepared: PreparedSource,
        #[cfg(test)] budget: budget::Harness,
    ) -> Result<Self, PreparationCommitError> {
        Self::register_with_resources(
            owner,
            store,
            prepared,
            None,
            #[cfg(test)]
            budget,
        )
    }
    fn register_with_resources(
        owner: &PendingDriverTasks,
        store: Arc<dyn Store>,
        prepared: PreparedSource,
        consumer: Option<Arc<consumer::InitialResources>>,
        #[cfg(test)] budget: budget::Harness,
    ) -> Result<Self, PreparationCommitError> {
        Self::register_with_preparation(
            owner,
            store,
            prepared,
            consumer,
            None,
            #[cfg(test)]
            budget,
        )
    }
    fn register_with_preparation(
        owner: &PendingDriverTasks,
        store: Arc<dyn Store>,
        prepared: PreparedSource,
        consumer: Option<Arc<consumer::InitialResources>>,
        fixed_preparation: Option<super::fixed_preparation::FixedPreparationLoan>,
        #[cfg(test)] budget: budget::Harness,
    ) -> Result<Self, PreparationCommitError> {
        let retained = Arc::new(RetainedInitial {
            source: prepared.source.clone(),
            store,
            consumer,
            calls: consumer::Calls::default(),
            fixed_preparation,
            state: Mutex::new(State {
                task: None,
                ingress: None,
                control: None,
                identity: None,
                status: PendingInitialProducerStatus::Parked,
                staged: None,
                enrolled: None,
                joined: false,
                observation_failed: false,
            }),
            #[cfg(test)]
            budget,
        });
        let current = retained.clone();
        let task = core::Task::register_body(
            owner,
            core::Family::EnrolledInitial,
            Some(core::RetainedOwner::Initial(retained.clone())),
            move |_, _| run(current, prepared),
        )?;
        Ok(Self {
            task,
            retained,
            observed: None,
        })
    }
    #[must_use]
    pub fn control(&self) -> PendingInitialProducerControl {
        PendingInitialProducerControl {
            retained: self.retained.clone(),
            control: self.task.control.clone(),
        }
    }
    /// Start the same original parked producer.
    /// # Errors
    /// The start gate was already consumed or closed.
    pub fn start(&mut self) -> Result<(), Error> {
        self.task.start()
    }
    /// Observe the original unbound task and its bounded native Seal attempt.
    /// # Errors
    /// Original supervision was lost or its private family was inconsistent.
    pub async fn join(&mut self) -> Result<&mut PendingInitialProducerObservation, Error> {
        if self.observed.is_none() {
            let mut observation = self.task.observation(core::Family::EnrolledInitial).await?;
            let mut protected = observation
                .protected
                .take()
                .ok_or(Error::UnknownStoreOutcome)?;
            protected.error = observation.error.take();
            self.observed = Some(protected);
        }
        self.observed.as_mut().ok_or(Error::UnknownStoreOutcome)
    }
}

pub use lifecycle::LocallyJoinedInitialSeal;

#[cfg(test)]
mod tests;

#[cfg(test)]
pub(crate) use scripted_binding::fixture as initial_consumer_fixture;

#[cfg(test)]
pub(crate) use consumer::{ConsumerProofCaps, ConsumerProofUsage};
