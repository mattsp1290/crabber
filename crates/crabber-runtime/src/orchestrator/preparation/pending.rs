//! Original retained preparation and the SAME preclaim main for both families.
#[path = "pending/factory.rs"]
mod factory;
#[path = "pending/main.rs"]
pub(in crate::orchestrator) mod main;
#[path = "pending/supervise.rs"]
mod supervise;
use super::{DurablePreparation, Orchestrator, RuntimeError, retained};
use crate::orchestrator::{RunHandle, worker};
use crabber_core::{AdmissionReceipt, SessionContext};
use crabber_session::session_owner::{
    owned_scope::{PendingInitialTask, PendingSuccessorReplay, PendingSuccessorTask},
    preparation::{PreparationCompletion, PreparationGrant},
};
use std::{
    future::Future,
    sync::{
        Arc, OnceLock,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::sync::{Mutex, oneshot};
use tokio_util::sync::CancellationToken;

pub(super) struct PendingPreparation {
    factory: Mutex<retained::OwnedEntry>,
    resources: crabber_extension::ToolResourceOwner,
    heartbeat: Mutex<retained::heartbeat::RetainedTask>,
    heartbeat_stop: CancellationToken,
    grant: PreparationGrant,
    main: OnceLock<Arc<Mutex<main::Main>>>,
    enrollment: std::sync::Mutex<()>,
    control: OnceLock<main::Control>,
    cancellation: CancellationToken,
    failed: AtomicBool,
    cleanup_active: AtomicBool,
    cleanup_failed: AtomicBool,
    committed: AtomicBool,
    completion: std::sync::Mutex<Option<main::Completion>>,
    cleanup: std::sync::Mutex<Option<Arc<crate::execution_tasks::ExecutionTask>>>,
    native_done: CancellationToken,
}
impl PendingPreparation {
    pub(super) fn failed(&self) -> bool {
        self.failed.load(Ordering::Acquire) || self.cleanup_failed.load(Ordering::Acquire)
    }
    fn existing_run(&self) -> Result<Option<crabber_core::RunId>, RuntimeError> {
        match &self.grant.original_reservation().purpose {
            super::PreparationPurpose::ExistingRun { run_id, .. } => Ok(Some(run_id.clone())),
            super::PreparationPurpose::OrdinaryAdmission => Ok(None),
            super::PreparationPurpose::Queue { .. } => {
                Err(RuntimeError::PreparationRecoveryRequired)
            }
        }
    }
    pub(super) fn committed(&self) -> bool {
        self.committed.load(Ordering::Acquire)
    }
    pub(super) fn retry_progress(
        &self,
        operation: crabber_session::session_owner::preparation::PreparationOperationKind,
    ) -> Result<super::progress::PreparationRetryProgress, RuntimeError> {
        if !self.committed.load(Ordering::Acquire) {
            return Err(RuntimeError::PreparationRecoveryRequired);
        }
        let metadata = self.completion.lock().unwrap();
        Ok(super::progress::PreparationRetryProgress {
            operation,
            replayed: metadata.as_ref().is_some_and(|metadata| metadata.replayed),
            unkeyed_admission: match metadata.as_ref().map(|completion| &completion.metadata) {
                Some(main::Metadata::Initial(
                    crabber_session::session_owner::owned_scope::InitialAdmissionReplay::Unkeyed(
                        receipt,
                    ),
                )) => Some(receipt.clone()),
                _ => None,
            },
        })
    }
    pub(super) fn stop(&self) {
        self.cancellation.cancel();
        if let Some(control) = self.control.get() {
            control.request_stop();
        }
    }
}
pub(in crate::orchestrator) enum ExistingOutcome {
    Started {
        handle: RunHandle,
        receipt: Option<AdmissionReceipt>,
    },
    Replayed(PendingSuccessorReplay),
}
pub(in crate::orchestrator) enum PendingOutcome {
    Started {
        handle: RunHandle,
        metadata: main::Metadata,
    },
    Replayed(main::Metadata),
}

enum Notice {
    Fresh {
        handle: Box<worker::HandleParts>,
        metadata: main::Metadata,
    },
    Replayed(main::Metadata),
    Failed(RuntimeError),
}
struct CancelOnDrop {
    pending: Arc<PendingPreparation>,
    armed: bool,
}
impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        if self.armed {
            self.pending.stop();
        }
    }
}
impl Orchestrator {
    pub(in crate::orchestrator) async fn finish_existing_preparation<T, F, W, B, BW>(
        &self,
        prepared: DurablePreparation,
        context: SessionContext,
        work: F,
        business: B,
    ) -> Result<ExistingOutcome, RuntimeError>
    where
        T: retained::PreparationValue,
        F: FnOnce(Option<super::scope::PlanScope>) -> W + Send + 'static,
        W: Future<Output = Result<(T, super::PreparedOperation), RuntimeError>> + Send + 'static,
        B: FnOnce(T, Orchestrator, CancellationToken, PreparationCompletion) -> BW + Send + 'static,
        BW: Future<Output = Result<crate::orchestrator::RunResult, RuntimeError>> + Send + 'static,
    {
        match self
            .finish_pending_preparation(prepared, context, work, business)
            .await?
        {
            PendingOutcome::Started { handle, metadata } => Ok(ExistingOutcome::Started {
                handle,
                receipt: metadata.admission().cloned(),
            }),
            PendingOutcome::Replayed(main::Metadata::Successor(metadata)) => {
                Ok(ExistingOutcome::Replayed(metadata))
            }
            PendingOutcome::Replayed(_) => Err(RuntimeError::PreparationRecoveryRequired),
        }
    }
    pub(in crate::orchestrator) async fn finish_pending_preparation<T, F, W, B, BW>(
        &self,
        prepared: DurablePreparation,
        context: SessionContext,
        work: F,
        business: B,
    ) -> Result<PendingOutcome, RuntimeError>
    where
        T: retained::PreparationValue,
        F: FnOnce(Option<super::scope::PlanScope>) -> W + Send + 'static,
        W: Future<Output = Result<(T, super::PreparedOperation), RuntimeError>> + Send + 'static,
        B: FnOnce(T, Orchestrator, CancellationToken, PreparationCompletion) -> BW + Send + 'static,
        BW: Future<Output = Result<crate::orchestrator::RunResult, RuntimeError>> + Send + 'static,
    {
        let (entry, pending, value, original) =
            self.observe_pending_factory(prepared, work).await?;
        let descriptor = { entry.lock().await.descriptor.clone() };
        let run = pending.existing_run()?;
        let (send, receive) = oneshot::channel();
        let notice = Arc::new(std::sync::Mutex::new(Some(send)));
        let (wrapper, envelope) = oneshot::channel::<worker::WorkerBusiness>();
        let body = move |context, completion| async move {
            if let Ok(wrapper) = envelope.await {
                wrapper
                    .run(move |runtime, cancellation| {
                        business(value, runtime, cancellation, completion)
                    })
                    .await;
            }
            drop(context);
        };
        let shared = {
            // Close the local no-main/start race with explicit cleanup. This
            // gate only covers synchronous registration; never native IO/waits.
            let _enrollment = pending.enrollment.lock().unwrap();
            if pending.cancellation.is_cancelled() {
                return Err(super::incomplete(
                    descriptor,
                    RuntimeError::PreparationRecoveryRequired,
                ));
            }
            let main = if run.is_some() {
                main::Main::Successor(PendingSuccessorTask::register(
                    &self.pending_drivers,
                    self.store.clone(),
                    context,
                    original,
                    body,
                )?)
            } else {
                main::Main::Initial(PendingInitialTask::register(
                    &self.pending_drivers,
                    self.store.clone(),
                    context,
                    original,
                    body,
                )?)
            };
            pending
                .control
                .set(main.control())
                .map_err(|_| RuntimeError::PreparationRecoveryRequired)?;
            let shared = Arc::new(Mutex::new(main));
            pending
                .main
                .set(shared.clone())
                .map_err(|_| RuntimeError::PreparationRecoveryRequired)?;
            if pending.cancellation.is_cancelled() {
                pending
                    .control
                    .get()
                    .expect("control installed")
                    .request_stop();
            }
            shared
        };
        let mut caller = CancelOnDrop {
            pending: pending.clone(),
            armed: true,
        };
        let (start, started) = oneshot::channel();
        let supervisor = self.execution_tasks.spawn_reporting(
            descriptor.session_id.clone(),
            run,
            supervise::run(
                self.clone(),
                entry,
                pending.clone(),
                shared.clone(),
                notice,
                started,
                wrapper,
            ),
        );
        // Retain the actual reporting supervisor and its exact local identity
        // before the original pending main can begin native completion IO.
        let _ = start.send(supervisor.clone());
        shared.lock().await.start()?;
        match receive
            .await
            .map_err(|_| RuntimeError::PreparationRecoveryRequired)?
        {
            Notice::Fresh { handle, metadata } => {
                caller.armed = false;
                Ok(PendingOutcome::Started {
                    handle: (*handle).finish(supervisor),
                    metadata,
                })
            }
            Notice::Replayed(metadata) => {
                caller.armed = false;
                Ok(PendingOutcome::Replayed(metadata))
            }
            Notice::Failed(error) => Err(super::incomplete(descriptor, error)),
        }
    }
}
fn publish(
    notice: &std::sync::Mutex<Option<oneshot::Sender<Notice>>>,
    value: Notice,
) -> Option<Notice> {
    if let Some(send) = notice.lock().unwrap().take() {
        send.send(value).err()
    } else {
        Some(value)
    }
}

struct CleanupAttempt {
    pending: Arc<PendingPreparation>,
    succeeded: bool,
}
impl Drop for CleanupAttempt {
    fn drop(&mut self) {
        let _slot = self.pending.cleanup.lock().unwrap();
        self.pending
            .cleanup_failed
            .store(!self.succeeded, Ordering::Release);
        self.pending.cleanup_active.store(false, Ordering::Release);
    }
}
pub(super) fn start_cleanup(
    runtime: Orchestrator,
    pending: &Arc<PendingPreparation>,
    entry: retained::SharedEntry,
) {
    let mut slot = pending.cleanup.lock().unwrap();
    if pending.cleanup_active.swap(true, Ordering::AcqRel) {
        return;
    }
    pending.cleanup_failed.store(false, Ordering::Release);
    let prior = slot.take();
    let retained = pending.clone();
    let tasks = runtime.execution_tasks.clone();
    *slot = Some(tasks.spawn_preparation(
        pending.grant.owner_authority().session_id().clone(),
        async move {
            let mut attempt = CleanupAttempt {
                pending: retained.clone(),
                succeeded: false,
            };
            // Retain and ACTUALLY observe the preceding supervisor before retry.
            // Its completion flag is only a wake-up, never a physical Join proof.
            if let Some(prior) = prior {
                runtime.execution_tasks.join(&prior).await;
            }
            if factory::finish(&retained, runtime.store.as_ref())
                .await
                .is_err()
            {
                return;
            }
            retained.native_done.cancelled().await;
            let cancelled_factory = if retained.main.get().is_none() {
                if factory::cancel_unstarted_root(&retained, runtime.store.as_ref())
                    .await
                    .is_err()
                {
                    return;
                }
                true
            } else {
                false
            };
            attempt.succeeded = true;
            if !retained.failed.load(Ordering::Acquire)
                && (retained.committed.load(Ordering::Acquire) || cancelled_factory)
            {
                let descriptor = { entry.lock().await.descriptor.clone() };
                runtime.retained_preparations.remove(&descriptor, &entry);
            }
        },
    ));
}

pub(super) fn cleanup_request(
    runtime: Orchestrator,
    pending: &Arc<PendingPreparation>,
    entry: retained::SharedEntry,
) {
    pending.stop();
    pending.resources.close();
    {
        let _enrollment = pending.enrollment.lock().unwrap();
        if pending.main.get().is_none() {
            pending.native_done.cancel();
        }
    }
    // Actual factory/resource cleanup may progress while the original main
    // retains an in-flight native ACK. Entry removal still awaits native_done.
    start_cleanup(runtime, pending, entry);
}
