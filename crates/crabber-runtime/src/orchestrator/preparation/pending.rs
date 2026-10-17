//! `ExistingRun` only: original retained preparation and SAME preclaim main.
#[path = "pending/factory.rs"]
mod factory;
#[path = "pending/supervise.rs"]
mod supervise;
use super::{DurablePreparation, Orchestrator, RuntimeError, retained};
use crate::orchestrator::{RunHandle, worker};
use crabber_core::{AdmissionReceipt, SessionContext};
use crabber_session::session_owner::{
    owned_scope::{PendingSuccessorControl, PendingSuccessorReplay, PendingSuccessorTask},
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

pub(super) struct PendingExisting {
    factory: Mutex<retained::OwnedEntry>,
    resources: crabber_extension::ToolResourceOwner,
    heartbeat: Mutex<retained::heartbeat::RetainedTask>,
    heartbeat_stop: CancellationToken,
    grant: PreparationGrant,
    main: OnceLock<Arc<Mutex<PendingSuccessorTask<()>>>>,
    control: OnceLock<PendingSuccessorControl>,
    cancellation: CancellationToken,
    failed: AtomicBool,
    committed: AtomicBool,
    replayed: std::sync::Mutex<Option<PendingSuccessorReplay>>,
    cleanup: std::sync::Mutex<Option<Arc<crate::execution_tasks::ExecutionTask>>>,
    native_done: CancellationToken,
}
impl PendingExisting {
    pub(super) fn failed(&self) -> bool {
        self.failed.load(Ordering::Acquire)
    }
    pub(super) fn retry_progress(
        &self,
        operation: crabber_session::session_owner::preparation::PreparationOperationKind,
    ) -> Result<super::progress::PreparationRetryProgress, RuntimeError> {
        if !self.committed.load(Ordering::Acquire) {
            return Err(RuntimeError::PreparationRecoveryRequired);
        }
        Ok(super::progress::PreparationRetryProgress {
            operation,
            replayed: self.replayed.lock().unwrap().is_some(),
            unkeyed_admission: None,
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
enum Notice {
    Fresh(Option<AdmissionReceipt>),
    Replayed(PendingSuccessorReplay),
    Failed(RuntimeError),
}
struct CancelOnDrop {
    pending: Arc<PendingExisting>,
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
        let (entry, pending, value, original) =
            self.observe_existing_factory(prepared, work).await?;
        let descriptor = { entry.lock().await.descriptor.clone() };
        let run = match &pending.grant.original_reservation().purpose {
            super::PreparationPurpose::ExistingRun { run_id, .. } => run_id.clone(),
            _ => return Err(RuntimeError::PreparationRecoveryRequired),
        };
        let worker::WorkerLaunch {
            handle,
            business: wrapper,
            resources,
            complete,
        } = self.worker_launch_with_cancellation(
            context.session_id().clone(),
            run.clone(),
            pending.cancellation.clone(),
        );
        let cancel = handle.cancellation.clone();
        let (send, receive) = oneshot::channel();
        let notice = Arc::new(std::sync::Mutex::new(Some(send)));
        let main = PendingSuccessorTask::register(
            &self.pending_successors,
            self.store.clone(),
            context,
            original,
            move |context, completion| async move {
                wrapper
                    .run(move |runtime, cancellation| {
                        business(value, runtime, cancellation, completion)
                    })
                    .await;
                drop(context);
            },
        )?;
        let control = main.control();
        pending
            .control
            .set(control)
            .map_err(|_| RuntimeError::PreparationRecoveryRequired)?;
        let shared = Arc::new(Mutex::new(main));
        pending
            .main
            .set(shared.clone())
            .map_err(|_| RuntimeError::PreparationRecoveryRequired)?;
        let mut caller = CancelOnDrop {
            pending: pending.clone(),
            armed: true,
        };
        let (start, started) = oneshot::channel();
        let supervisor = self.execution_tasks.spawn_reporting(
            descriptor.session_id.clone(),
            Some(run),
            supervise::run(
                self.clone(),
                entry,
                pending.clone(),
                shared.clone(),
                resources,
                complete,
                notice,
                started,
                cancel,
            ),
        );
        // The reporting supervisor is retained before administrative work starts.
        shared.lock().await.start()?;
        let _ = start.send(());
        let notice = receive
            .await
            .map_err(|_| RuntimeError::PreparationRecoveryRequired)?;
        match notice {
            Notice::Fresh(receipt) => {
                caller.armed = false;
                Ok(ExistingOutcome::Started {
                    handle: handle.finish(supervisor),
                    receipt,
                })
            }
            Notice::Replayed(metadata) => {
                caller.armed = false;
                Ok(ExistingOutcome::Replayed(metadata))
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

pub(super) fn start_cleanup(
    runtime: Orchestrator,
    pending: &Arc<PendingExisting>,
    entry: retained::SharedEntry,
) {
    let mut cleanup = pending.cleanup.lock().unwrap();
    if cleanup.is_some() {
        return;
    }
    let retained = pending.clone();
    let tasks = runtime.execution_tasks.clone();
    *cleanup = Some(tasks.spawn_preparation(
        pending.grant.owner_authority().session_id().clone(),
        async move {
            if factory::finish(&retained, runtime.store.as_ref())
                .await
                .is_err()
            {
                retained.failed.store(true, Ordering::Release);
            }
            retained.native_done.cancelled().await;
            if !retained.failed() && retained.committed.load(Ordering::Acquire) {
                let descriptor = { entry.lock().await.descriptor.clone() };
                runtime.retained_preparations.remove(&descriptor, &entry);
            }
        },
    ));
}
pub(super) fn cleanup_request(
    runtime: Orchestrator,
    pending: &Arc<PendingExisting>,
    entry: retained::SharedEntry,
) {
    pending.stop();
    if pending.main.get().is_none() {
        pending.failed.store(true, Ordering::Release);
        pending.native_done.cancel();
        start_cleanup(runtime, pending, entry);
    }
}
