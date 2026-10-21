//! Real cleanup may win before the owned Entry transfers into pending state.
use super::*;
use crabber_core::{AdmissionKey, AdmissionOptions, InputFingerprint, SessionContext, SessionId};
use crabber_extension::{
    ComponentIdentity, ExtensionError, PreparationResourceContract, RunPlan, RunPlanProvider,
    StaticPlanProvider,
};
use std::{
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};

struct DeclaredPlan(StaticPlanProvider);
#[async_trait::async_trait]
impl RunPlanProvider for DeclaredPlan {
    fn registration_fingerprint(
        &self,
        context: &SessionContext,
    ) -> Result<InputFingerprint, ExtensionError> {
        self.0.registration_fingerprint(context)
    }
    fn preparation_resource_contract(
        &self,
        _: &SessionContext,
    ) -> Result<Option<PreparationResourceContract>, ExtensionError> {
        Ok(Some(PreparationResourceContract::new(
            ComponentIdentity {
                id: "entry-transition-test".into(),
                version: "1".into(),
            },
            InputFingerprint::new("d".repeat(64)).unwrap(),
        )?))
    }
    async fn acquire_plan(&self, _: &SessionId) -> Result<RunPlan, ExtensionError> {
        panic!("parked factory must never acquire a plan")
    }
}
fn request(session: SessionId) -> crate::Request {
    crate::Request {
        selection_policy: crabber_session::AdmissionSelectionPolicy::Explicit,
        captured_revision: None,
        input_envelope: None,
        session_id: Some(session),
        workspace_id: "transition".into(),
        directory: "/transition".into(),
        title: "transition".into(),
        text: "input".into(),
        selection: crabber_providers::Selection {
            provider_id: "fake".into(),
            model_id: "scripted".into(),
        },
        system_prompt: None,
        max_output_tokens: None,
    }
}
async fn contract(store: Arc<dyn Store>) {
    let fake = crabber_providers::FakeProvider::scripted(vec![]);
    let runtime = Orchestrator::builder()
        .behavior_fingerprint(InputFingerprint::new("b".repeat(64)).unwrap())
        .store(store.clone())
        .resolver(Arc::new(fake.clone()))
        .plan_provider(Arc::new(DeclaredPlan(StaticPlanProvider::new(
            vec![],
            vec![],
        ))))
        .build()
        .unwrap();
    let session = SessionId::new();
    let mut request = request(session.clone());
    let options = AdmissionOptions {
        key: AdmissionKey::new("cleanup-before-entry-transfer").unwrap(),
        fingerprint: InputFingerprint::new("a".repeat(64)).unwrap(),
        behavior_fingerprint: runtime.behavior_fingerprint().clone(),
    };
    let DurablePreparation { grant, capacity } = runtime
        .begin_keyed_preparation(&mut request, &session, options)
        .await
        .unwrap();
    let descriptor = grant.reconciliation().unwrap();
    let entry = runtime
        .retained_preparations
        .reserve(grant.clone(), capacity, None)
        .unwrap();
    let PreparationResourceOutcome::Applied { receipt, grant } =
        store.retain_preparation_resources(grant).await.unwrap()
    else {
        panic!("actual fresh retained child")
    };
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let activation = runtime
        .register_owned_preparation(&entry, &grant, &receipt, move |_| async move {
            count.fetch_add(1, Ordering::SeqCst);
            Err::<
                (
                    (),
                    crabber_session::session_owner::preparation::PreparedOperation,
                ),
                _,
            >(RuntimeError::PreparationRecoveryRequired)
        })
        .await
        .unwrap();
    let heartbeat = {
        let held = entry.lock().await;
        assert!(held.pending.is_none());
        let retained::heartbeat::RetainedTask::Running(task) = &held.heartbeat else {
            panic!("original actual heartbeat")
        };
        task.id()
    };
    // This is the legitimate scheduling order at the old two-lock boundary:
    // child task registered, then explicit cleanup, then transfer attempts.
    runtime
        .cleanup_retained_preparation(&descriptor)
        .await
        .unwrap();
    let (refused, owner_preserved, heartbeat_preserved, no_pending, moved) = {
        let mut held = entry.lock().await;
        assert!(held.disposing);
        let transfer = install_pending(&mut held, &grant);
        let refused = matches!(&transfer, Err(RuntimeError::PreparationRecoveryRequired));
        let owner_preserved = held.owned.as_ref().is_some_and(|owned| {
            owned.task.activation() == &activation && owned.disposed.is_cancelled()
        });
        let heartbeat_preserved = match &held.heartbeat {
            retained::heartbeat::RetainedTask::Running(task) => task.id() == heartbeat,
            retained::heartbeat::RetainedTask::Joined => true,
            _ => false,
        };
        (
            refused,
            owner_preserved,
            heartbeat_preserved,
            held.pending.is_none(),
            transfer.ok(),
        )
    };
    // Even the deliberately guardless RED must retain and observe real cleanup,
    // instead of aborting tasks at the first expected assertion failure.
    if let Some(moved) = &moved {
        super::super::cleanup_request(runtime.clone(), moved, entry.clone());
    }
    observe_original_tasks(&runtime, &entry, &session, moved.as_ref()).await;
    assert!(
        refused,
        "cleanup-owned Entry was moved to a fresh pending controller"
    );
    assert!(
        owner_preserved && heartbeat_preserved && no_pending,
        "original owner/heartbeat/source was lost"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(fake.requests().len(), 0);
    assert!(store.get_session(&session).await.unwrap().is_none());
}
async fn observe_original_tasks(
    runtime: &Orchestrator,
    entry: &retained::SharedEntry,
    session: &SessionId,
    moved: Option<&Arc<PendingPreparation>>,
) {
    let drained = runtime
        .drain_session_execution_tasks(session, Duration::from_secs(5))
        .await;
    // Legacy cleanup is retained in Entry, independently of ExecutionTasks.
    // Let that actual task finish without holding the Entry lock it needs;
    // then observe its original handle rather than certifying from this signal.
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if entry.lock().await.cleanup_supervisor.is_finished() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("original cleanup supervisor must finish");
    entry.lock().await.cleanup_supervisor.join().await.unwrap();
    let disposition = if let Some(moved) = moved {
        moved.heartbeat.lock().await.join().await.unwrap();
        moved
            .factory
            .lock()
            .await
            .task
            .join()
            .await
            .unwrap()
            .disposition()
    } else {
        let mut held = entry.lock().await;
        held.cleanup_supervisor.join().await.unwrap();
        held.heartbeat.join().await.unwrap();
        held.owned
            .as_mut()
            .unwrap()
            .task
            .join()
            .await
            .unwrap()
            .disposition()
    };
    assert!(
        drained.pending.is_empty() && drained.pending_preparations.is_empty(),
        "{drained:?}"
    );
    assert_eq!(
        disposition,
        crabber_session::session_owner::owned_scope::OwnedMainDisposition::NoDispatch
    );
}
#[tokio::test]
async fn cleanup_wins_owned_pending_transfer_memory() {
    contract(Arc::new(crabber_session::MemoryStore::new())).await;
}
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn cleanup_wins_owned_pending_transfer_sqlite() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("transfer.sqlite");
    crabber_session::SqliteStore::migrate(&path).await.unwrap();
    contract(Arc::new(
        crabber_session::SqliteStore::connect(&path).await.unwrap(),
    ))
    .await;
}
#[cfg(feature = "postgres")]
#[tokio::test]
async fn cleanup_wins_owned_pending_transfer_postgres() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};

    TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {
        crabber_session::PostgresStore::migrate(&url).await.unwrap();
        contract(Arc::new(
            crabber_session::PostgresStore::connect(&url).await.unwrap(),
        ))
        .await;
    })
    .await
    .unwrap();
}
