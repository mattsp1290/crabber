//! Local capacity is acquired before any original owner publication or factory.
use super::*;
use crabber_core::{AdmissionKey, AdmissionOptions, InputFingerprint, SessionContext, SessionId};
use crabber_extension::{ExtensionError, RunPlan, RunPlanProvider, StaticPlanProvider};
use std::sync::atomic::{AtomicUsize, Ordering};

struct CountingPlan {
    base: StaticPlanProvider,
    calls: AtomicUsize,
}
#[async_trait::async_trait]
impl RunPlanProvider for CountingPlan {
    fn registration_fingerprint(
        &self,
        context: &SessionContext,
    ) -> Result<InputFingerprint, ExtensionError> {
        self.base.registration_fingerprint(context)
    }
    async fn acquire_plan(&self, session: &SessionId) -> Result<RunPlan, ExtensionError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.base.acquire_plan(session).await
    }
}
#[tokio::test]
async fn full_retained_capacity_refuses_before_reserve_and_reuses_actual_release() {
    let store = Arc::new(crabber_session::MemoryStore::new());
    let plan = Arc::new(CountingPlan {
        base: StaticPlanProvider::new(vec![], vec![]),
        calls: AtomicUsize::new(0),
    });
    let fake = crabber_providers::FakeProvider::scripted(vec![vec![
        crabber_providers::StreamDelta::TextDelta("done".into()),
        crabber_providers::StreamDelta::Completed,
    ]]);
    let runtime = crate::Orchestrator::builder()
        .behavior_fingerprint(InputFingerprint::new("b".repeat(64)).unwrap())
        .store(store)
        .resolver(Arc::new(fake.clone()))
        .plan_provider(plan.clone())
        .build()
        .unwrap();
    let mut capacity: Vec<_> = (0..MAX_RETAINED_PREPARATIONS)
        .map(|_| runtime.retained_preparations.reserve_capacity().unwrap())
        .collect();
    let session = SessionId::new();
    let request = crate::Request {
        selection_policy: crabber_session::AdmissionSelectionPolicy::Explicit,
        captured_revision: None,
        input_envelope: None,
        session_id: Some(session.clone()),
        workspace_id: "work".into(),
        directory: "/work".into(),
        title: "capacity".into(),
        text: "input".into(),
        selection: crabber_providers::Selection {
            provider_id: "fake".into(),
            model_id: "scripted".into(),
        },
        system_prompt: None,
        max_output_tokens: None,
        model_controls: Default::default(),
    };
    let options = AdmissionOptions {
        key: AdmissionKey::new("bounded-local-capacity").unwrap(),
        fingerprint: InputFingerprint::new("a".repeat(64)).unwrap(),
        behavior_fingerprint: runtime.behavior_fingerprint().clone(),
    };
    let error = runtime
        .start_keyed(request.clone(), options.clone())
        .await
        .err()
        .unwrap();
    assert!(matches!(
        error,
        RuntimeError::InvalidConfiguration("retained preparation capacity reached")
    ));
    assert_eq!(error.preparation_reconciliation(), None);
    assert_eq!(plan.calls.load(Ordering::SeqCst), 0);
    assert_eq!(fake.requests().len(), 0);
    assert_eq!(
        runtime
            .lookup_preparation_operation(
                &session,
                &crabber_session::session_owner::preparation::PreparationKey::Admission(
                    options.key.clone()
                )
            )
            .await
            .unwrap(),
        None
    );
    drop(capacity.pop());
    let crate::Admission::Started { handle, .. } =
        runtime.start_keyed(request, options).await.unwrap()
    else {
        panic!("released capacity")
    };
    handle.done().await.unwrap();
    assert_eq!(plan.calls.load(Ordering::SeqCst), 1);
    assert_eq!(fake.requests().len(), 1);
}
