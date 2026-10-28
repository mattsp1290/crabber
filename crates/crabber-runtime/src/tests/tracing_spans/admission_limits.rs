use super::*;
use crabber_core::{AdmissionLimit, SessionContext};
use crabber_extension::{
    ComponentIdentity, PreparationResourceContext, PreparationResourceContract, RunPlan,
};
use crabber_session::session_owner::preparation::PreparationKey;

struct DeclaredPlan(StaticPlanProvider);
#[async_trait]
impl RunPlanProvider for DeclaredPlan {
    fn registration_fingerprint(
        &self,
        context: &SessionContext,
    ) -> Result<crabber_core::InputFingerprint, ExtensionError> {
        self.0.registration_fingerprint(context)
    }
    fn preparation_resource_contract(
        &self,
        _: &SessionContext,
    ) -> Result<Option<PreparationResourceContract>, ExtensionError> {
        Ok(Some(PreparationResourceContract::new(
            ComponentIdentity {
                id: "limit-test".into(),
                version: "1".into(),
            },
            crabber_core::InputFingerprint::new("d".repeat(64)).unwrap(),
        )?))
    }
    async fn acquire_plan(&self, _: &SessionId) -> Result<RunPlan, ExtensionError> {
        panic!("the declared resource contract must use scoped acquisition")
    }
    async fn acquire_plan_in_preparation(
        &self,
        scope: &PreparationResourceContext,
    ) -> Result<RunPlan, ExtensionError> {
        self.0.acquire_plan(scope.session().session_id()).await
    }
}

fn owned_runtime(store: Arc<dyn Store>, fake: FakeProvider) -> Orchestrator {
    Orchestrator::builder()
        .behavior_version("admission-limits.v1")
        .store(store)
        .resolver(Arc::new(fake))
        .plan_provider(Arc::new(DeclaredPlan(StaticPlanProvider::new(
            vec![],
            vec![],
        ))))
        .build()
        .unwrap()
}

fn assert_unknown_limit(layer: &CaptureLayer) {
    let capture = layer.0.lock().unwrap();
    let span = capture
        .spans
        .iter()
        .find(|span| span.name == "crabber.admission")
        .unwrap();
    assert_eq!(span.fields["admission.limit_kind"], "proof_bytes");
    assert_eq!(span.fields["admission.limit_bytes"], "33554432");
    assert_eq!(span.fields["outcome"], "unknown");
    assert_eq!(span.fields["error.kind"], "preparation_incomplete");
    assert_eq!(span.fields["otel.status_code"], "OK");
}

#[tokio::test]
async fn production_registration_exhaustion_retains_original_preparation_without_dispatch() {
    let layer = CaptureLayer::default();
    let _subscriber = CaptureSubscriber::new(&layer);
    let memory = Arc::new(MemoryStore::new());
    let gate = Arc::new(SettlementGate::default());
    let fake = FakeProvider::scripted(vec![text_script("never invoked")]);
    let runtime = owned_runtime(
        Arc::new(DelayedTerminalStore {
            inner: memory.clone(),
            gate: gate.clone(),
        }),
        fake.clone(),
    );
    let mut input = request();
    let session = SessionId::new();
    input.session_id = Some(session.clone());
    input.text = "p".repeat(2 << 20);
    input.system_prompt = Some("s".repeat(5 << 20));
    let key = crabber_core::AdmissionKey::new("registration-limit").unwrap();
    let result = runtime
        .start_keyed(
            input,
            crabber_core::AdmissionOptions {
                key: key.clone(),
                fingerprint: crabber_core::InputFingerprint::new("a".repeat(64)).unwrap(),
                behavior_fingerprint: runtime.behavior_fingerprint().clone(),
            },
        )
        .await
        .map(|_| ());
    let error = result.unwrap_err();
    assert!(
        matches!(&error, RuntimeError::PreparationIncomplete { .. }),
        "{error:?}"
    );
    let descriptor = error.preparation_reconciliation().unwrap().clone();
    assert_eq!(
        error.admission_limit(),
        Some(&AdmissionLimit::proof_bytes(Some(32 << 20)))
    );
    assert_eq!(error.preparation_reconciliation(), Some(&descriptor));
    assert_eq!(
        runtime
            .lookup_preparation_operation(&session, &PreparationKey::Admission(key))
            .await
            .unwrap(),
        Some(descriptor.clone())
    );
    assert_eq!(
        runtime.retained_preparation_status(&session)[0].preparation,
        descriptor
    );
    assert_eq!(
        gate.preparation_calls.load(Ordering::SeqCst),
        0,
        "initial registration must fail before backend completion"
    );
    assert_eq!(fake.requests(), [] as [crabber_providers::ModelRequest; 0]);
    assert_unknown_limit(&layer);
    runtime.close(std::time::Duration::from_secs(2)).await;
}

#[tokio::test]
async fn production_backend_completion_exhaustion_follows_successful_registration() {
    let layer = CaptureLayer::default();
    let _subscriber = CaptureSubscriber::new(&layer);
    let gate = Arc::new(SettlementGate::default());
    let fake = FakeProvider::scripted(vec![text_script("never invoked")]);
    let runtime = owned_runtime(
        Arc::new(DelayedTerminalStore {
            inner: Arc::new(MemoryStore::new()),
            gate: gate.clone(),
        }),
        fake.clone(),
    );
    let mut input = request();
    input.text = "p".repeat(2_200_000);
    let error = runtime.start(input).await.err().unwrap();
    assert!(
        matches!(&error, RuntimeError::PreparationIncomplete { .. }),
        "{error:?}"
    );
    assert_eq!(
        error.admission_limit(),
        Some(&AdmissionLimit::proof_bytes(Some(32 << 20)))
    );
    assert_eq!(gate.preparation_calls.load(Ordering::SeqCst), 1);
    assert_eq!(fake.requests(), [] as [crabber_providers::ModelRequest; 0]);
    assert_unknown_limit(&layer);
    runtime.close(std::time::Duration::from_secs(2)).await;
}

#[tokio::test]
async fn fitting_admission_reaches_provider_without_limit_attributes() {
    let layer = CaptureLayer::default();
    let _subscriber = CaptureSubscriber::new(&layer);
    let fake = FakeProvider::scripted(vec![text_script("done")]);
    let runtime = owned_runtime(Arc::new(MemoryStore::new()), fake.clone());
    runtime
        .start(request())
        .await
        .unwrap()
        .done()
        .await
        .unwrap();
    runtime.close(std::time::Duration::from_secs(2)).await;
    assert_eq!(fake.requests().len(), 1);
    let capture = layer.0.lock().unwrap();
    let span = capture
        .spans
        .iter()
        .find(|span| span.name == "crabber.admission")
        .unwrap();
    assert!(!span.fields.contains_key("admission.limit_kind"));
    assert!(!span.fields.contains_key("admission.limit_bytes"));
}
