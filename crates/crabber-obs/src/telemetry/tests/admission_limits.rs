use super::*;
use crabber_core::{AdmissionLimitKind, ModelControls, RunStatus};
use crabber_extension::StaticPlanProvider;
use crabber_providers::{FakeProvider, Selection, StreamDelta};
use crabber_runtime::{Orchestrator, PermissionDecision, Request, StaticPolicy};
use crabber_session::{AdmissionSelectionPolicy, MemoryStore};
use opentelemetry_proto::tonic::common::v1::any_value::Value;

fn input(text: String) -> Request {
    Request {
        selection_policy: AdmissionSelectionPolicy::Explicit,
        captured_revision: None,
        input_envelope: None,
        session_id: None,
        workspace_id: "admission-export-test".into(),
        directory: "/tmp/ADMISSION_PATH_SENTINEL".into(),
        title: "ADMISSION_TITLE_SENTINEL".into(),
        text,
        selection: Selection {
            provider_id: "fake".into(),
            model_id: "scripted".into(),
        },
        system_prompt: None,
        max_output_tokens: None,
        model_controls: ModelControls::default(),
    }
}

fn runtime(provider: &FakeProvider) -> Orchestrator {
    Orchestrator::builder()
        .behavior_version("admission-export.v1")
        .store(Arc::new(MemoryStore::new()))
        .resolver(Arc::new(provider.clone()))
        .plan_provider(Arc::new(StaticPlanProvider::new(vec![], vec![])))
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .build()
        .unwrap()
}

fn field<'a>(
    span: &'a opentelemetry_proto::tonic::trace::v1::Span,
    key: &str,
) -> Option<&'a Value> {
    span.attributes
        .iter()
        .find(|attr| attr.key == key)
        .and_then(|attr| attr.value.as_ref())
        .and_then(|value| value.value.as_ref())
}

async fn fitting_admission() {
    let provider = FakeProvider::scripted(vec![vec![
        StreamDelta::TextDelta("done".into()),
        StreamDelta::Completed,
    ]]);
    let fitting = runtime(&provider);
    let result = fitting
        .start(input("ADMISSION_PROMPT_SENTINEL".into()))
        .await
        .unwrap()
        .done()
        .await
        .unwrap();
    assert_eq!(result.status, RunStatus::Completed);
    assert_eq!(provider.requests().len(), 1);
    let close = fitting.close(Duration::from_secs(2)).await;
    assert!(close.local_drained(), "{close:?}");
    drop(fitting);
}

async fn completion_refusal() {
    let provider = FakeProvider::default();
    let oversized = runtime(&provider);
    let content = "ADMISSION_PROMPT_SENTINEL https://ADMISSION_URL_SENTINEL/ ".repeat(40_000);
    assert!(content.len() < 32 << 20);
    let Err(error) = oversized.start(input(content)).await else {
        panic!("cumulative proof materialization must refuse admission")
    };
    let limit = error
        .admission_limit()
        .expect("actual bounded proof refusal");
    assert_eq!(limit.kind(), AdmissionLimitKind::ProofBytes);
    assert_eq!(limit.limit_bytes(), Some(33_554_432));
    assert_eq!(
        provider.requests(),
        [] as [crabber_providers::ModelRequest; 0]
    );
    let descriptor = error.preparation_reconciliation().unwrap().clone();
    let close = oversized.close(Duration::from_secs(2)).await;
    assert!(close.uncertain_preparations.contains(&descriptor));
    drop(oversized);
}

async fn ordinary_refusals() {
    let provider = FakeProvider::default();
    let validation = runtime(&provider);
    let mut invalid = input("ADMISSION_PROMPT_SENTINEL".into());
    invalid.max_output_tokens = Some(0);
    let Err(error) = validation.start(invalid).await else {
        panic!("invalid output limit must be rejected")
    };
    assert!(error.admission_limit().is_none());
    assert_eq!(
        provider.requests(),
        [] as [crabber_providers::ModelRequest; 0]
    );
    assert!(
        validation
            .close(Duration::from_secs(2))
            .await
            .local_drained()
    );
    drop(validation);

    let source_failure = Orchestrator::builder()
        .behavior_version("admission-export.v1")
        .store(Arc::new(MemoryStore::new()))
        .resolver(Arc::new(provider.clone()))
        .plan_provider(Arc::new(RejectedPlan))
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .build()
        .unwrap();
    let Err(error) = source_failure
        .start(input("ADMISSION_PROMPT_SENTINEL".into()))
        .await
    else {
        panic!("plan source failure must refuse admission")
    };
    assert!(error.admission_limit().is_none());
    assert_eq!(
        provider.requests(),
        [] as [crabber_providers::ModelRequest; 0]
    );
    assert!(
        source_failure
            .close(Duration::from_secs(2))
            .await
            .local_drained()
    );
    drop(source_failure);
}

fn decoded_requests(receiver: &Receiver) -> Vec<ExportTraceServiceRequest> {
    receiver
        .requests
        .lock()
        .unwrap()
        .iter()
        .map(|(_, _, bytes)| ExportTraceServiceRequest::decode(bytes.as_slice()).unwrap())
        .collect()
}

fn admissions(
    decoded: &[ExportTraceServiceRequest],
) -> Vec<&opentelemetry_proto::tonic::trace::v1::Span> {
    decoded
        .iter()
        .flat_map(|request| &request.resource_spans)
        .flat_map(|resource| &resource.scope_spans)
        .flat_map(|scope| &scope.spans)
        .filter(|span| span.name == "crabber.admission")
        .collect()
}

fn assert_unknown_limit(span: &opentelemetry_proto::tonic::trace::v1::Span) {
    assert_unknown(span);
    assert_eq!(
        field(span, "admission.limit_kind"),
        Some(&Value::StringValue("proof_bytes".into()))
    );
    assert_eq!(
        field(span, "admission.limit_bytes"),
        Some(&Value::IntValue(33_554_432))
    );
}

fn assert_unknown(span: &opentelemetry_proto::tonic::trace::v1::Span) {
    assert_eq!(
        field(span, "outcome"),
        Some(&Value::StringValue("unknown".into()))
    );
    assert_eq!(
        field(span, "error.kind"),
        Some(&Value::StringValue("preparation_incomplete".into()))
    );
    assert_eq!(span.status.as_ref().unwrap().code, 1);
}

fn assert_scrubbed(decoded: &[ExportTraceServiceRequest]) {
    for request in decoded {
        let serialized = format!("{request:?}");
        for sentinel in [
            "ADMISSION_PROMPT_SENTINEL",
            "ADMISSION_SOURCE_SENTINEL",
            "ADMISSION_URL_SENTINEL",
            "ADMISSION_PATH_SENTINEL",
            "ADMISSION_TITLE_SENTINEL",
            "ARBITRARY_ERROR_SOURCE_SENTINEL",
        ] {
            assert!(!serialized.contains(sentinel), "export leaked {sentinel}");
        }
        for span in request
            .resource_spans
            .iter()
            .flat_map(|resource| &resource.scope_spans)
            .flat_map(|scope| &scope.spans)
            .filter(|span| span.name.starts_with("crabber."))
        {
            assert_eq!(
                span.events,
                [] as [opentelemetry_proto::tonic::trace::v1::span::Event; 0]
            );
            assert!(
                span.attributes
                    .iter()
                    .all(|attr| ALLOWED_ATTRIBUTES.contains(&attr.key.as_str()))
            );
        }
    }
}

#[tokio::test]
async fn real_memory_admission_exports_only_finite_limit_metadata() {
    let receiver = Receiver::new();
    let telemetry = Telemetry::new(receiver.config(OtlpProtocol::HttpProtobuf)).unwrap();
    let _interest = tracing::Dispatch::new(tracing::subscriber::NoSubscriber::default());
    let _guard =
        tracing::subscriber::set_default(tracing_subscriber::registry().with(telemetry.layer()));
    fitting_admission().await;
    completion_refusal().await;
    ordinary_refusals().await;
    telemetry.flush().await.unwrap();
    telemetry.shutdown().await.unwrap();
    let decoded = decoded_requests(&receiver);
    let spans = admissions(&decoded);
    assert_eq!(spans.len(), 4);
    let limited: Vec<_> = spans
        .iter()
        .filter(|span| field(span, "admission.limit_kind").is_some())
        .collect();
    assert_eq!(limited.len(), 1);
    assert_unknown_limit(limited[0]);
    for span in spans
        .iter()
        .filter(|span| field(span, "admission.limit_kind").is_none())
    {
        assert!(field(span, "admission.limit_bytes").is_none());
    }
    assert_scrubbed(&decoded);
}

struct OwnedPlan(StaticPlanProvider);

#[async_trait::async_trait]
impl crabber_extension::RunPlanProvider for OwnedPlan {
    fn registration_fingerprint(
        &self,
        context: &crabber_core::SessionContext,
    ) -> Result<crabber_core::InputFingerprint, crabber_extension::ExtensionError> {
        self.0.registration_fingerprint(context)
    }

    fn preparation_resource_contract(
        &self,
        _: &crabber_core::SessionContext,
    ) -> Result<
        Option<crabber_extension::PreparationResourceContract>,
        crabber_extension::ExtensionError,
    > {
        Ok(Some(crabber_extension::PreparationResourceContract::new(
            crabber_extension::ComponentIdentity {
                id: "admission-limit-export".into(),
                version: "1".into(),
            },
            crabber_core::InputFingerprint::new("b".repeat(64)).unwrap(),
        )?))
    }

    async fn acquire_plan(
        &self,
        session: &crabber_core::SessionId,
    ) -> Result<crabber_extension::RunPlan, crabber_extension::ExtensionError> {
        self.0.acquire_plan(session).await
    }

    async fn acquire_plan_in_preparation(
        &self,
        scope: &crabber_extension::PreparationResourceContext,
    ) -> Result<crabber_extension::RunPlan, crabber_extension::ExtensionError> {
        self.0.acquire_plan(scope.session().session_id()).await
    }
}

#[tokio::test]
async fn registration_limit_exports_unknown_outcome_and_retains_reconciliation() {
    let receiver = Receiver::new();
    let telemetry = Telemetry::new(receiver.config(OtlpProtocol::HttpProtobuf)).unwrap();
    let _interest = tracing::Dispatch::new(tracing::subscriber::NoSubscriber::default());
    let _guard =
        tracing::subscriber::set_default(tracing_subscriber::registry().with(telemetry.layer()));
    let provider = FakeProvider::default();
    let store = Arc::new(MemoryStore::new());
    let runtime = Orchestrator::builder()
        .behavior_fingerprint(crabber_core::InputFingerprint::new("b".repeat(64)).unwrap())
        .store(store.clone())
        .resolver(Arc::new(provider.clone()))
        .plan_provider(Arc::new(OwnedPlan(StaticPlanProvider::new(vec![], vec![]))))
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .build()
        .unwrap();
    let mut request = input("ADMISSION_PROMPT_SENTINEL".repeat(91_200));
    request.session_id = Some(crabber_core::SessionId::new());
    request.system_prompt =
        Some("ADMISSION_SOURCE_SENTINEL https://ADMISSION_URL_SENTINEL/ ".repeat(95_000));
    let options = crabber_core::AdmissionOptions {
        key: crabber_core::AdmissionKey::new("registration-limit-export").unwrap(),
        fingerprint: crabber_core::InputFingerprint::new("a".repeat(64)).unwrap(),
        behavior_fingerprint: crabber_core::InputFingerprint::new("b".repeat(64)).unwrap(),
    };
    let key =
        crabber_session::session_owner::preparation::PreparationKey::Admission(options.key.clone());
    let Err(error) = runtime.start_keyed(request, options).await else {
        panic!("registration must refuse the production proof budget")
    };
    let descriptor = error.preparation_reconciliation().unwrap().clone();
    assert_eq!(
        error.admission_limit().unwrap().limit_bytes(),
        Some(33_554_432)
    );
    assert_eq!(error.preparation_reconciliation(), Some(&descriptor));
    assert_eq!(
        runtime
            .lookup_preparation_operation(&descriptor.session_id, &key)
            .await
            .unwrap(),
        Some(descriptor.clone())
    );
    assert!(
        crabber_session::Store::lookup_session_owner(
            store.as_ref(),
            &descriptor.session_id,
            &descriptor.reserve_id
        )
        .await
        .unwrap()
        .is_some()
    );
    assert_eq!(
        runtime
            .retained_preparation_status(&descriptor.session_id)
            .len(),
        1
    );
    assert_eq!(
        provider.requests(),
        [] as [crabber_providers::ModelRequest; 0]
    );
    runtime.close(Duration::from_secs(2)).await;
    drop(runtime);
    telemetry.flush().await.unwrap();
    telemetry.shutdown().await.unwrap();
    let decoded = decoded_requests(&receiver);
    let spans = admissions(&decoded);
    assert_eq!(spans.len(), 1);
    let span = spans[0];
    assert_unknown_limit(span);
    assert_scrubbed(&decoded);
}

struct RejectedPlan;

#[async_trait::async_trait]
impl crabber_extension::RunPlanProvider for RejectedPlan {
    fn registration_fingerprint(
        &self,
        context: &crabber_core::SessionContext,
    ) -> Result<crabber_core::InputFingerprint, crabber_extension::ExtensionError> {
        crabber_extension::RunPlanProvider::registration_fingerprint(
            &StaticPlanProvider::new(vec![], vec![]),
            context,
        )
    }

    async fn acquire_plan(
        &self,
        _: &crabber_core::SessionId,
    ) -> Result<crabber_extension::RunPlan, crabber_extension::ExtensionError> {
        Err(crabber_extension::ExtensionError::Plan(
            "ARBITRARY_ERROR_SOURCE_SENTINEL /private/source https://private.invalid/source".into(),
        ))
    }
}

#[tokio::test]
async fn injected_completion_limit_and_authoritative_cleanup_failure_export_truthfully() {
    for cleanup_failure in [false, true] {
        let receiver = Receiver::new();
        let telemetry = Telemetry::new(receiver.config(OtlpProtocol::HttpProtobuf)).unwrap();
        let _interest = tracing::Dispatch::new(tracing::subscriber::NoSubscriber::default());
        let _guard = tracing::subscriber::set_default(
            tracing_subscriber::registry().with(telemetry.layer()),
        );
        let provider = FakeProvider::default();
        let store = Arc::new(super::fault_store::FaultStore {
            inner: MemoryStore::new(),
            limit_on_completion: !cleanup_failure,
            lose_cleanup_ack: AtomicBool::new(cleanup_failure),
        });
        let plans: Arc<dyn crabber_extension::RunPlanProvider> = if cleanup_failure {
            Arc::new(RejectedPlan)
        } else {
            Arc::new(StaticPlanProvider::new(vec![], vec![]))
        };
        let runtime = Orchestrator::builder()
            .behavior_version("admission-export.v1")
            .store(store.clone())
            .resolver(Arc::new(provider.clone()))
            .plan_provider(plans)
            .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
            .build()
            .unwrap();
        let Err(error) = runtime
            .start(input("ADMISSION_PROMPT_SENTINEL".into()))
            .await
        else {
            panic!("faulted preparation must refuse admission")
        };
        assert!(
            error.preparation_reconciliation().is_some(),
            "cleanup_failure={cleanup_failure}: {error:?}"
        );
        let descriptor = error.preparation_reconciliation().unwrap().clone();
        if cleanup_failure {
            assert!(matches!(
                error.operation_error(),
                crabber_runtime::RuntimeError::PreparationRecoveryRequired
            ));
            assert!(error.admission_limit().is_none());
            let receipt = crabber_session::Store::lookup_session_owner(
                &store.inner,
                &descriptor.session_id,
                &descriptor.join_id,
            )
            .await
            .unwrap()
            .unwrap();
            assert_eq!(
                receipt.owner.termination,
                Some(crabber_session::session_owner::SessionOwnerTermination::Cancelled)
            );
        } else {
            assert_eq!(
                error.admission_limit().unwrap().limit_bytes(),
                Some(33_554_432)
            );
        }
        assert_eq!(error.preparation_reconciliation(), Some(&descriptor));
        assert_eq!(
            provider.requests(),
            [] as [crabber_providers::ModelRequest; 0]
        );
        runtime.close(Duration::from_secs(2)).await;
        drop(runtime);
        telemetry.flush().await.unwrap();
        telemetry.shutdown().await.unwrap();
        let decoded = decoded_requests(&receiver);
        let spans = admissions(&decoded);
        assert_eq!(spans.len(), 1);
        let span = spans[0];
        assert_unknown(span);
        if cleanup_failure {
            assert!(field(span, "admission.limit_kind").is_none());
            assert!(field(span, "admission.limit_bytes").is_none());
        } else {
            assert_unknown_limit(span);
        }
        assert_scrubbed(&decoded);
    }
}
