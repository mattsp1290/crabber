use crate::{CompactionPolicy, Orchestrator, PermissionDecision, Request, StaticPolicy};
use async_trait::async_trait;
use crabber_core::{
    InputFingerprint, ProviderBindingDescriptor, ProviderBindingLimits, ToolCallId, ToolInfo,
};
use crabber_extension::{ExtensionError, StaticPlanProvider, ToolDefinition, ToolExecutor};
use crabber_providers::{
    DeltaStream, FakeProvider, ModelRequest, ProviderBindingError, ProviderError, Resolver,
    Selection, StreamDelta, Streamer,
};
use crabber_session::{MemoryStore, Store};
use serde_json::{Value, json};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

struct MovingResolver {
    fake: FakeProvider,
    epoch: Arc<AtomicUsize>,
    descriptions: Arc<AtomicUsize>,
    resolutions: Arc<AtomicUsize>,
}
#[async_trait]
impl Resolver for MovingResolver {
    fn binding_descriptor(
        &self,
        selection: &Selection,
    ) -> Result<ProviderBindingDescriptor, ProviderBindingError> {
        self.descriptions.fetch_add(1, Ordering::SeqCst);
        let changed = self.epoch.load(Ordering::SeqCst) > 0;
        ProviderBindingDescriptor::new(
            &selection.provider_id,
            &selection.model_id,
            if changed {
                "new-backend"
            } else {
                "original-backend"
            },
            "1",
            InputFingerprint::new("c".repeat(64)).unwrap(),
            ProviderBindingLimits {
                context_tokens: Some(if changed { 64 } else { 8192 }),
                output_tokens: Some(64),
            },
        )
        .map_err(|_| ProviderBindingError::InvalidDescriptor)
    }
    async fn resolve(&self, _: &Selection) -> Result<Arc<dyn Streamer>, ProviderError> {
        self.resolutions.fetch_add(1, Ordering::SeqCst);
        Ok(Arc::new(MovingStream {
            fake: self.fake.clone(),
            epoch: self.epoch.clone(),
            descriptions: self.descriptions.clone(),
        }))
    }
}
struct MovingStream {
    fake: FakeProvider,
    epoch: Arc<AtomicUsize>,
    descriptions: Arc<AtomicUsize>,
}
#[async_trait]
impl Streamer for MovingStream {
    async fn stream(&self, request: ModelRequest) -> Result<DeltaStream, ProviderError> {
        // The declaration mutates after the first model call. An active run must
        // neither rediscover it nor reacquire a different implementation.
        assert_eq!(self.descriptions.load(Ordering::SeqCst), 3);
        self.epoch.fetch_add(1, Ordering::SeqCst);
        self.fake.stream(request).await
    }
}
struct Echo;
#[async_trait]
impl ToolExecutor for Echo {
    async fn execute(&self, arguments: Value) -> Result<Value, ExtensionError> {
        Ok(arguments)
    }
}
#[tokio::test]
async fn active_execution_holds_original_implementation_and_limits_across_turns() {
    let call = ToolCallId::new();
    let fake = FakeProvider::scripted(vec![
        vec![
            StreamDelta::ToolCallStart {
                call_id: call.clone(),
                name: "echo".into(),
            },
            StreamDelta::ToolCallArgsDelta {
                call_id: call.clone(),
                text: "{}".into(),
            },
            StreamDelta::ToolCallDone { call_id: call },
            StreamDelta::Completed,
        ],
        vec![
            StreamDelta::TextDelta("done".into()),
            StreamDelta::Completed,
        ],
    ]);
    let descriptions = Arc::new(AtomicUsize::new(0));
    let resolutions = Arc::new(AtomicUsize::new(0));
    let resolver = MovingResolver {
        fake: fake.clone(),
        epoch: Arc::new(AtomicUsize::new(0)),
        descriptions: descriptions.clone(),
        resolutions: resolutions.clone(),
    };
    let store = Arc::new(MemoryStore::new());
    let runtime = Orchestrator::builder()
        .behavior_version("held-provider-test.v1")
        .store(store.clone())
        .resolver(Arc::new(resolver))
        .plan_provider(Arc::new(StaticPlanProvider::new(
            vec![Arc::new(ToolDefinition {
                info: ToolInfo {
                    name: "echo".into(),
                    description: "echo".into(),
                    parameters: json!({"type":"object"}),
                    retry_safe: false,
                    required_permissions: vec![],
                },
                executor: Arc::new(Echo),
            })],
            vec![],
        )))
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .compaction(CompactionPolicy {
            trigger_ratio: 0.85,
            keep_tail_messages: 0,
        })
        .build()
        .unwrap();
    let handle = runtime
        .start(Request {
            session_id: None,
            workspace_id: "held".into(),
            directory: "/held".into(),
            title: "held".into(),
            text: "hello".into(),
            selection: Selection {
                provider_id: "fake".into(),
                model_id: "scripted".into(),
            },
            system_prompt: None,
            max_output_tokens: Some(32),
        })
        .await
        .unwrap();
    let session = handle.session_id().clone();
    let run = handle.run_id().clone();
    handle.done().await.unwrap();
    assert_eq!(resolutions.load(Ordering::SeqCst), 1);
    assert_eq!(descriptions.load(Ordering::SeqCst), 3);
    assert_eq!(fake.requests().len(), 2);
    let original = store
        .execution_binding(&session, &run)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(original.capsule.provider.backend_id(), "original-backend");
    assert_eq!(
        original.capsule.provider.limits().context_tokens,
        Some(8192)
    );
    assert_eq!(
        original.capsule.behavior_fingerprint,
        *runtime.behavior_fingerprint()
    );
}

#[tokio::test]
async fn recovery_rejects_changed_backend_before_claim() {
    use crabber_core::{Clock, ManualClock};
    let clock = Arc::new(ManualClock::new(time::OffsetDateTime::UNIX_EPOCH));
    let store = Arc::new(MemoryStore::with_clock(clock.clone()));
    let epoch = Arc::new(AtomicUsize::new(0));
    let resolutions = Arc::new(AtomicUsize::new(0));
    let runtime = Orchestrator::builder()
        .behavior_version("recovery-binding-test.v1")
        .store(store.clone())
        .clock(clock.clone())
        .resolver(Arc::new(MovingResolver {
            fake: FakeProvider::scripted(vec![]),
            epoch: epoch.clone(),
            descriptions: Arc::new(AtomicUsize::new(0)),
            resolutions: resolutions.clone(),
        }))
        .plan_provider(Arc::new(StaticPlanProvider::new(vec![], vec![])))
        .build()
        .unwrap();
    let request = binding_request();
    let (plan, admission, _) = runtime.prepare_admission(&request).await.unwrap();
    plan.release();
    let admitted = store.admit_bound_run(admission).await.unwrap();
    clock.set(admitted.run.lease_until + time::Duration::seconds(1));
    assert!(clock.now() > admitted.run.lease_until);
    let run_before = store.get_run(&admitted.run.id).await.unwrap();
    let events_before = store
        .list_events(&admitted.session.id, None, 1000)
        .await
        .unwrap();
    epoch.store(1, Ordering::SeqCst);
    let error = runtime.resume(&admitted.run.id).await.unwrap_err();
    assert!(
        matches!(
            error,
            crate::RuntimeError::ProviderBinding(ProviderBindingError::VersionConflict)
        ),
        "{error:?}"
    );
    assert_eq!(resolutions.load(Ordering::SeqCst), 0);
    assert_eq!(store.get_run(&admitted.run.id).await.unwrap(), run_before);
    assert_eq!(
        store
            .list_events(&admitted.session.id, None, 1000)
            .await
            .unwrap(),
        events_before
    );
    let report = runtime.recover().await.unwrap();
    assert_eq!(report.recovered, [] as [crate::RunResult; 0]);
    assert_eq!(report.skipped.len(), 1);
    assert_eq!(
        report.skipped[0].reason,
        crabber_session::StoreError::HostUnsupported
    );
    assert_eq!(store.get_run(&admitted.run.id).await.unwrap(), run_before);
}

#[tokio::test]
async fn legacy_run_without_binding_cannot_gain_recovery_authority() {
    use crabber_core::ManualClock;
    let clock = Arc::new(ManualClock::new(time::OffsetDateTime::UNIX_EPOCH));
    let store = Arc::new(MemoryStore::with_clock(clock.clone()));
    let fake = FakeProvider::scripted(vec![]);
    let runtime = Orchestrator::builder()
        .behavior_version("legacy-binding-test.v1")
        .store(store.clone())
        .clock(clock.clone())
        .resolver(Arc::new(fake.clone()))
        .plan_provider(Arc::new(StaticPlanProvider::new(vec![], vec![])))
        .build()
        .unwrap();
    let (plan, admission, _) = runtime.prepare_admission(&binding_request()).await.unwrap();
    plan.release();
    // Even with matching configuration hashes, legacy admission has no evidence.
    let admitted = store.admit_run(admission.request).await.unwrap();
    clock.set(admitted.run.lease_until + time::Duration::seconds(1));
    let run_before = store.get_run(&admitted.run.id).await.unwrap();
    let error = runtime.resume(&admitted.run.id).await.unwrap_err();
    assert!(matches!(
        error,
        crate::RuntimeError::AdmissionExecution(
            crabber_session::AdmissionExecutionError::MissingEvidence
        )
    ));
    assert_eq!(store.get_run(&admitted.run.id).await.unwrap(), run_before);
    assert_eq!(fake.requests(), [] as [ModelRequest; 0]);
    assert!(
        store
            .execution_binding(&admitted.session.id, &admitted.run.id)
            .await
            .unwrap()
            .is_none()
    );
}

#[test]
fn opaque_host_behavior_requires_explicit_declaration() {
    let result = Orchestrator::builder()
        .store(Arc::new(MemoryStore::new()))
        .resolver(Arc::new(FakeProvider::scripted(vec![])))
        .plan_provider(Arc::new(StaticPlanProvider::new(vec![], vec![])))
        .build();
    assert!(matches!(
        result,
        Err(crate::RuntimeError::Missing("host behavior fingerprint"))
    ));
}

fn binding_request() -> Request {
    Request {
        session_id: None,
        workspace_id: "binding-tests".into(),
        directory: "/binding-tests".into(),
        title: "test".into(),
        text: "hello".into(),
        selection: Selection {
            provider_id: "fake".into(),
            model_id: "scripted".into(),
        },
        system_prompt: None,
        max_output_tokens: Some(32),
    }
}

struct SubstitutingResolver(FakeProvider);
#[async_trait]
impl Resolver for SubstitutingResolver {
    fn binding_descriptor(
        &self,
        selection: &Selection,
    ) -> Result<ProviderBindingDescriptor, ProviderBindingError> {
        self.0.binding_descriptor(selection)
    }
    async fn resolve(&self, _: &Selection) -> Result<Arc<dyn Streamer>, ProviderError> {
        panic!("the custom restore path should be used")
    }
    async fn restore_binding(
        &self,
        descriptor: &ProviderBindingDescriptor,
    ) -> Result<crabber_providers::BoundProvider, ProviderBindingError> {
        let selection = Selection {
            provider_id: descriptor.provider_id().into(),
            model_id: descriptor.model_id().into(),
        };
        let substitute = ProviderBindingDescriptor::new(
            descriptor.provider_id(),
            descriptor.model_id(),
            "substitute-backend",
            "1",
            descriptor.config_fingerprint().clone(),
            descriptor.limits(),
        )
        .unwrap();
        crabber_providers::BoundProvider::new(&selection, substitute, Arc::new(self.0.clone()))
    }
}

#[tokio::test]
async fn custom_restore_cannot_substitute_a_different_binding_before_recovery_claim() {
    let clock = Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ));
    let store = Arc::new(MemoryStore::with_clock(clock.clone()));
    let fake = FakeProvider::scripted(vec![]);
    let runtime = Orchestrator::builder()
        .behavior_version("restore-substitution-test.v1")
        .store(store.clone())
        .clock(clock.clone())
        .resolver(Arc::new(SubstitutingResolver(fake.clone())))
        .plan_provider(Arc::new(StaticPlanProvider::new(vec![], vec![])))
        .build()
        .unwrap();
    let (plan, admission, _) = runtime.prepare_admission(&binding_request()).await.unwrap();
    plan.release();
    let admitted = store.admit_bound_run(admission).await.unwrap();
    clock.set(admitted.run.lease_until + time::Duration::seconds(1));
    let before = store.get_run(&admitted.run.id).await.unwrap();
    let events_before = store
        .list_events(&admitted.session.id, None, 1000)
        .await
        .unwrap();
    let error = runtime.resume(&admitted.run.id).await.unwrap_err();
    assert!(
        matches!(
            error,
            crate::RuntimeError::ProviderBinding(ProviderBindingError::VersionConflict)
        ),
        "{error:?}"
    );
    assert_eq!(store.get_run(&admitted.run.id).await.unwrap(), before);
    assert_eq!(
        store
            .list_events(&admitted.session.id, None, 1000)
            .await
            .unwrap(),
        events_before
    );
    assert_eq!(fake.requests(), [] as [ModelRequest; 0]);
}
