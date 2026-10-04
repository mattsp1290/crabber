use super::*;
use crate::{InterruptPolicy, PermissionPolicy, RuntimeError};
use crabber_core::ManualClock;
use crabber_extension::ModelStream;
use crabber_providers::{ModelDescriptor, ProviderAdapter, ProviderInfo, Streamer};
use std::time::Duration;
use tokio::sync::Notify;

struct PausePolicy;
impl PermissionPolicy for PausePolicy {
    fn decide(&self, _: &ToolInfo, _: &Value) -> PermissionDecision {
        PermissionDecision::Allow
    }
    fn interrupt_policy(&self, _: &ToolInfo, _: &Value) -> InterruptPolicy {
        InterruptPolicy::Pause
    }
}
#[tokio::test]
async fn resumed_run_invokes_contributors() {
    let h = FileHarness::new(vec![call_script(), text_script("done")], true).await;
    let runtime = Orchestrator::builder()
        .store(h.store.clone())
        .resolver(Arc::new(h.fake.clone()))
        .plan_provider(Arc::new(h.registry.clone()))
        .policy(Arc::new(PausePolicy))
        .build()
        .unwrap();
    let handle = runtime.start(request()).await.unwrap();
    let id = handle.run_id().clone();
    assert_eq!(handle.done().await.unwrap().status, RunStatus::Paused);
    std::fs::write(&h.file, "v2").unwrap();
    assert_eq!(
        runtime.resume(&id).await.unwrap().status,
        RunStatus::Completed
    );
    assert_eq!(
        h.fake.requests()[1].system.as_deref(),
        Some("base\nstatic\nv2")
    );
    let contexts = h.contexts.lock().unwrap();
    assert_eq!(contexts.len(), 2);
    assert_eq!(contexts[1].attempt(), 1);
    assert!(!contexts[1].after_compaction());
}
#[tokio::test]
async fn mounting_a_contributor_after_pause_refuses_resume() {
    let registry = Registry::new();
    let directory = tempfile::tempdir().unwrap();
    let file = directory.path().join("file");
    mount(&registry, "tool", Scope::Global, move |r| {
        rewrite_tool(r, file.clone());
    })
    .await;
    let store = Arc::new(MemoryStore::new());
    let fake = FakeProvider::scripted(vec![call_script(), text_script("unused")]);
    let runtime = Orchestrator::builder()
        .store(store.clone())
        .resolver(Arc::new(fake.clone()))
        .plan_provider(Arc::new(registry.clone()))
        .policy(Arc::new(PausePolicy))
        .build()
        .unwrap();
    let handle = runtime.start(request()).await.unwrap();
    let id = handle.run_id().clone();
    assert_eq!(handle.done().await.unwrap().status, RunStatus::Paused);
    let before = store.get_run(&id).await.unwrap().unwrap();
    mount(&registry, "new", Scope::Global, |r| {
        r.prompt_contributor(0, "new", constant("text"));
    })
    .await;
    assert!(matches!(
        runtime.resume(&id).await,
        Err(RuntimeError::PlanChanged)
    ));
    assert_eq!(store.get_run(&id).await.unwrap().unwrap(), before);
    assert_eq!(fake.requests().len(), 1);
}
#[tokio::test(start_paused = true)]
async fn lease_lost_during_contribution_makes_no_provider_call() {
    let now = time::OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();
    let wall = Arc::new(ManualClock::new(now));
    let store = Arc::new(MemoryStore::with_clock(wall.clone()));
    let registry = Registry::new();
    let entered = Arc::new(Notify::new());
    mount(&registry, "park", Scope::Global, {
        let entered = entered.clone();
        move |r| {
            let entered = entered.clone();
            r.prompt_contributor(
                0,
                "park",
                Arc::new(move |_| {
                    entered.notify_one();
                    Box::pin(futures::future::pending())
                }),
            );
        }
    })
    .await;
    let fake = FakeProvider::scripted(vec![text_script("unused")]);
    let runtime = Orchestrator::builder()
        .store(store.clone())
        .clock(wall.clone())
        .resolver(Arc::new(fake.clone()))
        .plan_provider(Arc::new(registry))
        .heartbeat_interval(Duration::from_secs(1))
        .build()
        .unwrap();
    let handle = runtime.start(request()).await.unwrap();
    let id = handle.run_id().clone();
    entered.notified().await;
    wall.set(now + time::Duration::seconds(31));
    let replacement = store.claim_expired_run(&id, "replacement").await.unwrap();
    tokio::time::advance(Duration::from_secs(1)).await;
    assert!(matches!(handle.done().await, Err(RuntimeError::LeaseLost)));
    assert_eq!(fake.requests().len(), 0);
    assert_eq!(
        store.get_run(&id).await.unwrap().unwrap().claim_token,
        replacement.claim_token
    );
}
#[tokio::test]
async fn model_stream_handler_cannot_change_assembled_system() {
    let registry = Registry::new();
    mount(&registry, "controls", Scope::Global, |r| {
        static_prompt(r);
        r.prompt_contributor(0, "text", constant("dynamic"));
        r.on_around(
            ModelStream::ID,
            0,
            "system",
            Arc::new(|mut input, next| {
                Box::pin(async move {
                    input["system"] = json!("replace");
                    let mut output = next.call(input).await?;
                    output["system"] = json!("replace after");
                    Ok(output)
                })
            }),
        );
    })
    .await;
    let fake = FakeProvider::scripted(vec![text_script("done")]);
    assert_eq!(
        runtime(Arc::new(MemoryStore::new()), &fake, registry)
            .start(request())
            .await
            .unwrap()
            .done()
            .await
            .unwrap()
            .status,
        RunStatus::Completed
    );
    assert_eq!(
        fake.requests()[0].system.as_deref(),
        Some("base\nstatic\ndynamic")
    );
}
struct SmallContext(FakeProvider);
#[async_trait]
impl ProviderAdapter for SmallContext {
    fn info(&self) -> ProviderInfo {
        ProviderInfo {
            id: "fake".into(),
            name: "small context".into(),
        }
    }
    async fn models(&self) -> Result<Vec<ModelDescriptor>, ProviderError> {
        Ok(vec![ModelDescriptor {
            provider_id: "fake".into(),
            id: "scripted".into(),
            context_limit: 4096,
        }])
    }
    async fn build(&self, _: &Selection) -> Result<Arc<dyn Streamer>, ProviderError> {
        Ok(Arc::new(self.0.clone()))
    }
}
#[tokio::test]
async fn proactive_compaction_sets_after_compaction_on_first_attempt() {
    let contexts = Arc::new(Mutex::new(Vec::new()));
    let registry = Registry::new();
    let fake = FakeProvider::scripted(vec![text_script("summary"), text_script("done")]);
    mount(&registry, "proactive", Scope::Global, {
        let contexts = contexts.clone();
        let fake = fake.clone();
        move |r| {
            r.provider(Arc::new(SmallContext(fake.clone())));
            let contexts = contexts.clone();
            r.prompt_contributor(
                0,
                "text",
                Arc::new(move |context| {
                    contexts.lock().unwrap().push(context);
                    Box::pin(async { Ok(Some("dynamic".into())) })
                }),
            );
        }
    })
    .await;
    let runtime = Orchestrator::builder()
        .store(Arc::new(MemoryStore::new()))
        .resolver(Arc::new(fake.clone()))
        .plan_provider(Arc::new(registry))
        .compaction(crate::CompactionPolicy {
            trigger_ratio: 0.01,
            keep_tail_messages: 0,
        })
        .build()
        .unwrap();
    let mut req = request();
    req.text = "long instructions ".repeat(10);
    assert_eq!(
        runtime
            .start(req)
            .await
            .unwrap()
            .done()
            .await
            .unwrap()
            .status,
        RunStatus::Completed
    );
    let requests = fake.requests();
    assert_eq!(requests.len(), 2);
    assert_eq!(
        requests[0].system.as_deref(),
        Some(
            "Summarize this context for continuation. Preserve every standing instruction and unresolved task from the previous summary and new context."
        )
    );
    assert_eq!(requests[1].system.as_deref(), Some("base\ndynamic"));
    let contexts = contexts.lock().unwrap();
    assert_eq!(contexts.len(), 1);
    assert_eq!(contexts[0].attempt(), 1);
    assert!(contexts[0].after_compaction());
}

#[tokio::test]
async fn recovered_run_invokes_contributors() {
    let h = FileHarness::new(vec![call_script(), text_script("recovered")], true).await;
    let now = time::OffsetDateTime::now_utc();
    let wall = Arc::new(ManualClock::new(now));
    let store = Arc::new(MemoryStore::with_clock(wall.clone()));
    let build = || {
        Orchestrator::builder()
            .store(store.clone())
            .clock(wall.clone())
            .resolver(Arc::new(h.fake.clone()))
            .plan_provider(Arc::new(h.registry.clone()))
            .policy(Arc::new(PausePolicy))
            .build()
            .unwrap()
    };
    let handle = build().start(request()).await.unwrap();
    assert_eq!(handle.done().await.unwrap().status, RunStatus::Paused);
    std::fs::write(&h.file, "v2").unwrap();
    wall.set(now + time::Duration::seconds(31));
    let report = build().recover().await.unwrap();
    assert_eq!(report.recovered.len(), 1);
    assert_eq!(report.recovered[0].status, RunStatus::Completed);
    assert_eq!(
        h.fake.requests()[1].system.as_deref(),
        Some("base\nstatic\nv2")
    );
    let contexts = h.contexts.lock().unwrap();
    assert_eq!(contexts.len(), 2);
    assert_eq!(contexts[1].attempt(), 1);
}
