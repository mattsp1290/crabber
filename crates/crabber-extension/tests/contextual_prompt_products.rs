use async_trait::async_trait;
use crabber_core::{RunId, SessionId, TurnId};
use crabber_extension::{
    Extension, ExtensionError, MiddlewareDescriptor, ModelAttemptContext, PromptAttemptContext,
    PromptContributionOutcome, PromptContributor, Registrar, Registry, Scope,
    SystemPromptMiddleware, WorkspaceContext, collect_prompt_contributions,
};
use futures::future::BoxFuture;
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

#[derive(Default)]
struct Seen {
    effects: AtomicUsize,
    drops: AtomicUsize,
}
struct Original(Arc<Seen>);
impl Original {
    fn native(&self) -> BoxFuture<'static, Result<Option<String>, ExtensionError>> {
        self.0.effects.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Ok(Some("original".into())) })
    }
}
impl Drop for Original {
    fn drop(&mut self) {
        self.0.drops.fetch_add(1, Ordering::SeqCst);
    }
}
#[async_trait]
impl SystemPromptMiddleware for Original {
    async fn contribute(&self, _: ModelAttemptContext) -> Result<Option<String>, String> {
        self.0.effects.fetch_add(1, Ordering::SeqCst);
        Ok(Some("original".into()))
    }
}
enum Body {
    Native(PromptContributor),
    Typed(Arc<dyn SystemPromptMiddleware>),
}
struct Legacy(Body);
#[async_trait]
impl Extension for Legacy {
    fn id(&self) -> &'static str {
        "original-prompt-arc"
    }
    fn version(&self) -> &'static str {
        "1"
    }
    fn config_hash(&self) -> String {
        "{}".into()
    }
    async fn install(&self, registrar: &mut Registrar) -> Result<(), ExtensionError> {
        match &self.0 {
            Body::Native(callback) => registrar.prompt_contributor(0, "original", callback.clone()),
            Body::Typed(callback) => registrar.system_prompt_middleware(
                "original",
                0,
                MiddlewareDescriptor::new("original", "1", "0".repeat(64)).unwrap(),
                callback.clone(),
            ),
        }
        Ok(())
    }
}
fn context() -> PromptAttemptContext {
    PromptAttemptContext::new(
        SessionId::new(),
        RunId::new(),
        TurnId::new(),
        WorkspaceContext::from_persisted("workspace", "/workspace"),
        "provider".into(),
        "model".into(),
        1,
        false,
    )
}
#[tokio::test]
async fn old_prompt_arc_join_is_not_original_callback_reference_disposal() {
    let mut observations = Vec::new();
    for typed in [false, true] {
        let seen = Arc::new(Seen::default());
        let original = Original(seen.clone());
        let body = if typed {
            Body::Typed(Arc::new(original))
        } else {
            Body::Native(Arc::new(move |_| original.native()))
        };
        let registry = Registry::new();
        let mount = registry
            .mount_owned(Arc::new(Legacy(body)), Scope::Global)
            .await
            .unwrap();
        let plan = registry.acquire(&SessionId::new());
        let escaped = plan.prompt_contributors.clone();
        let last = escaped.clone();
        drop(plan);
        let joined = mount.close(Duration::from_secs(2)).await.unwrap();
        registry.close_all().await.unwrap();
        drop(mount);
        drop(registry);
        let before = seen.drops.load(Ordering::SeqCst);
        let outcome = collect_prompt_contributions(&escaped, context()).await;
        drop(escaped);
        let after_first = seen.drops.load(Ordering::SeqCst);
        drop(last);
        let after_last = seen.drops.load(Ordering::SeqCst);
        // Every actual close and final original reference has been released before assertions.
        assert_eq!(joined.cleanup().pending, 0);
        assert_eq!(
            outcome,
            PromptContributionOutcome::Failed {
                contributor: "original".into()
            }
        );
        assert_eq!(seen.effects.load(Ordering::SeqCst), 0);
        observations.push((typed, before, after_first, after_last));
    }
    assert_eq!(observations, [(false, 0, 0, 1), (true, 0, 0, 1)]);
}
