use super::*;
use crabber_extension::{PromptContributionOutcome, ResourceFailure};
use futures::future::BoxFuture;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Clone, Copy)]
enum Mode {
    Ready,
    Semantic,
    Construct,
    Poll,
    Drop,
    Both,
}
struct Seen {
    mode: Mode,
    calls: AtomicUsize,
    drops: AtomicUsize,
}
struct Original<E> {
    seen: Arc<Seen>,
    semantic: fn() -> E,
}
impl<E> Future for Original<E> {
    type Output = Result<Option<String>, E>;
    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        assert!(
            !matches!(self.seen.mode, Mode::Poll | Mode::Both),
            "literal prompt poll failure"
        );
        Poll::Ready(if matches!(self.seen.mode, Mode::Semantic) {
            Err((self.semantic)())
        } else {
            Ok(Some("original text".into()))
        })
    }
}
impl<E> Drop for Original<E> {
    fn drop(&mut self) {
        self.seen.drops.fetch_add(1, Ordering::SeqCst);
        assert!(
            !matches!(self.seen.mode, Mode::Drop | Mode::Both),
            "literal prompt Drop failure"
        );
    }
}
fn construct<E>(seen: &Arc<Seen>, semantic: fn() -> E) -> Original<E> {
    seen.calls.fetch_add(1, Ordering::SeqCst);
    assert!(
        !matches!(seen.mode, Mode::Construct),
        "literal prompt construction failure"
    );
    Original {
        seen: seen.clone(),
        semantic,
    }
}
struct Typed(Arc<Seen>);
// Return the original future itself: an async wrapper would mask this regression.
impl SystemPromptMiddleware for Typed {
    fn contribute<'life0, 'async_trait>(
        &'life0 self,
        _: ModelAttemptContext,
    ) -> BoxFuture<'async_trait, Result<Option<String>, String>>
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(construct(&self.0, || "semantic prompt failure".into()))
    }
}
struct Prompts(Arc<Seen>, bool);
#[async_trait]
impl Extension for Prompts {
    fn id(&self) -> &'static str {
        "literal-prompt"
    }
    fn version(&self) -> &'static str {
        "1"
    }
    fn config_hash(&self) -> String {
        "{}".into()
    }
    async fn install(&self, registrar: &mut Registrar) -> Result<(), ExtensionError> {
        if self.1 {
            registrar.system_prompt_middleware(
                "literal",
                0,
                MiddlewareDescriptor::new("literal", "1", "0".repeat(64)).unwrap(),
                Arc::new(Typed(self.0.clone())),
            );
        } else {
            let seen = self.0.clone();
            registrar.prompt_contributor(
                0,
                "literal",
                Arc::new(move |_| {
                    Box::pin(construct(&seen, || {
                        ExtensionError::Tool("semantic prompt failure".into())
                    }))
                }),
            );
        }
        Ok(())
    }
}
async fn run(typed: bool, mode: Mode) {
    let seen = Arc::new(Seen {
        mode,
        calls: AtomicUsize::new(0),
        drops: AtomicUsize::new(0),
    });
    let registry = Registry::new();
    let mount = registry
        .mount_owned(Arc::new(Prompts(seen.clone(), typed)), Scope::Global)
        .await
        .unwrap();
    let plan = registry.acquire(&"session".into());
    let contributors = plan.prompt_contributors.clone();
    drop(plan);
    let resources = ToolResourceOwner::new();
    let owner = CallbackInvocationOwner::attach(resources.resources()).unwrap();
    let result = with_callback_invocations(
        owner.capability(),
        collect_prompt_contributions(&contributors, context()),
    )
    .await;
    drop(contributors);
    drop(owner);
    resources.close();
    let joined = resources.drain(Duration::from_secs(3)).await;
    let closed = mount.close(Duration::from_secs(3)).await;
    let panicked = matches!(mode, Mode::Construct | Mode::Poll | Mode::Drop | Mode::Both);
    assert_eq!(seen.calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        seen.drops.load(Ordering::SeqCst),
        usize::from(!matches!(mode, Mode::Construct))
    );
    assert_eq!(joined.pending, 0, "{joined:?}");
    assert_eq!(!joined.failures.is_empty(), panicked, "{joined:?}");
    assert_eq!(closed.is_err(), panicked, "{closed:?}");
    if matches!(mode, Mode::Ready) {
        assert!(
            matches!(result, PromptContributionOutcome::Completed { sections } if sections.len()==1 && sections[0].text=="original text")
        );
        assert_eq!(joined.failures, [] as [ResourceFailure; 0]);
    } else {
        assert_eq!(
            result,
            PromptContributionOutcome::Failed {
                contributor: "literal".into()
            }
        );
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn original_errors_and_panics_keep_their_actual_join_outcomes() {
    for typed in [false, true] {
        for mode in [
            Mode::Ready,
            Mode::Semantic,
            Mode::Construct,
            Mode::Poll,
            Mode::Drop,
        ] {
            run(typed, mode).await;
        }
    }
}
#[test]
fn original_poll_and_drop_panics_are_contained() {
    let mut results = Vec::new();
    for family in ["native", "typed"] {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "literal::double_panic_child", "--nocapture"])
            .env("CRABBER_LITERAL_PROMPT_CHILD", family)
            .env("RUST_BACKTRACE", "0")
            .output()
            .unwrap();
        results.push((family, output.status));
    }
    assert!(
        results.iter().all(|(_, status)| status.success()),
        "actual literal prompt children: {results:?}"
    );
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn double_panic_child() {
    let Ok(family) = std::env::var("CRABBER_LITERAL_PROMPT_CHILD") else {
        return;
    };
    run(family == "typed", Mode::Both).await;
}
