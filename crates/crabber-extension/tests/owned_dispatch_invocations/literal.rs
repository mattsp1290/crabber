//! The callback returns its literal future, without an extra async wrapper.
use super::*;
use crabber_core::{RunId, ToolCallId, ToolOutput};
use crabber_extension::{
    CallbackInvocationOwner, ToolInput, ToolOutcomeClass, ToolResourceOwner, ToolResultContext,
    ToolResultOutcome, ToolResultTransform, TransformOutput, with_callback_invocations,
};
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

#[derive(Clone, Copy)]
enum Mode {
    Ready,
    Semantic,
    InvalidEnvelope,
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
struct Original<T> {
    seen: Arc<Seen>,
    value: Option<T>,
}
impl<T: Unpin> Future for Original<T> {
    type Output = Result<T, ExtensionError>;
    fn poll(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        assert!(
            !matches!(self.seen.mode, Mode::Poll | Mode::Both),
            "literal result poll failure"
        );
        Poll::Ready(if matches!(self.seen.mode, Mode::Semantic) {
            Err(ExtensionError::Tool(
                "private callback-authored failure".into(),
            ))
        } else {
            Ok(self.value.take().unwrap())
        })
    }
}
impl<T> Drop for Original<T> {
    fn drop(&mut self) {
        self.seen.drops.fetch_add(1, Ordering::SeqCst);
        assert!(
            !matches!(self.seen.mode, Mode::Drop | Mode::Both),
            "literal result Drop failure"
        );
    }
}
fn construct<T>(seen: &Arc<Seen>, value: T) -> Original<T> {
    seen.calls.fetch_add(1, Ordering::SeqCst);
    assert!(
        !matches!(seen.mode, Mode::Construct),
        "literal result construction failure"
    );
    Original {
        seen: seen.clone(),
        value: Some(value),
    }
}
struct Results(Arc<Seen>, &'static str);
#[async_trait]
impl Extension for Results {
    fn id(&self) -> &'static str {
        "literal-result"
    }
    fn version(&self) -> &'static str {
        "1"
    }
    fn config_hash(&self) -> String {
        "{}".into()
    }
    async fn install(&self, registrar: &mut Registrar) -> Result<(), ExtensionError> {
        let seen = self.0.clone();
        if self.1 == "typed" {
            registrar.on_result_transform(
                0,
                "literal",
                Arc::new(move |_, value| Box::pin(construct(&seen, TransformOutput::new(value)))),
            );
        } else {
            let callback: crabber_extension::Callback = Arc::new(move |value| {
                let value = if matches!(seen.mode, Mode::InvalidEnvelope) {
                    Value::Null
                } else {
                    value
                };
                Box::pin(construct(&seen, value))
            });
            if self.1 == "ordinary-json" {
                registrar.on_transform(ToolResultTransform::ID, 0, "literal", callback);
            } else {
                registrar.on_final_redaction_json(0, "literal", callback);
            }
        }
        Ok(())
    }
}
async fn run(family: &'static str, mode: Mode) {
    let seen = Arc::new(Seen {
        mode,
        calls: AtomicUsize::new(0),
        drops: AtomicUsize::new(0),
    });
    let registry = Registry::new();
    let mount = registry
        .mount_owned(Arc::new(Results(seen.clone(), family)), Scope::Global)
        .await
        .unwrap();
    let plan = registry.acquire(&SessionId::new());
    let dispatcher = plan.dispatcher.clone();
    drop(plan);
    let resources = ToolResourceOwner::new();
    let owner = CallbackInvocationOwner::attach(resources.resources()).unwrap();
    let context = ToolResultContext::new(
        "fixture".into(),
        true,
        ToolInput::Normalized(Value::Null),
        ToolCallId::new(),
        SessionId::new(),
        RunId::new(),
        ToolOutcomeClass::Succeeded,
    );
    let value = ToolOutput::from_json(&Value::String("original output".into())).unwrap();
    let result = with_callback_invocations(
        owner.capability(),
        dispatcher.transform_tool_result(context, TransformOutput::new(value.clone())),
    )
    .await;
    drop(dispatcher);
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
        assert_eq!(
            result,
            ToolResultOutcome::Completed {
                result: value,
                is_error: false
            }
        );
    } else {
        assert_eq!(
            result,
            ToolResultOutcome::Failed {
                handler: "literal".into()
            }
        );
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn literal_result_semantics_and_actual_join_failures() {
    for family in ["typed", "ordinary-json", "final-json"] {
        for mode in [
            Mode::Ready,
            Mode::Semantic,
            Mode::Construct,
            Mode::Poll,
            Mode::Drop,
        ] {
            run(family, mode).await;
        }
        if family != "typed" {
            run(family, Mode::InvalidEnvelope).await;
        }
    }
}
#[test]
fn original_poll_and_drop_panics_are_contained() {
    let mut results = Vec::new();
    for family in ["typed", "ordinary-json", "final-json"] {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "literal::double_panic_child", "--nocapture"])
            .env("CRABBER_LITERAL_RESULT_CHILD", family)
            .env("RUST_BACKTRACE", "0")
            .output()
            .unwrap();
        results.push((family, output.status));
    }
    assert!(
        results.iter().all(|(_, status)| status.success()),
        "actual literal result children: {results:?}"
    );
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn double_panic_child() {
    let Ok(family) = std::env::var("CRABBER_LITERAL_RESULT_CHILD") else {
        return;
    };
    let family = match family.as_str() {
        "typed" => "typed",
        "ordinary-json" => "ordinary-json",
        "final-json" => "final-json",
        _ => panic!("unknown family"),
    };
    run(family, Mode::Both).await;
}
