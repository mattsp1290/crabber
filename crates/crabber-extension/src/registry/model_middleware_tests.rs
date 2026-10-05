use super::*;
use crate::{
    MAX_PROMPT_CONTRIBUTION_BYTES, MAX_PROMPT_CONTRIBUTIONS_TOTAL_BYTES, MiddlewareDescriptor,
    ModelAttemptContext, PromptAttemptContext, PromptContributionOutcome, SystemPromptMiddleware,
    WorkspaceContext, WorkspaceReadError, WorkspaceReader, WorkspaceReaderResolver,
    collect_prompt_contributions_with_resolver,
};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};

const HASH: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

fn descriptor() -> MiddlewareDescriptor {
    MiddlewareDescriptor::new("agents-md", "1.0.0", HASH).unwrap()
}

fn context() -> PromptAttemptContext {
    PromptAttemptContext::new(
        "session".into(),
        "run".into(),
        "turn".into(),
        WorkspaceContext::from_persisted("workspace", "/workspace"),
        "provider".into(),
        "model".into(),
        2,
        true,
    )
}

struct Callback {
    call: Arc<dyn Fn(ModelAttemptContext) -> Result<Option<String>, String> + Send + Sync>,
}

#[async_trait]
impl SystemPromptMiddleware for Callback {
    async fn contribute(&self, context: ModelAttemptContext) -> Result<Option<String>, String> {
        (self.call)(context)
    }
}

enum Registration {
    Legacy {
        order: i32,
        name: String,
        text: String,
    },
    Typed {
        order: i32,
        name: String,
        callback: Arc<dyn SystemPromptMiddleware>,
    },
}

struct Registered {
    registrations: Vec<Registration>,
    rolled_back: Arc<AtomicBool>,
}

#[async_trait]
impl Extension for Registered {
    fn id(&self) -> &'static str {
        "registered"
    }
    fn version(&self) -> &'static str {
        "1"
    }
    fn config_hash(&self) -> String {
        String::new()
    }

    async fn install(&self, registrar: &mut Registrar) -> Result<(), ExtensionError> {
        let rolled_back = self.rolled_back.clone();
        registrar.defer(move || rolled_back.store(true, Ordering::SeqCst));
        for registration in &self.registrations {
            match registration {
                Registration::Legacy { order, name, text } => {
                    let text = text.clone();
                    registrar.prompt_contributor(
                        *order,
                        name.clone(),
                        Arc::new(move |_| {
                            let text = text.clone();
                            Box::pin(async move { Ok(Some(text)) })
                        }),
                    );
                }
                Registration::Typed {
                    order,
                    name,
                    callback,
                } => registrar.system_prompt_middleware(
                    name.clone(),
                    *order,
                    descriptor(),
                    callback.clone(),
                )?,
            }
        }
        Ok(())
    }
}

fn extension(registrations: Vec<Registration>) -> Arc<Registered> {
    Arc::new(Registered {
        registrations,
        rolled_back: Arc::new(AtomicBool::new(false)),
    })
}

fn typed(order: i32, name: &str, text: &str) -> Registration {
    let text = text.to_owned();
    Registration::Typed {
        order,
        name: name.into(),
        callback: Arc::new(Callback {
            call: Arc::new(move |_| Ok(Some(text.clone()))),
        }),
    }
}

#[test]
fn descriptor_is_validated_and_exposes_only_getters() {
    assert_eq!(crate::SYSTEM_PROMPT_MIDDLEWARE_CONTRACT_VERSION, 1);
    let value = descriptor();
    assert_eq!(value.kind(), "agents-md");
    assert_eq!(value.version(), "1.0.0");
    assert_eq!(value.config_hash(), HASH);

    for (kind, version, hash) in [
        ("", "1", HASH),
        ("kind", "", HASH),
        (
            &"k".repeat(crate::MAX_MIDDLEWARE_DESCRIPTOR_FIELD_BYTES + 1),
            "1",
            HASH,
        ),
        (
            "kind",
            &"v".repeat(crate::MAX_MIDDLEWARE_DESCRIPTOR_FIELD_BYTES + 1),
            HASH,
        ),
        ("kind", "1", "abc"),
        (
            "kind",
            "1",
            "A123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        ),
        (
            "kind",
            "1",
            "g123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        ),
    ] {
        assert!(MiddlewareDescriptor::new(kind, version, hash).is_err());
    }
}

#[tokio::test]
async fn valid_registration_orders_both_variants_and_uses_registration_name() {
    let registry = Registry::new();
    registry
        .mount(
            extension(vec![
                typed(1, "typed-name", "typed"),
                Registration::Legacy {
                    order: 1,
                    name: "legacy-name".into(),
                    text: "legacy".into(),
                },
            ]),
            Scope::Global,
        )
        .await
        .unwrap();
    let plan = registry.acquire(&"session".into());
    let PromptContributionOutcome::Completed { sections } =
        collect_prompt_contributions_with_resolver(&plan.prompt_contributors, context(), None)
            .await
    else {
        panic!("collection failed")
    };
    assert_eq!(
        sections
            .iter()
            .map(|s| (s.name.as_str(), s.text.as_str()))
            .collect::<Vec<_>>(),
        vec![("legacy-name", "legacy"), ("typed-name", "typed")]
    );
}

#[tokio::test]
async fn invalid_names_are_rejected_at_mount_for_typed_registrations() {
    for name in [
        String::new(),
        "x".repeat(crate::MAX_PROMPT_CONTRIBUTOR_NAME_BYTES + 1),
        "bad\nname".into(),
    ] {
        let registry = Registry::new();
        let extension = extension(vec![typed(0, &name, "text")]);
        assert!(
            matches!(registry.mount(extension.clone(), Scope::Global).await,
            Err(ExtensionError::Plan(message)) if message == "invalid prompt contributor name")
        );
        assert!(extension.rolled_back.load(Ordering::SeqCst));
    }
}

#[tokio::test]
async fn typed_and_legacy_share_collision_namespace_and_scope_shadowing() {
    let registry = Registry::new();
    registry
        .mount(
            extension(vec![Registration::Legacy {
                order: 0,
                name: "same".into(),
                text: "global".into(),
            }]),
            Scope::Global,
        )
        .await
        .unwrap();
    assert!(
        matches!(registry.mount(extension(vec![typed(0, "same", "collision")]), Scope::Global).await,
        Err(ExtensionError::PromptContributorCollision(name)) if name == "same")
    );
    registry
        .mount(
            extension(vec![typed(0, "same", "session")]),
            Scope::Session("session".into()),
        )
        .await
        .unwrap();
    let plan = registry.acquire(&"session".into());
    let PromptContributionOutcome::Completed { sections } =
        collect_prompt_contributions_with_resolver(&plan.prompt_contributors, context(), None)
            .await
    else {
        panic!()
    };
    assert_eq!(sections[0].text, "session");
}

struct Reader;
#[async_trait]
impl WorkspaceReader for Reader {
    async fn read_limited(&self, _: &str, _: usize) -> Result<Vec<u8>, WorkspaceReadError> {
        Ok(vec![])
    }
}
struct Resolver;
#[async_trait]
impl WorkspaceReaderResolver for Resolver {
    async fn resolve(
        &self,
        _: &WorkspaceContext,
    ) -> Result<Arc<dyn WorkspaceReader>, WorkspaceReadError> {
        Ok(Arc::new(Reader))
    }
}

struct Pending {
    child: Arc<Mutex<Option<tokio_util::sync::CancellationToken>>>,
    cleanup: Arc<AtomicBool>,
}

#[async_trait]
impl SystemPromptMiddleware for Pending {
    async fn contribute(&self, context: ModelAttemptContext) -> Result<Option<String>, String> {
        *self.child.lock().unwrap() = Some(context.cancellation().clone());
        self.cleanup
            .store(!context.cleanup().is_closing(), Ordering::SeqCst);
        futures::future::pending().await
    }
}

#[tokio::test]
async fn only_typed_callback_receives_resolver_authority() {
    let typed_saw = Arc::new(AtomicBool::new(false));
    let saw = typed_saw.clone();
    let callback = Arc::new(Callback {
        call: Arc::new(move |context| {
            saw.store(
                context.workspace_reader_resolver().is_some(),
                Ordering::SeqCst,
            );
            Ok(Some("typed".into()))
        }),
    });
    let registry = Registry::new();
    registry
        .mount(
            extension(vec![
                Registration::Legacy {
                    order: 0,
                    name: "legacy".into(),
                    text: "legacy".into(),
                },
                Registration::Typed {
                    order: 1,
                    name: "typed".into(),
                    callback,
                },
            ]),
            Scope::Global,
        )
        .await
        .unwrap();
    let resolver: Arc<dyn WorkspaceReaderResolver> = Arc::new(Resolver);
    let plan = registry.acquire(&"session".into());
    assert!(matches!(
        collect_prompt_contributions_with_resolver(
            &plan.prompt_contributors,
            context(),
            Some(resolver)
        )
        .await,
        PromptContributionOutcome::Completed { .. }
    ));
    assert!(typed_saw.load(Ordering::SeqCst));
}

#[tokio::test(start_paused = true)]
async fn typed_shares_deadline_cancellation_cleanup_and_sanitized_failure() {
    let child = Arc::new(Mutex::new(None));
    let cleanup_is_mount = Arc::new(AtomicBool::new(false));
    let child_copy = child.clone();
    let cleanup_copy = cleanup_is_mount.clone();
    let registry = Registry::new();
    registry
        .mount(
            extension(vec![Registration::Typed {
                order: 0,
                name: "public-name".into(),
                callback: Arc::new(Pending {
                    child: child_copy,
                    cleanup: cleanup_copy,
                }),
            }]),
            Scope::Global,
        )
        .await
        .unwrap();
    let plan = registry.acquire(&"session".into());
    assert_eq!(
        collect_prompt_contributions_with_resolver(&plan.prompt_contributors, context(), None)
            .await,
        PromptContributionOutcome::Failed {
            contributor: "public-name".into()
        }
    );
    assert!(child.lock().unwrap().as_ref().unwrap().is_cancelled());
    assert!(cleanup_is_mount.load(Ordering::SeqCst));
}

#[tokio::test]
async fn typed_observes_parent_cancellation_without_invocation() {
    let invoked = Arc::new(AtomicBool::new(false));
    let invoked_copy = invoked.clone();
    let callback = Arc::new(Callback {
        call: Arc::new(move |_| {
            invoked_copy.store(true, Ordering::SeqCst);
            Ok(Some("discarded".into()))
        }),
    });
    let registry = Registry::new();
    registry
        .mount(
            extension(vec![Registration::Typed {
                order: 0,
                name: "cancelled".into(),
                callback,
            }]),
            Scope::Global,
        )
        .await
        .unwrap();
    let cancellation = tokio_util::sync::CancellationToken::new();
    cancellation.cancel();
    let plan = registry.acquire(&"session".into());
    assert_eq!(
        collect_prompt_contributions_with_resolver(
            &plan.prompt_contributors,
            context().with_cancellation(cancellation),
            None,
        )
        .await,
        PromptContributionOutcome::Interrupted
    );
    assert!(!invoked.load(Ordering::SeqCst));
}

#[tokio::test]
async fn typed_shares_panic_error_and_byte_limits_with_sanitized_name() {
    for callback in [
        Arc::new(Callback {
            call: Arc::new(|_| Err("secret error".into())),
        }) as Arc<dyn SystemPromptMiddleware>,
        Arc::new(Callback {
            call: Arc::new(|_| panic!("secret panic")),
        }) as Arc<dyn SystemPromptMiddleware>,
        Arc::new(Callback {
            call: Arc::new(|_| Ok(Some("x".repeat(MAX_PROMPT_CONTRIBUTION_BYTES + 1)))),
        }) as Arc<dyn SystemPromptMiddleware>,
    ] {
        let registry = Registry::new();
        registry
            .mount(
                extension(vec![Registration::Typed {
                    order: 0,
                    name: "safe-name".into(),
                    callback,
                }]),
                Scope::Global,
            )
            .await
            .unwrap();
        let plan = registry.acquire(&"session".into());
        assert_eq!(
            collect_prompt_contributions_with_resolver(&plan.prompt_contributors, context(), None)
                .await,
            PromptContributionOutcome::Failed {
                contributor: "safe-name".into()
            }
        );
    }

    let count = MAX_PROMPT_CONTRIBUTIONS_TOTAL_BYTES / MAX_PROMPT_CONTRIBUTION_BYTES + 1;
    let registrations = (0..count)
        .map(|index| {
            typed(
                i32::try_from(index).unwrap(),
                &format!("typed-{index}"),
                &"x".repeat(MAX_PROMPT_CONTRIBUTION_BYTES),
            )
        })
        .collect();
    let registry = Registry::new();
    registry
        .mount(extension(registrations), Scope::Global)
        .await
        .unwrap();
    let plan = registry.acquire(&"session".into());
    assert_eq!(
        collect_prompt_contributions_with_resolver(&plan.prompt_contributors, context(), None)
            .await,
        PromptContributionOutcome::Failed {
            contributor: format!("typed-{}", count - 1)
        }
    );
}
