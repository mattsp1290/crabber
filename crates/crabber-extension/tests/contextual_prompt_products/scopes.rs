use super::*;
use async_trait::async_trait;
struct Reader;
#[async_trait]
impl WorkspaceReader for Reader {
    async fn read_limited(&self, path: &str, max: usize) -> Result<Vec<u8>, WorkspaceReadError> {
        assert_eq!((path, max), ("file", 4));
        Ok(b"data".to_vec())
    }
}
struct Resolver;
#[async_trait]
impl WorkspaceReaderResolver for Resolver {
    async fn resolve(
        &self,
        workspace: &WorkspaceContext,
    ) -> Result<Arc<dyn WorkspaceReader>, WorkspaceReadError> {
        assert_eq!(workspace.workspace_id(), Some("workspace"));
        Ok(Arc::new(Reader))
    }
}
#[tokio::test]
async fn typed_only_callback_receives_exact_original_resolver_authority() {
    for typed in [false, true] {
        let seen = Seen::new();
        let resolver: Arc<dyn WorkspaceReaderResolver> = Arc::new(Resolver);
        *seen.reader.lock().unwrap() = Some(resolver.clone());
        let (provider, _, _) = provider(&seen, typed, FactoryScope::Session, "original");
        let plan = provider.acquire_plan_in(&session()).await.unwrap();
        let parent = ToolResourceOwner::new();
        let owner = CallbackInvocationOwner::attach(parent.resources()).unwrap();
        let outcome = with_callback_invocations(
            owner.capability(),
            collect_prompt_contributions_with_resolver(
                &plan.prompt_contributors,
                context(),
                Some(resolver),
            ),
        )
        .await;
        drop(owner);
        parent.close();
        let joined = parent.drain(BOUND).await;
        drop(plan);
        provider.close_all(BOUND).await.unwrap();
        completed(outcome);
        assert_eq!(joined.pending, 0);
        assert_eq!(
            seen.reader_visits.load(Ordering::SeqCst),
            usize::from(typed)
        );
        assert_eq!(seen.calls.load(Ordering::SeqCst), 1);
        assert_eq!(seen.drops.load(Ordering::SeqCst), 1);
    }
}
struct Global;
#[async_trait]
impl Extension for Global {
    fn id(&self) -> &'static str {
        "global-prompt-order"
    }
    fn version(&self) -> &'static str {
        "1"
    }
    fn config_hash(&self) -> String {
        "{}".into()
    }
    async fn install(&self, r: &mut Registrar) -> Result<(), ExtensionError> {
        for (name, order) in [("original", -10), ("before", 0), ("after", 10)] {
            r.prompt_contributor(
                order,
                name,
                Arc::new(move |_| Box::pin(async move { Ok(Some(name.into())) })),
            );
        }
        Ok(())
    }
}
#[tokio::test]
async fn scope_replacement_order_semantic_identity_and_workspace_sibling_survive() {
    for typed in [false, true] {
        let seen = Seen::new();
        let registry = Registry::new();
        registry
            .mount_owned(Arc::new(Global), Scope::Global)
            .await
            .unwrap();
        let host = Arc::new(Host::default());
        let provider = ContextualPlanProvider::new_with_owned_products(
            registry,
            vec![ContextualOwnedProductFactory::Prompt(Arc::new(Factory {
                seen: seen.clone(),
                typed,
                scope: FactoryScope::Workspace,
                name: "original".into(),
            }))],
            BOUND,
            host,
        )
        .unwrap();
        let a = session();
        let b = session();
        let first = provider.acquire_plan_in(&a).await.unwrap();
        let second = provider.acquire_plan_in(&b).await.unwrap();
        let first_hash = first.fingerprint.clone();
        let second_hash = second.fingerprint.clone();
        let relevant = first
            .components
            .iter()
            .filter(|c| {
                c.id == "prompt-contributor:original" || c.id == "model-middleware:original"
            })
            .cloned()
            .collect::<Vec<_>>();
        drop(first);
        provider.close_session(&a, BOUND).await.unwrap();
        let (outcome, joined) = call(&second.prompt_contributors).await;
        let before = seen.drops.load(Ordering::SeqCst);
        drop(second);
        provider.close_all(BOUND).await.unwrap();
        if typed {
            assert_eq!(relevant.len(), 1);
            assert_eq!(relevant[0].id, "model-middleware:original");
            assert_eq!(
                serde_json::from_str::<serde_json::Value>(&relevant[0].version).unwrap(),
                serde_json::json!({"order":7,"kind":"prompt","version":"1","config_hash":"0".repeat(64)})
            );
        } else {
            // Legacy-over-legacy retains BOTH frozen component entries; preserve it.
            assert_eq!(relevant.len(), 2);
            assert!(
                relevant
                    .iter()
                    .all(|c| c.id == "prompt-contributor:original")
            );
            assert!(relevant[0].version.starts_with("-10:"));
            assert!(relevant[1].version.starts_with("7:"));
        }
        let PromptContributionOutcome::Completed { sections } = outcome else {
            panic!("expected completed scoped prompts");
        };
        assert_eq!(
            sections.iter().map(|s| s.name.as_str()).collect::<Vec<_>>(),
            ["before", "original", "after"]
        );
        assert_eq!(sections[1].text, "original text");
        assert_eq!(first_hash, second_hash);
        assert_eq!(joined.pending, 0);
        assert_eq!(before, 0);
        assert_eq!(seen.made.load(Ordering::SeqCst), 1);
        assert_eq!(seen.drops.load(Ordering::SeqCst), 1);
    }
}
struct RootFactory(Arc<AtomicUsize>);
struct Root(Arc<AtomicUsize>);
impl Drop for Root {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
impl ContextualProductFactory for RootFactory {
    fn identity(&self) -> ComponentIdentity {
        ComponentIdentity {
            id: "first-root".into(),
            version: "1".into(),
        }
    }
    fn config_hash(&self) -> String {
        "{}".into()
    }
    fn scope(&self) -> FactoryScope {
        FactoryScope::Session
    }
    fn create_root(
        &self,
        _: FactoryContext,
    ) -> BoxFuture<'_, Result<Box<dyn ContextualProductRoot>, ExtensionError>> {
        Box::pin(async { Ok(Box::new(Root(self.0.clone())) as Box<dyn ContextualProductRoot>) })
    }
}
#[tokio::test]
async fn prompt_metadata_failure_in_mixed_generation_disposes_original_earlier_root() {
    let root = Arc::new(AtomicUsize::new(0));
    let seen = Seen::new();
    let provider = ContextualPlanProvider::new_with_owned_products(
        Registry::new(),
        vec![
            ContextualOwnedProductFactory::Root(Arc::new(RootFactory(root.clone()))),
            ContextualOwnedProductFactory::Prompt(Arc::new(Factory {
                seen: seen.clone(),
                typed: true,
                scope: FactoryScope::Session,
                name: String::new(),
            })),
        ],
        BOUND,
        Arc::new(Host::default()),
    )
    .unwrap();
    let result = provider.acquire_plan_in(&session()).await;
    let _closed = provider.close_all(BOUND).await;
    assert!(result.is_err());
    assert_eq!(root.load(Ordering::SeqCst), 1);
    assert_eq!(seen.drops.load(Ordering::SeqCst), 1);
    assert_eq!(seen.calls.load(Ordering::SeqCst), 0);
}
