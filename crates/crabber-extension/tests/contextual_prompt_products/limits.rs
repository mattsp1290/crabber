use super::*;
struct Named(&'static str, Arc<Seen>);
impl ContextualPromptProductFactory for Named {
    fn identity(&self) -> ComponentIdentity {
        ComponentIdentity {
            id: self.0.into(),
            version: "1".into(),
        }
    }
    fn config_hash(&self) -> String {
        "{}".into()
    }
    fn scope(&self) -> FactoryScope {
        FactoryScope::Session
    }
    fn create_prompt(
        &self,
        _: FactoryContext,
    ) -> BoxFuture<'_, Result<ContextualPromptProduct, ExtensionError>> {
        self.1.made.fetch_add(1, Ordering::SeqCst);
        let original = Original(self.1.clone());
        let product = ContextualPromptProduct::native(
            self.0.into(),
            0,
            Box::new(move |context| original.native(context)),
        );
        Box::pin(async move { Ok(product) })
    }
}
#[tokio::test]
async fn unchanged_individual_and_total_text_limits_report_only_original_contributor_name() {
    for total in [false, true] {
        let names = if total {
            vec!["a", "b", "c", "d", "e"]
        } else {
            vec!["a"]
        };
        let seen = names.iter().map(|_| Seen::new()).collect::<Vec<_>>();
        for original in &seen {
            *original.text.lock().unwrap() =
                "x".repeat(MAX_PROMPT_CONTRIBUTION_BYTES + usize::from(!total));
        }
        let factories = names
            .iter()
            .zip(&seen)
            .map(|(name, original)| {
                ContextualOwnedProductFactory::Prompt(Arc::new(Named(name, original.clone())))
            })
            .collect();
        let provider = ContextualPlanProvider::new_with_owned_products(
            Registry::new(),
            factories,
            BOUND,
            Arc::new(Host::default()),
        )
        .unwrap();
        let plan = provider.acquire_plan_in(&session()).await.unwrap();
        let (outcome, joined) = call(&plan.prompt_contributors).await;
        drop(plan);
        provider.close_all(BOUND).await.unwrap();
        assert_eq!(
            outcome,
            PromptContributionOutcome::Failed {
                contributor: names.last().unwrap().to_string()
            }
        );
        assert_eq!(joined.pending, 0);
        assert_eq!(joined.failures, [] as [ResourceFailure; 0]);
        for original in seen {
            assert_eq!(original.calls.load(Ordering::SeqCst), 1);
            assert_eq!(original.drops.load(Ordering::SeqCst), 1);
        }
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn original_contribution_deadline_returns_before_same_literal_disposal_and_joins() {
    for typed in [false, true] {
        let seen = Seen::new();
        seen.mode.store(2, Ordering::SeqCst);
        *seen.future_drop.open.lock().unwrap() = false;
        let release = Release(vec![seen.future_drop.clone()]);
        let (provider, _, _) = provider(&seen, typed, FactoryScope::Session, "original");
        let plan = provider.acquire_plan_in(&session()).await.unwrap();
        let callbacks = plan.prompt_contributors.clone();
        drop(plan);
        let parent = ToolResourceOwner::new();
        let owner = CallbackInvocationOwner::attach(parent.resources()).unwrap();
        let waiter = tokio::spawn(with_callback_invocations(owner.capability(), async move {
            collect_prompt_contributions(&callbacks, context()).await
        }));
        tokio::time::timeout(BOUND, seen.entered.notified())
            .await
            .unwrap();
        let outcome = tokio::time::timeout(PROMPT_CONTRIBUTION_DEADLINE + BOUND, waiter).await;
        seen.future_drop.entered().await;
        let pending = parent.drain(Duration::from_millis(20)).await;
        drop(release);
        drop(owner);
        parent.close();
        let joined = parent.drain(BOUND).await;
        provider.close_all(BOUND).await.unwrap();
        assert_eq!(
            outcome.unwrap().unwrap(),
            PromptContributionOutcome::Failed {
                contributor: "original".into()
            }
        );
        assert_eq!(pending.pending, 1);
        assert_eq!(joined.pending, 0);
        assert_eq!(joined.failures, [] as [ResourceFailure; 0]);
        assert_eq!(seen.calls.load(Ordering::SeqCst), 1);
        assert_eq!(seen.futures.load(Ordering::SeqCst), 1);
        assert_eq!(seen.drops.load(Ordering::SeqCst), 1);
    }
}
