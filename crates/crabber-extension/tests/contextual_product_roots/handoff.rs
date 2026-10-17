use super::*;
struct HeldFactory {
    seen: Arc<Seen>,
    entered: Notify,
    ready: Arc<tokio::sync::Semaphore>,
}
impl ContextualProductFactory for HeldFactory {
    fn identity(&self) -> ComponentIdentity {
        ComponentIdentity {
            id: "held-root".into(),
            version: "1".into(),
        }
    }
    fn config_hash(&self) -> String {
        "held-root".into()
    }
    fn scope(&self) -> FactoryScope {
        FactoryScope::Session
    }
    fn create_root(
        &self,
        _: FactoryContext,
    ) -> BoxFuture<'_, Result<Box<dyn ContextualProductRoot>, ExtensionError>> {
        self.seen.created.fetch_add(1, Ordering::SeqCst);
        self.entered.notify_one();
        let ready = self.ready.clone();
        Box::pin(HeldReady {
            ready: Box::pin(async move {
                ready.acquire().await.unwrap().forget();
            }),
            seen: self.seen.clone(),
        })
    }
}
struct HeldReady {
    ready: BoxFuture<'static, ()>,
    seen: Arc<Seen>,
}
impl Future for HeldReady {
    type Output = Result<Box<dyn ContextualProductRoot>, ExtensionError>;
    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        std::task::ready!(self.ready.as_mut().poll(context));
        Poll::Ready(Ok(Box::new(Root(self.seen.clone()))))
    }
}
impl Drop for HeldReady {
    fn drop(&mut self) {
        self.seen.future_drop.block();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn already_closed_cell_retains_ready_root_before_blocked_future_disposal() {
    let seen = Seen::new(false, true, Failure::None);
    let _release = Release(vec![seen.future_drop.clone(), seen.root_drop.clone()]);
    let host = Arc::new(Host::default());
    let factory = Arc::new(HeldFactory {
        seen: seen.clone(),
        entered: Notify::new(),
        ready: Arc::new(tokio::sync::Semaphore::new(0)),
    });
    let provider = ContextualPlanProvider::new_with_product_roots(
        Registry::new(),
        vec![factory.clone()],
        Duration::from_secs(3),
        host.clone(),
    )
    .unwrap();
    let acquire = tokio::spawn({
        let provider = provider.clone();
        async move { provider.acquire_plan_in(&context()).await }
    });
    tokio::time::timeout(Duration::from_secs(3), factory.entered.notified())
        .await
        .unwrap();
    let graph = graph(&host);
    let root = graph.product_root(0).unwrap();
    graph.request_close().unwrap();
    let no_ready_root = root.disposed(Duration::from_millis(20)).await;
    let before = seen.dropped.load(Ordering::SeqCst);
    factory.ready.add_permits(1);
    entered(&seen.future_drop).await;
    entered(&seen.root_drop).await;
    tokio::time::timeout(Duration::from_secs(3), async {
        while seen.dropped.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let pending_original_future = root.disposed(Duration::from_millis(20)).await;
    seen.future_drop.release();
    let result = acquire.await.unwrap();
    let disposed = root.disposed(Duration::from_secs(3)).await;
    provider.close_all(Duration::from_secs(3)).await.unwrap();
    assert!(no_ready_root.is_err() && pending_original_future.is_err());
    assert_eq!(before, 0);
    assert!(result.is_err() && disposed.is_ok());
    assert_eq!(seen.dropped.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn provider_close_drives_root_drop_before_original_create_future_disposal() {
    let mut observed = Vec::new();
    for (scope, all) in [
        (FactoryScope::Workspace, false),
        (FactoryScope::Session, false),
        (FactoryScope::Session, true),
    ] {
        let seen = Seen::new(false, true, Failure::None);
        let _release = Release(vec![seen.future_drop.clone()]);
        let host = Arc::new(Host::default());
        let provider = provider(host.clone(), seen.clone(), scope);
        let acquire = tokio::spawn({
            let provider = provider.clone();
            async move { provider.acquire_plan_in(&context()).await }
        });
        entered(&seen.future_drop).await;
        let close = tokio::spawn({
            let provider = provider.clone();
            async move {
                if all {
                    provider.close_all(Duration::from_secs(3)).await
                } else if scope == FactoryScope::Workspace {
                    provider
                        .close_workspace(&context(), Duration::from_secs(3))
                        .await
                } else {
                    provider
                        .close_session(&context(), Duration::from_secs(3))
                        .await
                }
            }
        });
        let started = tokio::time::timeout(
            Duration::from_millis(100),
            seen.root_drop.entered.notified(),
        )
        .await;
        // Always release the real original future and join cleanup before asserting.
        seen.future_drop.release();
        let acquired = acquire.await.unwrap();
        drop(acquired);
        close.await.unwrap().unwrap();
        graph(&host)
            .product_root(0)
            .unwrap()
            .disposed(Duration::from_secs(3))
            .await
            .unwrap();
        provider.close_all(Duration::from_secs(3)).await.unwrap();
        observed.push((
            scope,
            all,
            started.is_ok(),
            seen.dropped.load(Ordering::SeqCst),
        ));
    }
    assert!(
        observed
            .iter()
            .all(|(_, _, started, drops)| *started && *drops == 1),
        "{observed:?}"
    );
}
