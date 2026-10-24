use super::*;
#[tokio::test]
async fn known_active_replay_needs_no_new_local_capacity() {
    {
        let mut pending = PendingHost::new(false);
        let ext = configured(
            pending.runner.clone(),
            Limits {
                max_in_flight: 1,
                ..limits()
            },
        );
        let ext = Arc::new(ext);
        let (_, handle, tools) = mounted(ext.clone()).await;
        let host = Arc::new(Live {
            registrations: 0.into(),
            workers: Mutex::new(vec![]),
        });
        let context = context(CancellationToken::new())
            .with_session_invocations(SessionInvocations::from_host(host.clone()));
        let first = tools[START_TOOL]
            .execute_json_with_context(context.clone(), arguments())
            .await
            .unwrap();
        pending.entered(1).await;
        let second = tools[START_TOOL]
            .execute_json_with_context(context, arguments())
            .await
            .unwrap();
        assert_eq!(first, second);
        assert_eq!(host.registrations.load(Ordering::SeqCst), 1);
        assert_eq!(ext.owner_report().live, 1);
        assert_eq!(ext.owner_report().tracked, 1);
        pending.release.notify_one();
        let workers = std::mem::take(&mut *host.workers.lock().unwrap());
        for worker in workers {
            worker.await.unwrap();
        }
        assert_eq!(ext.live_runners(), 0);
        handle.close().await.unwrap();
    }
}
#[tokio::test]
async fn known_terminal_replay_with_one_retained_slot_preserves_id_and_output() {
    let ext = Arc::new(configured(
        runner(Response::Completed("retained".into())),
        Limits {
            max_tracked: 1,
            ..limits()
        },
    ));
    let (_, handle, tools) = mounted(ext.clone()).await;
    let context =
        context(CancellationToken::new()).with_session_invocations(SessionInvocations::from_host(
            Arc::new(crate::invocation_support::JoinedSpawner::default()),
        ));
    let first = tools[START_TOOL]
        .execute_json_with_context(context.clone(), arguments())
        .await
        .unwrap();
    let replay = tools[START_TOOL]
        .execute_json_with_context(context.clone(), arguments())
        .await
        .unwrap();
    assert_eq!(first, replay);
    let status = tools[STATUS_TOOL]
        .execute_json_with_context(context, json!({"task_id":first["task_id"]}))
        .await
        .unwrap();
    assert_eq!(status["output"], "retained");
    assert_eq!(ext.owner_report().tracked, 1);
    handle.close().await.unwrap();
}
struct CaptureFactory {
    inner: Arc<crabber_extensions::delegate_task::Factory>,
    instance: Mutex<Option<Arc<dyn crabber::extension::Extension>>>,
}
#[async_trait::async_trait]
impl crabber::extension::ExtensionFactory for CaptureFactory {
    fn identity(&self) -> crabber::extension::ComponentIdentity {
        self.inner.identity()
    }
    fn config_hash(&self) -> String {
        self.inner.config_hash()
    }
    fn scope(&self) -> crabber::extension::FactoryScope {
        self.inner.scope()
    }
    async fn create(
        &self,
        context: crabber::extension::FactoryContext,
    ) -> Result<Arc<dyn crabber::extension::Extension>, ExtensionError> {
        let instance = self.inner.create(context).await?;
        *self.instance.lock().unwrap() = Some(instance.clone());
        Ok(instance)
    }
}
#[tokio::test]
async fn known_replay_needs_no_new_aggregate_factory_permit() {
    use crabber::extension::{FactoryScope, Registry, Scope};
    let mut pending = PendingHost::new(false);
    let factory = Arc::new(
        crabber_extensions::delegate_task::Factory::new(
            Options {
                runner: pending.runner.clone(),
                runner_identity: "aggregate-replay".into(),
                limits: Limits {
                    max_in_flight: 2,
                    ..limits()
                },
            },
            FactoryScope::Session,
        )
        .unwrap()
        .with_shared_capacity(SharedCapacity::new(1).unwrap()),
    );
    let capture = Arc::new(CaptureFactory {
        inner: factory.clone(),
        instance: Mutex::new(None),
    });
    let agent = agent_support::builder()
        .memory()
        .provider(Arc::new(FakeProvider::scripted(vec![done()])))
        .config(agent_config())
        .extension_factory(capture.clone())
        .build()
        .unwrap();
    let execution = crabber::ExecutionContext::new(
        crabber::core::SessionContext::new("session".into(), "workspace", "/workspace"),
        agent_config().selection,
    );
    agent_support::first_prompt(&agent, &execution, "admit")
        .await
        .unwrap()
        .done()
        .await
        .unwrap();
    let registry = Registry::new();
    let instance = capture.instance.lock().unwrap().clone().unwrap();
    let handle = registry.mount(instance, Scope::Global).await.unwrap();
    let plan = registry.acquire(&SessionId::from("session"));
    let start = plan
        .tools
        .iter()
        .find(|t| t.info.name == START_TOOL)
        .unwrap()
        .executor
        .clone();
    let host = Arc::new(Live {
        registrations: 0.into(),
        workers: Mutex::new(vec![]),
    });
    let context = context(CancellationToken::new())
        .with_session_invocations(SessionInvocations::from_host(host.clone()));
    let first = start
        .execute_json_with_context(context.clone(), arguments())
        .await
        .unwrap();
    pending.entered(1).await;
    assert_eq!(
        start
            .execute_json_with_context(next(context.clone()), arguments())
            .await
            .unwrap()["status"],
        "unavailable"
    );
    assert_eq!(
        start
            .execute_json_with_context(context, arguments())
            .await
            .unwrap(),
        first
    );
    assert_eq!(host.registrations.load(Ordering::SeqCst), 1);
    assert_eq!(
        factory
            .owner_report("workspace", Some(&SessionId::from("session")))
            .unwrap()
            .live,
        1
    );
    pending.release.notify_one();
    let workers = std::mem::take(&mut *host.workers.lock().unwrap());
    for worker in workers {
        worker.await.unwrap();
    }
    drop(plan);
    handle.close().await.unwrap();
    assert!(agent.close(Duration::from_secs(5)).await.local_drained());
}
