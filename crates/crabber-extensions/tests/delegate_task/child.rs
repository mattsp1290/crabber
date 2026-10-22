use super::*;
use crabber::JsonToolExecutor;

struct ChildWait(Arc<tokio::sync::Notify>);
#[async_trait::async_trait]
impl JsonToolExecutor for ChildWait {
    async fn execute(&self, _: Value) -> Result<Value, ExtensionError> {
        self.0.notify_one();
        std::future::pending().await
    }
}
struct ChildTool(Arc<tokio::sync::Notify>);
#[async_trait::async_trait]
impl Extension for ChildTool {
    fn id(&self) -> &str {
        "synthetic-child"
    }
    fn version(&self) -> &str {
        "1"
    }
    fn config_hash(&self) -> String {
        "synthetic-child-v1".into()
    }
    async fn install(
        &self,
        registrar: &mut crabber::extension::Registrar,
    ) -> Result<(), ExtensionError> {
        registrar.tool(Arc::new(crabber::extension::ToolDefinition {
            info: crabber::core::ToolInfo {
                name: "child_wait".into(),
                description: "Synthetic pending child tool".into(),
                parameters: json!({"type":"object"}),
                retry_safe: false,
                required_permissions: vec![],
            },
            executor: Arc::new(ChildWait(self.0.clone())),
        }));
        Ok(())
    }
}

struct ChildHost {
    runner: Runner,
    entered: Arc<tokio::sync::Notify>,
    status: Arc<std::sync::Mutex<Option<RunStatus>>>,
    store: Arc<MemoryStore>,
    session: Arc<std::sync::Mutex<Option<SessionId>>>,
    run_id: Arc<std::sync::Mutex<Option<RunId>>>,
}
impl ChildHost {
    fn new(pending: bool) -> Self {
        let entered = Arc::new(tokio::sync::Notify::new());
        let status = Arc::new(std::sync::Mutex::new(None));
        let store = Arc::new(MemoryStore::new());
        let session = Arc::new(std::sync::Mutex::new(None));
        let run_id = Arc::new(std::sync::Mutex::new(None));
        let runner: Runner = {
            let entered = entered.clone();
            let status = status.clone();
            let store = store.clone();
            let session = session.clone();
            let run_id = run_id.clone();
            Arc::new(move |request| {
                let entered = entered.clone();
                let status = status.clone();
                let store = store.clone();
                let session = session.clone();
                let run_id = run_id.clone();
                Box::pin(async move {
                    let script = if pending {
                        let id = ToolCallId::new();
                        vec![
                            StreamDelta::ToolCallStart {
                                call_id: id.clone(),
                                name: "child_wait".into(),
                            },
                            StreamDelta::ToolCallArgsDelta {
                                call_id: id.clone(),
                                text: "{}".into(),
                            },
                            StreamDelta::ToolCallDone { call_id: id },
                            StreamDelta::Completed,
                        ]
                    } else {
                        vec![
                            StreamDelta::TextDelta("child result".into()),
                            StreamDelta::Completed,
                        ]
                    };
                    let child = Agent::builder()
                        .behavior_version("crabber-extensions-tests.v1")
                        .store(store.clone())
                        .provider(Arc::new(FakeProvider::scripted(vec![script])))
                        .config(agent_config())
                        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
                        .shared_extension(Arc::new(ChildTool(entered)), Scope::Global)
                        .build()
                        .unwrap();
                    let mut run = child.prompt(None, request.task()).await.unwrap();
                    *session.lock().unwrap() = Some(run.session_id().clone());
                    *run_id.lock().unwrap() = Some(run.run_id().clone());
                    let mut events = run.events();
                    loop {
                        tokio::select! {
                            biased;
                            _ = request.cancellation().cancelled() => {run.interrupt(); break;},
                            event = events.recv() => {if !matches!(event, Ok(Some(_))) {break;}},
                        }
                    }
                    let result = run.done().await;
                    let uncertain = matches!(
                        &result,
                        Err(crabber::RuntimeError::PhaseOutcomeUnknown { .. })
                    );
                    let observed = match result {
                        Ok(result) => result.status,
                        Err(crabber::RuntimeError::PhaseOutcomeUnknown {
                            run_id, phase, ..
                        }) => {
                            assert_eq!(
                                store
                                    .tool_phase(&run_id, &phase.id)
                                    .await
                                    .unwrap()
                                    .phase
                                    .state,
                                crabber::session::PhaseState::Uncertain
                            );
                            store.get_run(&run_id).await.unwrap().unwrap().status
                        }
                        Err(error) => panic!("unexpected child failure: {error:?}"),
                    };
                    *status.lock().unwrap() = Some(observed);
                    child.close_extensions().await.unwrap();
                    if uncertain {
                        Ok(Response::Failed("host child interrupted".into()))
                    } else {
                        Ok(Response::Completed("child result".into()))
                    }
                })
            })
        };
        Self {
            runner,
            entered,
            status,
            store,
            session,
            run_id,
        }
    }
}

#[tokio::test(start_paused = true)]
async fn host_owned_child_agent_completes_or_is_interrupted_by_parent_and_deadline() {
    for mode in 0..3 {
        let host = ChildHost::new(mode != 0);
        let parent_store = Arc::new(MemoryStore::new());
        let parent = agent(
            Arc::new(configured(host.runner.clone(), limits())),
            parent_store.clone(),
            Arc::new(FakeProvider::scripted(vec![call(), done()])),
            PermissionDecision::Allow,
        );
        let run = parent.prompt(None, "fixture").await.unwrap();
        let session = run.session_id().clone();
        if mode == 1 {
            host.entered.notified().await;
            run.interrupt();
        }
        if mode == 1 {
            assert!(matches!(
                run.done().await,
                Err(crabber::RuntimeError::PhaseOutcomeUnknown { .. })
            ));
        } else {
            assert_eq!(run.done().await.unwrap().status, RunStatus::Completed);
        }
        parent.close_extensions().await.unwrap();
        let status = host.status.lock().unwrap().unwrap();
        assert_eq!(
            status,
            if mode == 0 {
                RunStatus::Completed
            } else {
                RunStatus::Running
            }
        );
        let call = snapshot_tool(&parent_store, &session).await;
        match mode {
            0 => assert_eq!(
                result_value(&call),
                json!({"status":"completed","output":"child result"})
            ),
            1 => {
                assert_eq!(call.status, crabber::core::ToolCallStatus::Running);
                assert!(!format!("{call:?}").contains("host child interrupted"));
            }
            _ => assert_eq!(result_value(&call), json!({"status":"timed_out"})),
        }
        let child_run = host.run_id.lock().unwrap().clone().unwrap();
        assert_eq!(
            host.store
                .get_run(&child_run)
                .await
                .unwrap()
                .unwrap()
                .status,
            status
        );
        let child_session = host.session.lock().unwrap().clone().unwrap();
        let page = snapshot_page(&host.store, &child_session).await;
        if mode == 0 {
            assert!(page.tool_calls.is_empty());
        } else {
            assert_eq!(
                page.tool_calls[0].status,
                crabber::core::ToolCallStatus::Running
            );
        }
    }
}

#[tokio::test(start_paused = true)]
async fn host_owned_child_agent_runs_in_background() {
    for pending in [false, true] {
        let host = ChildHost::new(pending);
        let mut j = super::background::Journey::new(
            host.runner.clone(),
            Limits {
                max_background_duration: Duration::from_secs(1),
                ..limits()
            },
        );
        let id = j.start().await;
        if pending {
            host.entered.notified().await;
        }
        let reply = j.wait(&[id]).await;
        assert_eq!(
            reply["tasks"][0]["status"],
            if pending { "timed_out" } else { "completed" }
        );
        if !pending {
            assert_eq!(reply["tasks"][0]["output"], "child result");
        }
        let (_, handle, _) = mounted(j.ext.clone()).await;
        handle.close().await.unwrap();
        assert_eq!(
            *host.status.lock().unwrap(),
            Some(if pending {
                RunStatus::Running
            } else {
                RunStatus::Completed
            })
        );
    }
}
