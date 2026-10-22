use super::*;

#[tokio::test]
async fn concurrent_agents_receive_their_own_authoritative_identities() {
    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    let runner: Runner = {
        let seen = seen.clone();
        let barrier = barrier.clone();
        Arc::new(move |request| {
            let seen = seen.clone();
            let barrier = barrier.clone();
            Box::pin(async move {
                barrier.wait().await;
                seen.lock().unwrap().push(request);
                Ok(Response::Completed(String::new()))
            })
        })
    };
    let extension = Arc::new(
        DelegateTask::new(Options {
            runner_identity: "host-v1".into(),
            runner,
            limits: Limits {
                max_in_flight: 2,
                ..limits()
            },
        })
        .unwrap(),
    );
    let first_store = Arc::new(MemoryStore::new());
    let second_store = Arc::new(MemoryStore::new());
    let first = Agent::builder()
        .behavior_version("crabber-extensions-tests.v1")
        .store(first_store.clone())
        .provider(Arc::new(FakeProvider::scripted(vec![call(), done()])))
        .config(agent_config())
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .shared_extension(extension.clone(), Scope::Global)
        .build()
        .unwrap();
    let mut second_config = agent_config();
    second_config.workspace_id = "workspace-two".into();
    second_config.directory = "/workspace-two".into();
    let second = Agent::builder()
        .behavior_version("crabber-extensions-tests.v1")
        .store(second_store.clone())
        .provider(Arc::new(FakeProvider::scripted(vec![call(), done()])))
        .config(second_config)
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .shared_extension(extension, Scope::Global)
        .build()
        .unwrap();
    let (first_run, second_run) =
        tokio::join!(first.prompt(None, "one"), second.prompt(None, "two"));
    let first_run = first_run.unwrap();
    let second_run = second_run.unwrap();
    let first_session = first_run.session_id().clone();
    let second_session = second_run.session_id().clone();
    let (first_done, second_done) = tokio::join!(first_run.done(), second_run.done());
    assert_eq!(first_done.unwrap().status, RunStatus::Completed);
    assert_eq!(second_done.unwrap().status, RunStatus::Completed);
    let first_call = snapshot_tool(&first_store, &first_session).await;
    let second_call = snapshot_tool(&second_store, &second_session).await;
    {
        let requests = seen.lock().unwrap();
        assert_eq!(requests.len(), 2);
        for (session, call, workspace, directory) in [
            (&first_session, &first_call, "workspace", "/workspace"),
            (
                &second_session,
                &second_call,
                "workspace-two",
                "/workspace-two",
            ),
        ] {
            let request = requests.iter().find(|r| r.session_id() == session).unwrap();
            assert_eq!(request.call_id(), &call.id);
            assert_eq!(request.run_id(), &call.run_id);
            assert_eq!(request.workspace().workspace_id(), Some(workspace));
            assert_eq!(request.workspace().directory(), Some(directory));
        }
    }
    first.close_extensions().await.unwrap();
    second.close_extensions().await.unwrap();
}
