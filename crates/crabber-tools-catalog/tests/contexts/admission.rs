use super::*;
#[tokio::test]
async fn missing_identity_fails_closed() {
    let _cwd = Cwd::new();
    let a = tempfile::tempdir().unwrap();
    let b = tempfile::tempdir().unwrap();
    let (agent, store) = agent(a.path(), b.path(), base(), vec![], allow());
    for (ws, dir, expected) in [
        ("ws-a", Path::new(""), "context initialization failed"),
        ("", a.path(), "workspace"),
    ] {
        let ctx = execution(SessionId::new(), ws, dir);
        let err = match agent
            .prompt_keyed_in(&ctx, "missing", admission_options(&agent, "missing"), None)
            .await
        {
            Err(error) => error,
            Ok(_) => panic!("missing identity accepted"),
        };
        assert!(err.to_string().contains(expected), "{err}");
        assert!(
            store
                .get_session(ctx.session().session_id())
                .await
                .unwrap()
                .is_none()
        );
    }
    let root = WorkspaceRoot::open(a.path()).unwrap();
    let ctx = crabber::extension::ToolContext::new(
        SessionId::new(),
        crabber::core::RunId::new(),
        crabber::core::ToolCallId::new(),
        tokio_util::sync::CancellationToken::new(),
        crabber::extension::HostServices::default(),
        crabber::extension::WorkspaceContext::from_persisted("ws-a", ""),
        Arc::new(|_| {}),
    );
    let tools = StandardTools::new(root, base()).unwrap();
    let read = &tools
        .definitions()
        .iter()
        .find(|d| d.id == ToolId::FileRead)
        .unwrap()
        .definition;
    let output = read
        .executor
        .execute_with_context(ctx, json!({"path":"x"}))
        .await
        .unwrap();
    assert_eq!(
        output.json_value().unwrap()["error"]["category"],
        "workspace_mismatch"
    );
    agent.close_extensions().await.unwrap();
}
#[tokio::test]
async fn drifted_identity_is_refused() {
    let _cwd = Cwd::new();
    let a = tempfile::tempdir().unwrap();
    let b = tempfile::tempdir().unwrap();
    let (agent, _store) = agent(
        a.path(),
        b.path(),
        base(),
        vec![
            turn("file_write", json!({"path":"x","content":"A"})),
            done(),
            turn("file_write", json!({"path":"wrong","content":"bad"})),
            done(),
        ],
        allow(),
    );
    let drift = execution(SessionId::new(), "ws-a", b.path());
    assert!(
        agent
            .prompt_keyed_in(&drift, "drift", admission_options(&agent, "drift"), None)
            .await
            .is_err()
    );
    agent.close_extensions().await.unwrap();
    let (agent, store) = self::agent(
        a.path(),
        b.path(),
        base(),
        vec![
            turn("file_write", json!({"path":"x","content":"A"})),
            done(),
            turn("file_write", json!({"path":"wrong","content":"bad"})),
            done(),
        ],
        allow(),
    );
    let good_context = execution(SessionId::new(), "ws-a", a.path());
    admit(&agent, &good_context, "good")
        .await
        .done()
        .await
        .unwrap();
    let good = calls(&store, good_context.session().session_id()).await;
    assert_eq!(good[0].status, ToolCallStatus::Completed);
    let error = match agent
        .prompt_keyed_in(
            &drift,
            "drift-again",
            admission_options(&agent, "drift-again"),
            None,
        )
        .await
    {
        Err(error) => error,
        Ok(_) => panic!("cached owner accepted remapped directory"),
    };
    assert!(
        error.to_string().contains("context identity changed"),
        "{error}"
    );
    assert!(!a.path().join("wrong").exists());
    assert!(!b.path().join("wrong").exists());
    agent
        .close_workspace_resources(good_context.session(), Duration::from_secs(2))
        .await
        .unwrap();
    agent.close_extensions().await.unwrap();
    let (remapped, store) = self::agent(
        b.path(),
        a.path(),
        base(),
        vec![
            turn("file_write", json!({"path":"remapped","content":"B"})),
            done(),
        ],
        allow(),
    );
    execute_in(&remapped, &store, "ws-a", b.path()).await;
    assert_eq!(
        std::fs::read_to_string(b.path().join("remapped")).unwrap(),
        "B"
    );
    remapped.close_extensions().await.unwrap();
}
#[tokio::test]
async fn path_escape_and_absolute_symlink_fail() {
    let _cwd = Cwd::new();
    let a = tempfile::tempdir().unwrap();
    let b = tempfile::tempdir().unwrap();
    std::fs::write(b.path().join("outside"), "secret").unwrap();
    std::os::unix::fs::symlink(b.path().join("outside"), a.path().join("link")).unwrap();
    let (agent, store) = agent(
        a.path(),
        b.path(),
        base(),
        vec![
            turn("file_read", json!({"path":"../outside"})),
            turn("file_read", json!({"path":"link"})),
            done(),
        ],
        allow(),
    );
    for r in execute_in(&agent, &store, "ws-a", a.path()).await {
        assert_eq!(r.status, ToolCallStatus::Failed);
        assert_eq!(value(&r)["error"]["category"], "path_escape");
    }
    agent.close_extensions().await.unwrap();
}

#[tokio::test]
async fn custom_admission_directory_mismatch_fails_before_execution() {
    struct WrongRoot(Arc<WorkspaceRoot>);
    impl WorkspaceAdmission for WrongRoot {
        fn admit(
            &self,
            _: &crabber::extension::WorkspaceContext,
        ) -> Result<Arc<WorkspaceRoot>, crabber::ExtensionError> {
            Ok(self.0.clone())
        }
    }
    let _cwd = Cwd::new();
    let a = tempfile::tempdir().unwrap();
    let b = tempfile::tempdir().unwrap();
    let agent = Agent::builder()
        .behavior_version("custom-admission-fixture")
        .store(Arc::new(MemoryStore::new()))
        .provider(Arc::new(FakeProvider::scripted(vec![
            turn("file_write", json!({"path":"never","content":"bad"})),
            done(),
        ])))
        .config(config(a.path()))
        .policy(Arc::new(allow()))
        .extension_factory(Arc::new(
            StandardToolsFactory::new(
                base(),
                Arc::new(WrongRoot(WorkspaceRoot::open(b.path()).unwrap())),
            )
            .unwrap(),
        ))
        .build()
        .unwrap();
    let ctx = execution(SessionId::new(), "ws-a", a.path());
    assert!(
        agent
            .prompt_keyed_in(&ctx, "wrong", admission_options(&agent, "wrong"), None)
            .await
            .is_err()
    );
    assert!(!a.path().join("never").exists());
    assert!(!b.path().join("never").exists());
    agent.close_extensions().await.unwrap();
}

#[tokio::test]
async fn symlinked_host_root_persists_the_admitted_canonical_directory() {
    let _cwd = Cwd::new();
    let parent = tempfile::tempdir().unwrap();
    let real = parent.path().join("real");
    let alias = parent.path().join("alias");
    std::fs::create_dir(&real).unwrap();
    std::os::unix::fs::symlink(&real, &alias).unwrap();
    let b = tempfile::tempdir().unwrap();
    let (agent, store) = agent(
        &alias,
        b.path(),
        base(),
        vec![
            turn("file_write", json!({"path":"owned","content":"correct"})),
            done(),
        ],
        allow(),
    );
    let ctx = execution(SessionId::new(), "ws-a", &alias);
    assert_eq!(
        ctx.session().directory(),
        real.canonicalize().unwrap().to_str().unwrap()
    );
    admit(&agent, &ctx, "canonical").await.done().await.unwrap();
    assert_eq!(
        calls(&store, ctx.session().session_id()).await[0].status,
        ToolCallStatus::Completed
    );
    assert_eq!(
        std::fs::read_to_string(real.join("owned")).unwrap(),
        "correct"
    );
    agent.close_extensions().await.unwrap();
}
