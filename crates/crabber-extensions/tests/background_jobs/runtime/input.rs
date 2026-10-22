use super::*;

#[tokio::test(flavor = "multi_thread")]
async fn input_validation_is_durable_and_spawns_nothing() {
    if !shell_available() {
        return;
    }
    let h = Harness::new(|_| {}).await;
    let cases = vec![
        (START_TOOL, json!({}), "shape"),
        (START_TOOL, json!({"command":""}), "command"),
        (START_TOOL, json!({"command":"x\0"}), "shape"),
        (
            START_TOOL,
            json!({"command":"x","working_directory":"../x"}),
            "working-directory",
        ),
        (
            START_TOOL,
            json!({"command":"x","timeout_seconds":31}),
            "timeout",
        ),
        (
            START_TOOL,
            json!({"command":"x","timeout_seconds":null}),
            "shape",
        ),
        (STATUS_TOOL, json!({"id":"bad"}), "id"),
        (KILL_TOOL, json!({"id":"bad"}), "id"),
        (LIST_TOOL, json!({"extra":true}), "shape"),
        (START_TOOL, json!(null), "shape"),
        (START_TOOL, json!({"command":1}), "shape"),
        (START_TOOL, json!({"command":"x", "extra":true}), "shape"),
        (START_TOOL, json!({"command":"x".repeat(4097)}), "command"),
        (START_TOOL, json!({"command":"é".repeat(2049)}), "command"),
        (
            START_TOOL,
            json!({"command":"x","working_directory":"./".repeat(513)}),
            "working-directory",
        ),
        (
            START_TOOL,
            json!({"command":"x","working_directory":null}),
            "shape",
        ),
        (
            START_TOOL,
            json!({"command":"x","working_directory":"nul\0"}),
            "shape",
        ),
        (
            START_TOOL,
            json!({"command":"x","timeout_seconds":-1}),
            "timeout",
        ),
        (
            START_TOOL,
            json!({"command":"x","timeout_seconds":1.5}),
            "shape",
        ),
        (
            STATUS_TOOL,
            json!({"id":"job_00000000000000000000000000000000_000000000000000A"}),
            "id",
        ),
        (
            KILL_TOOL,
            json!({"id":"job_00000000000000000000000000000000_00000000000000000"}),
            "id",
        ),
        (STATUS_TOOL, json!({"id":"nul\0"}), "shape"),
    ];
    let mut agents = vec![];
    for (tool, args, code) in cases {
        let store = Arc::new(MemoryStore::new());
        let provider = Arc::new(FakeProvider::scripted(vec![call(tool, args), done()]));
        let probe = Arc::new(ResultProbe::default());
        let a = Agent::builder()
            .behavior_version("crabber-extensions-tests.v1")
            .store(store.clone())
            .provider(provider)
            .config(agent_config(h.dir.path()))
            .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
            .shared_extension(h.ext.clone(), Scope::Global)
            .shared_extension(Arc::new(ProbeMount(probe.clone())), Scope::Global)
            .build()
            .unwrap();
        let run = a.prompt(None, "invalid").await.unwrap();
        let session = run.session_id().clone();
        run.done().await.unwrap();
        let records = snapshot_tools(&store, &session).await;
        assert_eq!(records[0].status, ToolCallStatus::Failed);
        assert_eq!(
            *probe.classes.lock().unwrap(),
            vec![(
                records[0].id.clone(),
                if code == "shape" {
                    ToolOutcomeClass::PrepareFailed
                } else {
                    ToolOutcomeClass::ExecutionFailed
                }
            )]
        );
        agents.push(a);
        if code != "shape" {
            assert!(
                result_value(&records[0])
                    .as_str()
                    .unwrap()
                    .contains(&format!("background jobs input invalid: {code}"))
            );
        }
        assert_eq!(h.ext.live_jobs(), 0);
    }
    assert!(!h.dir.path().join("canary").exists());
    assert!(pgids(h.dir.path()).is_empty());
    for a in agents {
        a.close_extensions().await.unwrap();
    }
    h.close().await;
}
#[tokio::test(flavor = "multi_thread")]
async fn working_directory_is_contained_and_resolved() {
    if !shell_available() {
        return;
    }
    let h = Harness::new(|_| {}).await;
    std::fs::create_dir(h.dir.path().join("inside")).unwrap();
    let outside = tempfile::tempdir().unwrap();
    std::os::unix::fs::symlink(outside.path(), h.dir.path().join("escape")).unwrap();
    std::os::unix::fs::symlink(h.dir.path().join("inside"), h.dir.path().join("link")).unwrap();
    std::fs::write(h.dir.path().join("file"), b"fixture").unwrap();
    for cwd in ["inside", "link"] {
        let start = h
            .execute(
                START_TOOL,
                json!({"command":command("pwd -P"),"working_directory":cwd}),
            )
            .await
            .unwrap();
        let status = h.terminal(start["id"].as_str().unwrap()).await;
        assert_eq!(
            status["stdout"]["text"],
            format!(
                "{}\n",
                h.dir
                    .path()
                    .join("inside")
                    .canonicalize()
                    .unwrap()
                    .display()
            )
        );
    }
    for cwd in ["../x", "/tmp", "escape", "file"] {
        assert!(
            h.execute(START_TOOL, json!({"command":"pwd","working_directory":cwd}))
                .await
                .unwrap_err()
                .to_string()
                .ends_with("working-directory")
        );
    }
    let error = h.tools[START_TOOL]
        .execute_json_with_context(
            context(&h.dir.path().join("missing"), CancellationToken::new()),
            json!({"command":"pwd"}),
        )
        .await
        .unwrap_err();
    assert!(error.to_string().ends_with("workspace-root"));
    h.close().await;
}
#[tokio::test(flavor = "multi_thread")]
async fn direct_executor_rejects_missing_context_and_owner() {
    let h = Harness::new(|_| {}).await;
    assert!(
        h.tools[START_TOOL]
            .execute(json!({"command":"x"}))
            .await
            .unwrap_err()
            .to_string()
            .ends_with("context")
    );
    let mut ctx = context(h.dir.path(), CancellationToken::new());
    ctx = ToolContext::new(
        ctx.session_id,
        ctx.run_id,
        ctx.call_id,
        ctx.cancel,
        ctx.host,
        WorkspaceContext::from_persisted("", h.dir.path().to_str().unwrap()),
        Arc::new(|_| {}),
    );
    assert!(
        h.tools[START_TOOL]
            .execute_json_with_context(ctx, json!({"command":"x"}))
            .await
            .unwrap_err()
            .to_string()
            .ends_with("owner")
    );
    let ctx = ToolContext::new(
        SessionId::from("session"),
        RunId::from("run"),
        ToolCallId::from("call"),
        CancellationToken::new(),
        HostServices::default(),
        WorkspaceContext::from_persisted("workspace", ""),
        Arc::new(|_| {}),
    );
    assert!(
        h.tools[START_TOOL]
            .execute_json_with_context(ctx, json!({"command":"x"}))
            .await
            .unwrap_err()
            .to_string()
            .ends_with("workspace-root")
    );
    let cancel = CancellationToken::new();
    cancel.cancel();
    for tool in [START_TOOL, STATUS_TOOL, LIST_TOOL, KILL_TOOL] {
        assert!(
            h.tools[tool]
                .execute_json_with_context(context(h.dir.path(), cancel.clone()), json!({}))
                .await
                .unwrap_err()
                .to_string()
                .ends_with("cancelled")
        );
    }
    assert_eq!(h.ext.live_jobs(), 0);
    h.close().await;
}
#[tokio::test(flavor = "multi_thread")]
async fn exact_input_boundaries_are_admitted() {
    if !shell_available() {
        return;
    }
    let h = Harness::new(|_| {}).await;
    let mut script = command("exit 0 #");
    script.push_str(&"x".repeat(4096 - script.len()));
    assert_eq!(script.len(), 4096);
    let job = h
        .execute(
            START_TOOL,
            json!({"command":script,"working_directory":"./".repeat(512),"timeout_seconds":30}),
        )
        .await
        .unwrap();
    assert_eq!(job["timeout_seconds"], 30);
    assert_eq!(
        h.terminal(job["id"].as_str().unwrap()).await["state"],
        "succeeded"
    );
    h.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn hard_cap_tails_survive_typed_output() {
    if !shell_available() {
        return;
    }
    let h = Harness::new(|o| {
        o.limits.max_output_bytes_per_stream = 160 * 1024;
        o.environment.overrides.insert("PATH".into(), String::new());
    })
    .await;
    let script = r#"
out=$(printf '\001'); err=$(printf '\002'); n=0
while [ "$n" -lt 14 ]; do
    out="$out$out"; err="$err$err"; n=$((n + 1))
done
n=0
while [ "$n" -lt 10 ]; do
    printf '%s' "$out"
    printf '%s' "$err" >&2
    n=$((n + 1))
done
"#;
    let job = h.start(script).await;
    let result = h.terminal(job["id"].as_str().unwrap()).await;
    assert_eq!(result["state"], "succeeded");
    assert_eq!(result["stdout"]["text"].as_str().unwrap().len(), 160 * 1024);
    assert_eq!(result["stderr"]["text"].as_str().unwrap().len(), 160 * 1024);
    h.close().await;
}
