use super::*;

#[tokio::test(flavor = "multi_thread")]
async fn input_validation_is_durable_and_spawns_nothing() {
    if !python_available() {
        return;
    }
    let cases = [
        (EXECUTE_TOOL, json!(null), "shape"),
        (EXECUTE_TOOL, json!({"code":"1","extra":1}), "shape"),
        (EXECUTE_TOOL, json!({}), "code"),
        (EXECUTE_TOOL, json!({"code":""}), "code"),
        (EXECUTE_TOOL, json!({"code":"a\0"}), "code"),
        (EXECUTE_TOOL, json!({"code":"x".repeat(4097)}), "code"),
        (
            EXECUTE_TOOL,
            json!({"code":"1","timeout_seconds":null}),
            "timeout",
        ),
        (
            EXECUTE_TOOL,
            json!({"code":"1","timeout_seconds":-1}),
            "timeout",
        ),
        (
            EXECUTE_TOOL,
            json!({"code":"1","timeout_seconds":1.5}),
            "timeout",
        ),
        (
            EXECUTE_TOOL,
            json!({"code":"1","timeout_seconds":"1"}),
            "timeout",
        ),
        (
            EXECUTE_TOOL,
            json!({"code":"1","timeout_seconds":31}),
            "timeout",
        ),
        (CLEAR_TOOL, json!(null), "shape"),
        (CLEAR_TOOL, json!({"code":"1"}), "shape"),
    ];
    for (tool, input, expected) in cases {
        let h = Harness::new(|_| {}).await;
        assert!(
            h.execute(tool, input.clone())
                .await
                .unwrap_err()
                .to_string()
                .ends_with(expected)
        );
        let store = Arc::new(MemoryStore::new());
        let a = agent(
            h.ext.clone(),
            store.clone(),
            Arc::new(FakeProvider::scripted(vec![call(tool, input), done()])),
            PermissionDecision::Allow,
            h.dir.path(),
        );
        let run = a.prompt(None, "invalid").await.unwrap();
        let session = run.session_id().clone();
        run.done().await.unwrap();
        let records = snapshot_tools(&store, &session).await;
        assert_eq!(records[0].status, ToolCallStatus::Failed);
        let failure = result_value(&records[0]);
        // Crabber may enforce schema/NUL before the extension executor.
        assert!(
            failure.as_str().unwrap().ends_with(expected)
                || failure.as_str().unwrap().contains("invalid tool arguments")
                || failure.as_str().unwrap() == "unstorable arguments",
            "{failure}"
        );
        assert_eq!(h.ext.live_runners(), 0);
        assert_eq!(
            std::fs::read_dir(h.dir.path().join("temporary"))
                .unwrap()
                .count(),
            0
        );
        a.close_extensions().await.unwrap();
        h.close().await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn maximum_fields_remain_inline_and_survive_escaping() {
    if !python_available() {
        return;
    }
    let h = Harness::new(|o| {
        o.limits.max_output_bytes_per_stream = 32;
        o.limits.max_result_bytes = 32;
        o.limits.max_exception_bytes = 32;
    })
    .await;
    let store = Arc::new(MemoryStore::new());
    let provider = Arc::new(FakeProvider::scripted(vec![
        call(
            EXECUTE_TOOL,
            json!({"code":record_code("import sys; sys.stdout.write(('\\\"\\\\é'*8)); sys.stderr.write(('\\\"\\\\é'*8)); 'a'*30")}),
        ),
        done(),
        call(
            EXECUTE_TOOL,
            json!({"code":record_code("import sys; sys.stdout.write('b'*32); sys.stderr.write('c'*32); raise ValueError('d'*100)")}),
        ),
        done(),
    ]));
    let a = agent(
        h.ext.clone(),
        store.clone(),
        provider.clone(),
        PermissionDecision::Allow,
        h.dir.path(),
    );
    let run = a.prompt(None, "maximum").await.unwrap();
    let session = run.session_id().clone();
    run.done().await.unwrap();
    let records = snapshot_tools(&store, &session).await;
    let value = result_value(&records[0]);
    for field in ["stdout", "stderr", "result"] {
        assert_eq!(value[field]["text"].as_str().unwrap().len(), 32);
        assert_eq!(value[field]["truncated"], false);
    }
    assert_provider_record(&provider, 1, &records[0]);
    assert_settled(&store, &session, &records).await;
    a.prompt(Some(session.clone()), "error")
        .await
        .unwrap()
        .done()
        .await
        .unwrap();
    let records = snapshot_tools(&store, &session).await;
    let value = result_value(&records[1]);
    assert_eq!(value["exception"]["text"].as_str().unwrap().len(), 32);
    assert_eq!(value["exception"]["truncated"], true);
    for record in &records {
        assert!(
            result_text(record).len()
                <= Limits {
                    max_output_bytes_per_stream: 32,
                    max_result_bytes: 32,
                    max_exception_bytes: 32,
                    ..limits()
                }
                .worst_case_execute_bytes()
        );
    }
    assert_provider_record(&provider, 3, &records[1]);
    assert_settled(&store, &session, &records).await;
    a.close_extensions().await.unwrap();
    h.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn interpreter_runs_as_chosen_option_with_all_private_keys() {
    if !python_available() {
        return;
    }
    let h = Harness::new(|_| {}).await;
    let store = Arc::new(MemoryStore::new());
    let python = options(h.dir.path()).python_path;
    let script = "import sys,os,json; print(json.dumps({'flags':[sys.flags.isolated,sys.flags.dont_write_bytecode,sys.flags.no_site,sys.prefix==sys.base_prefix], 'executable':sys.executable,'environment':{k:os.environ[k] for k in ['HOME','XDG_CACHE_HOME','XDG_CONFIG_HOME','XDG_DATA_HOME','XDG_STATE_HOME','XDG_RUNTIME_DIR','TMPDIR','TMP','TEMP']}}),end='')";
    let a = agent(
        h.ext.clone(),
        store.clone(),
        Arc::new(FakeProvider::scripted(vec![
            call(EXECUTE_TOOL, json!({"code":record_code(script)})),
            done(),
        ])),
        PermissionDecision::Allow,
        h.dir.path(),
    );
    let run = a.prompt(None, "flags").await.unwrap();
    let session = run.session_id().clone();
    run.done().await.unwrap();
    let records = snapshot_tools(&store, &session).await;
    let value = result_value(&records[0]);
    let flags: Value = serde_json::from_str(value["stdout"]["text"].as_str().unwrap()).unwrap();
    assert_eq!(flags["flags"], json!([1, 1, 0, true]));
    assert_eq!(flags["executable"], python.to_str().unwrap());
    assert_eq!(flags["environment"].as_object().unwrap().len(), 9);
    let temporary = h.dir.path().join("temporary").canonicalize().unwrap();
    for path in flags["environment"].as_object().unwrap().values() {
        assert!(Path::new(path.as_str().unwrap()).starts_with(&temporary));
    }
    a.close_extensions().await.unwrap();
    h.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn zero_and_omitted_timeout_use_positive_default() {
    if !python_available() {
        return;
    }
    for zero in [false, true] {
        let h = Harness::new(|o| o.limits.default_timeout = Duration::from_secs(1)).await;
        let store = Arc::new(MemoryStore::new());
        let mut input = json!({"code":record_code("import time; time.sleep(30)")});
        if zero {
            input["timeout_seconds"] = json!(0);
        }
        let a = agent(
            h.ext.clone(),
            store.clone(),
            Arc::new(FakeProvider::scripted(vec![
                call(EXECUTE_TOOL, input),
                done(),
            ])),
            PermissionDecision::Allow,
            h.dir.path(),
        );
        let run = a.prompt(None, "default timeout").await.unwrap();
        let session = run.session_id().clone();
        run.done().await.unwrap();
        let records = snapshot_tools(&store, &session).await;
        assert_eq!(records[0].status, ToolCallStatus::Failed);
        assert!(
            result_value(&records[0])
                .as_str()
                .unwrap()
                .ends_with("timed-out")
        );
        a.close_extensions().await.unwrap();
        h.close().await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn hard_cap_texts_survive_typed_output() {
    if !python_available() {
        return;
    }
    let h = Harness::new(|o| {
        o.limits.max_output_bytes_per_stream = 100 * 1024;
        o.limits.max_result_bytes = 100 * 1024;
        o.limits.max_exception_bytes = 100 * 1024;
    })
    .await;
    let result = h.execute(EXECUTE_TOOL, json!({"code":record_code("import sys; sys.stdout.write(chr(1)*102400); sys.stderr.write(chr(2)*102400); 'x'*102400")})).await.unwrap();
    assert_eq!(result["status"], "completed");
    assert_eq!(result["stdout"]["text"].as_str().unwrap().len(), 100 * 1024);
    assert_eq!(result["stderr"]["text"].as_str().unwrap().len(), 100 * 1024);
    h.close().await;
}
