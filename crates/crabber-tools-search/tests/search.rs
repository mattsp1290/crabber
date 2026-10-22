//! Real ripgrep semantics and controlled process failure cases.
use crabber_tools_core::*;
use crabber_tools_search::*;
use crabber_tools_testkit as support;
use serde_json::{Value, json};
use std::{sync::Arc, time::Duration};
use support::*;
use tokio_util::sync::CancellationToken;
fn options(path: &std::path::Path) -> Options {
    let limits = limits();
    Options {
        root: WorkspaceRoot::open(path).unwrap(),
        policy: SearchPolicy::resolve_rg_from_path(EnvPolicy::minimal_allowlist())
            .expect("search tests require rg >=14 on PATH"),
        capacity: Capacity::new(&limits).unwrap(),
        limits,
    }
}
async fn call(o: Options, args: Value) -> Value {
    let root = o.root.clone();
    definition(Arc::new(o))
        .unwrap()
        .executor
        .execute_owned_context(context(&root, CancellationToken::new()), args)
        .await
        .unwrap()
        .json_value()
        .unwrap()
}
#[tokio::test]
async fn modes_context_globs_limits_and_runtime() {
    let tmp = tempfile::tempdir().unwrap();
    std::fs::write(tmp.path().join("one.txt"), "before\nabc\na.c\nABC\nafter\n").unwrap();
    std::fs::create_dir(tmp.path().join(".hidden")).unwrap();
    std::fs::write(tmp.path().join(".hidden/h"), "abc").unwrap();
    std::fs::create_dir(tmp.path().join(".git")).unwrap();
    std::fs::write(tmp.path().join(".git/h"), "abc").unwrap();
    let v = execute(
        tmp.path(),
        definition(Arc::new(options(tmp.path()))).unwrap(),
        json!({"pattern":"a.c"}),
    )
    .await;
    assert_eq!(v["match_count"], 2);
    assert_eq!(
        call(options(tmp.path()), json!({"pattern":"a.c","literal":true})).await["match_count"],
        1
    );
    assert_eq!(
        call(
            options(tmp.path()),
            json!({"pattern":"abc","ignore_case":true})
        )
        .await["match_count"],
        2
    );
    let v = call(options(tmp.path()), json!({"pattern":"abc","context":2})).await;
    assert_eq!(v["matches"][0]["before"][0]["line"], "before");
    assert_eq!(v["matches"][0]["after"].as_array().unwrap().len(), 2);
    for glob in [json!("*.txt"), json!(["*.txt"])] {
        assert_eq!(
            call(options(tmp.path()), json!({"pattern":"abc","glob":glob})).await["match_count"],
            1
        );
    }
    assert_eq!(
        call(options(tmp.path()), json!({"pattern":"abc","glob":"*.rs"})).await["match_count"],
        0
    );
    assert_eq!(
        call(options(tmp.path()), json!({"pattern":"a","limit":1})).await["truncation_reason"],
        "matches"
    );
    assert_eq!(
        call(options(tmp.path()), json!({"pattern":"("})).await["error"]["category"],
        "invalid_pattern"
    );
    for (path, cat) in [("../", "path_escape"), ("missing", "not_found")] {
        assert_eq!(
            call(options(tmp.path()), json!({"pattern":"a","path":path})).await["error"]["category"],
            cat
        );
    }
    assert_eq!(
        call(
            options(tmp.path()),
            json!({"pattern":"abc","limit":0,"timeout_seconds":0})
        )
        .await["outcome"],
        "succeeded"
    );
}
#[tokio::test]
async fn non_utf8_and_caps() {
    #[cfg(not(target_os = "macos"))]
    use std::os::unix::ffi::OsStringExt;
    let tmp = tempfile::tempdir().unwrap();
    // The macOS CI filesystem rejects invalid-UTF-8 names. Exercise raw
    // content here; parse.rs covers base64-encoded paths on every platform.
    #[cfg(not(target_os = "macos"))]
    let name = std::ffi::OsString::from_vec(b"bad\xff.txt".to_vec());
    #[cfg(target_os = "macos")]
    let name = std::ffi::OsString::from("bad.txt");
    std::fs::write(tmp.path().join(name), b"match \xff\n").unwrap();
    let v = call(options(tmp.path()), json!({"pattern":"match"})).await;
    assert_eq!(v["match_count"], 1);
    assert!(
        v["matches"][0]["line"]
            .as_str()
            .unwrap()
            .contains('\u{fffd}')
    );
    #[cfg(not(target_os = "macos"))]
    assert!(
        v["matches"][0]["path"]
            .as_str()
            .unwrap()
            .contains('\u{fffd}')
    );
    std::fs::write(
        tmp.path().join("long"),
        format!("match{}", "x".repeat(5000)),
    )
    .unwrap();
    let v = call(
        options(tmp.path()),
        json!({"pattern":"match","path":"long"}),
    )
    .await;
    assert_eq!(v["matches"][0]["line_truncated"], true);
    assert_eq!(v["matches"][0]["line"].as_str().unwrap().len(), 4096);
    std::fs::write(
        tmp.path().join("many"),
        format!("match{}\n", "x".repeat(4000)).repeat(200),
    )
    .unwrap();
    let v = call(
        options(tmp.path()),
        json!({"pattern":"match","path":"many","limit":1000}),
    )
    .await;
    assert_eq!(v["truncation_reason"], "bytes");
    assert!(serde_json::to_vec(&v["matches"]).unwrap().len() <= MAX_RESULT_BYTES);
}
#[tokio::test]
async fn fake_binary_errors_timeout_and_pipe_grace() {
    let tmp = tempfile::tempdir().unwrap();
    let binary = tmp.path().join("fake-rg");
    for (body, cat, partial) in [
        ("echo failure >&2; exit 2".to_owned(), "exec_failed", false),
        (
            format!(
                "printf '%s\\n' '{}'; echo failure >&2; exit 2",
                match_line()
            ),
            "exec_failed",
            true,
        ),
        (
            "echo 'rg: regex parse error:' >&2; exit 2".to_owned(),
            "invalid_pattern",
            false,
        ),
    ] {
        script(&binary, &format!("#!/bin/sh\n{body}\n"));
        let mut o = options(tmp.path());
        o.policy.rg_binary = binary.clone();
        let v = call(o, json!({"pattern":"x"})).await;
        assert_eq!(v["error"]["category"], cat);
        assert_eq!(
            v.get("partial").and_then(Value::as_bool).unwrap_or(false),
            partial
        );
    }
    for grace in [false, true] {
        let end = if grace {
            format!("printf '%s\\n' '{}'; exit 0", match_line())
        } else {
            "wait".into()
        };
        script(
            &binary,
            &format!("#!/bin/sh\necho $$ > leader; sleep 60 & echo $! > child; {end}\n"),
        );
        let mut o = options(tmp.path());
        o.policy.rg_binary = binary.clone();
        let start = tokio::time::Instant::now();
        let v = call(
            o,
            json!({"pattern":"x","timeout_seconds":if grace{30}else{1}}),
        )
        .await;
        assert!(start.elapsed() < Duration::from_secs(7));
        assert_eq!(v["outcome"], if grace { "succeeded" } else { "failed" });
        if !grace {
            assert_eq!(v["timed_out"], true);
        }
        assert_gone(pid(&tmp.path().join("leader"))).await;
        assert_gone(pid(&tmp.path().join("child"))).await;
    }
    let mut o = options(tmp.path());
    o.policy.rg_binary = "/missing/rg".into();
    assert!(definition(Arc::new(o)).is_err());
}
#[tokio::test]
async fn config_disabled_and_environment_replaced() {
    let tmp = tempfile::tempdir().unwrap();
    std::fs::write(tmp.path().join("text"), "hello").unwrap();
    std::fs::write(tmp.path().join("rgconfig"), "--pre\n/bin/false\n").unwrap();
    let mut o = options(tmp.path());
    o.policy.env = EnvPolicy::Allowlist {
        keep: vec!["PATH".into()],
        set: vec![(
            "RIPGREP_CONFIG_PATH".into(),
            tmp.path().join("rgconfig").to_string_lossy().into_owned(),
        )],
    };
    assert_eq!(
        call(o, json!({"pattern":"hello","path":"text"})).await["match_count"],
        1
    );
    let binary = tmp.path().join("fake");
    script(
        &binary,
        &format!(
            "#!/bin/sh\n[ -z \"${{HOME+x}}\" ] || exit 2\nprintf '%s\\n' '{}'\n",
            match_line()
        ),
    );
    let mut o = options(tmp.path());
    o.policy.rg_binary = binary;
    o.policy.env = EnvPolicy::Replace(vec![]);
    assert_eq!(
        call(o, json!({"pattern":"x"})).await["outcome"],
        "succeeded"
    );
}
#[tokio::test]
async fn runtime_interrupt_reaps_group() {
    use crabber::{
        Agent, FakeProvider, PermissionDecision, StaticPolicy, core::ToolCallStatus,
        session::MemoryStore,
    };
    let tmp = tempfile::tempdir().unwrap();
    let binary = tmp.path().join("fake-rg");
    script(
        &binary,
        "#!/bin/sh\necho $$ > leader; sleep 60 & echo $! > child; wait\n",
    );
    let mut o = options(tmp.path());
    o.policy.rg_binary = binary;
    let store = Arc::new(MemoryStore::new());
    let agent = Agent::builder()
        .behavior_version(concat!("crabber-tools/", env!("CARGO_PKG_VERSION")))
        .store(store.clone())
        .provider(Arc::new(FakeProvider::scripted(vec![
            turn("search", json!({"pattern":"x"})),
            done(),
        ])))
        .config(config(tmp.path()))
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .shared_tool(definition(Arc::new(o)).unwrap())
        .build()
        .unwrap();
    let run = agent.prompt(None, "interrupt").await.unwrap();
    let session = run.session_id().clone();
    wait_file(&tmp.path().join("child")).await;
    run.interrupt();
    support::assert_stopped_uncertain(run, &store).await;
    assert_eq!(
        calls(&store, &session).await[0].status,
        ToolCallStatus::Running
    );
    assert_gone(pid(&tmp.path().join("leader"))).await;
    assert_gone(pid(&tmp.path().join("child"))).await;
    agent.close_extensions().await.unwrap();
}

#[tokio::test]
async fn truncated_submatch_ranges_and_literal_parity() {
    let tmp = tempfile::tempdir().unwrap();
    for prefix in [4094, 4500] {
        std::fs::write(
            tmp.path().join("text"),
            format!("{}needle", "x".repeat(prefix)),
        )
        .unwrap();
        let v = call(
            options(tmp.path()),
            json!({"pattern":"needle","path":"text"}),
        )
        .await;
        assert_eq!(v["match_count"], 1);
        assert_eq!(v["matches"][0]["line_truncated"], true);
        assert_eq!(v["matches"][0]["submatches"], json!([]));
    }
    std::fs::write(tmp.path().join("text"), "needle").unwrap();
    let v = call(
        options(tmp.path()),
        json!({"pattern":"needle","path":"text","literal":true}),
    )
    .await;
    assert_eq!(v["matches"][0]["submatches"], json!([]));
}
