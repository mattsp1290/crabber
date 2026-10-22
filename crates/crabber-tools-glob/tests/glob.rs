//! Public runtime contract, confinement, capacity and cancellation regressions.
use crabber::{
    Agent, FakeProvider, PermissionDecision, StaticPolicy, core::ToolCallStatus,
    session::MemoryStore,
};
use crabber_tools_core::*;
use crabber_tools_glob::{Options, definition};
use crabber_tools_testkit as support;
use serde_json::{Value, json};
use std::{sync::Arc, time::Duration};
use support::*;
use tokio_util::sync::CancellationToken;
fn options(path: &std::path::Path) -> Arc<Options> {
    let limits = limits();
    Arc::new(Options {
        root: WorkspaceRoot::open(path).unwrap(),
        capacity: Capacity::new(&limits).unwrap(),
        limits,
    })
}
async fn call(o: &Arc<Options>, args: Value) -> Value {
    definition(o.clone())
        .executor
        .execute_owned_context(context(&o.root, CancellationToken::new()), args)
        .await
        .unwrap()
        .json_value()
        .unwrap()
}
#[tokio::test]
async fn matching_sorting_limits_hidden_vcs_and_search_root() {
    let temp = tempfile::tempdir().unwrap();
    for path in [
        "a/b.rs",
        "a/z.rs",
        "a.txt",
        "top.rs",
        ".hidden.rs",
        ".git/config.rs",
        ".hg/config.rs",
        ".svn/config.rs",
        ".jj/config.rs",
    ] {
        let path = temp.path().join(path);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, "").unwrap();
    }
    let o = options(temp.path());
    let v = execute(
        temp.path(),
        definition(o.clone()),
        json!({"pattern":"**/*.rs"}),
    )
    .await;
    assert_eq!(
        v,
        json!({"outcome":"succeeded","count":4,"paths":[{"path":".hidden.rs","is_dir":false},{"path":"a/b.rs","is_dir":false},{"path":"a/z.rs","is_dir":false},{"path":"top.rs","is_dir":false}],"truncated":false})
    );
    assert_eq!(call(&o, json!({"pattern":"*.rs"})).await["count"], 2);
    assert_eq!(
        call(&o, json!({"pattern":"?.rs","path":"a","limit":1})).await,
        json!({"outcome":"succeeded","paths":[{"path":"a/b.rs","is_dir":false}],"count":1,"truncated":true})
    );
    assert_eq!(
        call(&o, json!({"pattern":"*.rs","path":"a","limit":2})).await["truncated"],
        false
    );
    let all = call(&o, json!({"pattern":"**","limit":3})).await;
    assert_eq!(
        all["paths"],
        json!([{"path":".hidden.rs","is_dir":false},{"path":"a","is_dir":true},{"path":"a.txt","is_dir":false}])
    );
    assert_eq!(all["truncated"], true);
}
#[tokio::test]
async fn validation_and_capability_confinement() {
    use std::os::unix::fs::symlink;
    let temp = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    std::fs::write(outside.path().join("canary.rs"), "secret").unwrap();
    std::fs::create_dir(temp.path().join("inside")).unwrap();
    std::fs::write(temp.path().join("inside/ok.rs"), "").unwrap();
    symlink(outside.path(), temp.path().join("escape")).unwrap();
    symlink("inside", temp.path().join("alias")).unwrap();
    let o = options(temp.path());
    assert_eq!(
        call(&o, json!({"pattern":"**/*.rs"})).await["paths"],
        json!([{"path":"inside/ok.rs","is_dir":false}])
    );
    assert_eq!(
        call(&o, json!({"pattern":"**","path":"escape"})).await["error"]["category"],
        "path_escape"
    );
    assert_eq!(
        call(&o, json!({"pattern":"**","path":"inside/ok.rs"})).await["error"]["category"],
        "not_directory"
    );
    assert_eq!(
        call(&o, json!({"pattern":"**","path":"missing"})).await["error"]["category"],
        "not_found"
    );
    assert_eq!(
        call(&o, json!({"pattern":"**","path":"../"})).await["error"]["category"],
        "path_escape"
    );
    for args in [
        json!({}),
        json!({"pattern":""}),
        json!({"pattern":"["}),
        json!({"pattern":"/x"}),
        json!({"pattern":"../*"}),
        json!({"pattern":"x\u{0000}"}),
        json!({"pattern":"*","limit":0}),
        json!({"pattern":"*","limit":5001}),
        json!({"pattern":"*","limit":-1}),
        json!({"pattern":"*","extra":1}),
    ] {
        let v = call(&o, args).await;
        assert_eq!(v["error"]["category"], "validation", "{v}");
        assert_eq!(v["paths"], json!([]));
        assert_eq!(v["count"], 0);
    }
    let v = execute(
        outside.path(),
        definition(o.clone()),
        json!({"pattern":"**"}),
    )
    .await;
    assert_eq!(v["error"]["category"], "workspace_mismatch");
    assert_eq!(
        std::fs::read_to_string(outside.path().join("canary.rs")).unwrap(),
        "secret"
    );
}
#[tokio::test]
async fn capacity_and_agent_interruption() {
    let temp = tempfile::tempdir().unwrap();
    let o = options(temp.path());
    let held = acquire_read(&o.capacity, &o.limits, &CancellationToken::new())
        .await
        .unwrap();
    let cancel = CancellationToken::new();
    cancel.cancel();
    assert!(
        definition(o.clone())
            .executor
            .execute_owned_context(context(&o.root, cancel), json!({"pattern":"**"}))
            .await
            .is_err()
    );
    let short = Arc::new(Options {
        root: o.root.clone(),
        capacity: o.capacity.clone(),
        limits: Limits {
            max_blocking_wait: Duration::from_millis(10),
            ..limits()
        },
    });
    assert_eq!(
        call(&short, json!({"pattern":"**"})).await["error"]["category"],
        "unavailable"
    );
    let store = Arc::new(MemoryStore::new());
    let agent = Agent::builder()
        .behavior_version(concat!("crabber-tools/", env!("CARGO_PKG_VERSION")))
        .store(store.clone())
        .provider(Arc::new(FakeProvider::scripted(vec![
            turn("glob", json!({"pattern":"**"})),
            done(),
        ])))
        .config(config(temp.path()))
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .shared_tool(definition(o.clone()))
        .build()
        .unwrap();
    let run = agent.prompt(None, "discover").await.unwrap();
    let session = run.session_id().clone();
    tokio::time::timeout(Duration::from_secs(2), async {
        while calls(&store, &session).await.is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    run.interrupt();
    support::assert_stopped_uncertain(run, &store).await;
    assert_eq!(
        calls(&store, &session).await[0].status,
        ToolCallStatus::Running
    );
    drop(held);
    assert_eq!(
        call(&o, json!({"pattern":"**"})).await["outcome"],
        "succeeded"
    );
    agent.close_extensions().await.unwrap();
}

#[test]
fn upstream_metadata_and_documented_schema() {
    let fixture: Value =
        serde_json::from_str(include_str!("../../../fixtures/eino-tools/glob.json")).unwrap();
    let info = crabber_tools_glob::info();
    assert_eq!(
        fixture,
        json!({"name":info.name,"description":info.description,"parameters":info.parameters})
    );
    let docs = include_str!("../../../docs/tool-contract.md");
    let section = docs.split("## glob\n").nth(1).unwrap();
    let schema: Value = serde_json::from_str(
        section
            .split("```json\n")
            .nth(1)
            .unwrap()
            .split("```")
            .next()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(schema, info.parameters);
}

#[tokio::test]
async fn wide_batches_and_nested_resume_preserve_earliest_matches() {
    let temp = tempfile::tempdir().unwrap();
    for i in 0..600 {
        let path = temp.path().join(format!("d{i:04}"));
        std::fs::create_dir(&path).unwrap();
        std::fs::write(path.join("match.rs"), "").unwrap();
    }
    let o = options(temp.path());
    let v = call(&o, json!({"pattern":"**/*.rs","limit":2})).await;
    assert_eq!(
        v["paths"],
        json!([{"path":"d0000/match.rs","is_dir":false},{"path":"d0001/match.rs","is_dir":false}])
    );
    assert_eq!(v["truncated"], true);
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn lossy_name_collisions_preserve_identity_and_truncation() {
    use std::os::unix::ffi::OsStringExt;
    let temp = tempfile::tempdir().unwrap();
    std::fs::write(
        temp.path()
            .join(std::ffi::OsString::from_vec(vec![b'x', 0x80])),
        "",
    )
    .unwrap();
    std::fs::create_dir(
        temp.path()
            .join(std::ffi::OsString::from_vec(vec![b'x', 0x81])),
    )
    .unwrap();
    let o = options(temp.path());
    let v = call(&o, json!({"pattern":"*","limit":2})).await;
    assert_eq!(v["count"], 2);
    assert_eq!(v["truncated"], false);
    assert_eq!(
        v["paths"],
        json!([{"path":"x�","is_dir":false},{"path":"x�","is_dir":true}])
    );
    let v = call(&o, json!({"pattern":"*","limit":1})).await;
    assert_eq!(v["count"], 1);
    assert_eq!(v["truncated"], true);
}
#[tokio::test]
async fn evicted_ancestors_resume_unfinished_siblings() {
    let temp = tempfile::tempdir().unwrap();
    let mut prefix = std::path::PathBuf::new();
    let mut expected = vec![];
    for _ in 0..12 {
        let path = prefix.join("z.rs");
        std::fs::write(temp.path().join(&path), "").unwrap();
        expected.push(path.to_string_lossy().into_owned());
        prefix.push("a");
        std::fs::create_dir(temp.path().join(&prefix)).unwrap();
    }
    expected.sort();
    let o = options(temp.path());
    let v = call(&o, json!({"pattern":"**/*.rs"})).await;
    let actual: Vec<_> = v["paths"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| p["path"].as_str().unwrap().to_owned())
        .collect();
    assert_eq!(actual, expected);
    assert_eq!(v["count"], 12);
    assert_eq!(v["truncated"], false);
}

#[test]
fn pathological_patterns_do_not_abort_the_process() {
    if std::env::var_os("CRABBER_GLOB_PATTERN_WORKER").is_some() {
        return;
    }
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "pathological_pattern_worker", "--nocapture"])
        .env("CRABBER_GLOB_PATTERN_WORKER", "1")
        .status()
        .unwrap();
    assert!(status.success());
}
#[tokio::test]
async fn pathological_pattern_worker() {
    if std::env::var_os("CRABBER_GLOB_PATTERN_WORKER").is_none() {
        return;
    }
    let root = tempfile::tempdir().unwrap();
    let limits = support::limits();
    let options = std::sync::Arc::new(crabber_tools_glob::Options {
        root: crabber_tools_core::WorkspaceRoot::open(root.path()).unwrap(),
        capacity: crabber_tools_core::Capacity::new(&limits).unwrap(),
        limits,
    });
    for pattern in [
        "{".repeat(300) + "a" + &"}".repeat(300),
        "{".repeat(100000) + "a" + &"}".repeat(100000),
        "x".repeat(4097),
        "[".into(),
    ] {
        let result = crabber_tools_glob::definition(options.clone())
            .executor
            .execute_owned_context(
                support::context(&options.root, tokio_util::sync::CancellationToken::new()),
                serde_json::json!({"pattern":pattern}),
            )
            .await
            .unwrap()
            .json_value()
            .unwrap();
        assert_eq!(result["error"]["category"], "validation");
    }
}

#[tokio::test]
async fn interrupted_large_walk_joins_owned_resources() {
    let temp = tempfile::tempdir().unwrap();
    for i in 0..20_000 {
        std::fs::write(temp.path().join(format!("f{i:05}")), "").unwrap();
    }
    let options = options(temp.path());
    let owner = crabber::extension::ToolResourceOwner::new();
    let cancel = CancellationToken::new();
    let context = context(&options.root, cancel.clone()).with_resources(owner.resources());
    let tool = definition(options);
    let worker = tokio::spawn(async move {
        tool.executor
            .execute_with_context(context, json!({"pattern":"**"}))
            .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while owner.snapshot().pending == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    cancel.cancel();
    let _ = worker.await.unwrap();
    let report = owner.drain(std::time::Duration::from_secs(2)).await;
    assert!(report.sealed);
    assert_eq!(report.pending, 0);
    assert!(report.joined >= 1);
    assert!(report.failures.is_empty());
}
