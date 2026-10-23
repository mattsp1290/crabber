//! No-hub fake bn tests for argv boundaries, environment, exit codes and cancellation.
use crabber_tools_core::*;
use crabber_tools_testkit as support;
use crabber_tools_trackerwrite::{Options, TrackerPolicy, definition};
use serde_json::{Value, json};
use std::{collections::BTreeSet, path::Path, sync::Arc};
use support::*;
use tokio_util::sync::CancellationToken;
fn options(root: &Path, hub: &Path, program: &str) -> Arc<Options> {
    let binary = hub.join("fake-bn");
    script(&binary, program);
    let limits = limits();
    Arc::new(Options {
        root: WorkspaceRoot::open(root).unwrap(),
        limits: limits.clone(),
        capacity: Capacity::new(&limits).unwrap(),
        policy: TrackerPolicy {
            bn_binary: binary,
            project: "demo".into(),
            hub: hub.into(),
            actor: "test-actor".into(),
            statuses: BTreeSet::from(["open".into(), "in_progress".into(), "closed".into()]),
            environment: vec![("PATH".into(), "/usr/bin:/bin".into())],
        },
    })
}
async fn call(o: &Arc<Options>, args: Value) -> Value {
    definition(o.clone())
        .unwrap()
        .executor
        .execute_owned_context(context(&o.root, CancellationToken::new()), args)
        .await
        .unwrap()
        .json_value()
        .unwrap()
}
#[tokio::test]
async fn argv_environment_and_workspace_lock_independence() {
    let root = tempfile::tempdir().unwrap();
    let hub = tempfile::tempdir().unwrap();
    let o = options(
        root.path(),
        hub.path(),
        "#!/bin/sh\nprintf '%s\\000' \"$@\" > \"$BEANS_HUB/argv\"\nenv > \"$BEANS_HUB/env\"\npwd > \"$BEANS_HUB/cwd\"\n",
    );
    let other_capacity = Capacity::new(&limits()).unwrap();
    let _held = acquire_mutating(
        &RootLock::for_root(&o.root),
        &other_capacity,
        &limits(),
        &CancellationToken::new(),
    )
    .await
    .unwrap();
    for (args, expected) in [
        (
            json!({"op":"close","id":"demo-ab12"}),
            vec![
                "--project",
                "demo",
                "close",
                "-r",
                "closed by tracker_write",
                "--",
                "demo-ab12",
            ],
        ),
        (
            json!({"op":"close","id":"--project-ab12","reason":"literal $(touch never)"}),
            vec![
                "--project",
                "demo",
                "close",
                "-r",
                "literal $(touch never)",
                "--",
                "--project-ab12",
            ],
        ),
        (
            json!({"op":"transition","id":"demo-ab12","toState":"in_progress"}),
            vec![
                "--project",
                "demo",
                "update",
                "--status",
                "in_progress",
                "--",
                "demo-ab12",
            ],
        ),
        (
            json!({"op":"comment","id":"demo-ab12","body":"--hub /escape\n$(touch never)"}),
            vec![
                "--project",
                "demo",
                "note",
                "--",
                "demo-ab12",
                "--hub /escape\n$(touch never)",
            ],
        ),
    ] {
        let v = execute(root.path(), definition(o.clone()).unwrap(), args.clone()).await;
        assert_eq!(
            v,
            json!({"outcome":"succeeded","op":args["op"],"id":args["id"]})
        );
        let raw = std::fs::read(hub.path().join("argv")).unwrap();
        let argv: Vec<_> = raw
            .split(|b| *b == 0)
            .filter(|part| !part.is_empty())
            .map(|s| std::str::from_utf8(s).unwrap())
            .collect();
        assert_eq!(argv, expected);
    }
    let env = std::fs::read_to_string(hub.path().join("env")).unwrap();
    assert!(env.contains("BN_ACTOR=test-actor\n"));
    assert!(env.contains(&format!("BEANS_HUB={}\n", hub.path().display())));
    assert!(!env.lines().any(|l| l.starts_with("HOME=")));
    assert_eq!(
        std::fs::read_to_string(hub.path().join("cwd"))
            .unwrap()
            .trim(),
        o.root.path().to_str().unwrap()
    );
    assert!(!root.path().join("never").exists());
}
#[tokio::test]
async fn exit_codes_and_capped_sanitized_diagnostics() {
    let root = tempfile::tempdir().unwrap();
    let hub = tempfile::tempdir().unwrap();
    for (code, category) in [
        (1, "validation"),
        (2, "not_found"),
        (3, "api_request"),
        (4, "rate_limited"),
        (7, "unknown"),
    ] {
        let program = format!("#!/bin/sh\necho '/host/private secret-token' >&2\nexit {code}\n");
        let o = options(root.path(), hub.path(), &program);
        let v = call(&o, json!({"op":"close","id":"demo-ab12"})).await;
        assert_eq!(v["error"]["category"], category);
        assert_eq!(v["error"]["op"], "close");
        assert!(!v.to_string().contains("secret-token"));
        assert!(!v.to_string().contains("/host/private"));
    }
    let o = options(
        root.path(),
        hub.path(),
        "#!/bin/sh\nhead -c 100000 /dev/zero\nhead -c 8192 /dev/zero | tr '\\000' x >&2\necho status >&2\nexit 1\n",
    );
    let v = call(&o, json!({"op":"close","id":"demo-ab12"})).await;
    assert_eq!(v["error"]["message"], "tracker rejected the operation");
    assert!(v.to_string().len() < 1024);
}
#[tokio::test]
async fn invalid_arguments_and_policy_fail_before_execution() {
    let root = tempfile::tempdir().unwrap();
    let hub = tempfile::tempdir().unwrap();
    let o = options(
        root.path(),
        hub.path(),
        "#!/bin/sh\ntouch \"$BEANS_HUB/executed\"\n",
    );
    for args in [
        json!({}),
        json!({"op":"other","id":"demo-ab12"}),
        json!({"op":"close","id":"demo-abc"}),
        json!({"op":"close","id":"demo-AB12"}),
        json!({"op":"close","id":"demo-ab12; touch never"}),
        json!({"op":"comment","id":"demo-ab12"}),
        json!({"op":"comment","id":"demo-ab12","body":" "}),
        json!({"op":"transition","id":"demo-ab12","toState":"bogus"}),
        json!({"op":"close","id":"demo-ab12","reason":"--project"}),
        json!({"op":"close","id":"demo-ab12","reason":"x\u{0000}"}),
        json!({"op":"close","id":"demo-ab12","extra":1}),
        json!({"op":"comment","id":"demo-ab12","body":"x".repeat(crabber_tools_trackerwrite::MAX_TEXT_BYTES+1)}),
    ] {
        assert_eq!(call(&o, args).await["error"]["category"], "validation");
    }
    assert_eq!(
        call(
            &o,
            json!({"op":"link_pr","id":"demo-ab12","prURL":"https://example.test/pull/1"})
        )
        .await["error"]["category"],
        "unsupported_op"
    );
    assert!(!hub.path().join("executed").exists());
    let other = tempfile::tempdir().unwrap();
    assert_eq!(
        execute(
            other.path(),
            definition(o.clone()).unwrap(),
            json!({"op":"close","id":"demo-ab12"})
        )
        .await["error"]["category"],
        "workspace_mismatch"
    );
    for change in 0..7 {
        let mut policy = o.policy.clone();
        match change {
            0 => policy.project = "--project".into(),
            1 => policy.hub = "relative".into(),
            2 => policy.actor = String::new(),
            3 => policy.statuses.clear(),
            4 => policy
                .environment
                .push(("BN_ACTOR".into(), "override".into())),
            5 => policy.environment.push(("PATH".into(), "duplicate".into())),
            _ => policy
                .statuses
                .insert("--flag".into())
                .then_some(())
                .unwrap(),
        }
        assert!(policy.validate().is_err());
    }
}
#[tokio::test]
async fn agent_interruption_kills_tracker_process_group() {
    use crabber::{
        Agent, FakeProvider, PermissionDecision, StaticPolicy, core::ToolCallStatus,
        session::MemoryStore,
    };
    let root = tempfile::tempdir().unwrap();
    let hub = tempfile::tempdir().unwrap();
    let o = options(
        root.path(),
        hub.path(),
        "#!/bin/sh\nsleep 60 &\necho $! > \"$BEANS_HUB/child\"\necho $$ > \"$BEANS_HUB/leader\"\nwait\n",
    );
    let store = Arc::new(MemoryStore::new());
    let agent = Agent::builder()
        .behavior_version(concat!("crabber-tools/", env!("CARGO_PKG_VERSION")))
        .store(store.clone())
        .provider(Arc::new(FakeProvider::scripted(vec![
            turn("tracker_write", json!({"op":"close","id":"demo-ab12"})),
            done(),
        ])))
        .config(config(root.path()))
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .shared_tool(definition(o.clone()).unwrap())
        .build()
        .unwrap();
    let run = agent.prompt(None, "track").await.unwrap();
    let session = run.session_id().clone();
    wait_file(&hub.path().join("child")).await;
    wait_file(&hub.path().join("leader")).await;
    let child = pid(&hub.path().join("child"));
    let leader = pid(&hub.path().join("leader"));
    run.interrupt();
    support::assert_stopped_uncertain(run, &store).await;
    assert_eq!(
        calls(&store, &session).await[0].status,
        ToolCallStatus::Running
    );
    assert_gone(child).await;
    assert_gone(leader).await;
    agent.close_extensions().await.unwrap();
    let _recovered = acquire_read(&o.capacity, &o.limits, &CancellationToken::new())
        .await
        .unwrap();
}
#[test]
fn upstream_metadata_matches() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../fixtures/eino-tools/tracker_write.json"
    ))
    .unwrap();
    let info = crabber_tools_trackerwrite::info();
    assert_eq!(
        fixture,
        json!({"name":info.name,"description":info.description,"parameters":info.parameters})
    );
    let section = include_str!("../../../docs/tool-contract.md")
        .split("## tracker_write\n")
        .nth(1)
        .unwrap();
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
    assert!(!info.retry_safe);
    assert_eq!(info.required_permissions, ["tracker.write"]);
}
