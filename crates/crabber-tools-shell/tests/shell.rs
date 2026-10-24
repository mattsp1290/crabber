//! Shell behavior through direct context and Crabber runtime calls.
use crabber_tools_core::*;
use crabber_tools_shell::*;
use crabber_tools_testkit as support;
use serde_json::{Value, json};
use std::{sync::Arc, time::Duration};
use support::*;
use tokio_util::sync::CancellationToken;
fn options(path: &std::path::Path) -> Options {
    let l = limits();
    Options {
        root: WorkspaceRoot::open(path).unwrap(),
        policy: ShellPolicy {
            shell_binary: "/bin/sh".into(),
            startup_mode: StartupMode::NonLogin,
            env: EnvPolicy::minimal_allowlist(),
            run_as: None,
            output_cap_bytes: 262_144,
        },
        capacity: Capacity::new(&l).unwrap(),
        limits: l,
    }
}
async fn call(options: Options, args: Value) -> Value {
    let root = options.root.clone();
    definition(Arc::new(options))
        .unwrap()
        .executor
        .execute_owned_context(context(&root, CancellationToken::new()), args)
        .await
        .unwrap()
        .json_value()
        .unwrap()
}
#[tokio::test]
async fn behavior_caps_validation_and_runtime() {
    let tmp = tempfile::tempdir().unwrap();
    let value = execute(
        tmp.path(),
        definition(Arc::new(options(tmp.path()))).unwrap(),
        json!({"cmd":"printf hello; printf error >&2; exit 3"}),
    )
    .await;
    assert_eq!(value["outcome"], "succeeded");
    assert_eq!(value["exit_code"], 3);
    assert_eq!(value["stdout"], "hello");
    assert_eq!(value["stderr"], "error");
    let v = call(options(tmp.path()), json!({"cmd":"pwd"})).await;
    assert_eq!(
        v["stdout"],
        format!("{}\n", tmp.path().canonicalize().unwrap().display())
    );
    let v=call(options(tmp.path()),json!({"cmd":"head -c 300000 /dev/zero | tr '\\000' x; head -c 300000 /dev/zero | tr '\\000' y >&2"})).await;
    assert_eq!(v["stdout"].as_str().unwrap().len(), 262_144);
    assert_eq!(v["stderr"].as_str().unwrap().len(), 262_144);
    assert_eq!(v["stdout_truncated"], true);
    assert_eq!(v["stderr_truncated"], true);
    for a in [
        json!({"cmd":""}),
        json!({"cmd":"x\0"}),
        json!({"cmd":"true","timeout_seconds":601}),
        json!({"cmd":"true","extra":1}),
    ] {
        assert_eq!(
            call(options(tmp.path()), a).await["error"]["category"],
            "validation"
        );
    }
    assert_eq!(
        call(
            options(tmp.path()),
            json!({"cmd":"true","timeout_seconds":0})
        )
        .await["outcome"],
        "succeeded"
    );
}
#[tokio::test]
async fn timeout_and_pipe_grace_leave_no_descendant() {
    let tmp = tempfile::tempdir().unwrap();
    for (command, timeout, expected) in [
        (
            "echo $$ > leader; sleep 60 & echo $! > child; wait",
            1,
            true,
        ),
        (
            "echo $$ > leader; sleep 60 & echo $! > child; echo started",
            30,
            false,
        ),
    ] {
        let start = tokio::time::Instant::now();
        let v = call(
            options(tmp.path()),
            json!({"cmd":command,"timeout_seconds":timeout}),
        )
        .await;
        assert!(start.elapsed() < Duration::from_secs(7));
        assert_eq!(
            v.get("timed_out").and_then(Value::as_bool).unwrap_or(false),
            expected
        );
        if expected {
            assert_eq!(v["error"]["category"], "timeout");
        } else {
            assert_eq!(v["stdout"], "started\n");
        }
        assert_gone(pid(&tmp.path().join("leader"))).await;
        assert_gone(pid(&tmp.path().join("child"))).await;
    }
}
#[tokio::test]
async fn environment_and_policy_construction() {
    let tmp = tempfile::tempdir().unwrap();
    let mut o = options(tmp.path());
    o.policy.env = EnvPolicy::Replace(vec![("ONLY".into(), "1".into())]);
    let v = call(o, json!({"cmd":"env"})).await;
    let env = v["stdout"].as_str().unwrap();
    assert!(env.contains("ONLY=1"));
    assert!(!env.contains("HOME="));
    let mut o = options(tmp.path());
    o.policy.shell_binary = "/missing/shell".into();
    assert!(definition(Arc::new(o)).is_err());
    let mut o = options(tmp.path());
    o.policy.output_cap_bytes = 0;
    assert!(definition(Arc::new(o)).is_err());
    let o = options(tmp.path());
    let root = o.root.clone();
    let cancel = CancellationToken::new();
    cancel.cancel();
    assert!(
        definition(Arc::new(o))
            .unwrap()
            .executor
            .execute_owned_context(
                context(&root, cancel),
                json!({"cmd":"touch must-not-exist"})
            )
            .await
            .is_err()
    );
    assert!(!tmp.path().join("must-not-exist").exists());
}
#[tokio::test]
async fn runtime_interrupt_kills_group_and_permission_denies_effect() {
    use crabber::{
        Agent, FakeProvider, PermissionDecision, StaticPolicy, core::ToolCallStatus,
        session::MemoryStore,
    };
    let tmp = tempfile::tempdir().unwrap();
    let store = Arc::new(MemoryStore::new());
    let agent = Agent::builder()
        .behavior_version(concat!("crabber-tools/", env!("CARGO_PKG_VERSION")))
        .store(store.clone())
        .provider(Arc::new(FakeProvider::scripted(vec![
            turn(
                "shell",
                json!({"cmd":"echo $$ > leader; sleep 60 & echo $! > child; wait"}),
            ),
            done(),
        ])))
        .config(config(tmp.path()))
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .shared_tool(definition(Arc::new(options(tmp.path()))).unwrap())
        .build()
        .unwrap();
    let run = agent.prompt(None, "interrupt").await.unwrap();
    let session = run.session_id().clone();
    wait_file(&tmp.path().join("child")).await;
    let interrupted_run = run.run_id().clone();
    run.interrupt();
    support::assert_stopped_uncertain(run, &store).await;
    let drain = agent.drain_execution_tasks(Duration::from_secs(2)).await;
    let resources = drain
        .resources
        .iter()
        .find(|r| r.run_id == interrupted_run)
        .expect("interrupted call resource report");
    assert!(resources.resources.sealed);
    assert!(resources.resources.joined >= 1);
    assert_eq!(resources.resources.pending, 0);
    assert_eq!(
        resources.resources.failures,
        [] as [crabber::extension::ResourceFailure; 0]
    );

    assert_gone(pid(&tmp.path().join("leader"))).await;
    assert_gone(pid(&tmp.path().join("child"))).await;
    assert_eq!(
        calls(&store, &session).await[0].status,
        ToolCallStatus::Running
    );
    agent.close_extensions().await.unwrap();
    let store = Arc::new(MemoryStore::new());
    let agent = Agent::builder()
        .behavior_version(concat!("crabber-tools/", env!("CARGO_PKG_VERSION")))
        .store(store.clone())
        .provider(Arc::new(FakeProvider::scripted(vec![
            turn("shell", json!({"cmd":"touch canary"})),
            done(),
        ])))
        .config(config(tmp.path()))
        .policy(Arc::new(
            StaticPolicy::new(PermissionDecision::Allow)
                .with_rule("shell", PermissionDecision::Deny),
        ))
        .shared_tool(definition(Arc::new(options(tmp.path()))).unwrap())
        .build()
        .unwrap();
    let run = agent.prompt(None, "deny").await.unwrap();
    let session = run.session_id().clone();
    run.done().await.unwrap();
    assert_eq!(
        calls(&store, &session).await[0].status,
        ToolCallStatus::Failed
    );
    assert!(!tmp.path().join("canary").exists());
    agent.close_extensions().await.unwrap();
}
#[tokio::test]
async fn startup_profile() {
    let tmp = tempfile::tempdir().unwrap();
    std::fs::write(tmp.path().join(".profile"), "export STARTUP_CANARY=1\n").unwrap();
    let env = EnvPolicy::Replace(vec![(
        "HOME".into(),
        tmp.path().to_string_lossy().into_owned(),
    )]);
    let probe = std::process::Command::new("/bin/sh")
        .args(["-lc", "printf '%s' \"$STARTUP_CANARY\""])
        .env_clear()
        .env("HOME", tmp.path())
        .output()
        .unwrap();
    if probe.stdout != b"1" {
        assert!(
            std::env::var("CRABBER_TOOLS_REQUIRE_LOGIN_PROFILE").as_deref() != Ok("1"),
            "runner shell does not honor HOME .profile"
        );
        return;
    }
    for (mode, expected) in [(StartupMode::Login, "1"), (StartupMode::NonLogin, "")] {
        let mut o = options(tmp.path());
        o.policy.env = env.clone();
        o.policy.startup_mode = mode;
        assert_eq!(
            call(o, json!({"cmd":"printf '%s' \"$STARTUP_CANARY\""})).await["stdout"],
            expected
        );
    }
}

#[tokio::test]
async fn child_identity_and_detached_pipe_deadline() {
    let tmp = tempfile::tempdir().unwrap();
    let uid = std::process::Command::new("id").arg("-u").output().unwrap();
    let uid: u32 = String::from_utf8(uid.stdout)
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    let gid = std::process::Command::new("id").arg("-g").output().unwrap();
    let gid: u32 = String::from_utf8(gid.stdout)
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    let mut o = options(tmp.path());
    o.policy.run_as = Some(RunAs { uid, gid });
    assert_eq!(
        call(o, json!({"cmd":"id -u"})).await["stdout"],
        format!("{uid}\n")
    );
    if uid != 0 {
        let mut o = options(tmp.path());
        o.policy.run_as = Some(RunAs { uid: uid + 1, gid });
        assert_eq!(
            call(o, json!({"cmd":"id -u"})).await["error"]["category"],
            "exec_failed"
        );
    } else if std::env::var("CRABBER_TOOLS_TEST_AS_ROOT").as_deref() == Ok("1") {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(tmp.path(), std::fs::Permissions::from_mode(0o777)).unwrap();
        let mut o = options(tmp.path());
        o.policy.run_as = Some(RunAs {
            uid: 65534,
            gid: 65534,
        });
        assert_eq!(call(o, json!({"cmd":"id -u"})).await["stdout"], "65534\n");
    }
    if resolve_binary("setsid").is_ok() {
        let start = tokio::time::Instant::now();
        let v = call(
            options(tmp.path()),
            json!({"cmd":"setsid sleep 60 & echo $! > detached; wait","timeout_seconds":1}),
        )
        .await;
        let child = pid(&tmp.path().join("detached"));
        // Detachment explicitly escapes group ownership; the test host cleans it up.
        std::process::Command::new("kill")
            .args(["-KILL", &child.to_string()])
            .status()
            .unwrap();
        assert!(start.elapsed() < Duration::from_secs(7));
        assert_eq!(v["timed_out"], true);
        assert_gone(child).await;
    }
}

#[tokio::test]
async fn control_heavy_output_fits_typed_bound() {
    let dir = tempfile::tempdir().unwrap();
    let mut options = options(dir.path());
    options.policy.output_cap_bytes = 2 * 1024 * 1024;
    let value = execute(
        dir.path(),
        definition(Arc::new(options)).unwrap(),
        json!({"cmd":"head -c 2097152 /dev/zero | tr '\\000' '\\001'"}),
    )
    .await;
    assert_eq!(value["outcome"], "succeeded");
    assert_eq!(value["exit_code"], 0);
    assert_eq!(value["stdout_truncated"], true);
    assert!(serde_json::to_vec(&value).unwrap().len() <= 4 * 1024 * 1024);
}
