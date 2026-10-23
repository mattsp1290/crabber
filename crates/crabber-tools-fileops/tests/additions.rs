//! Additional writer contracts, lifecycle and capability regressions.
use crabber::ToolDefinition;
use crabber_tools_core::*;
use crabber_tools_fileops::{self as fileops, Options};
use serde_json::{Value, json};
use std::{sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;
fn options(root: &std::path::Path) -> Arc<Options> {
    let limits = Limits {
        max_in_flight: 1,
        max_blocking_wait: Duration::from_secs(1),
    };
    Arc::new(Options {
        deny: crabber_tools_core::PathDenyPolicy::none(),
        root: WorkspaceRoot::open(root).unwrap(),
        capacity: Capacity::new(&limits).unwrap(),
        limits,
        pre_write: None,
    })
}
async fn call(tool: Arc<ToolDefinition>, options: &Options, args: Value) -> Value {
    tool.executor
        .execute_owned_context(context(&options.root, CancellationToken::new()), args)
        .await
        .unwrap()
        .json_value()
        .unwrap()
}
fn keys(v: &Value) -> Vec<&str> {
    let mut k: Vec<_> = v.as_object().unwrap().keys().map(String::as_str).collect();
    k.sort_unstable();
    k
}
#[tokio::test]
async fn ordered_edits_are_atomic_and_bounded() {
    use std::os::unix::fs::symlink;
    let temp = tempfile::tempdir().unwrap();
    let o = options(temp.path());
    let tool = fileops::multi_edit::definition(o.clone());
    std::fs::write(temp.path().join("x"), "alpha").unwrap();
    let v=call(tool.clone(), &o, json!({"path":"x","edits":[{"anchor":"alpha","replacement":"beta"},{"anchor":"beta","replacement":"gamma"},{"anchor":"gamma","replacement":"done"}]})).await;
    assert_eq!(v["edits_applied"], 3);
    assert_eq!(std::fs::read(temp.path().join("x")).unwrap(), b"done");
    let v=call(tool.clone(), &o, json!({"path":"x","edits":[{"anchor":"done","replacement":"a a"},{"anchor":"a","replacement":"bad"}]})).await;
    assert_eq!(v["error"]["category"], "anchor_ambiguous");
    assert_eq!(v["edit_index"], 1);
    assert_eq!(v["anchor_occurrences"], 2);
    assert_eq!(v["edits_applied"], 0);
    assert_eq!(std::fs::read(temp.path().join("x")).unwrap(), b"done");
    for edits in [
        json!([]),
        json!(vec![json!({"anchor":"a","replacement":"b"}); 65]),
        json!([{"anchor":"","replacement":"b"}]),
    ] {
        assert_eq!(
            call(tool.clone(), &o, json!({"path":"x","edits":edits})).await["error"]["category"],
            "validation"
        );
    }
    for edits in [
        json!([{"anchor":"done","replacement":"x".repeat(fileops::MAX_OUTPUT_BYTES+1)}]),
        json!([{"anchor":"x".repeat(fileops::MAX_OUTPUT_BYTES+1),"replacement":"b"}]),
    ] {
        assert_eq!(
            call(tool.clone(), &o, json!({"path":"x","edits":edits})).await["error"]["category"],
            "too_large"
        );
    }
    std::fs::write(temp.path().join("x"), "ab").unwrap();
    assert_eq!(call(tool.clone(),&o,json!({"path":"x","edits":[{"anchor":"a","replacement":"c".repeat(fileops::MAX_OUTPUT_BYTES)},{"anchor":"c","replacement":""}]})).await["error"]["category"],"too_large");
    assert_eq!(std::fs::read(temp.path().join("x")).unwrap(), b"ab");
    symlink("x", temp.path().join("link")).unwrap();
    assert_eq!(
        call(
            tool.clone(),
            &o,
            json!({"path":"link","edits":[{"anchor":"ab","replacement":"target"}]})
        )
        .await["outcome"],
        "succeeded"
    );
    assert!(
        std::fs::symlink_metadata(temp.path().join("link"))
            .unwrap()
            .file_type()
            .is_symlink()
    );
    assert_eq!(std::fs::read(temp.path().join("x")).unwrap(), b"target");
    for (bytes, category) in [
        (vec![0, 255], "binary"),
        (vec![b'x'; fileops::MAX_OUTPUT_BYTES + 1], "too_large"),
    ] {
        std::fs::write(temp.path().join("x"), bytes).unwrap();
        assert_eq!(
            call(
                tool.clone(),
                &o,
                json!({"path":"x","edits":[{"anchor":"a","replacement":"b"}]})
            )
            .await["error"]["category"],
            category
        );
    }
}
#[tokio::test]
async fn delete_rejects_links_directories_and_escapes() {
    use std::os::unix::fs::symlink;
    let temp = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    let o = options(temp.path());
    let tool = fileops::file_delete::definition(o.clone());
    std::fs::write(temp.path().join("x"), "original").unwrap();
    symlink("x", temp.path().join("link")).unwrap();
    symlink(outside.path(), temp.path().join("escape")).unwrap();
    std::fs::create_dir(temp.path().join("dir")).unwrap();
    std::fs::write(outside.path().join("canary"), "safe").unwrap();
    for (path, category) in [
        ("missing", "not_found"),
        ("dir", "is_directory"),
        (".", "is_directory"),
        ("link", "validation"),
        ("escape/canary", "path_escape"),
    ] {
        assert_eq!(
            call(tool.clone(), &o, json!({"path":path})).await["error"]["category"],
            category
        );
        assert_eq!(std::fs::read(temp.path().join("x")).unwrap(), b"original");
    }
    assert_eq!(
        call(tool, &o, json!({"path":"x"})).await,
        json!({"outcome":"succeeded","path":"x"})
    );
    assert!(!temp.path().join("x").exists());
    assert_eq!(
        std::fs::read(outside.path().join("canary")).unwrap(),
        b"safe"
    );
}
#[tokio::test]
async fn move_preserves_inode_and_refuses_overwrite() {
    use std::os::unix::fs::{MetadataExt, PermissionsExt, symlink};
    let temp = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    let o = options(temp.path());
    let tool = fileops::file_move::definition(o.clone());
    std::fs::write(temp.path().join("x"), "original").unwrap();
    std::fs::write(temp.path().join("exists"), "keep").unwrap();
    std::fs::create_dir(temp.path().join("dir")).unwrap();
    symlink("x", temp.path().join("link")).unwrap();
    symlink(outside.path(), temp.path().join("escape")).unwrap();
    for (path, dest, category) in [
        ("x", "x", "validation"),
        ("x", "./x", "validation"),
        ("x", ".", "validation"),
        ("x", "missing/new", "not_found"),
        ("x", "exists", "conflict"),
        ("x", "dir", "conflict"),
        ("x", "link", "conflict"),
        ("link", "new", "validation"),
        ("x", "escape/new", "path_escape"),
        ("dir", "new", "is_directory"),
        ("missing", "new", "not_found"),
    ] {
        assert_eq!(
            call(tool.clone(), &o, json!({"path":path,"new_path":dest})).await["error"]["category"],
            category,
            "{path} {dest}"
        );
        assert_eq!(std::fs::read(temp.path().join("x")).unwrap(), b"original");
    }
    for mode in [0o640, 0o444] {
        std::fs::set_permissions(temp.path().join("x"), std::fs::Permissions::from_mode(mode))
            .unwrap();
        let before = std::fs::metadata(temp.path().join("x")).unwrap();
        let v = call(
            tool.clone(),
            &o,
            json!({"path":"x","new_path":"nested/new","create_dirs":true}),
        )
        .await;
        assert_eq!(
            v,
            json!({"outcome":"succeeded","path":"x","new_path":"nested/new"})
        );
        let after = std::fs::metadata(temp.path().join("nested/new")).unwrap();
        assert_eq!(before.ino(), after.ino());
        assert_eq!(before.uid(), after.uid());
        assert_eq!(after.mode() & 0o777, mode);
        assert!(!temp.path().join("x").exists());
        assert_eq!(
            call(
                tool.clone(),
                &o,
                json!({"path":"nested/new","new_path":"x"})
            )
            .await["outcome"],
            "succeeded"
        );
    }
    std::fs::hard_link(temp.path().join("x"), temp.path().join("other")).unwrap();
    assert_eq!(std::fs::metadata(temp.path().join("x")).unwrap().nlink(), 2);
    assert_eq!(
        call(tool, &o, json!({"path":"x","new_path":"new"})).await["outcome"],
        "succeeded"
    );
    assert_eq!(
        std::fs::read(temp.path().join("other")).unwrap(),
        b"original"
    );
    assert_eq!(
        std::fs::metadata(temp.path().join("new")).unwrap().nlink(),
        2
    );
}
fn constructors() -> [fn(Arc<Options>) -> Arc<ToolDefinition>; 3] {
    [
        fileops::multi_edit::definition,
        fileops::file_delete::definition,
        fileops::file_move::definition,
    ]
}
fn args(index: usize, path: &str) -> Value {
    match index {
        0 => json!({"path":path,"edits":[{"anchor":"a","replacement":"b"}]}),
        1 => json!({"path":path}),
        _ => json!({"path":path,"new_path":"new"}),
    }
}
#[tokio::test]
async fn failure_envelope_keys_and_lifecycle() {
    let temp = tempfile::tempdir().unwrap();
    let sibling = tempfile::tempdir().unwrap();
    let mut o = options(temp.path());
    Arc::get_mut(&mut o).unwrap().limits.max_blocking_wait = Duration::from_millis(20);
    std::fs::write(temp.path().join("x"), "a").unwrap();
    std::fs::create_dir(temp.path().join("dir")).unwrap();
    for (i, ctor) in constructors().into_iter().enumerate() {
        let tool = ctor(o.clone());
        let early = if i == 0 {
            vec!["edits_applied", "error", "outcome"]
        } else {
            vec!["error", "outcome"]
        };
        let v = call(tool.clone(), &o, json!({})).await;
        assert_eq!(keys(&v), early);
        let v = tool
            .executor
            .execute_owned_context(
                context(
                    &WorkspaceRoot::open(sibling.path()).unwrap(),
                    CancellationToken::new(),
                ),
                args(i, "x"),
            )
            .await
            .unwrap()
            .json_value()
            .unwrap();
        assert_eq!(v["error"]["category"], "workspace_mismatch");
        assert_eq!(keys(&v), early);
        let held = RootLock::for_root(&o.root);
        let guard = acquire_mutating(&held, &o.capacity, &o.limits, &CancellationToken::new())
            .await
            .unwrap();
        let v = call(tool.clone(), &o, args(i, "x")).await;
        assert_eq!(v["error"]["category"], "unavailable");
        assert_eq!(keys(&v), early);
        drop(guard);
        let cancel = CancellationToken::new();
        cancel.cancel();
        assert!(
            tool.executor
                .execute_owned_context(context(&o.root, cancel), args(i, "x"))
                .await
                .is_err()
        );
        for (path, category) in [("missing", "not_found"), ("dir", "is_directory")] {
            let v = call(tool.clone(), &o, args(i, path)).await;
            assert_eq!(v["error"]["category"], category);
            let expected = match i {
                0 => vec!["edits_applied", "error", "outcome", "path"],
                1 => vec!["error", "outcome", "path"],
                _ => vec!["error", "new_path", "outcome", "path"],
            };
            assert_eq!(keys(&v), expected);
        }
    }
    let mut p = options(temp.path());
    Arc::get_mut(&mut p).unwrap().pre_write = Some(Arc::new(|| panic!("injected")));
    assert_eq!(
        call(fileops::multi_edit::definition(p), &o, args(0, "x")).await["error"]["category"],
        "unknown"
    );
    assert_eq!(std::fs::read(temp.path().join("x")).unwrap(), b"a");
    assert_eq!(
        call(fileops::multi_edit::definition(o.clone()), &o, args(0, "x")).await["outcome"],
        "succeeded"
    );
}
#[test]
fn independent_metadata_goldens() {
    for (name, info) in [
        ("multi_edit", fileops::multi_edit::info()),
        ("file_delete", fileops::file_delete::info()),
        ("file_move", fileops::file_move::info()),
    ] {
        let file = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join(format!("../../fixtures/crabber-tools/{name}.json"));
        let golden: Value = serde_json::from_str(&std::fs::read_to_string(file).unwrap()).unwrap();
        assert_eq!(
            golden,
            json!({"name":info.name,"description":info.description,"parameters":info.parameters})
        );
        assert!(!info.retry_safe);
        assert_eq!(info.required_permissions, [permission::FS_WRITE]);
    }
}

use crabber::{
    Agent, FakeProvider, PermissionDecision, StaticPolicy, core::ToolCallStatus,
    session::MemoryStore,
};
use crabber_tools_testkit as support;
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn interrupted_move_leaves_one_name() {
    let temp = tempfile::tempdir().unwrap();
    let (start_tx, mut start_rx) = tokio::sync::mpsc::unbounded_channel();
    let gate = Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
    let closure_gate = gate.clone();
    let hook = Arc::new(move || {
        start_tx.send(()).unwrap();
        let (lock, cv) = &*closure_gate;
        let mut release = lock.lock().unwrap();
        while !*release {
            release = cv.wait(release).unwrap();
        }
    });
    std::fs::write(temp.path().join("source"), "complete").unwrap();
    let mut o = options(temp.path());
    Arc::get_mut(&mut o).unwrap().pre_write = Some(hook);
    let store = Arc::new(MemoryStore::new());
    let agent = Agent::builder()
        .behavior_version(concat!("crabber-tools/", env!("CARGO_PKG_VERSION")))
        .store(store.clone())
        .provider(Arc::new(FakeProvider::scripted(vec![
            support::turn("file_move", json!({"path":"source","new_path":"target"})),
            support::done(),
        ])))
        .config(support::config(temp.path()))
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .shared_tool(fileops::file_move::definition(o.clone()))
        .build()
        .unwrap();
    let run = agent.prompt(None, "move").await.unwrap();
    let session = run.session_id().clone();
    tokio::time::timeout(Duration::from_secs(2), start_rx.recv())
        .await
        .unwrap();
    run.interrupt();
    support::assert_stopped_uncertain(run, &store).await;
    assert_eq!(
        support::calls(&store, &session).await[0].status,
        ToolCallStatus::Running
    );
    assert!(temp.path().join("target").exists());
    assert!(temp.path().join("source").exists());
    // The move is already running off-thread: interruption cannot release its writer lock.
    let short = Limits {
        max_blocking_wait: Duration::from_millis(20),
        ..support::limits()
    };
    assert!(matches!(
        acquire_mutating(
            &RootLock::for_root(&o.root),
            &o.capacity,
            &short,
            &tokio_util::sync::CancellationToken::new()
        )
        .await,
        Err(AcquireError::Unavailable)
    ));
    {
        let (lock, cv) = &*gate;
        *lock.lock().unwrap() = true;
        cv.notify_all();
    }

    let guard = acquire_mutating(
        &RootLock::for_root(&o.root),
        &o.capacity,
        &support::limits(),
        &tokio_util::sync::CancellationToken::new(),
    )
    .await
    .unwrap();
    assert_eq!(
        std::fs::read_to_string(temp.path().join("target")).unwrap(),
        "complete"
    );
    assert!(!temp.path().join("source").exists());
    drop(guard);
    let mut next = options(temp.path());
    Arc::get_mut(&mut next).unwrap().capacity = o.capacity.clone();
    assert_eq!(
        call(
            fileops::write::definition(next.clone()),
            &next,
            json!({"path":"following","content":"ok"})
        )
        .await["outcome"],
        "succeeded"
    );
    assert!(std::fs::read_dir(temp.path()).unwrap().all(|e| {
        !e.unwrap()
            .file_name()
            .to_string_lossy()
            .starts_with(fileops::TEMP_PREFIX)
    }));
    agent.close_extensions().await.unwrap();
}

#[tokio::test]
async fn multi_edit_failure_keys_cover_each_stage() {
    use std::os::unix::fs::symlink;
    let temp = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    let o = options(temp.path());
    let tool = fileops::multi_edit::definition(o.clone());
    std::fs::write(temp.path().join("x"), "a a").unwrap();
    std::fs::write(temp.path().join("binary"), [0xff]).unwrap();
    std::fs::write(
        temp.path().join("big"),
        vec![b'a'; fileops::MAX_OUTPUT_BYTES + 1],
    )
    .unwrap();
    symlink(outside.path(), temp.path().join("escape")).unwrap();
    for (path, anchor, category, extra) in [
        ("x", "absent", "anchor_not_found", true),
        ("x", "a", "anchor_ambiguous", true),
        ("binary", "a", "binary", false),
        ("big", "a", "too_large", false),
        ("escape/x", "a", "path_escape", false),
        ("x/child", "a", "not_directory", false),
    ] {
        let v = call(
            tool.clone(),
            &o,
            json!({"path":path,"edits":[{"anchor":anchor,"replacement":""}]}),
        )
        .await;
        assert_eq!(v["error"]["category"], category);
        let expected = if extra {
            vec![
                "anchor_occurrences",
                "edit_index",
                "edits_applied",
                "error",
                "outcome",
                "path",
            ]
        } else {
            vec!["edits_applied", "error", "outcome", "path"]
        };
        assert_eq!(keys(&v), expected);
        assert_eq!(v["edits_applied"], 0);
    }
    let v=call(tool,&o,json!({"path":"x","edits":[{"anchor":"a","replacement":"x".repeat(fileops::MAX_OUTPUT_BYTES+1)}]})).await;
    assert_eq!(keys(&v), ["edits_applied", "error", "outcome"]);
}

#[tokio::test]
async fn panicking_move_hook_reports_state_and_releases_guards() {
    use std::os::unix::fs::PermissionsExt;
    struct Restore(std::path::PathBuf);
    impl Drop for Restore {
        fn drop(&mut self) {
            std::fs::set_permissions(&self.0, std::fs::Permissions::from_mode(0o700)).unwrap();
        }
    }
    for partial in [false, true] {
        if partial && rustix::process::geteuid().is_root() {
            assert!(
                std::env::var_os("CI").is_none(),
                "panicking_move_hook_reports_state_and_releases_guards requires non-root CI"
            );
            // Root bypasses directory permissions; retain the successful rollback case.
            continue;
        }
        let temp = tempfile::tempdir().unwrap();
        std::fs::write(temp.path().join("source"), b"original").unwrap();
        let destination = temp.path().join("dst");
        std::fs::create_dir(&destination).unwrap();
        let _restore = Restore(destination.clone());
        let mut o = options(temp.path());
        Arc::get_mut(&mut o).unwrap().pre_write = Some(Arc::new(move || {
            if partial {
                std::fs::set_permissions(&destination, std::fs::Permissions::from_mode(0o500))
                    .unwrap();
            }
            panic!("private panic payload must not enter tool result");
        }));
        let value = call(
            fileops::file_move::definition(o.clone()),
            &o,
            json!({"path":"source","new_path":"dst/new"}),
        )
        .await;
        assert_eq!(value["outcome"], "failed");
        assert_eq!(value["error"]["category"], "unknown");
        assert_eq!(value["path"], "source");
        assert_eq!(value["new_path"], "dst/new");
        let expected = if partial {
            vec!["error", "new_path", "outcome", "partial", "path"]
        } else {
            vec!["error", "new_path", "outcome", "path"]
        };
        assert_eq!(keys(&value), expected);
        if partial {
            assert_eq!(value["partial"], true);
        }
        assert!(!value.to_string().contains("private panic payload"));
        assert_eq!(temp.path().join("dst/new").exists(), partial);
        assert_eq!(
            std::fs::read(temp.path().join("source")).unwrap(),
            b"original"
        );
        let next = Arc::new(Options {
            deny: crabber_tools_core::PathDenyPolicy::none(),
            root: o.root.clone(),
            capacity: o.capacity.clone(),
            limits: o.limits.clone(),
            pre_write: None,
        });
        assert_eq!(
            call(
                fileops::write::definition(next.clone()),
                &next,
                json!({"path":"following","content":"ok"})
            )
            .await["outcome"],
            "succeeded"
        );
    }
}

use support::context;

use support::ExecuteOwned as _;
