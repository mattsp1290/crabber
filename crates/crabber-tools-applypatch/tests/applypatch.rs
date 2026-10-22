//! Pinned Go corpus and public runtime safety/ABI regressions.
use crabber_tools_applypatch::{Options, definition};
use crabber_tools_core::*;
use crabber_tools_testkit as support;
use serde::Deserialize;
use serde_json::{Value, json};
use std::{collections::BTreeMap, sync::Arc};
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
async fn call(o: &Arc<Options>, patch: &str) -> Value {
    definition(o.clone())
        .executor
        .execute_owned_context(
            context(&o.root, CancellationToken::new()),
            json!({"patch_text":patch}),
        )
        .await
        .unwrap()
        .json_value()
        .unwrap()
}
#[derive(Deserialize)]
struct Case {
    name: String,
    patch_text: String,
    files: BTreeMap<String, String>,
    symlinks: BTreeMap<String, String>,
    category: Option<String>,
}
#[tokio::test]
async fn pinned_go_corpus_through_agent() {
    use std::os::unix::fs::symlink;
    let cases: Vec<Case> = serde_json::from_str(include_str!("fixtures/go-cases.json")).unwrap();
    assert_eq!(cases.len(), 12);
    for case in cases {
        let temp = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        std::fs::write(outside.path().join("outside.txt"), "outside\n").unwrap();
        for (path, content) in &case.files {
            let path = temp.path().join(path);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(path, content).unwrap();
        }
        for (path, target) in &case.symlinks {
            symlink(
                target.replace("$OUTSIDE", outside.path().to_str().unwrap()),
                temp.path().join(path),
            )
            .unwrap();
        }
        let o = options(temp.path());
        let v = execute(
            temp.path(),
            definition(o),
            json!({"patch_text":case.patch_text}),
        )
        .await;
        if let Some(category) = case.category {
            assert_eq!(v["outcome"], "failed", "{}: {v}", case.name);
            assert_eq!(v["error"]["category"], category, "{}: {v}", case.name);
            assert_eq!(v["partial"], false, "{}: {v}", case.name);
            for (path, content) in &case.files {
                assert_eq!(
                    std::fs::read_to_string(temp.path().join(path)).unwrap(),
                    *content
                );
            }
            assert!(!temp.path().join("created.txt").exists());
            assert!(!temp.path().join("foo").exists());
        } else {
            assert_eq!(v["outcome"], "succeeded", "{}: {v}", case.name);
            assert_eq!(v["partial"], false);
            assert!(
                v["files"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .all(|f| f["status"] == "applied")
            );
            if case.name.starts_with("TestRunApplies") {
                assert_eq!(
                    std::fs::read_to_string(temp.path().join("new/added.txt")).unwrap(),
                    "first\nsecond\n"
                );
                assert_eq!(
                    std::fs::read_to_string(temp.path().join("old.txt")).unwrap(),
                    "hello\nnew\nbye\n"
                );
                assert_eq!(
                    std::fs::read_to_string(temp.path().join("moved.txt")).unwrap(),
                    "renamed\n"
                );
                assert!(!temp.path().join("move.txt").exists());
                assert!(!temp.path().join("delete.txt").exists());
            } else {
                assert!(!temp.path().join("bin.dat").exists());
            }
        }
        assert_eq!(
            std::fs::read_to_string(outside.path().join("outside.txt")).unwrap(),
            "outside\n"
        );
    }
}
#[tokio::test]
async fn grammar_hunks_line_endings_and_modes() {
    use std::os::unix::fs::PermissionsExt;
    let temp = tempfile::tempdir().unwrap();
    let o = options(temp.path());
    for patch in [
        "",
        "*** Begin Patch\n*** End Patch",
        "*** Begin Patch\n*** Add File: x\n*** End Patch",
        "*** Begin Patch\n*** Add File: x\nno prefix\n*** End Patch",
        "*** Begin Patch\n*** Update File: x\n@@\n+only addition\n*** End Patch",
        "*** Begin Patch\n*** Update File: x\n@@\n\n*** End Patch",
        "*** Begin Patch\n*** Update File: x\n*** End Patch",
        "*** Begin Patch\n*** Other File: x\n*** End Patch",
        "*** Begin Patch\n*** Add File: \n+x\n*** End Patch",
    ] {
        assert_eq!(
            call(&o, patch).await["error"]["category"],
            "validation",
            "{patch}"
        );
    }
    let patch =
        "*** Begin Patch\n*** Update File: x\n@@ header ignored\n old\n-a\n+b\n*** End Patch";
    std::fs::write(temp.path().join("x"), "old\r\na\r\n").unwrap();
    std::fs::set_permissions(
        temp.path().join("x"),
        std::fs::Permissions::from_mode(0o640),
    )
    .unwrap();
    let v = call(&o, patch).await;
    assert_eq!(v["outcome"], "succeeded");
    assert_eq!(
        std::fs::read_to_string(temp.path().join("x")).unwrap(),
        "old\r\nb\r\n"
    );
    assert_eq!(
        std::fs::metadata(temp.path().join("x"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o640
    );
    std::fs::write(temp.path().join("x"), "a\na\na\n").unwrap();
    let ambiguous = "*** Begin Patch\n*** Update File: x\n@@\n-a\n-a\n+b\n*** End Patch";
    assert_eq!(call(&o, ambiguous).await["error"]["category"], "conflict");
    assert_eq!(
        std::fs::read_to_string(temp.path().join("x")).unwrap(),
        "a\na\na\n"
    );
    let moved = call(
        &o,
        "*** Begin Patch\n*** Update File: x\n*** Move to: pure/move\n*** End Patch",
    )
    .await;
    assert_eq!(moved["outcome"], "succeeded");
    assert!(!temp.path().join("x").exists());
    assert_eq!(
        std::fs::read_to_string(temp.path().join("pure/move")).unwrap(),
        "a\na\na\n"
    );
    let added = call(
        &o,
        "*** Begin Patch\r\n*** Add File: added\r\n+text\r\n*** End Patch\r\n",
    )
    .await;
    assert_eq!(added["outcome"], "succeeded");
    assert_eq!(
        std::fs::read_to_string(temp.path().join("added")).unwrap(),
        "text\n"
    );
    assert_eq!(
        std::fs::metadata(temp.path().join("added"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o644
    );
}
#[tokio::test]
async fn validation_binary_and_alias_overlap_fail_before_writes() {
    use std::os::unix::fs::symlink;
    let temp = tempfile::tempdir().unwrap();
    let other = tempfile::tempdir().unwrap();
    let o = options(temp.path());
    std::fs::create_dir(temp.path().join("real")).unwrap();
    symlink("real", temp.path().join("alias")).unwrap();
    let v = call(
        &o,
        "*** Begin Patch\n*** Add File: real/x\n+one\n*** Add File: alias/x\n+two\n*** End Patch",
    )
    .await;
    assert_eq!(v["error"]["category"], "validation");
    assert!(!temp.path().join("real/x").exists());
    std::fs::write(temp.path().join("binary"), b"\xff\0").unwrap();
    assert_eq!(
        call(
            &o,
            "*** Begin Patch\n*** Update File: binary\n@@\n-x\n+y\n*** End Patch"
        )
        .await["error"]["category"],
        "binary"
    );
    for path in ["../escape", "/escape", "bad\0name"] {
        let patch = format!("*** Begin Patch\n*** Add File: {path}\n+x\n*** End Patch");
        let v = call(&o, &patch).await;
        assert_eq!(v["outcome"], "failed");
        assert_eq!(v["partial"], false);
    }
    let big = "x".repeat(crabber_tools_applypatch::MAX_PATCH_BYTES + 1);
    assert_eq!(call(&o, &big).await["error"]["category"], "too_large");
    let d = definition(o.clone());
    for args in [
        json!({}),
        json!({"patch_text":1}),
        json!({"patch_text":"x","other":1}),
    ] {
        assert_eq!(
            d.executor
                .execute_owned_context(context(&o.root, CancellationToken::new()), args)
                .await
                .unwrap()
                .json_value()
                .unwrap()["error"]["category"],
            "validation"
        );
    }
    let v = execute(
        other.path(),
        d,
        json!({"patch_text":"*** Begin Patch\n*** Add File: x\n+x\n*** End Patch"}),
    )
    .await;
    assert_eq!(v["error"]["category"], "workspace_mismatch");
}

#[tokio::test]
async fn source_and_aggregate_content_caps_prevent_writes() {
    let temp = tempfile::tempdir().unwrap();
    let o = options(temp.path());
    let too_large = vec![b'x'; crabber_tools_applypatch::MAX_FILE_BYTES + 1];
    std::fs::write(temp.path().join("large"), &too_large).unwrap();
    let patch = "*** Begin Patch\n*** Update File: large\n*** Move to: new\n*** End Patch";
    assert_eq!(call(&o, patch).await["error"]["category"], "too_large");
    assert!(!temp.path().join("new").exists());
    drop(too_large);
    let contents = vec![b'x'; 13 * 1024 * 1024];
    let mut patch = "*** Begin Patch\n".to_owned();
    for i in 0..5 {
        std::fs::write(temp.path().join(format!("old{i}")), &contents).unwrap();
        patch.push_str(&format!("*** Update File: old{i}\n*** Move to: new{i}\n"));
    }
    patch.push_str("*** End Patch");
    let v = call(&o, &patch).await;
    assert_eq!(v["error"]["category"], "too_large");
    assert_eq!(v["partial"], false);
    for i in 0..5 {
        assert!(temp.path().join(format!("old{i}")).exists());
        assert!(!temp.path().join(format!("new{i}")).exists());
    }
}
#[tokio::test]
async fn interruption_and_permission_denial_prevent_effects() {
    use crabber::{
        Agent, FakeProvider, PermissionDecision, StaticPolicy, core::ToolCallStatus,
        session::MemoryStore,
    };
    let temp = tempfile::tempdir().unwrap();
    let o = options(temp.path());
    let patch = "*** Begin Patch\n*** Add File: never\n+content\n*** End Patch";
    let cancel = CancellationToken::new();
    cancel.cancel();
    assert!(
        definition(o.clone())
            .executor
            .execute_owned_context(context(&o.root, cancel), json!({"patch_text":patch}))
            .await
            .is_err()
    );
    let held = acquire_read(&o.capacity, &o.limits, &CancellationToken::new())
        .await
        .unwrap();
    let store = Arc::new(MemoryStore::new());
    let agent = Agent::builder()
        .behavior_version(concat!("crabber-tools/", env!("CARGO_PKG_VERSION")))
        .store(store.clone())
        .provider(Arc::new(FakeProvider::scripted(vec![
            turn("apply_patch", json!({"patch_text":patch})),
            done(),
        ])))
        .config(config(temp.path()))
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .shared_tool(definition(o.clone()))
        .build()
        .unwrap();
    let run = agent.prompt(None, "patch").await.unwrap();
    let session = run.session_id().clone();
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
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
    assert!(!temp.path().join("never").exists());
    drop(held);
    agent.close_extensions().await.unwrap();
    let agent = Agent::builder()
        .behavior_version(concat!("crabber-tools/", env!("CARGO_PKG_VERSION")))
        .provider(Arc::new(FakeProvider::scripted(vec![
            turn("apply_patch", json!({"patch_text":patch})),
            done(),
        ])))
        .config(config(temp.path()))
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Deny)))
        .shared_tool(definition(o))
        .build()
        .unwrap();
    agent
        .prompt(None, "denied patch")
        .await
        .unwrap()
        .done()
        .await
        .unwrap();
    assert!(!temp.path().join("never").exists());
    agent.close_extensions().await.unwrap();
}
#[test]
fn upstream_metadata_and_documented_schema() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../fixtures/eino-tools/apply_patch.json"
    ))
    .unwrap();
    let info = crabber_tools_applypatch::info();
    assert_eq!(
        fixture,
        json!({"name":info.name,"description":info.description,"parameters":info.parameters})
    );
    let section = include_str!("../../../docs/tool-contract.md")
        .split("## apply_patch\n")
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
}

#[tokio::test]
async fn repetitive_near_match_is_preflighted_without_effects() {
    let root = tempfile::tempdir().unwrap();
    let content = "a\n".repeat(4 * 1024 * 1024);
    std::fs::write(root.path().join("large"), &content).unwrap();
    let patch = "*** Begin Patch\n*** Update File: large\n@@\n".to_owned()
        + &" a\n".repeat(262144)
        + " b\n+c\n*** End Patch\n";
    let limits = support::limits();
    let options = std::sync::Arc::new(crabber_tools_applypatch::Options {
        root: crabber_tools_core::WorkspaceRoot::open(root.path()).unwrap(),
        capacity: crabber_tools_core::Capacity::new(&limits).unwrap(),
        limits,
    });
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        crabber_tools_applypatch::definition(options.clone())
            .executor
            .execute_owned_context(
                support::context(&options.root, tokio_util::sync::CancellationToken::new()),
                serde_json::json!({"patch_text":patch}),
            ),
    )
    .await
    .unwrap()
    .unwrap()
    .json_value()
    .unwrap();
    assert_eq!(result["error"]["category"], "conflict");
    assert_eq!(result["partial"], false);
    assert_eq!(
        std::fs::read_to_string(root.path().join("large")).unwrap(),
        content
    );
}
