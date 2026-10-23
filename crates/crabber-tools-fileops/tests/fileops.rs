//! Tool contracts and capability escape regressions.
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
#[allow(
    clippy::too_many_lines,
    reason = "Keep the public API scenario and its ordered assertions together."
)]
async fn abi_atomic_modes_symlinks_and_edits() {
    use std::os::unix::fs::{PermissionsExt, symlink};
    let temp = tempfile::tempdir().unwrap();
    let o = options(temp.path());
    let write = fileops::write::definition(o.clone());
    let read = fileops::read::definition(o.clone());
    let edit = fileops::edit::definition(o.clone());
    let list = fileops::list::definition(o.clone());
    let result = call(write.clone(), &o, json!({"path":"x","content":"abc"})).await;
    assert_eq!(
        keys(&result),
        ["bytes_written", "created", "outcome", "path"]
    );
    assert_eq!(result["created"], true);
    assert_eq!(
        std::fs::metadata(temp.path().join("x"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o644
    );
    let result = call(read.clone(), &o, json!({"path":"x"})).await;
    assert_eq!(
        keys(&result),
        ["content", "content_bytes", "outcome", "path"]
    );
    let result = call(
        edit.clone(),
        &o,
        json!({"path":"x","anchor":"b","replacement":"B"}),
    )
    .await;
    assert_eq!(
        keys(&result),
        ["anchor_occurrences", "bytes_written", "outcome", "path"]
    );
    assert_eq!(
        call(list, &o, json!({})).await["entries"],
        json!([{"path":"x","is_dir":false}])
    );
    std::fs::set_permissions(
        temp.path().join("x"),
        std::fs::Permissions::from_mode(0o755),
    )
    .unwrap();
    symlink("x", temp.path().join("link")).unwrap();
    assert_eq!(
        call(write.clone(), &o, json!({"path":"link","content":"new"})).await["created"],
        false
    );
    assert!(
        std::fs::symlink_metadata(temp.path().join("link"))
            .unwrap()
            .file_type()
            .is_symlink()
    );
    assert_eq!(
        std::fs::read_to_string(temp.path().join("x")).unwrap(),
        "new"
    );
    assert_eq!(
        std::fs::metadata(temp.path().join("x"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o755
    );
    assert_eq!(
        call(
            edit.clone(),
            &o,
            json!({"path":"x","anchor":"absent","replacement":""})
        )
        .await["anchor_occurrences"],
        0
    );
    call(write.clone(), &o, json!({"path":"x","content":"a a"})).await;
    assert_eq!(
        call(edit, &o, json!({"path":"x","anchor":"a","replacement":""})).await["anchor_occurrences"],
        2
    );
    assert_eq!(
        call(write.clone(), &o, json!({"path":"parent/x","content":""})).await["error"]["category"],
        "not_found"
    );
    assert_eq!(
        call(
            write,
            &o,
            json!({"path":"parent/x","content":"","create_dirs":true})
        )
        .await["outcome"],
        "succeeded"
    );
    let empty = call(read, &o, json!({"path":"parent/x"})).await;
    assert!(empty.get("content").is_none());
    assert!(std::fs::read_dir(temp.path()).unwrap().all(|e| {
        !e.unwrap()
            .file_name()
            .to_string_lossy()
            .starts_with(fileops::TEMP_PREFIX)
    }));
}
#[tokio::test]
async fn shared_path_security_matrix_and_canary() {
    use std::os::unix::fs::symlink;
    let temp = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    let o = options(temp.path());
    std::fs::write(outside.path().join("canary"), "untouched").unwrap();
    std::fs::write(temp.path().join("real"), "hello").unwrap();
    symlink(outside.path().join("canary"), temp.path().join("escape")).unwrap();
    symlink(outside.path(), temp.path().join("escape_dir")).unwrap();
    symlink(temp.path().join("real"), temp.path().join("absolute")).unwrap();
    let constructors = [
        fileops::read::definition,
        fileops::write::definition,
        fileops::edit::definition,
        fileops::list::definition,
    ];
    for (i, ctor) in constructors.iter().enumerate() {
        for path in [
            "../x",
            "/etc/passwd",
            "a/../../x",
            "bad\0path",
            "escape",
            "escape_dir/canary",
            "absolute",
        ] {
            let mut args = json!({"path":path});
            if i == 1 {
                args["content"] = json!("changed");
            }
            if i == 2 {
                args["anchor"] = json!("untouched");
                args["replacement"] = json!("changed");
            }
            let result = call(ctor(o.clone()), &o, args).await;
            let cat = result["error"]["category"].as_str().unwrap();
            assert!(
                ["path_escape", "validation"].contains(&cat),
                "{i} {path}: {result}"
            );
        }
    }
    assert_eq!(
        std::fs::read_to_string(outside.path().join("canary")).unwrap(),
        "untouched"
    );
}
#[tokio::test]
async fn read_windows_binary_caps_and_fifo() {
    let temp = tempfile::tempdir().unwrap();
    let o = options(temp.path());
    let read = fileops::read::definition(o.clone());
    std::fs::write(temp.path().join("x"), "one\ntwo\nthree").unwrap();
    let v = call(read.clone(), &o, json!({"path":"x","offset":2,"limit":1})).await;
    assert_eq!(v["content"], "two\n");
    assert_eq!(v["numbered_content"], "2: two\n");
    assert_eq!(v["total_lines"], 3);
    assert_eq!(v["next_offset"], 3);
    assert_eq!(
        call(read.clone(), &o, json!({"path":"x","offset":4})).await["error"]["message"],
        "offset 4 is past EOF; file has 3 total lines"
    );
    assert_eq!(
        call(read.clone(), &o, json!({"path":"x","limit":5001})).await["error"]["category"],
        "validation"
    );
    std::fs::write(temp.path().join("x"), "é".repeat(fileops::MAX_OUTPUT_BYTES)).unwrap();
    let v = call(read.clone(), &o, json!({"path":"x"})).await;
    assert_eq!(
        v["content"].as_str().unwrap().len(),
        fileops::MAX_OUTPUT_BYTES
    );
    assert_eq!(v["truncated"], true);
    let v = call(read.clone(), &o, json!({"path":"x","limit":1})).await;
    assert_eq!(v["line_truncated"], true);
    std::fs::write(temp.path().join("binary"), [0xff, 0]).unwrap();
    assert_eq!(
        call(read.clone(), &o, json!({"path":"binary"})).await["error"]["category"],
        "binary"
    );
    assert!(
        std::process::Command::new("mkfifo")
            .arg(temp.path().join("fifo"))
            .status()
            .unwrap()
            .success()
    );
    let v = tokio::time::timeout(
        Duration::from_secs(1),
        call(read.clone(), &o, json!({"path":"fifo"})),
    )
    .await
    .unwrap();
    assert_eq!(v["error"]["category"], "validation");
    assert_eq!(
        call(read, &o, json!({"path":"x"})).await["outcome"],
        "succeeded"
    );
}
#[tokio::test]
async fn recursive_order_limits_and_no_symlink_descent() {
    let temp = tempfile::tempdir().unwrap();
    let o = options(temp.path());
    let list = fileops::list::definition(o.clone());
    std::fs::create_dir_all(temp.path().join("a")).unwrap();
    std::fs::write(temp.path().join("a/b"), "").unwrap();
    std::fs::write(temp.path().join("a.txt"), "").unwrap();
    let v = call(list.clone(), &o, json!({"recursive":true})).await;
    let paths: Vec<_> = v["entries"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["path"].as_str().unwrap())
        .collect();
    assert_eq!(paths, ["a", "a/b", "a.txt"]);
    assert_eq!(
        call(list.clone(), &o, json!({"path":"a"})).await["entries"][0]["path"],
        "a/b"
    );
    std::os::unix::fs::symlink("a", temp.path().join("zlink")).unwrap();
    assert_eq!(
        call(list.clone(), &o, json!({"recursive":true})).await["entries"]
            .as_array()
            .unwrap()
            .len(),
        4
    );
    for i in 0..6000 {
        std::fs::write(temp.path().join(format!("f{i:04}")), "").unwrap();
    }
    let v = call(list, &o, json!({"recursive":true})).await;
    assert_eq!(v["entries"].as_array().unwrap().len(), 5000);
    assert_eq!(v["truncated"], true);
}
#[tokio::test]
async fn cancelled_and_context_free_calls_have_no_effect() {
    let temp = tempfile::tempdir().unwrap();
    let o = options(temp.path());
    let write = fileops::write::definition(o.clone());
    let cancel = CancellationToken::new();
    cancel.cancel();
    assert!(
        write
            .executor
            .execute_owned_context(
                context(&o.root, cancel),
                json!({"path":"x","content":"bad"})
            )
            .await
            .is_err()
    );
    assert_eq!(
        write
            .executor
            .execute(json!({"path":"x","content":"bad"}))
            .await
            .unwrap()
            .json_value()
            .unwrap()["error"]["category"],
        "validation"
    );
    assert!(!temp.path().join("x").exists());
}

#[tokio::test]
async fn input_caps_ownership_and_unreadable_directory() {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};
    let tmp = tempfile::tempdir().unwrap();
    let o = options(tmp.path());
    let v = call(
        fileops::write::definition(o.clone()),
        &o,
        json!({"path":"big","content":"x".repeat(fileops::MAX_OUTPUT_BYTES+1)}),
    )
    .await;
    assert_eq!(v["error"]["category"], "too_large");
    assert!(!tmp.path().join("big").exists());
    std::fs::write(tmp.path().join("x"), "anchor").unwrap();
    let uid = std::fs::metadata(tmp.path().join("x")).unwrap().uid();
    if uid == 0 && std::env::var("CRABBER_TOOLS_TEST_AS_ROOT").as_deref() == Ok("1") {
        assert!(
            std::process::Command::new("chown")
                .args(["65534:65534", tmp.path().join("x").to_str().unwrap()])
                .status()
                .unwrap()
                .success()
        );
        call(
            fileops::write::definition(o.clone()),
            &o,
            json!({"path":"x","content":"preserved"}),
        )
        .await;
        assert_eq!(
            std::fs::metadata(tmp.path().join("x")).unwrap().uid(),
            65534
        );
        assert!(
            std::process::Command::new("chown")
                .args(["65534:65534", tmp.path().to_str().unwrap()])
                .status()
                .unwrap()
                .success()
        );
        call(
            fileops::write::definition(o.clone()),
            &o,
            json!({"path":"new","content":"created"}),
        )
        .await;
        assert_eq!(
            std::fs::metadata(tmp.path().join("new")).unwrap().uid(),
            65534
        );
    } else {
        call(
            fileops::write::definition(o.clone()),
            &o,
            json!({"path":"new","content":"created"}),
        )
        .await;
        assert_eq!(
            std::fs::metadata(tmp.path().join("new")).unwrap().uid(),
            uid
        );
    }
    std::fs::create_dir(tmp.path().join("unreadable")).unwrap();
    std::fs::set_permissions(
        tmp.path().join("unreadable"),
        std::fs::Permissions::from_mode(0o0),
    )
    .unwrap();
    let v = call(
        fileops::list::definition(o),
        &options(tmp.path()),
        json!({"recursive":true}),
    )
    .await;
    std::fs::set_permissions(
        tmp.path().join("unreadable"),
        std::fs::Permissions::from_mode(0o755),
    )
    .unwrap();
    if uid != 0 {
        assert_eq!(v["error"]["category"], "io");
    }
}

use crabber_tools_testkit as support;

use support::context;

use support::ExecuteOwned as _;
