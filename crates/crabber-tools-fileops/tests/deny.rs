//! Host exclusions cover every filesystem tool, including the writing tier.
use crabber_tools_core::{Capacity, PathDenyPolicy, WorkspaceRoot};
use crabber_tools_fileops::{Options, edit, file_delete, file_move, list, multi_edit, read, write};
use crabber_tools_testkit::{execute, limits};
use serde_json::json;
use std::{os::unix::fs::symlink, sync::Arc};

#[tokio::test]
async fn every_file_tool_denies_direct_and_aliased_paths_before_effects() {
    let temp = tempfile::tempdir().unwrap();
    std::fs::create_dir(temp.path().join("secrets")).unwrap();
    std::fs::write(temp.path().join(".env"), "private fixture").unwrap();
    std::fs::write(temp.path().join("secrets/x"), "private fixture").unwrap();
    std::fs::write(temp.path().join("ok"), "public fixture").unwrap();
    symlink(".env", temp.path().join("notes")).unwrap();
    symlink("secrets", temp.path().join("d")).unwrap();
    let bounds = limits();
    let options = Arc::new(Options {
        deny: PathDenyPolicy::new([".env", "secrets"].map(String::from)).unwrap(),
        root: WorkspaceRoot::open(temp.path()).unwrap(),
        capacity: Capacity::new(&bounds).unwrap(),
        limits: bounds,
        pre_write: None,
    });
    for path in [".env", "notes", "secrets/x", "d/x"] {
        for (tool, args) in [
            (read::definition(options.clone()), json!({"path":path})),
            (list::definition(options.clone()), json!({"path":path})),
            (
                write::definition(options.clone()),
                json!({"path":path,"content":"changed"}),
            ),
            (
                edit::definition(options.clone()),
                json!({"path":path,"anchor":"fixture","replacement":"changed"}),
            ),
            (
                multi_edit::definition(options.clone()),
                json!({"path":path,"edits":[{"anchor":"fixture","replacement":"changed"}]}),
            ),
            (
                file_delete::definition(options.clone()),
                json!({"path":path}),
            ),
            (
                file_move::definition(options.clone()),
                json!({"path":path,"new_path":"moved"}),
            ),
            (
                file_move::definition(options.clone()),
                json!({"path":"ok","new_path":path}),
            ),
        ] {
            let result = execute(temp.path(), tool, args).await;
            assert_eq!(result["error"]["category"], "path_denied", "{result}");
            assert_eq!(result["error"]["message"], "path denied by host policy");
        }
    }
    for path in [".env", "secrets/x"] {
        assert_eq!(
            std::fs::read_to_string(temp.path().join(path)).unwrap(),
            "private fixture"
        );
    }
    assert_eq!(
        std::fs::read_to_string(temp.path().join("ok")).unwrap(),
        "public fixture"
    );
    assert!(!temp.path().join("moved").exists());
    assert!(!std::fs::read_dir(temp.path()).unwrap().any(|entry| {
        entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .starts_with(crabber_tools_fileops::TEMP_PREFIX)
    }));
    let visible = execute(
        temp.path(),
        list::definition(options),
        json!({"recursive":true}),
    )
    .await;
    assert_eq!(visible["entries"], json!([{"path":"ok","is_dir":false}]));
    assert!(visible.get("truncated").is_none());
}

#[tokio::test]
async fn intermediate_route_denies_every_file_tool_before_effects() {
    for target in ["route", "route/sub/.."] {
        let temp = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(temp.path().join("real/sub")).unwrap();
        std::fs::write(temp.path().join("real/file"), "private fixture").unwrap();
        std::fs::write(temp.path().join("ok"), "public fixture").unwrap();
        symlink("real", temp.path().join("route")).unwrap();
        symlink(target, temp.path().join("alias")).unwrap();
        let bounds = limits();
        let options = Arc::new(Options {
            deny: PathDenyPolicy::new(["route/file".to_owned()]).unwrap(),
            root: WorkspaceRoot::open(temp.path()).unwrap(),
            capacity: Capacity::new(&bounds).unwrap(),
            limits: bounds,
            pre_write: None,
        });
        for (tool, args) in [
            (
                read::definition(options.clone()),
                json!({"path":"alias/file"}),
            ),
            (
                list::definition(options.clone()),
                json!({"path":"alias/file"}),
            ),
            (
                write::definition(options.clone()),
                json!({"path":"alias/file","content":"changed"}),
            ),
            (
                edit::definition(options.clone()),
                json!({"path":"alias/file","anchor":"fixture","replacement":"changed"}),
            ),
            (
                multi_edit::definition(options.clone()),
                json!({"path":"alias/file","edits":[{"anchor":"fixture","replacement":"changed"}]}),
            ),
            (
                file_delete::definition(options.clone()),
                json!({"path":"alias/file"}),
            ),
            (
                file_move::definition(options.clone()),
                json!({"path":"alias/file","new_path":"moved"}),
            ),
            (
                file_move::definition(options),
                json!({"path":"ok","new_path":"alias/file"}),
            ),
        ] {
            let result = execute(temp.path(), tool, args).await;
            assert_eq!(result["error"]["category"], "path_denied", "{result}");
        }
        assert_eq!(
            std::fs::read_to_string(temp.path().join("real/file")).unwrap(),
            "private fixture"
        );
        assert_eq!(
            std::fs::read_to_string(temp.path().join("ok")).unwrap(),
            "public fixture"
        );
        assert!(!temp.path().join("moved").exists());
    }
}

#[tokio::test]
async fn nonempty_policy_keeps_allowed_alias_write_edit_move_and_delete_working() {
    let temp = tempfile::tempdir().unwrap();
    std::fs::create_dir(temp.path().join("real")).unwrap();
    std::fs::create_dir(temp.path().join("links")).unwrap();
    symlink("../real", temp.path().join("links/up")).unwrap();
    symlink("links/up", temp.path().join("alias")).unwrap();
    let bounds = limits();
    let options = Arc::new(Options {
        deny: PathDenyPolicy::new(["unrelated".to_owned()]).unwrap(),
        root: WorkspaceRoot::open(temp.path()).unwrap(),
        capacity: Capacity::new(&bounds).unwrap(),
        limits: bounds,
        pre_write: None,
    });
    for (tool, args) in [
        (
            write::definition(options.clone()),
            json!({"path":"alias/new/file","content":"first","create_dirs":true}),
        ),
        (
            edit::definition(options.clone()),
            json!({"path":"alias/new/file","anchor":"first","replacement":"second"}),
        ),
        (
            multi_edit::definition(options.clone()),
            json!({"path":"alias/new/file","edits":[{"anchor":"second","replacement":"third"}]}),
        ),
        (
            file_move::definition(options.clone()),
            json!({"path":"alias/new/file","new_path":"links/up/moved/file","create_dirs":true}),
        ),
    ] {
        let result = execute(temp.path(), tool, args).await;
        assert_eq!(result["outcome"], "succeeded", "{result}");
    }
    assert!(!temp.path().join("real/new/file").exists());
    assert_eq!(
        std::fs::read_to_string(temp.path().join("real/moved/file")).unwrap(),
        "third"
    );
    let result = execute(
        temp.path(),
        file_delete::definition(options),
        json!({"path":"alias/moved/file"}),
    )
    .await;
    assert_eq!(result["outcome"], "succeeded", "{result}");
    assert!(!temp.path().join("real/moved/file").exists());
}
