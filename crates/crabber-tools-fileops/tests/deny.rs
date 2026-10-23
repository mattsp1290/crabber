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
