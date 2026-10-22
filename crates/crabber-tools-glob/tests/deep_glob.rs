//! Process-isolated regression for stack-safe deep glob discovery.
use crabber_tools_core::{Capacity, WorkspaceRoot};
use crabber_tools_glob::{Options, definition};
use crabber_tools_testkit as support;
use serde_json::json;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

#[test]
fn deep_glob_does_not_abort_host() {
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "deep_glob_worker", "--nocapture"])
        .env("CRABBER_TOOLS_DEEP_GLOB_WORKER", "1")
        .status()
        .unwrap();
    assert!(status.success(), "deep glob worker failed: {status}");
}

#[test]
fn deep_glob_worker() {
    if std::env::var("CRABBER_TOOLS_DEEP_GLOB_WORKER").as_deref() != Ok("1") {
        return;
    }
    let temp = tempfile::tempdir().unwrap();
    let root = WorkspaceRoot::open(temp.path()).unwrap();
    let mut leaf = std::path::PathBuf::new();
    for _ in 0..1500 {
        leaf.push("d");
        root.dir().create_dir(&leaf).unwrap();
    }
    let limits = support::limits();
    let tool = definition(Arc::new(Options {
        root: root.clone(),
        capacity: Capacity::new(&limits).unwrap(),
        limits,
    }));
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let result = runtime
        .block_on(tool.executor.execute_owned_context(
            support::context(&root, CancellationToken::new()),
            json!({"pattern":"**", "limit":5000}),
        ))
        .unwrap()
        .json_value()
        .unwrap();
    assert_eq!(result["outcome"], "succeeded");
    assert_eq!(result["paths"].as_array().unwrap().len(), 1500);
    assert_eq!(
        result["paths"][1499]["path"],
        leaf.to_string_lossy().as_ref()
    );
    // Remove deepest-first rather than asking a recursive cleanup helper to
    // exercise its own unrelated stack limit.
    while !leaf.as_os_str().is_empty() {
        root.dir().remove_dir(&leaf).unwrap();
        leaf.pop();
    }
}

use support::ExecuteOwned as _;
