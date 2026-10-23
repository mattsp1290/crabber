//! Process-isolated regression for stack-safe deep workspace listings.
use crabber_tools_core::{Capacity, WorkspaceRoot};
use crabber_tools_fileops::{Options, list};
use crabber_tools_testkit as support;
use serde_json::json;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

#[test]
fn deep_listing_does_not_abort_host() {
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "deep_listing_worker", "--nocapture"])
        .env("CRABBER_TOOLS_DEEP_LIST_WORKER", "1")
        .status()
        .unwrap();
    assert!(status.success(), "deep listing worker failed: {status}");
}

#[test]
fn deep_listing_worker() {
    if std::env::var("CRABBER_TOOLS_DEEP_LIST_WORKER").as_deref() != Ok("1") {
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
    let tool = list::definition(Arc::new(Options {
        deny: crabber_tools_core::PathDenyPolicy::none(),
        root: root.clone(),
        capacity: Capacity::new(&limits).unwrap(),
        limits,
        pre_write: None,
    }));
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let result = runtime
        .block_on(tool.executor.execute_owned_context(
            support::context(&root, CancellationToken::new()),
            json!({"recursive":true}),
        ))
        .unwrap()
        .json_value()
        .unwrap();
    assert_eq!(result["outcome"], "succeeded");
    assert_eq!(result["entries"].as_array().unwrap().len(), 1500);
    assert_eq!(
        result["entries"][1499]["path"],
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
