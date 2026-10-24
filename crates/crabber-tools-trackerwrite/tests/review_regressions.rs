//! Boundary and admission regressions from independent review.
use crabber_tools_core::*;
use crabber_tools_testkit as support;
use crabber_tools_trackerwrite::{MAX_TEXT_BYTES, Options, TrackerPolicy, definition};
use serde_json::{Value, json};
use std::{collections::BTreeSet, sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;

fn options(root: &std::path::Path, hub: &std::path::Path) -> Arc<Options> {
    let binary = hub.join("fake-bn");
    support::script(&binary, "#!/bin/sh\ntouch \"$BEANS_HUB/executed\"\n");
    let limits = Limits {
        max_in_flight: 1,
        max_blocking_wait: Duration::from_millis(30),
    };
    Arc::new(Options {
        root: WorkspaceRoot::open(root).unwrap(),
        capacity: Capacity::new(&limits).unwrap(),
        limits,
        policy: TrackerPolicy {
            bn_binary: binary,
            project: "demo".into(),
            hub: hub.into(),
            actor: "test".into(),
            statuses: BTreeSet::from(["open".into()]),
            environment: vec![("PATH".into(), "/usr/bin:/bin".into())],
        },
    })
}
async fn call(o: &Arc<Options>, value: Value) -> Value {
    definition(o.clone())
        .unwrap()
        .executor
        .execute_owned_context(support::context(&o.root, CancellationToken::new()), value)
        .await
        .unwrap()
        .json_value()
        .unwrap()
}
#[tokio::test]
async fn text_and_failure_envelope_bounds() {
    let root = tempfile::tempdir().unwrap();
    let hub = tempfile::tempdir().unwrap();
    let o = options(root.path(), hub.path());
    for field in ["body", "reason"] {
        let op = if field == "body" { "comment" } else { "close" };
        let mut args = json!({"op":op,"id":"demo-ab12"});
        args[field] = json!("x".repeat(MAX_TEXT_BYTES));
        assert_eq!(call(&o, args.clone()).await["outcome"], "succeeded");
        assert!(hub.path().join("executed").exists());
        std::fs::remove_file(hub.path().join("executed")).unwrap();
        args[field] = json!("x".repeat(MAX_TEXT_BYTES + 1));
        assert_eq!(call(&o, args).await["error"]["category"], "validation");
        assert!(!hub.path().join("executed").exists());
    }
    for (field, len) in [("op", 65), ("id", 257)] {
        let mut args = json!({"op":"close","id":"demo-ab12"});
        args[field] = json!("x".repeat(len));
        let result = call(&o, args).await;
        assert_eq!(result["error"]["category"], "validation");
        assert_eq!(result[field], "");
        assert!(result.to_string().len() < 1024);
        assert!(!hub.path().join("executed").exists());
    }
}
#[tokio::test]
async fn cancelled_and_busy_calls_never_spawn() {
    let root = tempfile::tempdir().unwrap();
    let hub = tempfile::tempdir().unwrap();
    let o = options(root.path(), hub.path());
    let cancel = CancellationToken::new();
    cancel.cancel();
    let result = definition(o.clone())
        .unwrap()
        .executor
        .execute_owned_context(
            support::context(&o.root, cancel),
            json!({"op":"close","id":"demo-ab12"}),
        )
        .await;
    assert!(result.is_err());
    assert!(!hub.path().join("executed").exists());
    let _held = acquire_read(&o.capacity, &o.limits, &CancellationToken::new())
        .await
        .unwrap();
    let result = call(&o, json!({"op":"close","id":"demo-ab12"})).await;
    assert_eq!(result["error"]["category"], "unavailable");
    assert!(!hub.path().join("executed").exists());
}

use support::ExecuteOwned as _;
