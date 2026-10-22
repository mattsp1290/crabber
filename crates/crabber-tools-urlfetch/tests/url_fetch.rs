//! Public Agent integration using only synthetic workspace resources.
use crabber_tools_core::*;
use crabber_tools_testkit as support;
use crabber_tools_urlfetch::{MAX_BODY_BYTES, Options, UrlFetchPolicy, definition};
use serde_json::{Value, json};
use std::{path::Path, sync::Arc};
use tokio_util::sync::CancellationToken;
fn options(root: &Path) -> Arc<Options> {
    let limits = support::limits();
    Arc::new(Options {
        root: WorkspaceRoot::open(root).unwrap(),
        policy: UrlFetchPolicy::default(),
        capacity: Capacity::new(&limits).unwrap(),
        limits,
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
fn file(path: &Path) -> String {
    url::Url::from_file_path(path).unwrap().to_string()
}
#[tokio::test]
async fn confined_file_urls_utf8_caps_and_nonregular_paths() {
    let root = tempfile::tempdir().unwrap();
    let canonical_root = root.path().canonicalize().unwrap();
    let outside = tempfile::tempdir().unwrap();
    let o = options(canonical_root.as_path());
    let name = canonical_root.as_path().join("space # λ.txt");
    std::fs::write(&name, "hello λ\n").unwrap();
    assert_eq!(
        support::execute(
            o.root.path(),
            definition(o.clone()).unwrap(),
            json!({"url":file(&name)})
        )
        .await,
        json!({"outcome":"succeeded","content":"hello λ\n"})
    );
    let link = canonical_root.as_path().join("link");
    std::os::unix::fs::symlink("space # λ.txt", &link).unwrap();
    assert_eq!(
        call(&o, json!({"url":file(&link)})).await["content"],
        "hello λ\n"
    );
    std::fs::write(canonical_root.as_path().join("empty"), "").unwrap();
    assert_eq!(
        call(
            &o,
            json!({"url":file(&canonical_root.as_path().join("empty"))})
        )
        .await["content"],
        ""
    );
    std::fs::write(
        canonical_root.as_path().join("cap"),
        vec![b'x'; MAX_BODY_BYTES],
    )
    .unwrap();
    assert_eq!(
        call(
            &o,
            json!({"url":file(&canonical_root.as_path().join("cap"))})
        )
        .await["content"]
            .as_str()
            .unwrap()
            .len(),
        MAX_BODY_BYTES
    );
    std::fs::write(
        canonical_root.as_path().join("large"),
        vec![b'x'; MAX_BODY_BYTES + 1],
    )
    .unwrap();
    std::fs::write(canonical_root.as_path().join("binary"), [0xff]).unwrap();
    std::fs::write(outside.path().join("private"), "outside secret").unwrap();
    std::os::unix::fs::symlink(outside.path(), canonical_root.as_path().join("escape")).unwrap();
    let fifo = canonical_root.as_path().join("fifo");
    assert!(
        std::process::Command::new("mkfifo")
            .arg(&fifo)
            .status()
            .unwrap()
            .success()
    );
    for (path, category) in [
        (canonical_root.as_path().join("large"), "too_large"),
        (canonical_root.as_path().join("binary"), "binary"),
        (canonical_root.as_path().join("missing"), "not_found"),
        (canonical_root.as_path().to_path_buf(), "io"),
        (fifo, "io"),
        (outside.path().join("private"), "path_escape"),
        (
            canonical_root.as_path().join("escape/private"),
            "path_escape",
        ),
    ] {
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            call(&o, json!({"url":file(&path)})),
        )
        .await
        .unwrap();
        assert_eq!(result["error"]["category"], category, "{path:?}: {result}");
        assert!(!result.to_string().contains("outside secret"));
    }
}
#[tokio::test]
async fn validation_cancellation_and_workspace_routing_fail_closed() {
    let root = tempfile::tempdir().unwrap();
    let canonical_root = root.path().canonicalize().unwrap();
    let other = tempfile::tempdir().unwrap();
    let o = options(canonical_root.as_path());
    for value in [
        json!({}),
        json!({"url":" "}),
        json!({"url":"http://example.com/"}),
        json!({"url":"ftp://example.com/"}),
        json!({"url":"relative"}),
        json!({"url":"https://example.com/","extra":1}),
        json!({"url":"https://127.0.0.1/"}),
        json!({"url":"file://remote/path"}),
        json!({"url":format!("{}%00b",file(&canonical_root.as_path().join("a")))}),
        json!({"url":"x".repeat(crabber_tools_urlfetch::MAX_URL_BYTES+1)}),
    ] {
        assert_eq!(call(&o, value).await["error"]["category"], "validation");
    }
    assert_eq!(
        support::execute(
            other.path(),
            definition(o.clone()).unwrap(),
            json!({"url":file(&canonical_root.as_path().join("missing"))})
        )
        .await["error"]["category"],
        "workspace_mismatch"
    );
    let cancel = CancellationToken::new();
    cancel.cancel();
    assert!(
        definition(o.clone())
            .unwrap()
            .executor
            .execute_owned_context(
                support::context(&o.root, cancel),
                json!({"url":file(&canonical_root.as_path().join("missing"))})
            )
            .await
            .is_err()
    );
}
#[test]
fn upstream_and_documented_metadata_match() {
    let fixture: Value =
        serde_json::from_str(include_str!("../../../fixtures/eino-tools/url_fetch.json")).unwrap();
    let info = crabber_tools_urlfetch::info();
    assert_eq!(
        fixture,
        json!({"name":info.name,"description":info.description,"parameters":info.parameters})
    );
    let section = include_str!("../../../docs/tool-contract.md")
        .split("## url_fetch\n")
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
    assert!(info.retry_safe);
    assert_eq!(
        info.required_permissions,
        ["network.http.fetch", "workspace.fs.read"]
    );
}

use support::ExecuteOwned as _;
