//! A deeply nested unknown-tool call must leave a decodable record in PostgreSQL (crabber-zv2d).
//!
//! The record wraps the provider arguments in `{"$crabber_unknown_tool":{"raw":..}}`. PostgreSQL
//! writes records without a depth limit but reads them back with `serde_json::from_str`, which
//! stops at 128 levels, so arguments nested 125 to 127 levels deep used to poison the row.
#![cfg(any(feature = "postgres", feature = "sqlite"))]

use crabber::extension::StaticPlanProvider;
use crabber::runtime::{Orchestrator, Request};
#[cfg(feature = "postgres")]
use crabber::session::PostgresStore;
use crabber::session::Store;
use crabber::{FakeProvider, Selection, StreamDelta};
use crabber_core::{RunStatus, ToolCallId};
use std::sync::Arc;

#[cfg(feature = "postgres")]
fn url() -> Option<String> {
    if let Ok(url) = std::env::var("CRABBER_TEST_POSTGRES_URL") {
        return Some(url);
    }
    assert!(
        std::env::var("CRABBER_REQUIRE_POSTGRES").is_err(),
        "required PostgreSQL environment missing"
    );
    eprintln!("PostgreSQL unknown-tool depth test skipped: CRABBER_TEST_POSTGRES_URL unset");
    None
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn deeply_nested_unknown_tool_arguments_settle_and_stay_decodable() {
    let Some(url) = url() else { return };
    PostgresStore::migrate(&url).await.unwrap();
    let store = Arc::new(PostgresStore::connect(&url).await.unwrap());
    unknown_tool_depth(store).await;
}

async fn unknown_tool_depth(store: Arc<dyn Store>) {
    let call_id = ToolCallId::new();
    let arguments = "[".repeat(125) + &"]".repeat(125);
    let script = vec![
        StreamDelta::ToolCallStart {
            call_id: call_id.clone(),
            name: "missing".into(),
        },
        StreamDelta::ToolCallArgsDelta {
            call_id: call_id.clone(),
            text: arguments,
        },
        StreamDelta::ToolCallDone { call_id },
        StreamDelta::Completed,
    ];
    let runtime = Orchestrator::builder()
        .behavior_fingerprint(crabber::core::InputFingerprint::new("b".repeat(64)).unwrap())
        .store(Arc::clone(&store))
        .resolver(Arc::new(FakeProvider::scripted(vec![
            script,
            vec![
                StreamDelta::TextDelta("done".into()),
                StreamDelta::Completed,
            ],
        ])))
        .plan_provider(Arc::new(StaticPlanProvider::new(Vec::new(), Vec::new())))
        .build()
        .unwrap();
    let handle = runtime
        .start(Request {
            input_envelope: None,
            session_id: None,
            workspace_id: "test".into(),
            directory: ".".into(),
            title: "test".into(),
            text: "hello".into(),
            selection: Selection {
                provider_id: "fake".into(),
                model_id: "scripted".into(),
            },
            system_prompt: None,
            max_output_tokens: None,
        })
        .await
        .unwrap();
    let run_id = handle.run_id().clone();
    // The assistant message at this depth is already unreadable (pre-existing), so the run may
    // end in an error; what matters is that it settles and leaves no poisoned call row.
    let _ = handle.done().await;
    let status = store.get_run(&run_id).await.unwrap().unwrap().status;
    assert!(
        !matches!(
            status,
            RunStatus::Pending | RunStatus::Running | RunStatus::Paused
        ),
        "run left non-terminal: {status:?}"
    );
    let unfinished = store
        .list_unfinished_tool_calls(&run_id)
        .await
        .expect("stored tool-call records decode");
    assert!(unfinished.is_empty(), "call left unsettled: {unfinished:?}");
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_deeply_nested_unknown_tool_arguments_settle_and_stay_decodable() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("unknown-tool.sqlite");
    crabber::session::SqliteStore::migrate(&path).await.unwrap();
    unknown_tool_depth(Arc::new(
        crabber::session::SqliteStore::connect(&path).await.unwrap(),
    ))
    .await;
}
