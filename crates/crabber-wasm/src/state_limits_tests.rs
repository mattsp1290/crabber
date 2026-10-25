//! Exercise real state imports at default bounds without hundreds of model turns.
use super::{
    LoadedModule, Loader, Val,
    tests::{TestSink, fixture},
};
use crabber_extension::StateSink;
use std::{collections::BTreeMap, sync::Arc};
use tokio::sync::Mutex;

fn sink() -> Arc<TestSink> {
    Arc::new(TestSink {
        session: crabber_core::SessionId::new(),
        state: Mutex::new(BTreeMap::new()),
    })
}

async fn emit(
    module: &LoadedModule,
    sink: &Arc<TestSink>,
    kind: &str,
    key: &str,
    value: &str,
) -> Val {
    let event = Val::Record(vec![
        ("kind".into(), Val::String(kind.into())),
        ("session-id".into(), Val::String(String::new())),
        ("run-id".into(), Val::String(key.into())),
        ("turn-id".into(), Val::String(String::new())),
        ("message-id".into(), Val::String(String::new())),
        ("tool-call-id".into(), Val::String(String::new())),
        ("epoch-id".into(), Val::String(String::new())),
        ("timestamp-unix-millis".into(), Val::S64(0)),
        ("payload-summary".into(), Val::String(value.into())),
    ]);
    let state: Arc<dyn StateSink> = sink.clone();
    crabber_extension::with_state_sink(state, module.call("event-sink-api", "emit", &[event]))
        .await
        .unwrap()
}

#[tokio::test]
async fn state_import_enforces_default_entry_and_value_limits() {
    let config = fixture("counter-sink");
    let max_entries = config.limits.max_state_entries;
    let max_bytes = config.limits.max_state_value_bytes;
    assert_eq!(max_entries, 64);
    assert_eq!(max_bytes, 16 << 10);
    let loader = Loader::new().unwrap();
    let module = loader.load(config).await.unwrap();
    let state = sink();
    for n in 0..max_entries {
        let result = emit(
            &module,
            &state,
            "test-state-write",
            &format!("entry-{n}"),
            "x",
        )
        .await;
        assert!(matches!(result, Val::Result(Ok(None))));
        assert_eq!(state.state.lock().await.len(), n + 1);
    }
    let before = state.state.lock().await.clone();
    let rejected = emit(&module, &state, "test-state-write", "one-too-many", "x").await;
    assert_state_limit(rejected);
    assert_eq!(*state.state.lock().await, before);

    // Updating an existing entry is allowed at the exact default byte limit.
    let accepted = emit(
        &module,
        &state,
        "test-state-write",
        "entry-0",
        &"x".repeat(max_bytes),
    )
    .await;
    assert!(matches!(accepted, Val::Result(Ok(None))));
    let before = state.state.lock().await.clone();
    let rejected = emit(
        &module,
        &state,
        "test-state-write",
        "entry-0",
        &"x".repeat(max_bytes + 1),
    )
    .await;
    assert_state_limit(rejected);
    assert_eq!(*state.state.lock().await, before);

    // Keys share the same real import byte bound, independently of entry count.
    let state = sink();
    let accepted = emit(
        &module,
        &state,
        "test-state-write",
        &"k".repeat(max_bytes),
        "x",
    )
    .await;
    assert!(matches!(accepted, Val::Result(Ok(None))));
    let before = state.state.lock().await.clone();
    let rejected = emit(
        &module,
        &state,
        "test-state-write",
        &"k".repeat(max_bytes + 1),
        "x",
    )
    .await;
    assert_state_limit(rejected);
    assert_eq!(*state.state.lock().await, before);
}

#[tokio::test]
async fn counter_state_crosses_thousand_between_fresh_instances() {
    let loader = Loader::new().unwrap();
    let module = loader.load(fixture("counter-sink")).await.unwrap();
    let state = sink();
    let initialized = emit(&module, &state, "test-state-write", "count", "999").await;
    assert!(matches!(initialized, Val::Result(Ok(None))));
    let result = emit(&module, &state, "test", "", "").await;
    assert!(matches!(result, Val::Result(Ok(None))));
    let saved = state.state.lock().await;
    assert_eq!(saved.len(), 1);
    assert_eq!(saved.get("count").map(String::as_str), Some("1000"));
}

fn assert_state_limit(value: Val) {
    let Val::Result(Err(Some(error))) = value else {
        panic!("expected structured state refusal");
    };
    let Val::Record(fields) = *error else {
        panic!("expected structured state error");
    };
    assert!(
        fields
            .iter()
            .any(|(key, value)| key == "code"
                && matches!(value, Val::String(code) if code == "state"))
    );
    assert!(fields.iter().any(|(key, value)| key == "message"
        && matches!(value, Val::String(message) if message == "state limit exceeded")));
}
