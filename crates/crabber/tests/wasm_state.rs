#![cfg(feature = "wasm")]
use crabber::{
    Agent, AgentConfig, FakeProvider, Selection, StreamDelta,
    session::{MemoryStore, Store},
    wasm::{InstanceMode, Limits, ModuleConfig},
};
use sha2::{Digest, Sha256};
use std::{path::PathBuf, sync::Arc};

#[tokio::test]
async fn counter_state_survives_runs_and_is_bounded() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../fixtures/wasm/target/wasm32-wasip2/release")
        .canonicalize()
        .expect("cargo xtask build-fixtures");
    let path = root.join("counter_sink.wasm");
    let hash = Sha256::digest(std::fs::read(&path).unwrap()).into();
    let provider = FakeProvider::scripted(vec![
        vec![
            StreamDelta::TextDelta("x".into()),
            StreamDelta::Completed
        ];
        220
    ]);
    let store = Arc::new(MemoryStore::new());
    let agent = Agent::builder()
        .store(store.clone())
        .provider(Arc::new(provider))
        .config(AgentConfig::new(Selection {
            provider_id: "fake".into(),
            model_id: "scripted".into(),
        }))
        .wasm_extension(ModuleConfig {
            name: "counter-sink".into(),
            path,
            allowed_root: root,
            expected_sha256: hash,
            config_json: "{}".into(),
            limits: Limits::default(),
            instance_mode: InstanceMode::PerCall,
        })
        .build()
        .unwrap();
    let first = agent
        .prompt(None, "first")
        .await
        .unwrap()
        .done()
        .await
        .unwrap();
    let session = first.session_id;
    let before = store
        .get_extension_state("counter-sink", &session)
        .await
        .unwrap();
    assert_eq!(before.len(), 1);
    let before_count: u64 = before["count"].parse().unwrap();
    assert!(before_count > 0);
    agent
        .prompt(Some(session.clone()), "second")
        .await
        .unwrap()
        .done()
        .await
        .unwrap();
    let after = store
        .get_extension_state("counter-sink", &session)
        .await
        .unwrap();
    assert_eq!(after.len(), 1);
    assert!(after["count"].parse::<u64>().unwrap() > before_count);
    for _ in 0..210 {
        agent
            .prompt(Some(session.clone()), "more")
            .await
            .unwrap()
            .done()
            .await
            .unwrap();
    }
    let after_many = store
        .get_extension_state("counter-sink", &session)
        .await
        .unwrap();
    assert_eq!(after_many.len(), 1);
    assert!(after_many["count"].parse::<u64>().unwrap() >= 1000);
}
