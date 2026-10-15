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
    // The captured fake context limit permits one proactive summary per run.
    // Reserve both its summary and its actual turn for this long history.
    let provider = FakeProvider::scripted(vec![
        vec![
            StreamDelta::TextDelta("x".into()),
            StreamDelta::Completed
        ];
        212 * 2
    ]);
    let store = Arc::new(MemoryStore::new());
    let agent = Agent::builder()
        .behavior_fingerprint(crabber::core::InputFingerprint::new("b".repeat(64)).unwrap())
        .store(store.clone())
        .provider(Arc::new(provider))
        .config(AgentConfig::new(Selection {
            provider_id: "fake".into(),
            model_id: "scripted".into(),
        }))
        .shared_wasm_extension(ModuleConfig {
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

#[tokio::test]
async fn keyed_wasm_replay_checks_configuration_without_reloading_artifact() {
    use crabber::{
        Admission, AdmissionKey, AdmissionOptions, InputFingerprint, RuntimeError, SessionId,
        core::CoreError,
    };
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../fixtures/wasm/target/wasm32-wasip2/release")
        .canonicalize()
        .expect("cargo xtask build-fixtures");
    let path = root.join("counter_sink.wasm");
    let config = ModuleConfig {
        name: "counter-sink".into(),
        expected_sha256: Sha256::digest(std::fs::read(&path).unwrap()).into(),
        path,
        allowed_root: root,
        config_json: "{}".into(),
        limits: Limits::default(),
        instance_mode: InstanceMode::PerCall,
    };
    let store = Arc::new(MemoryStore::new());
    let provider = Arc::new(FakeProvider::scripted(vec![vec![
        StreamDelta::TextDelta("done".into()),
        StreamDelta::Completed,
    ]]));
    let build = |config: ModuleConfig| {
        Agent::builder()
            .behavior_fingerprint(crabber::core::InputFingerprint::new("b".repeat(64)).unwrap())
            .store(store.clone())
            .provider(provider.clone())
            .config(AgentConfig::new(Selection {
                provider_id: "fake".into(),
                model_id: "scripted".into(),
            }))
            .shared_wasm_extension(config)
            .build()
            .unwrap()
    };
    let options = AdmissionOptions {
        key: AdmissionKey::new("wasm-declaration").unwrap(),
        fingerprint: InputFingerprint::new("a".repeat(64)).unwrap(),
        behavior_fingerprint: InputFingerprint::new("b".repeat(64)).unwrap(),
    };
    let session = SessionId::new();
    let agent = build(config.clone());
    let Admission::Started { receipt, handle } = agent
        .prompt_keyed(session.clone(), "hello", options.clone())
        .await
        .unwrap()
    else {
        panic!("fresh admission must start")
    };
    handle.done().await.unwrap();
    agent.close_extensions().await.unwrap();
    let revision = store.session_revision(&session).await.unwrap();
    let mut unavailable = config;
    unavailable.path = unavailable.allowed_root.join("unavailable-artifact.wasm");
    let replay = build(unavailable.clone());
    assert!(
        matches!(replay.prompt_keyed(session.clone(), "hello", options.clone()).await.unwrap(), Admission::Replayed(ref actual) if actual == &receipt)
    );
    assert!(
        matches!(replay.recover_admission(session.clone(), "hello", options.clone()).await.unwrap(), Admission::Replayed(ref actual) if actual == &receipt)
    );
    let mut changed = unavailable.clone();
    changed.config_json = r#"{"mode":"changed"}"#.into();
    let mut changed_limits = unavailable.clone();
    changed_limits.limits.max_state_entries += 1;
    let mut changed_instance = unavailable;
    changed_instance.instance_mode = InstanceMode::Persistent;
    for config in [changed, changed_limits, changed_instance] {
        let outcome = build(config)
            .prompt_keyed(session.clone(), "hello", options.clone())
            .await;
        assert!(matches!(
            outcome,
            Err(RuntimeError::Store(CoreError::AdmissionConflict))
        ));
    }
    assert_eq!(provider.requests().len(), 1);
    assert_eq!(store.session_revision(&session).await.unwrap(), revision);
}
