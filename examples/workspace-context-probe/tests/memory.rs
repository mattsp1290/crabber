//! Credential-free proofs against the in-memory store, public API only.

use crabber::{
    AdmissionKey, AdmissionOptions, InputFingerprint, RuntimeError,
    core::{CoreError, RunStatus},
    session::MemoryStore,
};
use std::sync::Arc;
use workspace_context_probe::{Identity, Observations, config, host, identity, text, tool_call};

const NONE: [Identity; 0] = [];

fn options() -> AdmissionOptions {
    AdmissionOptions {
        key: AdmissionKey::new("probe-key").unwrap(),
        fingerprint: InputFingerprint::new("a".repeat(64)).unwrap(),
        behavior_fingerprint: InputFingerprint::new("b".repeat(64)).unwrap(),
    }
}

fn mismatch<T>(result: &Result<T, RuntimeError>) {
    assert!(matches!(
        result,
        Err(RuntimeError::Store(CoreError::SessionIdentityMismatch))
    ));
}

#[tokio::test]
async fn two_sessions_observe_their_own_workspace_across_runs() {
    let store = Arc::new(MemoryStore::new());
    let mut sessions = Vec::new();
    for (workspace_id, directory) in [("ws-a", "/srv/a"), ("ws-b", "/srv/b")] {
        let seen = Arc::new(Observations::default());
        let agent = host(
            &seen,
            vec![tool_call(), text("one"), tool_call(), text("two")],
            config(workspace_id, directory),
            false,
        )
        .store(store.clone())
        .build()
        .unwrap();
        let run = agent.prompt(None, "first").await.unwrap();
        let session = run.session_id().clone();
        run.done().await.unwrap();
        // A later run in the same session observes the same values.
        agent
            .prompt(Some(session.clone()), "second")
            .await
            .unwrap()
            .done()
            .await
            .unwrap();
        let expected = identity(Some(workspace_id), Some(directory));
        assert_eq!(seen.tool(), vec![expected.clone(); 2]);
        // The prompt contributor sees what the tool sees, on every turn.
        assert_eq!(seen.assemble(), vec![expected; 4]);
        sessions.push(session);
    }
    assert_ne!(sessions[0], sessions[1]);
}

#[tokio::test]
async fn drifted_identity_is_rejected_and_the_tool_is_not_invoked() {
    let store = Arc::new(MemoryStore::new());
    let seen = Arc::new(Observations::default());
    let agent = host(
        &seen,
        vec![tool_call(), text("done")],
        config("ws-a", "/srv/a"),
        false,
    )
    .store(store.clone())
    .build()
    .unwrap();
    let run = agent.prompt(None, "first").await.unwrap();
    let session = run.session_id().clone();
    run.done().await.unwrap();

    for (workspace_id, directory) in [("ws-a", "/srv/replacement"), ("ws-b", "/srv/a")] {
        let drifted = Arc::new(Observations::default());
        let other = host(
            &drifted,
            vec![tool_call(), text("never")],
            config(workspace_id, directory),
            false,
        )
        .store(store.clone())
        .build()
        .unwrap();
        mismatch(&other.prompt(Some(session.clone()), "again").await);
        mismatch(
            &other
                .prompt_keyed(session.clone(), "again", options())
                .await,
        );
        mismatch(
            &other
                .recover_admission(session.clone(), "again", options())
                .await,
        );
        assert_eq!(drifted.tool(), NONE);
        assert_eq!(drifted.assemble(), NONE);
    }
    assert_eq!(seen.tool().len(), 1);
}

#[tokio::test]
async fn empty_identity_is_unavailable_not_a_host_default_or_model_argument() {
    let store = Arc::new(MemoryStore::new());
    let seen = Arc::new(Observations::default());
    // The session persists an empty workspace ID and directory.
    let first = host(&seen, vec![tool_call()], config("", ""), true)
        .store(store.clone())
        .build()
        .unwrap();
    let run = first.prompt(None, "first").await.unwrap();
    let (session, run_id) = (run.session_id().clone(), run.run_id().clone());
    assert_eq!(run.done().await.unwrap().status, RunStatus::Paused);
    assert_eq!(seen.tool(), NONE);

    // A second host with different defaults ("default", ".") resumes the run;
    // the model's tool arguments name yet another workspace. Neither leaks.
    let resumed = Arc::new(Observations::default());
    let second = host(&resumed, vec![text("done")], config("default", "."), false)
        .store(store.clone())
        .build()
        .unwrap();
    assert_eq!(
        second.resume(&run_id).await.unwrap().status,
        RunStatus::Completed
    );
    assert_eq!(resumed.tool(), [identity(None, None)]);
    assert_eq!(resumed.assemble(), [identity(None, None)]);
    // That host cannot admit into the session under its own defaults.
    mismatch(&second.prompt(Some(session), "again").await);
}

#[tokio::test]
async fn one_empty_field_is_unavailable_while_the_other_is_exposed() {
    let seen = Arc::new(Observations::default());
    let agent = host(
        &seen,
        vec![tool_call(), text("done")],
        config("ws-only", ""),
        false,
    )
    .memory()
    .build()
    .unwrap();
    let run = agent.prompt(None, "first").await.unwrap();
    run.done().await.unwrap();
    assert_eq!(seen.tool(), [identity(Some("ws-only"), None)]);
    assert_eq!(seen.assemble(), vec![identity(Some("ws-only"), None); 2]);
}
