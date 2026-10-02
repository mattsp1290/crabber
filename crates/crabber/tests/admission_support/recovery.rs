//! Real-process proof of original-turn completion and conservative start ambiguity.
use super::*;
use crabber::session::{AdmissionExecutionError, AdmissionExecutionState};
use crabber_agui::{Completion, ProjectionConfig, Projector, encode_sse};

#[allow(clippy::too_many_lines)] // Fresh-process modes share one original request and receipt.
pub(super) async fn child(
    mode: &str,
    dir: &Path,
    session: &SessionId,
    agent: &Agent,
    store: &PostgresStore,
    context: &TraceContext,
) {
    if mode.starts_with("unstarted-race-") {
        fs::write(dir.join(mode), "ready").unwrap();
        wait(&dir.join("go")).await;
    }
    if mode == "commit-loss" || mode == "unstarted-provider-loss" {
        let result = agent
            .prompt_keyed_with_context(
                session.clone(),
                "process input",
                options(),
                Some(context.clone()),
            )
            .await;
        if mode == "commit-loss" {
            assert!(matches!(
                result,
                Err(RuntimeError::Store(StoreError::Validation(_)))
            ));
            assert_eq!(count(&dir.join("ledger"), "provider"), 0);
            return;
        }
        let Admission::Started { handle, .. } = result.unwrap() else {
            panic!()
        };
        handle.done().await.unwrap();
        return;
    }
    let receipt: AdmissionReceipt =
        serde_json::from_slice(&fs::read(dir.join("committed.json")).unwrap()).unwrap();
    assert_eq!(
        agent
            .lookup_admission(session, &options().key)
            .await
            .unwrap(),
        Some(receipt.clone())
    );
    assert!(
        matches!(agent.prompt_keyed(session.clone(), "process input", options()).await.unwrap(), Admission::Replayed(r) if r == receipt)
    );
    if mode == "unstarted-live" {
        let before = store.get_run(&receipt.run_id).await.unwrap();
        assert!(matches!(
            agent
                .recover_admission(session.clone(), "process input", options())
                .await,
            Err(RuntimeError::AdmissionExecution(
                AdmissionExecutionError::LiveLease
            ))
        ));
        assert_eq!(before, store.get_run(&receipt.run_id).await.unwrap());
        assert_eq!(count(&dir.join("ledger"), "provider"), 0);
        return;
    }
    if mode == "unstarted-replay" {
        assert!(
            matches!(agent.recover_admission(session.clone(), "process input", options()).await.unwrap(), Admission::Replayed(r) if r == receipt)
        );
        project(store, &receipt).await;
        return;
    }
    if mode == "unstarted-interrupt" {
        assert!(matches!(
            agent
                .recover_admission(session.clone(), "process input", options())
                .await
                .unwrap(),
            Admission::Replayed(_)
        ));
        assert_eq!(
            agent.resume(&receipt.run_id).await.unwrap().status,
            RunStatus::Interrupted
        );
        return;
    }
    let result = agent
        .recover_admission_with_context(
            session.clone(),
            "process input",
            options(),
            Some(context.clone()),
        )
        .await;
    if mode == "unstarted-claim-loss" {
        assert!(matches!(
            result,
            Err(RuntimeError::AdmissionExecution(
                AdmissionExecutionError::UnknownStoreFailure
            ))
        ));
        assert_eq!(count(&dir.join("ledger"), "provider"), 0);
        return;
    }
    match result {
        Ok(Admission::Started {
            handle,
            receipt: claimed,
        }) => {
            assert_eq!(claimed, receipt);
            fs::write(dir.join(format!("{mode}-winner")), "claimed").unwrap();
            let result = handle.done().await;
            if mode == "unstarted-begin-loss" || mode == "unstarted-begin-fail" {
                assert!(matches!(
                    result,
                    Err(RuntimeError::AdmissionExecution(
                        AdmissionExecutionError::UnknownStoreFailure
                    ))
                ));
                assert_eq!(count(&dir.join("ledger"), "provider"), 0);
                return;
            }
            let result = result.unwrap();
            assert_eq!(result.status, RunStatus::Completed);
            assert_eq!(result.run_id, receipt.run_id);
            assert_eq!(result.session_id, receipt.session_id);
            project(store, &receipt).await;
        }
        Ok(Admission::Replayed(r)) if mode.starts_with("unstarted-race-") => {
            assert_eq!(r, receipt);
            fs::write(dir.join(format!("{mode}-denied")), "metadata").unwrap();
        }
        Err(RuntimeError::AdmissionExecution(
            AdmissionExecutionError::LiveLease
            | AdmissionExecutionError::StaleOwner
            | AdmissionExecutionError::AlreadyStarted
            | AdmissionExecutionError::AlreadyTerminal,
        )) if mode.starts_with("unstarted-race-") => {
            fs::write(dir.join(format!("{mode}-denied")), "denied").unwrap();
        }
        other => panic!("unexpected recovery: {other:?}"),
    }
}

async fn project(store: &PostgresStore, receipt: &AdmissionReceipt) {
    let run = store.get_run(&receipt.run_id).await.unwrap().unwrap();
    assert_eq!(run.status, RunStatus::Completed);
    let history = store.list_all_messages(&receipt.session_id).await.unwrap();
    assert!(
        history
            .iter()
            .filter(|m| m.role == Role::Assistant)
            .flat_map(|m| &m.parts)
            .any(|p| p.content
                == crabber::core::ContentBlock::Text {
                    text: "original recovered output".into()
                })
    );
    let records = store
        .list_events(&receipt.session_id, None, 1000)
        .await
        .unwrap();
    let mut projector = Projector::new(
        receipt.session_id.clone(),
        receipt.run_id.clone(),
        receipt.session_id.to_string(),
        receipt.run_id.to_string(),
        ProjectionConfig::default(),
    )
    .unwrap();
    let mut output = Vec::new();
    for record in records {
        if record.kind == crabber::core::EventKind::MessageCommitted {
            // Durable events retain commit identity; presentation deltas/boundaries
            // are live-only. Rebuild them from the exact committed history row.
            let id: crabber::core::MessageId =
                serde_json::from_value(record.payload["message_id"].clone()).unwrap();
            let message = history.iter().find(|m| m.id == id).unwrap();
            assert_eq!(message.run_id.as_ref(), Some(&receipt.run_id));
            assert_eq!(message.role, Role::Assistant);
            let mut presentation = record.clone();
            presentation.cursor = None;
            presentation.live_only = true;
            presentation.kind = crabber::core::EventKind::MessageStarted;
            presentation.payload = serde_json::json!({"message_id": id});
            output.extend(projector.push(&presentation).unwrap());
            for part in &message.parts {
                if let crabber::core::ContentBlock::Text { text } = &part.content {
                    presentation.kind = crabber::core::EventKind::TextDelta;
                    presentation.payload = serde_json::json!({"message_id": id, "text": text});
                    output.extend(projector.push(&presentation).unwrap());
                }
            }
            presentation.kind = crabber::core::EventKind::MessageStreamEnded;
            presentation.payload = serde_json::json!({"message_id": id, "outcome": "completed"});
            output.extend(projector.push(&presentation).unwrap());
        }
        output.extend(projector.push(&record).unwrap());
    }
    output.extend(projector.finish(Completion::Completed).unwrap());
    let wire: Vec<_> = output
        .iter()
        .map(|e| serde_json::to_value(e).unwrap())
        .collect();
    assert_eq!(
        wire.iter().filter(|e| e["type"] == "RUN_FINISHED").count(),
        1
    );
    assert!(!wire.iter().any(|e| e["type"] == "RUN_ERROR"));
    let finished = wire.iter().find(|e| e["type"] == "RUN_FINISHED").unwrap();
    assert_eq!(finished["threadId"], receipt.session_id.to_string());
    assert_eq!(finished["runId"], receipt.run_id.to_string());
    assert_eq!(
        wire.iter()
            .filter(|e| e["type"] == "TEXT_MESSAGE_CONTENT")
            .map(|e| e["delta"].as_str().unwrap())
            .collect::<String>(),
        "original recovered output"
    );
    for event in output {
        let frame = encode_sse(&event, 1_048_576).unwrap();
        assert!(frame.starts_with(b"data: "));
    }
}

async fn expired(store: &PostgresStore, receipt: &AdmissionReceipt) {
    let deadline = Instant::now() + Duration::from_secs(40);
    loop {
        let run = store.get_run(&receipt.run_id).await.unwrap().unwrap();
        if run.lease_until <= time::OffsetDateTime::now_utc() {
            return;
        }
        assert!(Instant::now() < deadline, "durable lease did not expire");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn original(dir: &Path) -> AdmissionReceipt {
    serde_json::from_slice(&fs::read(dir.join("committed.json")).unwrap()).unwrap()
}
async fn unstarted(store: &PostgresStore, session: &SessionId) {
    assert_eq!(
        store
            .load_admission_execution(session, &options().key)
            .await
            .unwrap()
            .unwrap()
            .state,
        AdmissionExecutionState::Unstarted
    );
}
fn test_url() -> Option<String> {
    if let Ok(url) = std::env::var("CRABBER_TEST_POSTGRES_URL") {
        Some(url)
    } else {
        assert_ne!(
            std::env::var("CRABBER_REQUIRE_POSTGRES").as_deref(),
            Ok("1"),
            "CRABBER_TEST_POSTGRES_URL required"
        );
        None
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn postgres_unstarted_completion_and_uncertainty_journey() {
    let Some(url) = test_url() else { return };
    PostgresStore::migrate(&url).await.unwrap();
    let store = PostgresStore::connect(&url).await.unwrap();
    let (dir, session) = handoff();
    fs::write(dir.0.join("text-only"), "yes").unwrap();
    finish(spawn(&dir.0, "commit-loss")).await;
    let receipt = original(&dir.0);
    finish(spawn(&dir.0, "unstarted-live")).await;
    unstarted(&store, &session).await;
    evidence(&url, &dir.0, &session, 0, 0).await;
    assert_eq!(
        store
            .claim_expired_run(&receipt.run_id, "generic")
            .await
            .unwrap_err(),
        StoreError::AdmissionRecoveryRequired
    );
    expired(&store, &receipt).await;
    let old = store.get_run(&receipt.run_id).await.unwrap().unwrap();
    let first = spawn(&dir.0, "unstarted-race-a");
    let second = spawn(&dir.0, "unstarted-race-b");
    wait(&dir.0.join("unstarted-race-a")).await;
    wait(&dir.0.join("unstarted-race-b")).await;
    fs::write(dir.0.join("go"), "go").unwrap();
    wait(&dir.0.join("begun")).await;
    let deadline = Instant::now() + Duration::from_secs(10);
    while !dir.0.join("unstarted-race-a-denied").exists()
        && !dir.0.join("unstarted-race-b-denied").exists()
    {
        assert!(Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(count(&dir.0.join("ledger"), "provider"), 0);
    let current = store.get_run(&receipt.run_id).await.unwrap().unwrap();
    assert_ne!(old.claim_token, current.claim_token);
    assert!(current.lease_until > time::OffsetDateTime::now_utc());
    assert!(
        store
            .execution(RunFence {
                run_id: old.id,
                claim_token: old.claim_token
            })
            .await
            .is_err()
    );
    fs::write(dir.0.join("begin-release"), "go").unwrap();
    finish(first).await;
    finish(second).await;
    let winners = ["unstarted-race-a-winner", "unstarted-race-b-winner"]
        .iter()
        .filter(|f| dir.0.join(f).exists())
        .count();
    assert_eq!(winners, 1);
    let events = store.list_events(&session, None, 1000).await.unwrap();
    finish(spawn(&dir.0, "unstarted-replay")).await;
    finish(spawn(&dir.0, "unstarted-replay")).await;
    assert_eq!(
        events,
        store.list_events(&session, None, 1000).await.unwrap()
    );
    evidence(&url, &dir.0, &session, 1, 0).await;
    // Each uncertainty boundary starts from a real committed original admission.
    for mode in [
        "unstarted-claim-loss",
        "unstarted-before-begin",
        "unstarted-begin-fail",
        "unstarted-begin-loss",
        "unstarted-after-begin",
    ] {
        let (dir, session) = handoff();
        fs::write(dir.0.join("text-only"), "yes").unwrap();
        // These cases do not assert live eligibility: shorten the original lease,
        // then wait for its durable real-clock expiry rather than editing SQL state.
        fs::write(dir.0.join("short-admission-lease"), "yes").unwrap();
        finish(spawn(&dir.0, "commit-loss")).await;
        let receipt = original(&dir.0);
        expired(&store, &receipt).await;
        let mut process = spawn(&dir.0, mode);
        if mode == "unstarted-before-begin" || mode == "unstarted-after-begin" {
            wait(&dir.0.join(if mode == "unstarted-before-begin" {
                "claimed"
            } else {
                "begun"
            }))
            .await;
            process.0.kill().unwrap();
            process.0.wait().unwrap();
        } else {
            finish(process).await;
        }
        assert_eq!(count(&dir.0.join("ledger"), "provider"), 0);
        expired(&store, &receipt).await;
        if mode == "unstarted-begin-loss" || mode == "unstarted-after-begin" {
            assert_eq!(
                store
                    .load_admission_execution(&session, &options().key)
                    .await
                    .unwrap()
                    .unwrap()
                    .state,
                AdmissionExecutionState::Started
            );
            finish(spawn(&dir.0, "unstarted-interrupt")).await;
            evidence(&url, &dir.0, &session, 0, 0).await;
        } else {
            unstarted(&store, &session).await;
            finish(spawn(&dir.0, "unstarted-complete")).await;
            finish(spawn(&dir.0, "unstarted-replay")).await;
            evidence(&url, &dir.0, &session, 1, 0).await;
        }
    }
    // A provider stream actually began before death: Started cannot be retried.
    let (dir, session) = handoff();
    fs::write(dir.0.join("text-only"), "yes").unwrap();
    let mut process = spawn(&dir.0, "unstarted-provider-loss");
    wait(&dir.0.join("executing")).await;
    process.0.kill().unwrap();
    process.0.wait().unwrap();
    let receipt = original(&dir.0);
    expired(&store, &receipt).await;
    finish(spawn(&dir.0, "unstarted-interrupt")).await;
    evidence(&url, &dir.0, &session, 1, 0).await;
}
