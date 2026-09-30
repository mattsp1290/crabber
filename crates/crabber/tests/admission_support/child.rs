use super::*;

// Invoked by the parent harness in a genuinely fresh OS process, never recursively.
#[tokio::test]
async fn facade_child() {
    let Ok(mode) = std::env::var("CRABBER_RECEIPT_CHILD") else {
        return;
    };
    let dir = PathBuf::from(std::env::var_os("CRABBER_RECEIPT_HANDOFF").unwrap());
    let session: SessionId =
        serde_json::from_slice(&fs::read(dir.join("session.json")).unwrap()).unwrap();
    let url = std::env::var("CRABBER_TEST_POSTGRES_URL").unwrap();
    let store = Arc::new(PostgresStore::connect(&url).await.unwrap());
    let wrapped = Arc::new(FaultStore {
        inner: store.clone(),
        mode: mode.clone(),
        dir: dir.clone(),
    });
    let agent = agent(wrapped, &dir, mode == "execution-loss");
    if mode.starts_with("race-") {
        fs::write(dir.join(&mode), "ready").unwrap();
        wait(&dir.join("go")).await;
    }
    if mode == "absent" {
        assert!(
            agent
                .lookup_admission(&session, &options().key)
                .await
                .unwrap()
                .is_none()
        );
        assert!(store.get_session(&session).await.unwrap().is_none());
        return;
    }
    let original = if mode == "reconcile" || mode == "recover" {
        let expected: AdmissionReceipt =
            serde_json::from_slice(&fs::read(dir.join("committed.json")).unwrap()).unwrap();
        assert_eq!(
            agent
                .lookup_admission(&session, &options().key)
                .await
                .unwrap(),
            Some(expected.clone())
        );
        Some(expected)
    } else {
        None
    };
    let before = if let Some(receipt) = &original {
        store.get_run(&receipt.run_id).await.unwrap()
    } else {
        None
    };
    let result = agent
        .prompt_keyed(session.clone(), "process input", options())
        .await;
    if mode == "precommit" || mode == "commit-loss" {
        assert!(matches!(
            result,
            Err(RuntimeError::Store(StoreError::Validation(_)))
        ));
        assert_eq!(count(&dir.join("ledger"), "provider"), 0);
        return;
    }
    let admission = result.unwrap();
    let receipt = admission.receipt().clone();
    if let Some(expected) = original {
        assert_eq!(receipt, expected);
        assert!(matches!(admission, Admission::Replayed(_)));
        let after = store.get_run(&receipt.run_id).await.unwrap();
        if dir.join("executing").exists() && !dir.join("execute-release").exists() {
            // The original worker may heartbeat; replay must not take ownership.
            let before = before.as_ref().unwrap();
            let after = after.as_ref().unwrap();
            assert_eq!(after.owner, before.owner);
            assert_eq!(after.claim_token, before.claim_token);
            assert_eq!(after.status, before.status);
        } else {
            assert_eq!(
                after, before,
                "quiescent replay must not mutate run or lease"
            );
        }
    }
    match admission {
        Admission::Started { handle, .. } => {
            assert_eq!(handle.done().await.unwrap().status, RunStatus::Completed);
        }
        Admission::Replayed(_) => {}
    }
    if mode == "execution-loss" {
        // The worker completed, but its host-to-caller response is discarded.
        // Fresh hosts can only reconcile through the public receipt API.
        fs::write(dir.join("response-discarded"), "lost").unwrap();
        return;
    }
    if mode == "recover" {
        recover(&agent, &store, &session, &receipt, before.unwrap()).await;
    }
    fs::write(
        dir.join(format!("{mode}.json")),
        serde_json::to_vec(&receipt).unwrap(),
    )
    .unwrap();
    println!(
        "child={mode} session={} receipt={} run={} user_message={}",
        receipt.session_id, receipt.run_id, receipt.run_id, receipt.user_message_id
    );
}

async fn recover(
    agent: &Agent,
    store: &PostgresStore,
    session: &SessionId,
    receipt: &AdmissionReceipt,
    old: Run,
) {
    let stale = RunFence {
        run_id: old.id.clone(),
        claim_token: old.claim_token,
    };
    let result = agent.resume(&receipt.run_id).await.unwrap();
    assert_eq!(result.status, RunStatus::Interrupted);
    assert_ne!(
        store
            .get_run(&receipt.run_id)
            .await
            .unwrap()
            .unwrap()
            .claim_token,
        stale.claim_token
    );
    assert!(
        store.execution(stale).await.is_err(),
        "old owner remains fenced"
    );
    assert_eq!(
        agent
            .lookup_admission(session, &options().key)
            .await
            .unwrap(),
        Some(receipt.clone())
    );
}
