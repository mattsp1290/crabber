use super::*;

// Invoked by the parent harness in a genuinely fresh OS process, never recursively.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn facade_child() {
    let Some((backend, dir, mode)) = Backend::child() else {
        return;
    };
    let session: SessionId =
        serde_json::from_slice(&fs::read(dir.join("session.json")).unwrap()).unwrap();
    let store = backend.connect().await;
    let wrapped = Arc::new(FaultStore {
        inner: store.clone(),
        failure: backend.failure(),
        mode: mode.clone(),
        dir: dir.clone(),
    });
    let capture = Arc::new(ContextCapture::default());
    let context: TraceContext =
        serde_json::from_slice(&fs::read(dir.join("context.json")).unwrap()).unwrap();
    let agent = agent(
        wrapped,
        &dir,
        mode == "execution-loss" || mode == "unstarted-provider-loss",
        capture.clone(),
        mode == "tool-loss",
    );
    if dir.join("text-only").exists() {
        super::recovery::child(&mode, &dir, &session, &agent, store.as_ref(), &context).await;
        return;
    }
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
    let factories = count(&dir.join("ledger"), "factory");
    let result = agent
        .prompt_keyed_with_context(
            session.clone(),
            "process input",
            options(),
            Some(context.clone()),
        )
        .await;
    if mode == "precommit"
        || mode == "commit-loss"
        || (mode == "preparation-blocked" || mode == "preparation-inflight")
    {
        preparation_failure(
            &agent,
            store.as_ref(),
            &dir,
            &session,
            &mode,
            result,
            factories,
        )
        .await;
        return;
    }
    if mode.starts_with("race-") && result.is_err() {
        let error = result.err().unwrap();
        assert!(error.preparation_reconciliation().is_some());
        assert!(matches!(
            error.operation_error(),
            RuntimeError::PreparationRecoveryRequired
                | RuntimeError::Store(StoreError::AdmissionRecoveryRequired)
        ));
        return;
    }
    let admission = result.unwrap();
    let receipt = admission.receipt().clone();
    if let Some(expected) = original {
        assert_eq!(receipt, expected);
        assert!(matches!(admission, Admission::Replayed(_)));
        assert_eq!(count(&dir.join("ledger"), "factory"), factories);
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
        super::reconcile_started::recover(
            &agent,
            store.as_ref(),
            &session,
            &receipt,
            before.unwrap(),
            &dir,
        )
        .await;
        assert!(
            capture.0.lock().unwrap().is_empty(),
            "metadata controls do not notify business observers"
        );
    }
    if mode != "recover" {
        assert!(
            capture
                .0
                .lock()
                .unwrap()
                .iter()
                .all(|(event, observed)| event.run_id == receipt.run_id
                    && observed.as_ref() == Some(&context))
        );
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

async fn preparation_failure(
    agent: &Agent,
    store: &dyn Store,
    dir: &Path,
    session: &SessionId,
    mode: &str,
    result: Result<Admission, RuntimeError>,
    factories: usize,
) {
    use crabber::session::session_owner::{
        SessionOwnerState,
        preparation::{PreparationKey, PreparationReconciliation},
    };
    let Err(error) = result else {
        panic!("injected preparation failure")
    };
    let descriptor = error.preparation_reconciliation().unwrap();
    assert_eq!(&descriptor.session_id, session);
    assert_eq!(
        agent
            .lookup_preparation_operation(session, &PreparationKey::Admission(options().key))
            .await
            .unwrap()
            .as_ref(),
        Some(descriptor)
    );
    if mode == "preparation-blocked" || mode == "preparation-inflight" {
        assert!(matches!(
            error.operation_error(),
            RuntimeError::PreparationRecoveryRequired
        ));
        if mode == "preparation-blocked" {
            let original: PreparationReconciliation =
                serde_json::from_slice(&fs::read(dir.join("preparation.meta")).unwrap()).unwrap();
            assert_eq!(*descriptor, original);
        }
        assert_eq!(count(&dir.join("ledger"), "factory"), factories);
    } else {
        assert!(matches!(
            error.operation_error(),
            RuntimeError::Store(StoreError::Validation(_))
        ));
        fs::write(
            dir.join("preparation.meta"),
            serde_json::to_vec(descriptor).unwrap(),
        )
        .unwrap();
        assert_eq!(count(&dir.join("ledger"), "factory"), factories + 1);
    }
    assert_eq!(
        store
            .get_session_owner(session, &descriptor.owner.id)
            .await
            .unwrap()
            .state,
        if mode == "commit-loss" {
            SessionOwnerState::Joined
        } else {
            SessionOwnerState::Active
        }
    );
    assert_eq!(count(&dir.join("ledger"), "provider"), 0);
    assert_eq!(count(&dir.join("ledger"), "tool"), 0);
    agent.close_extensions().await.unwrap();
    assert_eq!(
        agent
            .lookup_preparation_operation(session, &PreparationKey::Admission(options().key))
            .await
            .unwrap()
            .as_ref(),
        Some(descriptor)
    );
}
