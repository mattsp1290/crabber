//! Shared contract for initial admission authority and denial of effect writes.
use crate::*;
use crabber_core::*;
use std::{sync::Arc, time::Duration};
use time::OffsetDateTime;

pub(crate) fn keyed(session: &SessionId, now: OffsetDateTime) -> KeyedAdmitRequest {
    let id = MessageId::new();
    let semantics = serde_json::json!([
        "crabber.runtime.admission.v1",
        "provider",
        "model",
        "system-secret",
        "Sequential",
        0.85,
        8,
        64,
        [],
        [],
        [],
        [],
        [],
        []
    ]);
    let mut keyed = KeyedAdmitRequest {
        request: AdmitRequest {
            session_id: Some(session.clone()),
            workspace_id: "workspace-secret".into(),
            directory: "/private/secret".into(),
            title: "title-secret".into(),
            user_message: Message {
                id: id.clone(),
                session_id: session.clone(),
                run_id: None,
                role: Role::User,
                parent_id: None,
                created_at: now,
                parts: vec![Part {
                    id: PartId::new(),
                    message_id: id,
                    ordinal: 0,
                    kind: PartKind::UserInputText,
                    content: ContentBlock::Text {
                        text: "prompt-secret".into(),
                    },
                }],
            },
            config_hash: admission_config_hash(&semantics),
            plan_fingerprint: "plan".into(),
            owner: "owner-secret".into(),
            lease: Duration::from_secs(30),
        },
        options: AdmissionOptions {
            key: AdmissionKey::new("original-key").unwrap(),
            fingerprint: InputFingerprint::new("a".repeat(64)).unwrap(),
            behavior_fingerprint: InputFingerprint::new("b".repeat(64)).unwrap(),
        },
        execution: None,
    };
    keyed.execution = Some(AdmissionExecutionCapsule {
        version: 1,
        request: AdmissionRequestData {
            session_id: session.clone(),
            workspace_id: keyed.request.workspace_id.clone(),
            directory: keyed.request.directory.clone(),
            title: keyed.request.title.clone(),
            text: "prompt-secret".into(),
            provider_id: "provider".into(),
            model_id: "model".into(),
            system_prompt: Some("system-secret".into()),
        },
        runtime_semantics: semantics,
        config_hash: keyed.request.config_hash.clone(),
        plan_fingerprint: "plan".into(),
        fingerprint: keyed.options.fingerprint.clone(),
        behavior_fingerprint: keyed.options.behavior_fingerprint.clone(),
        semantic_digest: keyed.semantic_digest().unwrap(),
    });
    keyed
}

fn event(run: &Run, now: OffsetDateTime) -> EventRecord {
    EventRecord {
        cursor: None,
        session_id: run.session_id.clone(),
        run_id: run.id.clone(),
        turn_id: None,
        kind: EventKind::RunSettled,
        payload: serde_json::Value::Null,
        correlation: None,
        live_only: false,
        created_at: now,
    }
}

// Even invalid payloads must first fail the execution boundary. This exercises
// every effect-bearing method with one retained Unstarted fence and later a stale fence.
#[allow(clippy::too_many_lines)] // One assertion per effect-bearing trait method.
async fn denied_writes(
    execution: &dyn ExecutionStore,
    run: &Run,
    input: &Message,
    now: OffsetDateTime,
    error: StoreError,
) {
    let ev = event(run, now);
    let id = ToolCallId::new();
    let call = ToolCallRecord {
        id: id.clone(),
        run_id: run.id.clone(),
        name: "tool".into(),
        arguments: serde_json::Value::Null,
        status: ToolCallStatus::Pending,
        result: None,
        retry_safe: false,
    };
    let epoch = ContextEpoch {
        id: EpochId::new(),
        session_id: run.session_id.clone(),
        run_id: run.id.clone(),
        parent: Some(run.epoch_id.clone()),
        summarized_range: None,
        summary_message_id: None,
        tail_start_message_id: None,
        provider_id: "provider".into(),
        model_id: "model".into(),
        reason: "test".into(),
        next_policy: None,
    };
    assert_eq!(
        execution.append_message(input.clone()).await.unwrap_err(),
        error
    );
    assert_eq!(
        execution
            .append_part(input.parts[0].clone())
            .await
            .unwrap_err(),
        error
    );
    assert_eq!(execution.append_event(ev.clone()).await.unwrap_err(), error);
    assert_eq!(
        execution
            .create_tool_call(call, ev.clone())
            .await
            .unwrap_err(),
        error
    );
    assert_eq!(
        execution
            .claim_tool_call(&id, ev.clone())
            .await
            .unwrap_err(),
        error
    );
    assert_eq!(
        execution
            .settle_tool_call(
                &id,
                ToolResult {
                    status: ToolResultStatus::Completed,
                    content: vec![]
                },
                input.clone(),
                ev.clone()
            )
            .await
            .unwrap_err(),
        error
    );
    assert_eq!(execution.start_epoch(epoch).await.unwrap_err(), error);
    assert_eq!(
        execution
            .finish_epoch(&run.epoch_id, input.clone())
            .await
            .unwrap_err(),
        error
    );
    assert_eq!(
        execution
            .pause_run(serde_json::Value::Null, ev.clone())
            .await
            .unwrap_err(),
        error
    );
    assert_eq!(
        execution
            .settle_run(RunStatus::Completed, None, Usage::default(), ev)
            .await
            .unwrap_err(),
        error
    );
    assert_eq!(
        execution
            .put_extension_state("extension", vec![])
            .await
            .unwrap_err(),
        error
    );
    assert_eq!(
        execution.claim_inbox(InboxKind::Steer).await.unwrap_err(),
        error
    );
    assert_eq!(
        execution
            .claim_inbox_into_history(InboxKind::Steer)
            .await
            .unwrap_err(),
        error
    );
}

async fn malformed_capsules(store: &impl Store, original: &KeyedAdmitRequest) {
    for change in 0..9 {
        let mut request = original.clone();
        let capsule = request.execution.as_mut().unwrap();
        match change {
            0 => capsule.version = 2,
            1 => capsule.request.text = "inconsistent".into(),
            2 => capsule.request.provider_id = "inconsistent".into(),
            3 => capsule.request.system_prompt = None,
            4 => capsule.config_hash = "inconsistent".into(),
            5 => capsule.plan_fingerprint = "inconsistent".into(),
            6 => capsule.fingerprint = InputFingerprint::new("c".repeat(64)).unwrap(),
            7 => capsule.behavior_fingerprint = InputFingerprint::new("c".repeat(64)).unwrap(),
            8 => capsule.runtime_semantics = serde_json::Value::Null,
            _ => unreachable!(),
        }
        assert_eq!(
            store.admit_keyed_run(request).await.unwrap_err(),
            StoreError::AdmissionConflict,
            "capsule mutation {change}"
        );
    }
}

/// # Panics
/// Panics when a store grants authority without retained unstarted evidence.
#[allow(clippy::too_many_lines)]
pub async fn run_contract<S: Store>(factory: impl FnOnce(Arc<ManualClock>) -> S) {
    let now = OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();
    let clock = Arc::new(ManualClock::new(now));
    let store = factory(clock.clone());
    let session = SessionId::new();
    let original = keyed(&session, now);
    malformed_capsules(&store, &original).await;
    assert!(store.get_session(&session).await.unwrap().is_none());
    let KeyedAdmitOutcome::Started { receipt, admitted } =
        store.admit_keyed_run(original.clone()).await.unwrap()
    else {
        panic!()
    };
    let record = store
        .load_admission_execution(&session, &original.options.key)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(record.state, AdmissionExecutionState::Unstarted);
    assert_eq!(record.receipt, receipt);
    assert_eq!(record.capsule, original.execution.clone().unwrap());
    for secret in [
        "prompt-secret",
        "system-secret",
        "/private/secret",
        "owner-secret",
        &admitted.fence.claim_token,
    ] {
        assert!(!format!("{record:?} {original:?} {receipt:?}").contains(secret));
    }
    let old = store.execution(admitted.fence.clone()).await.unwrap();
    denied_writes(
        old.as_ref(),
        &admitted.run,
        &original.request.user_message,
        now,
        StoreError::AdmissionRecoveryRequired,
    )
    .await;
    old.renew_lease(now + time::Duration::seconds(30))
        .await
        .unwrap();
    let claim = ClaimUnstartedAdmissionRequest {
        session_id: session.clone(),
        key: original.options.key.clone(),
        expected_fence: admitted.fence.clone(),
        expected_owner: admitted.run.owner.clone(),
        fingerprint: receipt.fingerprint.clone(),
        behavior_fingerprint: original.options.behavior_fingerprint.clone(),
        semantic_digest: receipt.semantic_digest.clone(),
        capsule_digest: record.capsule.digest().unwrap(),
        owner: "replacement".into(),
        lease: Duration::from_secs(30),
    };
    assert_eq!(
        store
            .claim_unstarted_admission(claim.clone())
            .await
            .unwrap_err(),
        AdmissionExecutionError::LiveLease
    );
    clock.set(now + time::Duration::seconds(30));
    assert_eq!(
        store
            .claim_expired_run(&receipt.run_id, "generic")
            .await
            .unwrap_err(),
        StoreError::AdmissionRecoveryRequired
    );
    let mut conflict = claim.clone();
    conflict.capsule_digest = "wrong".into();
    assert_eq!(
        store.claim_unstarted_admission(conflict).await.unwrap_err(),
        AdmissionExecutionError::SemanticConflict
    );
    let (a, b) = tokio::join!(
        store.claim_unstarted_admission(claim.clone()),
        store.claim_unstarted_admission(claim)
    );
    let claimed = match (a, b) {
        (Ok(c), Err(AdmissionExecutionError::StaleOwner))
        | (Err(AdmissionExecutionError::StaleOwner), Ok(c)) => c,
        other => panic!("{other:?}"),
    };
    assert_eq!(claimed.record.receipt, receipt);
    assert_ne!(claimed.fence.claim_token, admitted.fence.claim_token);
    denied_writes(
        old.as_ref(),
        &admitted.run,
        &original.request.user_message,
        now,
        StoreError::Conflict,
    )
    .await;
    assert_eq!(
        old.begin_admission_execution().await.unwrap_err(),
        AdmissionExecutionError::StaleOwner
    );
    // Claim success is not start: a lost reply/crash can be replaced after expiry.
    clock.set(now + time::Duration::seconds(60));
    let next_claim = ClaimUnstartedAdmissionRequest {
        expected_fence: claimed.fence,
        expected_owner: claimed.run.owner,
        owner: "next".into(),
        ..ClaimUnstartedAdmissionRequest {
            session_id: session.clone(),
            key: original.options.key.clone(),
            expected_fence: admitted.fence.clone(),
            expected_owner: String::new(),
            fingerprint: receipt.fingerprint.clone(),
            behavior_fingerprint: original.options.behavior_fingerprint.clone(),
            semantic_digest: receipt.semantic_digest.clone(),
            capsule_digest: record.capsule.digest().unwrap(),
            owner: String::new(),
            lease: Duration::from_secs(30),
        }
    };
    let next = store.claim_unstarted_admission(next_claim).await.unwrap();
    let execution = store.execution(next.fence.clone()).await.unwrap();
    let (a, b) = tokio::join!(
        execution.begin_admission_execution(),
        execution.begin_admission_execution()
    );
    assert!(matches!(
        (a, b),
        (Ok(()), Err(AdmissionExecutionError::AlreadyStarted))
            | (Err(AdmissionExecutionError::AlreadyStarted), Ok(()))
    ));
    execution.append_event(event(&next.run, now)).await.unwrap();
    execution
        .put_extension_state("extension", vec![("key".into(), Some("value".into()))])
        .await
        .unwrap();
    execution.claim_inbox(InboxKind::Steer).await.unwrap();
    execution
        .settle_run(
            RunStatus::Completed,
            None,
            Usage::default(),
            event(&next.run, now),
        )
        .await
        .unwrap();
    assert_eq!(
        execution.begin_admission_execution().await.unwrap_err(),
        AdmissionExecutionError::AlreadyTerminal
    );
    assert_eq!(
        store
            .lookup_admission(&session, &original.options.key)
            .await
            .unwrap(),
        Some(receipt)
    );
    assert_eq!(
        store
            .load_admission_execution(&session, &original.options.key)
            .await
            .unwrap()
            .unwrap()
            .state,
        AdmissionExecutionState::Started
    );
    malformed_capsules(&store, &original).await;
    let mut legacy = keyed(&SessionId::new(), now);
    legacy.execution = None;
    let legacy_session = legacy.request.session_id.clone().unwrap();
    store.admit_keyed_run(legacy.clone()).await.unwrap();
    let upgraded = keyed(&legacy_session, now);
    assert!(matches!(
        store.admit_keyed_run(upgraded).await.unwrap(),
        KeyedAdmitOutcome::Replayed(_)
    ));
    assert!(
        store
            .load_admission_execution(&legacy_session, &legacy.options.key)
            .await
            .unwrap()
            .is_none()
    );
}
