//! Reusable keyed-admission contract for durable Store implementations.
use crate::{AdmitRequest, KeyedAdmitOutcome, KeyedAdmitRequest, Store, StoreError};
use crabber_core::{
    AdmissionKey, AdmissionOptions, AdmissionReceipt, ContentBlock, EventKind, EventRecord,
    InputFingerprint, ManualClock, Message, MessageId, Part, PartId, PartKind, Role, RunStatus,
    SessionId, Usage,
};
use std::{sync::Arc, time::Duration};
use time::OffsetDateTime;

fn request(session: &SessionId, now: OffsetDateTime) -> KeyedAdmitRequest {
    let id = MessageId::new();
    KeyedAdmitRequest {
        execution: None,
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
                parts: vec![Part {
                    id: PartId::new(),
                    message_id: id,
                    ordinal: 0,
                    kind: PartKind::UserInputText,
                    content: ContentBlock::Text {
                        text: "prompt-secret".into(),
                    },
                }],
                created_at: now,
            },
            config_hash: "config".into(),
            plan_fingerprint: "plan".into(),
            owner: "owner-secret".into(),
            lease: Duration::from_secs(1),
        },
        options: AdmissionOptions {
            key: AdmissionKey::new("key-secret").unwrap(),
            fingerprint: InputFingerprint::new("a".repeat(64)).unwrap(),
            behavior_fingerprint: InputFingerprint::new("b".repeat(64)).unwrap(),
        },
    }
}

fn replay(outcome: KeyedAdmitOutcome) -> AdmissionReceipt {
    match outcome {
        KeyedAdmitOutcome::Replayed(receipt) => receipt,
        KeyedAdmitOutcome::Started { .. } => panic!("replay granted execution authority"),
    }
}

/// Runs against a fresh store; assertions cover replay, conflict, identity,
/// fencing, terminal retention and rollback. The factory supplies its test clock.
/// # Panics
/// Panics when the store violates the contract.
#[allow(clippy::too_many_lines)]
pub async fn run_contract<S: Store>(factory: impl FnOnce(Arc<ManualClock>) -> S) {
    let now = OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();
    let clock = Arc::new(ManualClock::new(now));
    let store = factory(clock.clone());
    let session = SessionId::new();
    let original = request(&session, now);
    assert!(
        store
            .lookup_admission(&session, &original.options.key)
            .await
            .unwrap()
            .is_none()
    );
    let first = store.admit_keyed_run(original.clone()).await.unwrap();
    let debug = format!("{first:?} {original:?}");
    let KeyedAdmitOutcome::Started { receipt, admitted } = first else {
        panic!("first admission must start")
    };
    let encoded = serde_json::to_string(&receipt).unwrap();
    for secret in [
        "prompt-secret",
        "key-secret",
        "owner-secret",
        "/private/secret",
        "workspace-secret",
        "title-secret",
        &admitted.fence.claim_token,
    ] {
        assert!(!debug.contains(secret));
        assert!(!encoded.contains(secret));
    }
    assert_eq!(receipt.session_id, session);
    let mut retry = request(&session, now + time::Duration::seconds(1));
    retry.request.owner = "another owner".into();
    retry.request.lease = Duration::from_secs(100);
    assert_eq!(
        replay(store.admit_keyed_run(retry.clone()).await.unwrap()),
        receipt
    );
    assert_eq!(
        store.get_run(&receipt.run_id).await.unwrap().unwrap(),
        admitted.run
    );

    let mut other = retry.clone();
    other.options.key = AdmissionKey::new("other-key").unwrap();
    assert_eq!(
        store.admit_keyed_run(other).await.unwrap_err(),
        StoreError::Busy
    );
    for change in 0..7 {
        let mut changed = retry.clone();
        match change {
            0 => changed.options.fingerprint = InputFingerprint::new("c".repeat(64)).unwrap(),
            1 => {
                changed.request.user_message.parts[0].content = ContentBlock::Text {
                    text: "changed".into(),
                }
            }
            2 => changed.request.config_hash = "changed".into(),
            3 => changed.request.plan_fingerprint = "changed".into(),
            4 => {
                changed.options.behavior_fingerprint =
                    InputFingerprint::new("c".repeat(64)).unwrap();
            }
            5 => changed.request.title = "changed".into(),
            _ => changed.request.user_message.parts[0].ordinal = 10,
        }
        assert_eq!(
            store.admit_keyed_run(changed).await.unwrap_err(),
            StoreError::AdmissionConflict
        );
    }
    for change in 0..4 {
        let mut changed = retry.clone();
        let expected = match change {
            0 => {
                changed.request.workspace_id = "changed".into();
                StoreError::SessionIdentityMismatch
            }
            1 => {
                changed.request.directory = "changed".into();
                StoreError::SessionIdentityMismatch
            }
            2 => {
                changed.request.user_message.session_id = SessionId::new();
                StoreError::Validation("invalid admission message identity".into())
            }
            _ => {
                changed.request.user_message.parts[0].message_id = MessageId::new();
                StoreError::Validation("invalid admission message identity".into())
            }
        };
        assert_eq!(store.admit_keyed_run(changed).await.unwrap_err(), expected);
    }
    let other_session = SessionId::new();
    assert!(matches!(
        store
            .admit_keyed_run(request(&other_session, now))
            .await
            .unwrap(),
        KeyedAdmitOutcome::Started { .. }
    ));
    assert_eq!(store.list_all_messages(&session).await.unwrap().len(), 1);
    assert_eq!(
        store
            .list_unfinished_runs()
            .await
            .unwrap()
            .iter()
            .filter(|run| run.session_id == session)
            .count(),
        1
    );

    clock.set(now + time::Duration::seconds(2));
    assert_eq!(
        replay(store.admit_keyed_run(retry.clone()).await.unwrap()),
        receipt
    );
    let fence = store
        .claim_expired_run(&receipt.run_id, "new-owner")
        .await
        .unwrap();
    assert_ne!(fence.claim_token, admitted.fence.claim_token);
    assert!(matches!(
        store.execution(admitted.fence).await,
        Err(StoreError::Conflict)
    ));
    let claimed = store.get_run(&receipt.run_id).await.unwrap().unwrap();
    assert_eq!(
        replay(store.admit_keyed_run(retry.clone()).await.unwrap()),
        receipt
    );
    assert_eq!(
        store.get_run(&receipt.run_id).await.unwrap().unwrap(),
        claimed
    );
    store
        .execution(fence)
        .await
        .unwrap()
        .settle_run(
            RunStatus::Completed,
            None,
            Usage::default(),
            EventRecord {
                cursor: None,
                session_id: session.clone(),
                run_id: receipt.run_id.clone(),
                turn_id: None,
                kind: EventKind::RunSettled,
                payload: serde_json::Value::Null,
                correlation: None,
                live_only: false,
                created_at: now,
            },
        )
        .await
        .unwrap();
    assert_eq!(replay(store.admit_keyed_run(retry).await.unwrap()), receipt);
    assert_eq!(
        store
            .lookup_admission(&session, &original.options.key)
            .await
            .unwrap(),
        Some(receipt.clone())
    );

    // Duplicate durable message ID fails inside admission; no receipt or run leaks.
    let mut rollback = original.clone();
    rollback.options.key = AdmissionKey::new("rollback").unwrap();
    assert_eq!(
        store.admit_keyed_run(rollback.clone()).await.unwrap_err(),
        StoreError::Conflict
    );
    assert!(
        store
            .lookup_admission(&session, &rollback.options.key)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(store.list_all_messages(&session).await.unwrap().len(), 1);
    assert!(
        !store
            .list_unfinished_runs()
            .await
            .unwrap()
            .iter()
            .any(|run| run.session_id == session)
    );
}
