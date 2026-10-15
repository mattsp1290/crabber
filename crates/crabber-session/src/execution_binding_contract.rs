//! Public Store-adapter qualification for atomic immutable provider bindings.
use crate::{
    AdmissionExecutionCapsule, AdmissionRequestData, AdmitRequest, BoundAdmitRequest,
    BoundKeyedAdmitRequest, ExecutionBindingCapsule, HOST_BEHAVIOR_COMPONENT, KeyedAdmitOutcome,
    KeyedAdmitRequest, PROVIDER_BINDING_COMPONENT, Store, StoreError, admission_config_hash,
};
use crabber_core::{
    AdmissionKey, AdmissionOptions, ContentBlock, EventKind, EventRecord, InputFingerprint,
    ManualClock, Message, MessageId, Part, PartId, PartKind, ProviderBindingDescriptor,
    ProviderBindingLimits, Role, RunStatus, SessionId, Usage,
};
use serde_json::json;
use std::{sync::Arc, time::Duration};
use time::OffsetDateTime;

fn fingerprint(c: char) -> InputFingerprint {
    InputFingerprint::new(c.to_string().repeat(64)).unwrap()
}

pub(crate) fn fixture(
    session: &SessionId,
    now: OffsetDateTime,
    backend: &str,
    behavior: char,
) -> BoundKeyedAdmitRequest {
    let data = AdmissionRequestData {
        session_id: session.clone(),
        workspace_id: "bound-workspace".into(),
        directory: "/bound/workspace".into(),
        title: "private-title".into(),
        text: "private-input".into(),
        provider_id: "provider".into(),
        model_id: "model".into(),
        system_prompt: Some("private-system".into()),
        max_output_tokens: Some(32),
    };
    let provider = ProviderBindingDescriptor::new(
        "provider",
        "model",
        backend,
        "version-1",
        fingerprint('c'),
        ProviderBindingLimits {
            context_tokens: Some(1024),
            output_tokens: Some(64),
        },
    )
    .unwrap();
    let behavior_fingerprint = fingerprint(behavior);
    let semantics = json!(["crabber.runtime.admission.v1", "provider", "model", data.system_prompt, data.max_output_tokens, "Sequential", 0.85, 8, 64, [], [], [], [
        {"id": PROVIDER_BINDING_COMPONENT, "version": serde_json::to_string(&provider).unwrap()},
        {"id": HOST_BEHAVIOR_COMPONENT, "version": behavior_fingerprint.as_str()},
    ], [], []]);
    let config_hash = InputFingerprint::new(admission_config_hash(&semantics)).unwrap();
    let binding = ExecutionBindingCapsule {
        version: 2,
        provider,
        request: data.clone(),
        runtime_semantics: semantics.clone(),
        config_hash: config_hash.clone(),
        plan_fingerprint: fingerprint('d'),
        behavior_fingerprint: behavior_fingerprint.clone(),
    };
    let message_id = MessageId::new();
    let request = AdmitRequest {
        session_id: Some(session.clone()),
        workspace_id: data.workspace_id.clone(),
        directory: data.directory.clone(),
        title: data.title.clone(),
        user_message: Message {
            id: message_id.clone(),
            session_id: session.clone(),
            run_id: None,
            role: Role::User,
            parent_id: None,
            parts: vec![Part {
                id: PartId::new(),
                message_id,
                ordinal: 0,
                kind: PartKind::UserInputText,
                content: ContentBlock::Text {
                    text: data.text.clone(),
                },
            }],
            created_at: now,
        },
        config_hash: config_hash.as_str().to_owned(),
        plan_fingerprint: binding.plan_fingerprint.as_str().to_owned(),
        owner: "private-owner".into(),
        lease: Duration::from_secs(30),
    };
    let mut keyed = KeyedAdmitRequest {
        request,
        options: AdmissionOptions {
            key: AdmissionKey::new("bound-admission").unwrap(),
            fingerprint: fingerprint('a'),
            behavior_fingerprint,
        },
        execution: None,
    };
    keyed.execution = Some(AdmissionExecutionCapsule {
        version: 1,
        request: data,
        runtime_semantics: semantics,
        config_hash: config_hash.as_str().to_owned(),
        plan_fingerprint: binding.plan_fingerprint.as_str().to_owned(),
        fingerprint: keyed.options.fingerprint.clone(),
        behavior_fingerprint: keyed.options.behavior_fingerprint.clone(),
        semantic_digest: keyed.semantic_digest().unwrap(),
    });
    BoundKeyedAdmitRequest { keyed, binding }
}

/// Fresh Store required. Checks exact replay/no grant, changed descriptors and
/// opaque behavior with stale caller hashes, cross-scope denial and retained
/// unkeyed evidence. Does not initialize a provider or dispatch any callback.
/// # Panics
/// Panics when an adapter violates the binding admission contract.
#[allow(clippy::too_many_lines)]
pub async fn run_contract<S: Store>(factory: impl FnOnce(Arc<ManualClock>) -> S) {
    let now = OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();
    let clock = Arc::new(ManualClock::new(now));
    let store = factory(clock.clone());
    let session = SessionId::new();
    let original = fixture(&session, now, "backend-original", 'b');
    let (left, right) = tokio::join!(
        store.admit_bound_keyed_run(original.clone()),
        store.admit_bound_keyed_run(original.clone())
    );
    let (receipt, admitted) = match (left.unwrap(), right.unwrap()) {
        (KeyedAdmitOutcome::Started { receipt, admitted }, KeyedAdmitOutcome::Replayed(other))
        | (KeyedAdmitOutcome::Replayed(other), KeyedAdmitOutcome::Started { receipt, admitted }) => {
            assert_eq!(receipt, other);
            (receipt, admitted)
        }
        _ => panic!("same-key race must grant exactly once"),
    };
    let retained = store
        .execution_binding(&session, &receipt.run_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(retained.capsule, original.binding);
    assert_eq!(retained.user_message_id, receipt.user_message_id);
    assert_eq!(
        store
            .execution_binding(&SessionId::new(), &receipt.run_id)
            .await
            .unwrap(),
        None
    );
    let revision = store.session_revision(&session).await.unwrap();
    for changed in [
        fixture(&session, now, "backend-changed", 'b'),
        fixture(&session, now, "backend-original", 'e'),
    ] {
        assert_eq!(
            store.admit_bound_keyed_run(changed).await.unwrap_err(),
            StoreError::AdmissionConflict
        );
    }
    let mut inconsistent = original.clone();
    inconsistent.binding.provider = fixture(&session, now, "backend-changed", 'b')
        .binding
        .provider;
    assert_eq!(
        store.admit_bound_keyed_run(inconsistent).await.unwrap_err(),
        StoreError::AdmissionConflict
    );
    assert_eq!(store.session_revision(&session).await.unwrap(), revision);
    assert_eq!(
        store
            .execution_binding(&session, &receipt.run_id)
            .await
            .unwrap(),
        Some(retained.clone())
    );
    // Generated message identity, owner and time are not retry semantics.
    let mut retry = fixture(
        &session,
        now + time::Duration::seconds(1),
        "backend-original",
        'b',
    );
    retry.keyed.request.owner = "other-owner".into();
    assert!(
        matches!(store.admit_bound_keyed_run(retry.clone()).await.unwrap(), KeyedAdmitOutcome::Replayed(actual) if actual == receipt)
    );
    assert_eq!(store.session_revision(&session).await.unwrap(), revision);
    let execution = store.execution(admitted.fence.clone()).await.unwrap();
    execution.begin_admission_execution().await.unwrap();
    execution
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
    clock.set(now + time::Duration::days(1));
    assert!(
        matches!(store.admit_bound_keyed_run(retry).await.unwrap(), KeyedAdmitOutcome::Replayed(actual) if actual == receipt)
    );
    let encoded = serde_json::to_string(&receipt).unwrap();
    let debug = format!("{original:?} {retained:?}");
    for private in [
        "private-input",
        "private-system",
        "private-title",
        "private-owner",
        "backend-original",
        &admitted.fence.claim_token,
    ] {
        assert!(!debug.contains(private));
        assert!(!encoded.contains(private));
    }
    // A legacy receipt never acquires missing binding evidence on replay.
    let other = SessionId::new();
    let legacy = fixture(&other, now, "backend-original", 'b');
    store.admit_keyed_run(legacy.keyed.clone()).await.unwrap();
    assert_eq!(
        store.admit_bound_keyed_run(legacy).await.unwrap_err(),
        StoreError::AdmissionConflict
    );
    // Unkeyed admissions retain their binding in the same initial transaction.
    let other = SessionId::new();
    let mut unkeyed = fixture(&other, now, "backend-unkeyed", 'b');
    unkeyed.keyed.request.session_id = None;
    let result = store
        .admit_bound_run(BoundAdmitRequest {
            request: unkeyed.keyed.request,
            binding: unkeyed.binding.clone(),
        })
        .await
        .unwrap();
    assert_eq!(
        store
            .execution_binding(&other, &result.run.id)
            .await
            .unwrap()
            .unwrap()
            .capsule,
        unkeyed.binding
    );
    assert_eq!(
        store
            .lookup_admission(&other, &original.keyed.options.key)
            .await
            .unwrap(),
        None
    );
    // Invalid requested caps are rejected before accepting ordinary input, even
    // when the caller recomputes a consistent configuration hash.
    for cap in [0, 65] {
        let absent = SessionId::new();
        let mut invalid = fixture(&absent, now, "backend-original", 'b');
        invalid.keyed.request.session_id = None;
        invalid.binding.request.max_output_tokens = Some(cap);
        invalid.binding.runtime_semantics[4] = json!(cap);
        invalid.binding.config_hash =
            InputFingerprint::new(admission_config_hash(&invalid.binding.runtime_semantics))
                .unwrap();
        invalid.keyed.request.config_hash = invalid.binding.config_hash.as_str().into();
        assert_eq!(
            store
                .admit_bound_run(BoundAdmitRequest {
                    request: invalid.keyed.request,
                    binding: invalid.binding
                })
                .await
                .unwrap_err(),
            StoreError::AdmissionConflict
        );
        assert_eq!(store.get_session(&absent).await.unwrap(), None);
    }
    // Invalid bound input cannot create a session, input, receipt or owner.
    let absent = SessionId::new();
    let mut invalid = fixture(&absent, now, "backend-original", 'b');
    invalid.binding.version = 99;
    assert_eq!(
        store.admit_bound_keyed_run(invalid).await.unwrap_err(),
        StoreError::AdmissionConflict
    );
    assert_eq!(store.get_session(&absent).await.unwrap(), None);
}

#[cfg(test)]
pub(crate) mod faults;

#[cfg(all(test, any(feature = "sqlite", feature = "postgres")))]
pub(crate) mod process;
