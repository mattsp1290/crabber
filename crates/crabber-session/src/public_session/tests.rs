use crate::{
    PublicResyncReason, PublicSessionLimit, PublicSessionLimits, PublicSessionOutcome,
    PublicSessionPoll, PublicSessionRequest, PublicSessionSubscription, SnapshotLimits, Store,
};
use crabber_core::{Clock, ControlId, ManualClock, SessionId};
pub(crate) fn limits() -> PublicSessionLimits {
    PublicSessionLimits {
        header_bytes: 64 * 1024,
        proof_bytes: 2 * 1024 * 1024,
        interactions: 64,
        history: SnapshotLimits {
            messages: 64,
            tool_calls: 64,
            parts: 1024,
            text_bytes: 1024 * 1024,
            encoded_bytes: 2 * 1024 * 1024,
        },
    }
}
pub(crate) fn request(session_id: SessionId) -> PublicSessionRequest {
    PublicSessionRequest {
        session_id,
        limits: limits(),
        continuation: None,
    }
}
pub(super) async fn page(
    store: &impl Store,
    request: PublicSessionRequest,
) -> crate::PublicSessionPage {
    let PublicSessionOutcome::Page(page) = store.public_session_snapshot(request).await.unwrap()
    else {
        panic!("complete public page");
    };
    *page
}
pub(crate) async fn run(store: &impl Store, clock: &ManualClock) {
    empty(store).await;
    approval(store, clock).await;
    polling(store, clock).await;
    pagination(store).await;
    super::races::run(store).await;
    super::rounds::run(store, clock).await;
}
async fn empty(store: &impl Store) {
    let id = SessionId::new();
    let created = store
        .manage_session(crate::ManageSession {
            id: ControlId::new(),
            session_id: id.clone(),
            expected_revision: crabber_core::SessionRevision::INITIAL,
            change: crate::SessionChange::Create {
                workspace_id: "public-workspace".into(),
                directory: "/public/workspace".into(),
                title: "empty".into(),
                defaults: crate::SessionDefaults::default(),
            },
        })
        .await
        .unwrap();
    let result = page(store, request(id.clone())).await;
    assert_eq!(result.captured_revision, created.revision);
    assert!(result.run.is_none());
    assert!(result.header.attempt.is_none());
    assert!(result.header.approval.is_none());
    assert!(result.header.barrier.is_none());
    assert_eq!(result.history.messages, [] as [crabber_core::Message; 0]);
    assert!(result.history.continuation.is_none());
    assert_eq!(result.header.session.title, "empty");
    let mut small = request(id.clone());
    small.limits.header_bytes = 1;
    assert!(matches!(
        store.public_session_snapshot(small).await.unwrap(),
        PublicSessionOutcome::Limited {
            limit: PublicSessionLimit::HeaderBytes,
            ..
        }
    ));
    assert_eq!(store.session_revision(&id).await.unwrap(), created.revision);
}
async fn approval(store: &impl Store, clock: &ManualClock) {
    let f = crate::approval_contract::fixture::staged(store, clock).await;
    let before = page(store, request(f.run.session_id.clone())).await;
    assert_eq!(
        before.header.attempt.unwrap().state,
        crate::AttemptState::Started
    );
    f.execution
        .mutate_public_state(crate::PublicStateMutation {
            id: ControlId::new(),
            expected_revision: store.session_revision(&f.run.session_id).await.unwrap(),
            contents: crabber_core::PublicStateContents::new(serde_json::Value::Null, vec![])
                .unwrap(),
        })
        .await
        .unwrap();
    // Public state is a mutation, so recapture the suspension CAS.
    let mut batch = f.batch.clone();
    batch.expected_revision = store.session_revision(&f.run.session_id).await.unwrap();
    f.execution
        .suspend_for_approval(batch, f.owner.clone())
        .await
        .unwrap();
    let result = page(store, request(f.run.session_id.clone())).await;
    assert_eq!(
        result.run.as_ref().unwrap().status,
        crabber_core::RunStatus::Paused
    );
    assert_eq!(
        result.header.attempt.as_ref().unwrap().state,
        crate::AttemptState::Suspended
    );
    assert_eq!(
        result.header.barrier.as_ref().unwrap().continuation,
        f.batch.continuation
    );
    assert_eq!(
        result.header.approval.as_ref().unwrap().interactions.len(),
        2
    );
    assert!(result.header.contents.state().is_null());
    assert!(result.header.read_usage.proof_bytes > 0);
    assert!(result.header.read_usage.encoded_bytes <= limits().header_bytes);
    let encoded = serde_json::to_string(&result).unwrap();
    for secret in [
        "PRIVATE_PROMPT",
        "host-verified",
        "claim_token",
        "checkpoint",
        "system_prompt",
        "authority_claim",
    ] {
        assert!(!encoded.contains(secret), "public header leaked {secret}");
    }
    let mut small = request(f.run.session_id.clone());
    small.limits.proof_bytes = 1;
    assert!(matches!(
        store.public_session_snapshot(small).await.unwrap(),
        PublicSessionOutcome::Limited {
            limit: PublicSessionLimit::ProofBytes,
            ..
        }
    ));
    let mut small = request(f.run.session_id.clone());
    small.limits.interactions = 1;
    assert!(matches!(
        store.public_session_snapshot(small).await.unwrap(),
        PublicSessionOutcome::Limited {
            limit: PublicSessionLimit::Interactions,
            ..
        }
    ));
    let answer =
        crate::approval_contract::fixture::answer(store, &f, 0, crate::ApprovalReply::Grant).await;
    store.answer_approval(answer).await.unwrap();
    let result = page(store, request(f.run.session_id.clone())).await;
    assert_eq!(
        result.header.approval.unwrap().interactions[0].state,
        crabber_core::InteractionState::Answered
    );
    store
        .terminate_approval(crate::approval_contract::termination::request(store, &f, false).await)
        .await
        .unwrap();
    let result = page(store, request(f.run.session_id.clone())).await;
    assert!(result.is_terminal());
    let mut attached = PublicSessionSubscription::new(f.run.session_id.clone(), limits());
    assert!(
        matches!(attached.poll(store).await.unwrap(), PublicSessionPoll::Page {page,..} if page.is_terminal())
    );
    assert!(result.header.barrier.is_none());
    assert_eq!(
        result.header.approval.unwrap().state,
        crate::ApprovalContinuationState::Released
    );
}
async fn polling(store: &impl Store, clock: &ManualClock) {
    let f = crate::approval_contract::fixture::staged(store, clock).await;
    let mut poller = PublicSessionSubscription::new(f.run.session_id.clone(), limits());
    let PublicSessionPoll::Page { reset: true, page } = poller.poll(store).await.unwrap() else {
        panic!("initial attachment");
    };
    assert!(page.history.continuation.is_none());
    assert!(matches!(
        poller.poll(store).await.unwrap(),
        PublicSessionPoll::Unchanged { .. }
    ));
    store
        .manage_session(crate::ManageSession {
            id: ControlId::new(),
            session_id: f.run.session_id.clone(),
            expected_revision: store.session_revision(&f.run.session_id).await.unwrap(),
            change: crate::SessionChange::Rename {
                title: "from another controller".into(),
            },
        })
        .await
        .unwrap();
    // No notification hint is needed to discover the committed change.
    assert!(matches!(
        poller.poll(store).await.unwrap(),
        PublicSessionPoll::Resync {
            reason: PublicResyncReason::RevisionChanged
        }
    ));
    assert!(matches!(
        poller.poll(store).await.unwrap(),
        PublicSessionPoll::Page { reset: true, .. }
    ));
    poller.overflow();
    assert!(matches!(
        poller.poll(store).await.unwrap(),
        PublicSessionPoll::Resync {
            reason: PublicResyncReason::Overflow
        }
    ));
    assert!(matches!(
        poller.poll(store).await.unwrap(),
        PublicSessionPoll::Page { reset: true, .. }
    ));
    let _ = clock.now();
}
#[tokio::test]
async fn memory_public_session_contract() {
    let clock = std::sync::Arc::new(ManualClock::new(time::OffsetDateTime::UNIX_EPOCH));
    let store = crate::MemoryStore::with_clock(clock.clone());
    run(&store, &clock).await;
}

async fn pagination(store: &impl Store) {
    let (session, message) = crate::public_history::tests::seed(store).await;
    let mut request = request(session.clone());
    request.limits.history.messages = 1;
    let first = page(store, request.clone()).await;
    assert!(first.history.continuation.is_some());
    request.continuation = first.history.continuation.clone();
    let second = page(store, request.clone()).await;
    assert_eq!(second.captured_revision, first.captured_revision);
    assert_eq!(second.high_water(), first.high_water());
    assert_eq!(second.history.messages[0].id, message.id);
    let encoded = serde_json::to_string(&second).unwrap();
    for secret in [
        "private-codec",
        "private-payload",
        "private-reasoning",
        "private-state",
        "private-kind",
        "private-system",
        "private-owner",
    ] {
        assert!(!encoded.contains(secret), "leaked {secret}");
    }
    assert!(second.history.continuation.is_none());
    let mut tiny = limits();
    tiny.history.messages = 1;
    let mut poller = PublicSessionSubscription::new(session.clone(), tiny);
    assert!(matches!(
        poller.poll(store).await.unwrap(),
        PublicSessionPoll::Page { reset: true, .. }
    ));
    store
        .manage_session(crate::ManageSession {
            id: ControlId::new(),
            session_id: session.clone(),
            expected_revision: store.session_revision(&session).await.unwrap(),
            change: crate::SessionChange::Rename {
                title: "between pages".into(),
            },
        })
        .await
        .unwrap();
    assert!(matches!(
        store.public_session_snapshot(request).await.unwrap(),
        PublicSessionOutcome::Invalidated
    ));
    assert!(matches!(
        poller.poll(store).await.unwrap(),
        PublicSessionPoll::Resync {
            reason: PublicResyncReason::RevisionChanged
        }
    ));
    assert!(matches!(
        poller.poll(store).await.unwrap(),
        PublicSessionPoll::Page { reset: true, .. }
    ));
    poller.overflow();
    assert!(matches!(
        poller.poll(store).await.unwrap(),
        PublicSessionPoll::Resync {
            reason: PublicResyncReason::Overflow
        }
    ));
    assert!(matches!(
        poller.poll(store).await.unwrap(),
        PublicSessionPoll::Page { reset: true, .. }
    ));
    assert!(matches!(
        poller.poll(store).await.unwrap(),
        PublicSessionPoll::Page { reset: false, .. }
    ));
    let mut tiny = limits();
    tiny.header_bytes = 1;
    poller.set_limits(tiny);
    assert!(matches!(
        poller.poll(store).await.unwrap(),
        PublicSessionPoll::Limited {
            limit: PublicSessionLimit::HeaderBytes
        }
    ));
    poller.set_limits(limits());
    assert!(matches!(
        poller.poll(store).await.unwrap(),
        PublicSessionPoll::Page { reset: true, .. }
    ));
}

#[cfg(any(feature = "postgres", feature = "sqlite"))]
pub(crate) async fn cross_connection(reader: &impl Store, writer: &impl Store) {
    let session = SessionId::new();
    let created = writer
        .manage_session(crate::ManageSession {
            id: ControlId::new(),
            session_id: session.clone(),
            expected_revision: crabber_core::SessionRevision::INITIAL,
            change: crate::SessionChange::Create {
                workspace_id: "cross-public".into(),
                directory: "/public".into(),
                title: "before reopen".into(),
                defaults: crate::SessionDefaults::default(),
            },
        })
        .await
        .unwrap();
    let mut poller = PublicSessionSubscription::new(session.clone(), limits());
    assert!(
        matches!(poller.poll(reader).await.unwrap(),PublicSessionPoll::Page{page,..} if page.header.session.title=="before reopen")
    );
    writer
        .manage_session(crate::ManageSession {
            id: ControlId::new(),
            session_id: session.clone(),
            expected_revision: created.revision,
            change: crate::SessionChange::Rename {
                title: "after reopen".into(),
            },
        })
        .await
        .unwrap();
    assert!(matches!(
        poller.poll(reader).await.unwrap(),
        PublicSessionPoll::Resync {
            reason: PublicResyncReason::RevisionChanged
        }
    ));
    assert!(
        matches!(poller.poll(reader).await.unwrap(),PublicSessionPoll::Page{page,..} if page.header.session.title=="after reopen")
    );
}
