use super::super::{MemoryStore, SessionId};
use crate::public_session::v2::proof::Proof;
use crate::{PublicSessionOutcomeV2, PublicSessionRequestV2, Store};

#[tokio::test]
async fn physical_purge_keeps_surviving_v2_snapshot_capture_coherent() {
    let store = MemoryStore::new();
    let target = SessionId::new();
    let survivor = SessionId::new();
    store
        .admit_run(crate::deletion::contract::admission(&target))
        .await
        .unwrap();
    store
        .admit_run(crate::deletion::contract::admission(&survivor))
        .await
        .unwrap();
    let captured = {
        let mut state = store.state.lock().unwrap();
        let mut message = state.messages.for_session(&survivor)[0].clone();
        message.id = crabber_core::MessageId::new();
        message.run_id = None;
        for part in &mut message.parts {
            part.id = crabber_core::PartId::new();
            part.message_id = message.id.clone();
        }
        state.messages.push(survivor.clone(), message.clone());
        message
    };
    let mut limits = crate::public_session::v2::tests::limits();
    limits.history.messages = 1;
    let mut request = PublicSessionRequestV2 {
        session_id: survivor.clone(),
        limits,
        continuation: None,
    };
    let PublicSessionOutcomeV2::Page(first) = store
        .public_session_snapshot_v2(request.clone())
        .await
        .unwrap()
    else {
        panic!("first page")
    };
    request.continuation = first.history.continuation.clone();
    assert!(request.continuation.is_some());
    {
        let mut state = store.state.lock().unwrap();
        super::purge::purge(
            &mut state,
            &target,
            &mut Proof::bounded(crate::deletion::coverage::MAX_PROOF_BYTES),
        )
        .unwrap();
        let mut after_capture = captured.clone();
        after_capture.id = crabber_core::MessageId::new();
        for part in &mut after_capture.parts {
            part.message_id = after_capture.id.clone();
            part.id = crabber_core::PartId::new();
        }
        state.messages.push(survivor.clone(), after_capture);
    }
    let PublicSessionOutcomeV2::Page(last) =
        store.public_session_snapshot_v2(request).await.unwrap()
    else {
        panic!("coherent continuation")
    };
    assert_eq!(last.history.messages, vec![captured]);
    assert_eq!(last.history.high_water, first.history.high_water);
    assert_eq!(last.captured_revision, first.captured_revision);
    assert!(last.history.continuation.is_none());
}
