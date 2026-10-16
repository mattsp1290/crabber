use super::super::*;
use crate::Store;
use crate::public_session::v2::proof::{Failure, Proof};

#[tokio::test]
async fn physical_purge_preserves_survivors_and_monotonic_cursors() {
    let store = MemoryStore::new();
    let target = SessionId::new();
    let survivor = SessionId::new();
    let first = store
        .admit_run(crate::deletion::contract::admission(&target))
        .await
        .unwrap();
    let second = store
        .admit_run(crate::deletion::contract::admission(&survivor))
        .await
        .unwrap();
    notice(&store, &first).await;
    notice(&store, &second).await;
    notice(&store, &first).await;
    let mut state = store.state.lock().unwrap();
    let events: Vec<_> = state
        .events
        .iter()
        .filter(|event| event.session_id == survivor)
        .cloned()
        .collect();
    let messages = state.messages.for_session(&survivor).to_vec();
    let surviving_run = state.runs[&second.fence.run_id].clone();
    let next = state.events.next_cursor().unwrap();
    let registry = ToolCallId::new();
    state.imported_call_ids.insert(registry.clone());
    let mut proof = Proof::bounded(crate::deletion::coverage::MAX_PROOF_BYTES);
    super::purge::purge(&mut state, &target, &mut proof).unwrap();
    assert!(!state.sessions.contains_key(&target));
    assert!(!state.runs.contains_key(&first.fence.run_id));
    assert!(state.runs.contains_key(&second.fence.run_id));
    assert!(state.receipts.keys().all(|(session, _)| session != &target));
    assert_eq!(state.runs[&second.fence.run_id], surviving_run);
    assert_eq!(state.messages.for_session(&survivor), messages);
    assert_eq!(state.events.iter().cloned().collect::<Vec<_>>(), events);
    assert_eq!(state.events.next_cursor().unwrap(), next);
    assert!(state.imported_call_ids.contains(&registry));
    let mut event = state.events.last().unwrap().clone();
    event.cursor = Some(next);
    state.events.append(event).unwrap();
    assert_eq!(state.events.next_cursor().unwrap().0, next.0 + 1);
    assert!(proof.used() > 0);
}

#[tokio::test]
async fn physical_purge_refuses_budget_or_cross_scope_before_any_mutation() {
    let store = MemoryStore::new();
    let target = SessionId::new();
    let admission = store
        .admit_run(crate::deletion::contract::admission(&target))
        .await
        .unwrap();
    let mut state = store.state.lock().unwrap();
    let before = state.events.iter().cloned().collect::<Vec<_>>();
    assert!(matches!(
        super::purge::purge(&mut state, &target, &mut Proof::bounded(1)),
        Err(Failure::Limited(_))
    ));
    assert!(state.sessions.contains_key(&target));
    assert_eq!(state.events.iter().cloned().collect::<Vec<_>>(), before);
    let message = state
        .messages
        .iter_mut()
        .find(|message| message.run_id.as_ref() == Some(&admission.fence.run_id))
        .unwrap();
    message.session_id = SessionId::new();
    assert!(matches!(
        super::purge::purge(
            &mut state,
            &target,
            &mut Proof::bounded(crate::deletion::coverage::MAX_PROOF_BYTES)
        ),
        Err(Failure::Error(_))
    ));
    assert!(state.sessions.contains_key(&target));
    assert!(state.runs.contains_key(&admission.fence.run_id));
    assert_eq!(state.events.iter().cloned().collect::<Vec<_>>(), before);
}

async fn notice(store: &MemoryStore, admitted: &crate::AdmitOutcome) {
    store
        .execution(admitted.fence.clone())
        .await
        .unwrap()
        .append_event(crabber_core::EventRecord {
            cursor: None,
            session_id: admitted.session.id.clone(),
            run_id: admitted.run.id.clone(),
            turn_id: None,
            kind: crabber_core::EventKind::ExtensionNotice,
            payload: serde_json::json!({"fixture": "purge routing"}),
            correlation: None,
            live_only: false,
            created_at: time::OffsetDateTime::UNIX_EPOCH,
        })
        .await
        .unwrap();
}
