//! Global-key rollback on the public legacy keyed Store path. Typed envelope
//! projection and original binding retention have their separate typed contract.
use super::{AdmissionReceipt, KeyedAdmitOutcome, KeyedAdmitRequest, Store, StoreError, request};
use crabber_core::{EventRecord, Message, Run, Session, SessionId, SessionRevision};
use time::OffsetDateTime;

#[derive(Debug, PartialEq)]
struct OriginalState {
    session: Option<Session>,
    messages: Vec<Message>,
    revision: SessionRevision,
    events: Vec<EventRecord>,
    run: Option<Run>,
    receipt: Option<AdmissionReceipt>,
}

async fn original_state<S: Store>(
    store: &S,
    original: &KeyedAdmitRequest,
    receipt: &AdmissionReceipt,
) -> OriginalState {
    let session = &receipt.session_id;
    let events = store.list_events(session, None, 64).await.unwrap();
    assert!(
        events.len() < 64,
        "fixture event inventory must be complete"
    );
    OriginalState {
        session: store.get_session(session).await.unwrap(),
        messages: store.list_all_messages(session).await.unwrap(),
        revision: store.session_revision(session).await.unwrap(),
        events,
        run: store.get_run(&receipt.run_id).await.unwrap(),
        receipt: store
            .lookup_admission(session, &original.options.key)
            .await
            .unwrap(),
    }
}

async fn unfinished<S: Store>(store: &S) -> Vec<Run> {
    let mut runs = store.list_unfinished_runs().await.unwrap();
    runs.sort_by(|left, right| left.id.0.cmp(&right.id.0));
    runs
}

async fn assert_absent<S: Store>(store: &S, attempted: &KeyedAdmitRequest) {
    let session = attempted.request.session_id.as_ref().unwrap();
    assert_eq!(store.get_session(session).await.unwrap(), None);
    assert_eq!(
        store.session_revision(session).await.unwrap_err(),
        StoreError::NotFound
    );
    assert_eq!(
        store.list_all_messages(session).await.unwrap(),
        Vec::<Message>::new()
    );
    assert_eq!(
        store.list_events(session, None, 64).await.unwrap(),
        Vec::<EventRecord>::new()
    );
    assert_eq!(
        store
            .lookup_admission(session, &attempted.options.key)
            .await
            .unwrap(),
        None
    );
}

enum Collision {
    Message,
    Part,
}

pub(super) async fn run<S: Store>(
    store: &S,
    original: &KeyedAdmitRequest,
    receipt: &AdmissionReceipt,
    now: OffsetDateTime,
) {
    let retained = original_state(store, original, receipt).await;
    for collision in [Collision::Message, Collision::Part] {
        let session = SessionId::new();
        let mut attempted = request(&session, now);
        match collision {
            Collision::Message => {
                attempted
                    .request
                    .user_message
                    .id
                    .clone_from(&original.request.user_message.id);
                attempted.request.user_message.parts[0]
                    .message_id
                    .clone_from(&original.request.user_message.id);
                assert_ne!(
                    attempted.request.user_message.parts[0].id,
                    original.request.user_message.parts[0].id
                );
            }
            Collision::Part => {
                attempted.request.user_message.parts[0]
                    .id
                    .clone_from(&original.request.user_message.parts[0].id);
                assert_ne!(
                    attempted.request.user_message.id,
                    original.request.user_message.id
                );
            }
        }
        assert_ne!(session, receipt.session_id);
        let before_runs = unfinished(store).await;
        assert_eq!(
            store.admit_keyed_run(attempted.clone()).await.unwrap_err(),
            StoreError::Conflict
        );
        assert_absent(store, &attempted).await;
        assert_eq!(unfinished(store).await, before_runs);
        assert_eq!(original_state(store, original, receipt).await, retained);

        assert_unique_admission(store, &session, now, &before_runs).await;
        assert_eq!(original_state(store, original, receipt).await, retained);
    }
}

async fn assert_unique_admission<S: Store>(
    store: &S,
    session: &SessionId,
    now: OffsetDateTime,
    before_runs: &[Run],
) {
    // After checking the definitive native rejection, a new unique input can
    // use B's still-unaccepted key. This is not an unknown-outcome ID rewrite.
    let fresh = request(session, now);
    let KeyedAdmitOutcome::Started {
        receipt: accepted,
        admitted,
    } = store.admit_keyed_run(fresh.clone()).await.unwrap()
    else {
        panic!("unique cross-session IDs must admit after rolled-back collision")
    };
    assert_eq!(&accepted.session_id, session);
    assert_eq!(accepted.run_id, admitted.run.id);
    assert_eq!(accepted.user_message_id, fresh.request.user_message.id);
    assert_eq!(
        store.get_session(session).await.unwrap(),
        Some(admitted.session)
    );
    assert_eq!(
        store.get_run(&accepted.run_id).await.unwrap(),
        Some(admitted.run)
    );
    assert_eq!(
        store
            .lookup_admission(session, &fresh.options.key)
            .await
            .unwrap(),
        Some(accepted.clone())
    );
    let mut expected_message = fresh.request.user_message;
    expected_message.run_id = Some(accepted.run_id.clone());
    assert_eq!(
        store.list_all_messages(session).await.unwrap(),
        vec![expected_message]
    );
    let after_runs = unfinished(store).await;
    assert_eq!(after_runs.len(), before_runs.len() + 1);
    assert!(before_runs.iter().all(|run| after_runs.contains(run)));
    assert_eq!(
        after_runs
            .iter()
            .filter(|run| &run.session_id == session)
            .count(),
        1
    );
}
