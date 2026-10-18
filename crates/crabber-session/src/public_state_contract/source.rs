//! Actual native credentials, atomic RFC6902 effects, and metadata-only retries.
use super::*;
use crate::{
    AttemptCommand, AttemptMutation, AttemptMutationOutcome, PublicSourceAuthority as Authority,
    PublicSourceMutation,
};
use crabber_core::{PublicPatchOperation, PublicSourceChange as Change, PublicSourceEvent};
fn patch(value: serde_json::Value) -> PublicPatchOperation {
    PublicPatchOperation::new(value).unwrap()
}
async fn publish(
    store: &impl Store,
    session: &SessionId,
    authority: &crate::AttemptAuthority,
    change: Change,
) -> crate::PublicStateReceipt {
    store
        .mutate_bound_public_source(
            PublicSourceMutation {
                id: ControlId::new(),
                expected_revision: store.session_revision(session).await.unwrap(),
                change,
            },
            Authority::Attempt(authority.clone()),
        )
        .await
        .unwrap()
}
pub(crate) async fn run(store: &impl Store) {
    let (admitted, execution, authority, _claim) = crate::phase_contract::fixture(store).await;
    let session = &admitted.session.id;
    let (request, receipt) = delta_and_replay(
        store, session, &authority, &admitted.session.workspace_id,
    ).await;
    reject_atomic_patch(store, session, &authority).await;
    activity(store, session, &authority).await;
    publish(
        store,
        session,
        &authority,
        Change::StateSnapshot {
            state: serde_json::Value::Null,
        },
    )
    .await;
    assert!(
        store
            .public_state(session)
            .await
            .unwrap()
            .contents
            .state()
            .is_null()
    );
    let finished = execution
        .mutate_attempt(
            AttemptMutation {
                id: ControlId::new(),
                expected_revision: store.session_revision(session).await.unwrap(),
                command: AttemptCommand::Finish {
                    target: authority.snapshot.target.clone(),
                    outcome: crate::AttemptFinish::Suspended,
                },
            },
            Some(authority.clone()),
        )
        .await
        .unwrap();
    assert!(matches!(finished, AttemptMutationOutcome::Applied { .. }));
    let replay = store
        .mutate_bound_public_source(request.clone(), Authority::Attempt(authority.clone()))
        .await
        .unwrap();
    assert_eq!(
        replay, receipt,
        "committed metadata survives later attempt/revision advancement"
    );
    let mut stale = request;
    stale.id = ControlId::new();
    stale.expected_revision = store.session_revision(session).await.unwrap();
    assert_eq!(
        store
            .mutate_bound_public_source(stale, Authority::Attempt(authority))
            .await
            .unwrap_err(),
        Error::StaleOwner
    );
}
async fn delta_and_replay(
    store: &impl Store,
    session: &SessionId,
    authority: &crate::AttemptAuthority,
    workspace: &str,
) -> (PublicSourceMutation, crate::PublicStateReceipt) {
    publish(
        store,
        session,
        authority,
        Change::StateSnapshot {
            state: json!({"nested":{"value":null},"array":[1,2]}),
        },
    )
    .await;
    let before = store.public_state(session).await.unwrap();
    let request = PublicSourceMutation {
        id: ControlId::new(),
        expected_revision: before.revision,
        change: Change::StateDelta {
            delta: vec![
                patch(
                    json!({"op":"replace","path":"/nested/value","value":null,"extension":{"n":null}}),
                ),
                patch(json!({"op":"remove","path":"/array/0","value":"legal-extra"})),
            ],
        },
    };
    let receipt = store
        .mutate_bound_public_source(request.clone(), Authority::Attempt(authority.clone()))
        .await
        .unwrap();
    let after = store.public_state(session).await.unwrap();
    assert_eq!(
        after.contents.state(),
        &json!({"nested":{"value":null},"array":[2]})
    );
    let events = store.list_events(session, None, 100).await.unwrap();
    let delta = events
        .iter()
        .find(|event| event.cursor == Some(receipt.event_cursor))
        .unwrap();
    let source: PublicSourceEvent = serde_json::from_value(delta.payload.clone()).unwrap();
    assert_eq!(source.change, request.change);
    assert_eq!(source.source.workspace_id, workspace);
    assert_eq!(source.source.attempt.id, authority.snapshot.target.id);
    let prior = events
        .iter()
        .find(|event| event.cursor == source.before_event_cursor)
        .unwrap();
    assert_eq!(
        prior.payload["contents"],
        serde_json::to_value(&before.contents).unwrap()
    );
    assert_eq!(source.before, Some(before.contents));
    assert_eq!(
        store
            .mutate_bound_public_source(request.clone(), Authority::Attempt(authority.clone()))
            .await
            .unwrap(),
        receipt
    );
    assert_eq!(store.list_events(session, None, 100).await.unwrap(), events);
    (request, receipt)
}
async fn reject_atomic_patch(
    store: &impl Store,
    session: &SessionId,
    authority: &crate::AttemptAuthority,
) {
    let before = store.public_state(session).await.unwrap();
    let history = store.list_events(session, None, 100).await.unwrap();
    let bad = PublicSourceMutation {
        id: ControlId::new(),
        expected_revision: before.revision,
        change: Change::StateDelta {
            delta: vec![
                patch(json!({"op":"add","path":"/tentative","value":1})),
                patch(json!({"op":"test","path":"/array/0","value":999})),
            ],
        },
    };
    let result = store
        .mutate_bound_public_source(bad.clone(), Authority::Attempt(authority.clone()))
        .await;
    assert_eq!(result.unwrap_err(), Error::InvalidRequest);
    assert_eq!(store.public_state(session).await.unwrap(), before);
    assert_eq!(
        store.list_events(session, None, 100).await.unwrap(),
        history
    );
    assert_eq!(
        store
            .lookup_public_state_mutation(session, &bad.id)
            .await
            .unwrap(),
        None
    );
}
async fn activity(store: &impl Store, session: &SessionId, authority: &crate::AttemptAuthority) {
    let state = store
        .public_state(session)
        .await
        .unwrap()
        .contents
        .state()
        .clone();
    publish(
        store,
        session,
        authority,
        Change::ActivitySnapshot {
            activity: PublicActivity::new("progress", "first", json!({"count":0,"null":null}))
                .unwrap(),
        },
    )
    .await;
    publish(
        store,
        session,
        authority,
        Change::ActivityDelta {
            activity_id: "progress".into(),
            activity_type: "first".into(),
            delta: vec![patch(
                json!({"op":"replace","path":"/count","value":1,"from":"legal-extra"}),
            )],
        },
    )
    .await;
    assert_eq!(
        store
            .public_state(session)
            .await
            .unwrap()
            .contents
            .activities()[0]
            .content(),
        &json!({"count":1,"null":null})
    );
    publish(
        store,
        session,
        authority,
        Change::ActivitySnapshot {
            activity: PublicActivity::new("progress", "second", json!({"count":2})).unwrap(),
        },
    )
    .await;
    let before = store.public_state(session).await.unwrap();
    assert_eq!(before.contents.state(), &state);
    let wrong = PublicSourceMutation {
        id: ControlId::new(),
        expected_revision: before.revision,
        change: Change::ActivityDelta {
            activity_id: "progress".into(),
            activity_type: "first".into(),
            delta: vec![],
        },
    };
    assert_eq!(
        store
            .mutate_bound_public_source(wrong, Authority::Attempt(authority.clone()))
            .await
            .unwrap_err(),
        Error::InvalidRequest
    );
    assert_eq!(store.public_state(session).await.unwrap(), before);
}
pub(crate) async fn faults(store: &impl Store, inject: impl Fn(FaultPoint)) {
    let (admitted, _execution, authority, _claim) = crate::phase_contract::fixture(store).await;
    let session = &admitted.session.id;
    let before = store.public_state(session).await.unwrap();
    let events = store.list_events(session, None, 100).await.unwrap();
    let request = PublicSourceMutation {
        id: ControlId::new(),
        expected_revision: before.revision,
        change: Change::StateDelta {
            delta: vec![patch(
                json!({"op":"replace","path":"","value":{"committed":null}}),
            )],
        },
    };
    inject(FaultPoint::BeforeCommit);
    assert_eq!(
        store
            .mutate_bound_public_source(request.clone(), Authority::Attempt(authority.clone()))
            .await
            .unwrap_err(),
        Error::UnknownStoreOutcome
    );
    inject(FaultPoint::Off);
    assert_eq!(store.public_state(session).await.unwrap(), before);
    assert_eq!(store.list_events(session, None, 100).await.unwrap(), events);
    assert_eq!(
        store
            .lookup_public_state_mutation(session, &request.id)
            .await
            .unwrap(),
        None
    );
    inject(FaultPoint::AfterCommit);
    assert_eq!(
        store
            .mutate_bound_public_source(request.clone(), Authority::Attempt(authority.clone()))
            .await
            .unwrap_err(),
        Error::UnknownStoreOutcome
    );
    inject(FaultPoint::Off);
    let receipt = store
        .lookup_public_state_mutation(session, &request.id)
        .await
        .unwrap()
        .unwrap();
    let committed = store.list_events(session, None, 100).await.unwrap();
    assert_eq!(committed.len(), events.len() + 2);
    assert_eq!(
        store
            .mutate_bound_public_source(request, Authority::Attempt(authority))
            .await
            .unwrap(),
        receipt
    );
    assert_eq!(
        store.list_events(session, None, 100).await.unwrap(),
        committed
    );
}
