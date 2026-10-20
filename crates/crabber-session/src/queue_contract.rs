//! Shared native FIFO, authority, revision and receipt qualification.
#[cfg(any(feature = "postgres", feature = "sqlite"))]
#[path = "queue_contract/process.rs"]
pub(crate) mod process;
use crate::{queue::*, *};
use crabber_core::{
    AdmissionKey, Clock, ControlId, InputEnvelope, ManualClock, QueueId, SessionId, SessionRevision,
};
use std::{sync::Arc, time::Duration};
use time::OffsetDateTime;
#[derive(Clone)]
pub(crate) struct Fixture {
    pub session: SessionId,
    pub queue: QueueId,
    pub prepared: BoundKeyedAdmitRequest,
    pub enqueue: QueueMutation,
}
pub(crate) async fn fixture(store: &dyn Store) -> Fixture {
    let session = SessionId::new();
    store
        .manage_session(ManageSession {
            session_id: session.clone(),
            id: ControlId::new(),
            expected_revision: SessionRevision::INITIAL,
            change: SessionChange::Create {
                workspace_id: "bound-workspace".into(),
                directory: "/bound/workspace".into(),
                title: "queue".into(),
                defaults: SessionDefaults::default(),
            },
        })
        .await
        .unwrap();
    entry(store, session).await
}
pub(crate) async fn entry(store: &dyn Store, session: SessionId) -> Fixture {
    let revision = store.session_revision(&session).await.unwrap();
    let input = InputEnvelope::text("PRIVATE_QUEUE_INPUT", revision).unwrap();
    let mut prepared = crate::typed_admission_contract::prepare(
        crate::execution_binding_contract::fixture(
            &session,
            OffsetDateTime::UNIX_EPOCH,
            "queue-backend",
            'b',
        ),
        input.clone(),
        OffsetDateTime::UNIX_EPOCH,
    );
    prepared.keyed.options.key = AdmissionKey::new(ControlId::new().0).unwrap();
    let queue = QueueId::new();
    let enqueue = QueueMutation {
        session_id: session.clone(),
        id: ControlId::new(),
        expected_revision: revision,
        action: QueueAction::Enqueue {
            queue_id: queue.clone(),
            input: Box::new(QueueInput {
                request: prepared.binding.request.clone(),
                input,
                admission: prepared.keyed.options.clone(),
            }),
        },
    };
    Fixture {
        session,
        queue,
        prepared,
        enqueue,
    }
}
pub(crate) async fn mutation(
    store: &dyn Store,
    fixture: &Fixture,
    action: QueueAction,
) -> QueueMutation {
    QueueMutation {
        session_id: fixture.session.clone(),
        id: ControlId::new(),
        expected_revision: store.session_revision(&fixture.session).await.unwrap(),
        action,
    }
}
pub(crate) async fn claim(store: &dyn Store, fixture: &Fixture) -> QueueAuthority {
    let request = mutation(
        store,
        fixture,
        QueueAction::Claim {
            owner: "queue-host".into(),
            lease: Duration::from_secs(30),
        },
    )
    .await;
    match store.mutate_queue(request).await.unwrap() {
        QueueOutcome::Applied {
            authority: Some(authority),
            admitted: None,
            ..
        } => authority,
        other => panic!("claim must grant once: {other:?}"),
    }
}
pub(crate) use journey::run;
pub(crate) use limits::{
    bounds, byte_bound, domains_archive_and_reserved_keys, faults, lazy_empty_scope,
};
#[path = "queue_contract/journey.rs"]
mod journey;
#[path = "queue_contract/limits.rs"]
mod limits;

#[path = "queue_contract/corruption.rs"]
pub(crate) mod corruption;

#[path = "queue_contract/publications.rs"]
pub(crate) mod publications;
