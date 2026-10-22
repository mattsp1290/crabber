#[path = "contexts.rs"]
mod contexts;
#[path = "model_controls.rs"]
mod model_controls;
#[path = "races.rs"]
mod races;
use super::{fixture::*, store::TestStore};
use crabber::{Admission, Agent, QueueControl, RuntimeError, SessionContext, core::*, session::*};
use std::{
    sync::{Arc, atomic::Ordering},
    time::Duration,
};
async fn claim(host: &Agent, native: &dyn Store, session: &SessionId) -> QueueAuthority {
    let request = QueueMutation {
        session_id: session.clone(),
        id: ControlId::new(),
        expected_revision: native.session_revision(session).await.unwrap(),
        action: QueueAction::Claim {
            owner: "public-host".into(),
            lease: Duration::from_secs(30),
        },
    };
    match host.mutate_queue(request.clone()).await.unwrap() {
        QueueOutcome::Applied {
            authority: Some(authority),
            ..
        } => {
            assert!(matches!(
                host.mutate_queue(request).await.unwrap(),
                QueueOutcome::Replayed(_)
            ));
            authority
        }
        other => panic!("claim needs authority: {other:?}"),
    }
}
fn context(session: &SessionId) -> SessionContext {
    SessionContext::new(session.clone(), "defaults-workspace", "/defaults")
}
pub(super) async fn run(native: Arc<dyn Store>, clock: Arc<ManualClock>) {
    fallback_and_defaults(native.clone()).await;
    Box::pin(unknown_ack(native.clone(), &clock)).await;
    contexts::explicit_and_foreign_context(native.clone()).await;
    Box::pin(model_controls::retained(native.clone())).await;
    contexts::unsupported(native.clone()).await;
    races::defaults_cas(native.clone()).await;
    Box::pin(races::same_key_prepare(native.clone())).await;
    Box::pin(races::same_key_prepare_during_inspection(native.clone())).await;
    closed(native).await;
}
async fn enqueue(
    host: &Agent,
    native: &dyn Store,
    session: &SessionId,
) -> (QueueId, InputEnvelope, crabber::AdmissionOptions) {
    let input = InputEnvelope::text(
        "queued actual provider request",
        native.session_revision(session).await.unwrap(),
    )
    .unwrap();
    let queue = QueueId::new();
    let key = options();
    let id = ControlId::new();
    let receipt = host
        .enqueue_session_input_in(
            &context(session),
            input.clone(),
            queue.clone(),
            id.clone(),
            key.clone(),
        )
        .await
        .unwrap();
    assert_eq!(receipt.summary.state, QueueState::Queued);
    assert_eq!(
        receipt,
        host.enqueue_session_input_in(
            &context(session),
            input.clone(),
            queue.clone(),
            id,
            key.clone()
        )
        .await
        .unwrap()
    );
    (queue, input, key)
}
async fn initial_preparation(
    native: Arc<dyn Store>,
) -> (
    Probe,
    SessionId,
    QueueId,
    crabber::AdmissionOptions,
    QueueAuthority,
    BoundKeyedAdmitRequest,
) {
    let probe = Probe::new(3);
    let first = agent_with_selection(
        native.clone(),
        &probe,
        selection("enqueue-provider", "enqueue-model"),
    );
    let session = create(native.as_ref(), defaults(None, None)).await;
    let (queue, input, key) = enqueue(&first, native.as_ref(), &session).await;
    assert_eq!(probe.factories.load(Ordering::SeqCst), 0);
    assert_eq!(probe.fake.requests().len(), 0);
    drop(first);
    let preparing = agent_with_selection(
        native.clone(),
        &probe,
        selection("preparing-provider", "preparing-model"),
    );
    let authority = claim(&preparing, native.as_ref(), &session).await;
    let prepare = QueueControl {
        id: ControlId::new(),
        expected_revision: native.session_revision(&session).await.unwrap(),
        authority: authority.clone(),
    };
    preparing
        .prepare_queue_in(&context(&session), prepare.clone())
        .await
        .unwrap();
    let frozen = native
        .retained_queue(&session, &queue)
        .await
        .unwrap()
        .prepared
        .unwrap();
    assert_eq!(frozen.binding.provider.provider_id(), "preparing-provider");
    assert_eq!(frozen.binding.provider.model_id(), "preparing-model");
    assert_eq!(frozen.binding.input_envelope, Some(input));
    (probe, session, queue, key, authority, frozen)
}
async fn fallback_and_defaults(native: Arc<dyn Store>) {
    let (probe, session, _queue, key, authority, frozen) =
        initial_preparation(native.clone()).await;
    update(
        native.as_ref(),
        &session,
        defaults(Some("later-provider"), Some("later-model")),
    )
    .await;
    let dispatching = agent_with_selection(
        native.clone(),
        &probe,
        selection("later-fallback", "later-fallback-model"),
    );
    let admit = QueueControl {
        id: ControlId::new(),
        expected_revision: native.session_revision(&session).await.unwrap(),
        authority,
    };
    let admitted = dispatching
        .admit_queue_in(&context(&session), admit.clone(), None)
        .await
        .unwrap();
    let run = match admitted {
        Admission::Started { receipt, handle } => {
            handle.done().await.unwrap();
            receipt.run_id
        }
        Admission::Replayed(_) => panic!("first dispatch needs handle"),
    };
    assert_eq!(
        probe.fake.requests()[0].selection,
        selection("preparing-provider", "preparing-model")
    );
    assert_eq!(
        native
            .execution_binding(&session, &run)
            .await
            .unwrap()
            .unwrap()
            .capsule,
        frozen.binding
    );
    let effects = (
        probe.factories.load(Ordering::SeqCst),
        probe.resolutions.load(Ordering::SeqCst),
        probe.fake.requests().len(),
    );
    assert!(matches!(
        dispatching
            .admit_queue_in(&context(&session), admit, None)
            .await
            .unwrap(),
        Admission::Replayed(_)
    ));
    assert_eq!(
        effects,
        (
            probe.factories.load(Ordering::SeqCst),
            probe.resolutions.load(Ordering::SeqCst),
            probe.fake.requests().len()
        )
    );
    assert!(
        native
            .lookup_admission(&session, &key.key)
            .await
            .unwrap()
            .is_some()
    );
    // Changed stored defaults affect the next unprepared item on the same Agent.
    let (_, _, _) = enqueue(&dispatching, native.as_ref(), &session).await;
    let authority = claim(&dispatching, native.as_ref(), &session).await;
    let prepare = QueueControl {
        id: ControlId::new(),
        expected_revision: native.session_revision(&session).await.unwrap(),
        authority,
    };
    match dispatching
        .dispatch_queue_in(&context(&session), prepare, ControlId::new(), None)
        .await
        .unwrap()
    {
        Admission::Started { handle, .. } => {
            handle.done().await.unwrap();
        }
        Admission::Replayed(_) => panic!("new queue item must start"),
    }
    assert_eq!(
        probe.fake.requests()[1].selection,
        selection("later-provider", "later-model")
    );
}
async fn unknown_ack(native: Arc<dyn Store>, clock: &ManualClock) {
    for point in [1, 2] {
        let wrapped = Arc::new(TestStore::new(native.clone()));
        let probe = Probe::new(1);
        let host = agent(wrapped.clone(), &probe);
        let session = create(
            native.as_ref(),
            defaults(Some("stored-provider"), Some("stored-model")),
        )
        .await;
        let (queue, _, _) = enqueue(&host, native.as_ref(), &session).await;
        let authority = claim(&host, native.as_ref(), &session).await;
        let prepare = QueueControl {
            id: ControlId::new(),
            expected_revision: native.session_revision(&session).await.unwrap(),
            authority: authority.clone(),
        };
        if point == 1 {
            wrapped.queue_loss.store(1, Ordering::SeqCst);
            assert!(
                host.prepare_queue_in(&context(&session), prepare.clone())
                    .await
                    .is_err()
            );
        }
        let prepared = host
            .prepare_queue_in(&context(&session), prepare.clone())
            .await
            .unwrap();
        let factories = probe.factories.load(Ordering::SeqCst);
        assert_eq!(
            host.prepare_queue_in(&context(&session), prepare)
                .await
                .unwrap(),
            prepared
        );
        assert_eq!(probe.factories.load(Ordering::SeqCst), factories);
        assert_eq!(probe.fake.requests().len(), 0);
        if point == 1 {
            continue;
        }
        let admit = QueueControl {
            id: ControlId::new(),
            expected_revision: prepared.revision,
            authority,
        };
        wrapped.queue_loss.store(2, Ordering::SeqCst);
        let failed = host
            .admit_queue_in(&context(&session), admit.clone(), None)
            .await
            .err()
            .unwrap();
        let retained = failed.preparation_reconciliation().unwrap().clone();
        assert_eq!(probe.fake.requests().len(), 0);
        assert_eq!(probe.resolutions.load(Ordering::SeqCst), 0);
        update(
            native.as_ref(),
            &session,
            defaults(Some("changed-provider"), Some("changed-model")),
        )
        .await;
        let restarted = agent_with_selection(
            native.clone(),
            &probe,
            selection("changed-fallback", "changed-fallback-model"),
        );
        let before = (
            probe.factories.load(Ordering::SeqCst),
            probe.resolutions.load(Ordering::SeqCst),
        );
        assert!(matches!(
            restarted
                .admit_queue_in(&context(&session), admit.clone(), None)
                .await
                .unwrap(),
            Admission::Replayed(_)
        ));
        assert_eq!(
            before,
            (
                probe.factories.load(Ordering::SeqCst),
                probe.resolutions.load(Ordering::SeqCst)
            )
        );
        assert_eq!(
            native.get_queue(&session, &queue).await.unwrap().state,
            QueueState::Admitted
        );
        reconcile_retained(&host, &session, &retained, &probe).await;
        recover_expired(&restarted, clock, &session, &queue, &probe).await;
        host.close_extensions().await.unwrap();
        assert!(matches!(
            host.admit_queue_in(&context(&session), admit, None)
                .await
                .unwrap(),
            Admission::Replayed(_)
        ));
    }
}
async fn recover_expired(
    restarted: &Agent,
    clock: &ManualClock,
    session: &SessionId,
    queue: &QueueId,
    probe: &Probe,
) {
    clock.set(clock.now() + time::Duration::seconds(31));
    match restarted
        .recover_queue_in(&context(session), queue, None)
        .await
        .unwrap()
    {
        Admission::Started { handle, .. } => {
            handle.done().await.unwrap();
        }
        Admission::Replayed(_) => panic!("explicit Unstarted recovery must start once"),
    }
    assert_eq!(probe.fake.requests().len(), 1);
    assert_eq!(
        probe.fake.requests()[0].selection,
        selection("stored-provider", "stored-model")
    );
}
async fn closed(native: Arc<dyn Store>) {
    let probe = Probe::new(1);
    let host = agent(native.clone(), &probe);
    let session = create(native.as_ref(), defaults(None, None)).await;
    let (queue, _, _) = enqueue(&host, native.as_ref(), &session).await;
    host.close_extensions().await.unwrap();
    let claim = QueueMutation {
        session_id: session.clone(),
        id: ControlId::new(),
        expected_revision: native.session_revision(&session).await.unwrap(),
        action: QueueAction::Claim {
            owner: "closed-host".into(),
            lease: Duration::from_secs(30),
        },
    };
    assert!(matches!(
        host.mutate_queue(claim).await,
        Err(RuntimeError::HostClosed)
    ));
    let cancel = QueueMutation {
        session_id: session.clone(),
        id: ControlId::new(),
        expected_revision: native.session_revision(&session).await.unwrap(),
        action: QueueAction::Cancel {
            queue_id: queue.clone(),
        },
    };
    assert_eq!(
        host.mutate_queue(cancel)
            .await
            .unwrap()
            .receipt()
            .summary
            .state,
        QueueState::Cancelled
    );
    assert_eq!(
        host.get_queue(&session, &queue).await.unwrap().state,
        QueueState::Cancelled
    );
    let input = InputEnvelope::text(
        "closed fresh",
        native.session_revision(&session).await.unwrap(),
    )
    .unwrap();
    assert!(matches!(
        host.enqueue_session_input_in(
            &context(&session),
            input,
            QueueId::new(),
            ControlId::new(),
            options()
        )
        .await,
        Err(RuntimeError::HostClosed)
    ));
    assert_eq!(probe.factories.load(Ordering::SeqCst), 0);
    assert_eq!(probe.resolutions.load(Ordering::SeqCst), 0);
}

async fn reconcile_retained(
    host: &Agent,
    session: &SessionId,
    retained: &session_owner::preparation::PreparationReconciliation,
    probe: &Probe,
) {
    assert_eq!(host.retained_preparation_status(session).len(), 1);
    let before_retry = probe.factories.load(Ordering::SeqCst);
    let progress = host.retry_preparation_completion(retained).await.unwrap();
    assert!(progress.replayed);
    assert_eq!(probe.factories.load(Ordering::SeqCst), before_retry);
    assert_eq!(host.retained_preparation_status(session), []);
}
