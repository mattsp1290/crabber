use super::*;
pub(super) async fn defaults_cas(native: Arc<dyn Store>) {
    let wrapped = Arc::new(TestStore::new(native.clone()));
    let probe = Probe::new(1);
    let host = agent(wrapped.clone(), &probe);
    let session = create(native.as_ref(), defaults(Some("before"), Some("before"))).await;
    let (queue, _, _) = enqueue(&host, native.as_ref(), &session).await;
    let authority = claim(&host, native.as_ref(), &session).await;
    let prepare = QueueControl {
        id: ControlId::new(),
        expected_revision: native.session_revision(&session).await.unwrap(),
        authority,
    };
    wrapped.hold_summary.store(true, Ordering::SeqCst);
    let admitted_context = context(&session);
    let preparing = host.prepare_queue_in(&admitted_context, prepare.clone());
    let changing = async {
        wrapped.entered.notified().await;
        update(
            native.as_ref(),
            &session,
            defaults(Some("after"), Some("after")),
        )
        .await;
        wrapped.release.notify_one();
    };
    let (outcome, ()) = tokio::join!(preparing, changing);
    assert!(matches!(
        outcome,
        Err(RuntimeError::Store(StoreError::AdmissionRevisionConflict))
    ));
    assert!(!native.get_queue(&session, &queue).await.unwrap().prepared);
    assert!(
        native
            .lookup_queue(&session, &prepare.id)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(probe.fake.requests().len(), 0);
    let latest = QueueControl {
        id: ControlId::new(),
        expected_revision: native.session_revision(&session).await.unwrap(),
        ..prepare
    };
    match host
        .dispatch_queue_in(&context(&session), latest, ControlId::new(), None)
        .await
        .unwrap()
    {
        Admission::Started { handle, .. } => {
            handle.done().await.unwrap();
        }
        Admission::Replayed(_) => panic!("fresh queue should dispatch"),
    }
    assert_eq!(
        probe.fake.requests()[0].selection,
        selection("after", "after")
    );
}
pub(super) async fn same_key_prepare(native: Arc<dyn Store>) {
    let wrapped = Arc::new(TestStore::new(native.clone()));
    let probe = Probe::new(1);
    let host = agent(wrapped.clone(), &probe);
    let session = create(native.as_ref(), defaults(None, None)).await;
    let (queue, _, _) = enqueue(&host, native.as_ref(), &session).await;
    let authority = claim(&host, native.as_ref(), &session).await;
    let control = QueueControl {
        id: ControlId::new(),
        expected_revision: native.session_revision(&session).await.unwrap(),
        authority,
    };
    wrapped.hold_summary.store(true, Ordering::SeqCst);
    let scope = context(&session);
    let first = host.prepare_queue_in(&scope, control.clone());
    let second = async {
        wrapped.entered.notified().await;
        let receipt = host
            .prepare_queue_in(&scope, control.clone())
            .await
            .unwrap();
        wrapped.release.notify_one();
        receipt
    };
    let (left, right) = tokio::join!(first, second);
    assert_eq!(left.unwrap(), right);
    assert_eq!(
        native.get_queue(&session, &queue).await.unwrap(),
        right.summary
    );
    assert_eq!(probe.fake.requests().len(), 0);
    assert_eq!(probe.resolutions.load(Ordering::SeqCst), 0);
}

pub(super) async fn same_key_prepare_during_inspection(native: Arc<dyn Store>) {
    let wrapped = Arc::new(TestStore::new(native.clone()));
    let probe = Probe::new(1);
    let host = agent(wrapped.clone(), &probe);
    let session = create(native.as_ref(), defaults(None, None)).await;
    let (queue, _, _) = enqueue(&host, native.as_ref(), &session).await;
    let authority = claim(&host, native.as_ref(), &session).await;
    let control = QueueControl {
        id: ControlId::new(),
        expected_revision: native.session_revision(&session).await.unwrap(),
        authority,
    };
    wrapped.hold_queue_inspection.store(true, Ordering::SeqCst);
    let scope = context(&session);
    let first = host.prepare_queue_in(&scope, control.clone());
    let second = async {
        wrapped.entered.notified().await;
        let receipt = host
            .prepare_queue_in(&scope, control.clone())
            .await
            .unwrap();
        wrapped.release.notify_one();
        receipt
    };
    let (left, right) = tokio::join!(first, second);
    assert_eq!(left.unwrap(), right);
    assert_eq!(
        native.get_queue(&session, &queue).await.unwrap(),
        right.summary
    );
    assert_eq!(probe.fake.requests().len(), 0);
    assert_eq!(probe.resolutions.load(Ordering::SeqCst), 0);
}
