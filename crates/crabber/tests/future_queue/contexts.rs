use super::*;
pub(super) async fn explicit_and_foreign_context(native: Arc<dyn Store>) {
    let probe = Probe::new(1);
    let host = agent(native.clone(), &probe);
    let session = SessionId::new();
    native
        .manage_session(ManageSession {
            session_id: session.clone(),
            id: ControlId::new(),
            expected_revision: SessionRevision::INITIAL,
            change: SessionChange::Create {
                workspace_id: "other-workspace".into(),
                directory: "/other".into(),
                title: "other".into(),
                defaults: defaults(Some("unused"), Some("unused")),
            },
        })
        .await
        .unwrap();
    let context = SessionContext::new(session.clone(), "other-workspace", "/other");
    let explicit = crabber::ExecutionContext::new(
        context.clone(),
        selection("explicit-provider", "explicit-model"),
    );
    let input = InputEnvelope::text(
        "explicit other workspace",
        native.session_revision(&session).await.unwrap(),
    )
    .unwrap();
    let queue = QueueId::new();
    host.enqueue_input_in(&explicit, input, queue.clone(), ControlId::new(), options())
        .await
        .unwrap();
    assert!(
        host.queued_intent(
            &SessionContext::new(session.clone(), "forged", "/other"),
            &queue
        )
        .await
        .is_err()
    );
    let authority = claim(&host, native.as_ref(), &session).await;
    let prepare = QueueControl {
        id: ControlId::new(),
        expected_revision: native.session_revision(&session).await.unwrap(),
        authority,
    };
    match host
        .dispatch_queue_in(&context, prepare, ControlId::new(), None)
        .await
        .unwrap()
    {
        Admission::Started { handle, .. } => {
            handle.done().await.unwrap();
        }
        Admission::Replayed(_) => panic!("explicit queue must dispatch once"),
    }
    assert_eq!(
        probe.fake.requests()[0].selection,
        selection("explicit-provider", "explicit-model")
    );
}
pub(super) async fn unsupported(native: Arc<dyn Store>) {
    let wrapped = Arc::new(TestStore::new(native.clone()));
    wrapped.unsupported.store(true, Ordering::SeqCst);
    let probe = Probe::new(1);
    let host = agent(wrapped, &probe);
    let session = create(native.as_ref(), defaults(None, None)).await;
    let input = InputEnvelope::text(
        "unsupported",
        native.session_revision(&session).await.unwrap(),
    )
    .unwrap();
    let queue = QueueId::new();
    assert!(matches!(
        host.enqueue_session_input_in(
            &context(&session),
            input,
            queue.clone(),
            ControlId::new(),
            options()
        )
        .await,
        Err(RuntimeError::Store(StoreError::HostUnsupported))
    ));
    assert!(native.get_queue(&session, &queue).await.is_err());
    assert_eq!(probe.factories.load(Ordering::SeqCst), 0);
    assert_eq!(probe.resolutions.load(Ordering::SeqCst), 0);
}
