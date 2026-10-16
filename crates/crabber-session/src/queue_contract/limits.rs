use super::*;
pub(crate) async fn faults(store: &dyn Store, arm: impl Fn(u8)) {
    for stage in [1, 2] {
        for step in 0..5 {
            let f = fixture(store).await;
            let request = if step == 0 {
                f.enqueue.clone()
            } else {
                store.mutate_queue(f.enqueue.clone()).await.unwrap();
                if step == 4 {
                    mutation(
                        store,
                        &f,
                        QueueAction::Cancel {
                            queue_id: f.queue.clone(),
                        },
                    )
                    .await
                } else if step == 1 {
                    mutation(
                        store,
                        &f,
                        QueueAction::Claim {
                            owner: "host".into(),
                            lease: Duration::from_secs(30),
                        },
                    )
                    .await
                } else {
                    let authority = claim(store, &f).await;
                    let prepare = mutation(
                        store,
                        &f,
                        QueueAction::Prepare {
                            authority: authority.clone(),
                            prepared: Box::new(f.prepared.clone()),
                        },
                    )
                    .await;
                    if step == 2 {
                        prepare
                    } else {
                        store.mutate_queue(prepare).await.unwrap();
                        mutation(
                            store,
                            &f,
                            QueueAction::Admit {
                                authority,
                                owner: "host".into(),
                                lease: Duration::from_secs(30),
                            },
                        )
                        .await
                    }
                }
            };
            let before = store.session_revision(&f.session).await.unwrap();
            arm(stage);
            assert!(
                store.mutate_queue(request.clone()).await.is_err(),
                "fault did not trip stage {stage} step {step}"
            );
            arm(0);
            let saved = store.lookup_queue(&f.session, &request.id).await.unwrap();
            if stage == 1 {
                assert!(saved.is_none());
                assert_eq!(store.session_revision(&f.session).await.unwrap(), before);
                assert!(matches!(
                    store.mutate_queue(request).await.unwrap(),
                    QueueOutcome::Applied { .. }
                ));
            } else {
                let saved = saved.unwrap();
                assert!(saved.revision > before);
                assert!(
                    matches!(store.mutate_queue(request).await.unwrap(), QueueOutcome::Replayed(actual) if actual == saved)
                );
                if step == 3 {
                    assert!(saved.summary.admission.is_some());
                }
            }
        }
    }
}
pub(crate) async fn bounds(store: &dyn Store) {
    let first = fixture(store).await;
    store.mutate_queue(first.enqueue).await.unwrap();
    for _ in 1..64 {
        let next = entry(store, first.session.clone()).await;
        store.mutate_queue(next.enqueue).await.unwrap();
    }
    let next = entry(store, first.session.clone()).await;
    assert!(matches!(
        store.mutate_queue(next.enqueue).await,
        Err(StoreError::Busy)
    ));
    for (limit, after) in [(0, None), (65, None), (1, Some(1))] {
        assert!(
            store
                .list_queue(QueuePageRequest {
                    session_id: first.session.clone(),
                    limit,
                    after_sequence: after,
                    expected_revision: None
                })
                .await
                .is_err()
        );
    }
    let page = store
        .list_queue(QueuePageRequest {
            session_id: first.session.clone(),
            limit: 64,
            after_sequence: None,
            expected_revision: None,
        })
        .await
        .unwrap();
    assert_eq!(page.entries.len(), 64);
    assert!(page.next_after.is_none());
}
pub(crate) async fn byte_bound(store: &dyn Store) {
    let first = fixture(store).await;
    let mut accepted = 0;
    let mut head = None;
    loop {
        let mut next = entry(store, first.session.clone()).await;
        let input =
            InputEnvelope::text("x".repeat(3 << 20), next.enqueue.expected_revision).unwrap();
        if let QueueAction::Enqueue { input: intent, .. } = &mut next.enqueue.action {
            intent.input = input;
        }
        match store.mutate_queue(next.enqueue.clone()).await {
            Ok(_) => {
                accepted += 1;
                if head.is_none() {
                    head = Some(next);
                }
            }
            Err(StoreError::Busy) => break,
            Err(error) => panic!("byte bound returned wrong error: {error:?}"),
        }
        assert!(
            accepted < 12,
            "pending byte budget must refuse before count limit"
        );
    }
    assert!((9..=10).contains(&accepted));
    let head = head.unwrap();
    let retained = store
        .retained_queue(&head.session, &head.queue)
        .await
        .unwrap();
    let mut prepared = crate::typed_admission_contract::prepare(
        crate::execution_binding_contract::fixture(
            &head.session,
            OffsetDateTime::UNIX_EPOCH,
            "full-queue",
            'b',
        ),
        retained.input.input,
        OffsetDateTime::UNIX_EPOCH,
    );
    prepared.keyed.options = retained.input.admission;
    let authority = claim(store, &head).await;
    let prepare = mutation(
        store,
        &head,
        QueueAction::Prepare {
            authority: authority.clone(),
            prepared: Box::new(prepared),
        },
    )
    .await;
    assert!(
        store
            .mutate_queue(prepare)
            .await
            .unwrap()
            .receipt()
            .summary
            .prepared,
        "a full accepted backlog must retain preparation capacity for its head"
    );
    let admit = mutation(
        store,
        &head,
        QueueAction::Admit {
            authority,
            owner: "full-queue".into(),
            lease: Duration::from_secs(30),
        },
    )
    .await;
    assert!(
        matches!(
            store.mutate_queue(admit).await.unwrap(),
            QueueOutcome::Applied {
                admitted: Some(_),
                ..
            }
        ),
        "full-backlog head must reach its real atomic admission"
    );
}
pub(crate) async fn domains_archive_and_reserved_keys(store: &dyn Store) {
    let f = fixture(store).await;
    store.mutate_queue(f.enqueue.clone()).await.unwrap();
    let management = ManageSession {
        session_id: f.session.clone(),
        id: f.enqueue.id.clone(),
        expected_revision: store.session_revision(&f.session).await.unwrap(),
        change: SessionChange::Rename {
            title: "different domain".into(),
        },
    };
    assert_eq!(
        store.manage_session(management).await.unwrap_err(),
        ManagementError::SemanticConflict
    );
    let mut stolen = crate::execution_binding_contract::fixture(
        &f.session,
        OffsetDateTime::UNIX_EPOCH,
        "key-thief",
        'b',
    );
    stolen.keyed.options.key = f.prepared.keyed.options.key.clone();
    assert!(matches!(
        store.admit_bound_keyed_run(stolen).await,
        Err(StoreError::AdmissionConflict)
    ));
    store
        .manage_session(ManageSession {
            session_id: f.session.clone(),
            id: ControlId::new(),
            expected_revision: store.session_revision(&f.session).await.unwrap(),
            change: SessionChange::SetArchived { archived: true },
        })
        .await
        .unwrap();
    assert!(matches!(
        store.mutate_queue(f.enqueue.clone()).await.unwrap(),
        QueueOutcome::Replayed(_)
    ));
    let claim = mutation(
        store,
        &f,
        QueueAction::Claim {
            owner: "archived".into(),
            lease: Duration::from_secs(30),
        },
    )
    .await;
    assert!(matches!(
        store.mutate_queue(claim).await,
        Err(StoreError::Conflict)
    ));
    let fresh = entry(store, f.session.clone()).await;
    assert!(matches!(
        store.mutate_queue(fresh.enqueue).await,
        Err(StoreError::Conflict)
    ));
    let cancel = mutation(
        store,
        &f,
        QueueAction::Cancel {
            queue_id: f.queue.clone(),
        },
    )
    .await;
    assert_eq!(
        store
            .mutate_queue(cancel)
            .await
            .unwrap()
            .receipt()
            .summary
            .state,
        QueueState::Cancelled
    );
    let other = fixture(store).await;
    store
        .manage_session(ManageSession {
            session_id: other.session.clone(),
            id: other.enqueue.id.clone(),
            expected_revision: store.session_revision(&other.session).await.unwrap(),
            change: SessionChange::Rename {
                title: "different domain".into(),
            },
        })
        .await
        .unwrap();
    assert!(matches!(
        store.mutate_queue(other.enqueue).await,
        Err(StoreError::AdmissionConflict)
    ));
}
pub(crate) async fn lazy_empty_scope(store: &dyn Store) {
    let session = SessionId(format!("opaque/session 🦀 {}", SessionId::new()));
    let mut original = crate::execution_binding_contract::fixture(
        &session,
        OffsetDateTime::UNIX_EPOCH,
        "lazy",
        'b',
    );
    original.keyed.request.workspace_id.clear();
    original.keyed.request.directory.clear();
    original.binding.request.workspace_id.clear();
    original.binding.request.directory.clear();
    let capsule = original.keyed.execution.as_mut().unwrap();
    capsule.request.workspace_id.clear();
    capsule.request.directory.clear();
    let digest = original.keyed.semantic_digest().unwrap();
    original.keyed.execution.as_mut().unwrap().semantic_digest = digest;
    store.admit_bound_keyed_run(original).await.unwrap();
    let mut queued = entry(store, session.clone()).await;
    if let QueueAction::Enqueue { input, .. } = &mut queued.enqueue.action {
        input.request.workspace_id.clear();
        input.request.directory.clear();
    }
    let saved = store
        .mutate_queue(queued.enqueue.clone())
        .await
        .unwrap()
        .receipt()
        .clone();
    assert_eq!(
        store.get_queue(&session, &queued.queue).await.unwrap(),
        saved.summary
    );
    let page = store
        .list_queue(QueuePageRequest {
            session_id: session,
            limit: 1,
            after_sequence: None,
            expected_revision: None,
        })
        .await
        .unwrap();
    assert_eq!(page.entries, vec![saved.summary]);
}
