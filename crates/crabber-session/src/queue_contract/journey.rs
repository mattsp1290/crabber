use super::*;
pub(crate) async fn run(store: &dyn Store, clock: Arc<ManualClock>) {
    let (first, second, authority, prepare, prepared_receipt) = seeded(store, &clock).await;
    let receipt = reclaimed(store, &clock, &first, authority, prepare, prepared_receipt).await;
    finalized(store, &first, &second, receipt).await;
    cancellation(store).await;
    races(store).await;
    cancellation_admission_race(store).await;
    preparation_ordinary_race(store).await;
}
async fn seeded(
    store: &dyn Store,
    clock: &ManualClock,
) -> (
    Fixture,
    Fixture,
    QueueAuthority,
    QueueMutation,
    QueueReceipt,
) {
    let first = fixture(store).await;
    let original = store
        .mutate_queue(first.enqueue.clone())
        .await
        .unwrap()
        .receipt()
        .clone();
    assert_eq!(original.summary.sequence, 1);
    let second = entry(store, first.session.clone()).await;
    let next = store
        .mutate_queue(second.enqueue.clone())
        .await
        .unwrap()
        .receipt()
        .clone();
    assert_eq!(next.summary.sequence, 2);
    assert!(
        matches!(store.mutate_queue(first.enqueue.clone()).await.unwrap(), QueueOutcome::Replayed(saved) if saved == original)
    );
    let page_request = QueuePageRequest {
        session_id: first.session.clone(),
        limit: 1,
        after_sequence: None,
        expected_revision: None,
    };
    let page = store.list_queue(page_request.clone()).await.unwrap();
    assert_eq!(page.entries, vec![original.summary]);
    assert_eq!(page.next_after, Some(1));
    let authority = claim(store, &first).await;
    let forged = mutation(
        store,
        &first,
        QueueAction::Prepare {
            authority: QueueAuthority {
                queue_id: second.queue.clone(),
                ..authority.clone()
            },
            prepared: Box::new(second.prepared.clone()),
        },
    )
    .await;
    assert!(store.mutate_queue(forged).await.is_err());
    let stale_page = QueuePageRequest {
        after_sequence: page.next_after,
        expected_revision: Some(page.revision),
        ..page_request
    };
    assert!(matches!(
        store.list_queue(stale_page).await,
        Err(StoreError::AdmissionRevisionConflict)
    ));
    let prepare = mutation(
        store,
        &first,
        QueueAction::Prepare {
            authority: authority.clone(),
            prepared: Box::new(first.prepared.clone()),
        },
    )
    .await;
    let prepared_receipt = store
        .mutate_queue(prepare.clone())
        .await
        .unwrap()
        .receipt()
        .clone();
    assert!(prepared_receipt.summary.prepared);
    assert!(matches!(
        store
            .admit_bound_keyed_run(crate::execution_binding_contract::fixture(
                &first.session,
                clock.now(),
                "ordinary",
                'b'
            ))
            .await,
        Err(StoreError::Busy)
    ));
    assert!(
        store
            .mutate_queue(mutation(store, &first, prepare.action.clone()).await)
            .await
            .is_err()
    );
    (first, second, authority, prepare, prepared_receipt)
}
async fn reclaimed(
    store: &dyn Store,
    clock: &ManualClock,
    first: &Fixture,
    authority: QueueAuthority,
    prepare: QueueMutation,
    prepared_receipt: QueueReceipt,
) -> QueueReceipt {
    clock.set(clock.now() + time::Duration::seconds(31));
    let new_authority = claim(store, first).await;
    assert_eq!(new_authority.generation, authority.generation + 1);
    assert_ne!(new_authority.token, authority.token);
    let retained = store
        .retained_queue(&first.session, &first.queue)
        .await
        .unwrap();
    assert_eq!(retained.prepared.unwrap().binding, first.prepared.binding);
    let stale_admit = mutation(
        store,
        first,
        QueueAction::Admit {
            authority,
            owner: "late".into(),
            lease: Duration::from_secs(30),
        },
    )
    .await;
    assert!(store.mutate_queue(stale_admit).await.is_err());
    assert!(
        matches!(store.mutate_queue(prepare).await.unwrap(), QueueOutcome::Replayed(saved) if saved == prepared_receipt)
    );
    let admit = mutation(
        store,
        first,
        QueueAction::Admit {
            authority: new_authority,
            owner: "winner".into(),
            lease: Duration::from_secs(30),
        },
    )
    .await;
    let (receipt, admitted) = match store.mutate_queue(admit.clone()).await.unwrap() {
        QueueOutcome::Applied {
            receipt,
            authority: None,
            admitted: Some(admitted),
        } => (receipt, admitted),
        other => panic!("admission must grant once: {other:?}"),
    };
    assert_eq!(
        receipt.summary.admission,
        store
            .lookup_admission(&first.session, &first.prepared.keyed.options.key)
            .await
            .unwrap()
    );
    assert_eq!(
        store
            .execution_binding(&first.session, &admitted.run.id)
            .await
            .unwrap()
            .unwrap()
            .capsule,
        first.prepared.binding
    );
    assert!(
        matches!(store.mutate_queue(admit).await.unwrap(), QueueOutcome::Replayed(saved) if saved == receipt)
    );
    receipt
}
async fn finalized(store: &dyn Store, first: &Fixture, second: &Fixture, receipt: QueueReceipt) {
    let cancel = mutation(
        store,
        first,
        QueueAction::Cancel {
            queue_id: first.queue.clone(),
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
            .admission,
        receipt.summary.admission
    );
    let cancel_second = mutation(
        store,
        second,
        QueueAction::Cancel {
            queue_id: second.queue.clone(),
        },
    )
    .await;
    let canceled = store
        .mutate_queue(cancel_second.clone())
        .await
        .unwrap()
        .receipt()
        .clone();
    assert_eq!(canceled.summary.state, QueueState::Cancelled);
    assert!(
        matches!(store.mutate_queue(cancel_second).await.unwrap(), QueueOutcome::Replayed(saved) if saved == canceled)
    );
    assert_eq!(
        store
            .get_queue(&first.session, &second.queue)
            .await
            .unwrap(),
        canceled.summary
    );
    let safe = serde_json::to_string(
        &store
            .list_queue(QueuePageRequest {
                session_id: first.session.clone(),
                limit: 64,
                after_sequence: None,
                expected_revision: None,
            })
            .await
            .unwrap(),
    )
    .unwrap();
    for secret in [
        "PRIVATE_QUEUE_INPUT",
        "private-system",
        "winner",
        "claim_token",
        "runtime_semantics",
    ] {
        assert!(!safe.contains(secret), "public DTO leaked {secret}");
    }
}
async fn cancellation(store: &dyn Store) {
    let f = fixture(store).await;
    store.mutate_queue(f.enqueue.clone()).await.unwrap();
    let authority = claim(store, &f).await;
    let cancel = mutation(
        store,
        &f,
        QueueAction::Cancel {
            queue_id: f.queue.clone(),
        },
    )
    .await;
    store.mutate_queue(cancel).await.unwrap();
    let prepare = mutation(
        store,
        &f,
        QueueAction::Prepare {
            authority,
            prepared: Box::new(f.prepared.clone()),
        },
    )
    .await;
    assert!(store.mutate_queue(prepare).await.is_err());
    assert!(
        store
            .lookup_admission(&f.session, &f.prepared.keyed.options.key)
            .await
            .unwrap()
            .is_none()
    );
}
async fn races(store: &dyn Store) {
    let f = fixture(store).await;
    store.mutate_queue(f.enqueue.clone()).await.unwrap();
    let left = mutation(
        store,
        &f,
        QueueAction::Claim {
            owner: "left".into(),
            lease: Duration::from_secs(30),
        },
    )
    .await;
    let mut right = left.clone();
    right.id = ControlId::new();
    let (a, b) = futures::join!(store.mutate_queue(left.clone()), store.mutate_queue(right));
    assert_eq!(usize::from(a.is_ok()) + usize::from(b.is_ok()), 1);
    if a.is_ok() {
        assert!(matches!(
            store.mutate_queue(left).await.unwrap(),
            QueueOutcome::Replayed(_)
        ));
    }
}
pub(crate) async fn cancellation_admission_race(store: &dyn Store) {
    let f = fixture(store).await;
    store.mutate_queue(f.enqueue.clone()).await.unwrap();
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
    store.mutate_queue(prepare).await.unwrap();
    let cancel = mutation(
        store,
        &f,
        QueueAction::Cancel {
            queue_id: f.queue.clone(),
        },
    )
    .await;
    let admit = mutation(
        store,
        &f,
        QueueAction::Admit {
            authority,
            owner: "race".into(),
            lease: Duration::from_secs(30),
        },
    )
    .await;
    let (left, right) = futures::join!(store.mutate_queue(cancel), store.mutate_queue(admit));
    assert_eq!(usize::from(left.is_ok()) + usize::from(right.is_ok()), 1);
    let stored = store.get_queue(&f.session, &f.queue).await.unwrap();
    match stored.state {
        QueueState::Cancelled => assert!(
            store
                .lookup_admission(&f.session, &f.prepared.keyed.options.key)
                .await
                .unwrap()
                .is_none()
        ),
        QueueState::Admitted => assert_eq!(
            stored.admission,
            store
                .lookup_admission(&f.session, &f.prepared.keyed.options.key)
                .await
                .unwrap()
        ),
        other => panic!("race failed to settle: {other:?}"),
    }
}
pub(crate) async fn preparation_ordinary_race(store: &dyn Store) {
    let f = fixture(store).await;
    store.mutate_queue(f.enqueue.clone()).await.unwrap();
    let authority = claim(store, &f).await;
    let prepare = mutation(
        store,
        &f,
        QueueAction::Prepare {
            authority,
            prepared: Box::new(f.prepared.clone()),
        },
    )
    .await;
    let ordinary = crate::execution_binding_contract::fixture(
        &f.session,
        OffsetDateTime::UNIX_EPOCH,
        "ordinary-race",
        'b',
    );
    let (left, right) = futures::join!(
        store.mutate_queue(prepare),
        store.admit_bound_keyed_run(ordinary)
    );
    assert_eq!(usize::from(left.is_ok()) + usize::from(right.is_ok()), 1);
}
