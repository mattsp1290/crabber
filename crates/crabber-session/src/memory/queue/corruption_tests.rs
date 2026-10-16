use crate::queue_contract::corruption;
#[tokio::test]
async fn queue_rejects_replaced_prepared_and_coherent_input() {
    let store = crate::MemoryStore::new();
    for change_input in [false, true] {
        let (fixture, prepare, authority) = corruption::prepared(&store).await;
        {
            let mut state = store.state.lock().unwrap();
            let record = state
                .queue
                .get_mut(&(fixture.session.clone(), fixture.queue.clone()))
                .unwrap();
            corruption::replace(record, change_input);
        }
        corruption::refused(&store, &fixture, prepare, authority).await;
    }
}
#[tokio::test]
async fn queue_rejects_replaced_claim_authority() {
    let store = crate::MemoryStore::new();
    let (fixture, prepare, authority) = corruption::prepared(&store).await;
    {
        let mut state = store.state.lock().unwrap();
        let record = state
            .queue
            .get_mut(&(fixture.session.clone(), fixture.queue.clone()))
            .unwrap();
        record.claim.as_mut().unwrap().authority.token = uuid::Uuid::new_v4().to_string();
    }
    corruption::refused(&store, &fixture, prepare, authority).await;
}
#[tokio::test]
async fn queue_rejects_cancelled_state_and_old_claim_resurrection() {
    use crate::{QueueAction, Store};
    for claimed in [false, true] {
        let store = crate::MemoryStore::new();
        let fixture = crate::queue_contract::fixture(&store).await;
        store.mutate_queue(fixture.enqueue.clone()).await.unwrap();
        if claimed {
            crate::queue_contract::claim(&store, &fixture).await;
        }
        let key = (fixture.session.clone(), fixture.queue.clone());
        let old = store.state.lock().unwrap().queue[&key].clone();
        let cancel = crate::queue_contract::mutation(
            &store,
            &fixture,
            QueueAction::Cancel {
                queue_id: fixture.queue.clone(),
            },
        )
        .await;
        store.mutate_queue(cancel.clone()).await.unwrap();
        store.state.lock().unwrap().queue.insert(key, old);
        assert!(
            store
                .retained_queue(&fixture.session, &fixture.queue)
                .await
                .is_err()
        );
        assert!(
            store
                .get_queue(&fixture.session, &fixture.queue)
                .await
                .is_err()
        );
        let claim = crate::queue_contract::mutation(
            &store,
            &fixture,
            QueueAction::Claim {
                owner: "resurrection".into(),
                lease: std::time::Duration::from_secs(30),
            },
        )
        .await;
        assert!(store.mutate_queue(claim).await.is_err());
        assert!(matches!(
            store.mutate_queue(cancel).await.unwrap(),
            crate::QueueOutcome::Replayed(_)
        ));
    }
}
#[tokio::test]
async fn queue_rejects_replaced_admission_mapping() {
    use crate::{QueueAction, Store};
    let store = crate::MemoryStore::new();
    let (fixture, _, authority) = corruption::prepared(&store).await;
    let admit = crate::queue_contract::mutation(
        &store,
        &fixture,
        QueueAction::Admit {
            authority,
            owner: "mapping".into(),
            lease: std::time::Duration::from_secs(30),
        },
    )
    .await;
    store.mutate_queue(admit.clone()).await.unwrap();
    {
        let mut state = store.state.lock().unwrap();
        state
            .queue
            .get_mut(&(fixture.session.clone(), fixture.queue.clone()))
            .unwrap()
            .summary
            .admission
            .as_mut()
            .unwrap()
            .run_id = crabber_core::RunId::new();
    }
    assert!(
        store
            .retained_queue(&fixture.session, &fixture.queue)
            .await
            .is_err()
    );
    assert!(
        store
            .get_queue(&fixture.session, &fixture.queue)
            .await
            .is_err()
    );
    let cancel = crate::queue_contract::mutation(
        &store,
        &fixture,
        QueueAction::Cancel {
            queue_id: fixture.queue.clone(),
        },
    )
    .await;
    assert!(store.mutate_queue(cancel).await.is_err());
    assert!(matches!(
        store.mutate_queue(admit).await.unwrap(),
        crate::QueueOutcome::Replayed(_)
    ));
}
#[tokio::test]
async fn queue_cannot_hide_corrupt_head_behind_changed_session_scope() {
    use crate::{QueueAction, QueuePageRequest, Store};
    let store = crate::MemoryStore::new();
    let fixture = crate::queue_contract::fixture(&store).await;
    store.mutate_queue(fixture.enqueue.clone()).await.unwrap();
    let second = crate::queue_contract::entry(&store, fixture.session.clone()).await;
    store.mutate_queue(second.enqueue).await.unwrap();
    {
        let mut state = store.state.lock().unwrap();
        state
            .queue
            .get_mut(&(fixture.session.clone(), fixture.queue.clone()))
            .unwrap()
            .summary
            .session_id = crabber_core::SessionId::new();
    }
    assert!(
        store
            .list_queue(QueuePageRequest {
                session_id: fixture.session.clone(),
                limit: 1,
                after_sequence: None,
                expected_revision: None
            })
            .await
            .is_err()
    );
    let claim = crate::queue_contract::mutation(
        &store,
        &fixture,
        QueueAction::Claim {
            owner: "skip-head".into(),
            lease: std::time::Duration::from_secs(30),
        },
    )
    .await;
    assert!(store.mutate_queue(claim).await.is_err());
}
