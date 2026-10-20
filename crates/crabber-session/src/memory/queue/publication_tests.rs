use crate::{QueueAction, QueueOutcome, Store};
#[tokio::test]
async fn journal_authenticates_generated_authority_and_historical_receipts() {
    let store = crate::MemoryStore::new();
    let f = crate::queue_contract::fixture(&store).await;
    store.mutate_queue(f.enqueue.clone()).await.unwrap();
    let authority = crate::queue_contract::claim(&store, &f).await;
    let (claim_id, claim_audit) = {
        let state = store.state.lock().unwrap();
        let record = &state.queue[&(f.session.clone(), f.queue.clone())];
        let id = record.claim.as_ref().unwrap().operation.clone();
        (
            id.clone(),
            state.queue_audits[&(f.session.clone(), id)].clone(),
        )
    };
    let cancel = crate::queue_contract::mutation(
        &store,
        &f,
        QueueAction::Cancel {
            queue_id: f.queue.clone(),
        },
    )
    .await;
    store.mutate_queue(cancel).await.unwrap();
    // Historical original selection stays independent of later current labels.
    assert!(
        store
            .lookup_queue(&f.session, &claim_id)
            .await
            .unwrap()
            .is_some()
    );
    {
        let mut state = store.state.lock().unwrap();
        let audit = state
            .queue_audits
            .get_mut(&(f.session.clone(), claim_id.clone()))
            .unwrap();
        audit.authority.as_mut().unwrap().token.push_str("changed");
    }
    assert!(store.lookup_queue(&f.session, &claim_id).await.is_err());
    assert!(store.retained_queue(&f.session, &f.queue).await.is_err());
    // Restoring the exact original body restores metadata, never authority.
    store
        .state
        .lock()
        .unwrap()
        .queue_audits
        .insert((f.session.clone(), claim_id.clone()), claim_audit);
    assert!(
        store
            .lookup_queue(&f.session, &claim_id)
            .await
            .unwrap()
            .is_some()
    );
    let _ = authority;
}
#[tokio::test]
async fn whole_scope_projection_rejects_erased_current_even_without_mutable_audits() {
    let store = crate::MemoryStore::new();
    let f = crate::queue_contract::fixture(&store).await;
    store.mutate_queue(f.enqueue.clone()).await.unwrap();
    {
        let mut state = store.state.lock().unwrap();
        state.queue.remove(&(f.session.clone(), f.queue.clone()));
        state
            .queue_audits
            .retain(|(session, _), _| session != &f.session);
        state
            .operation_receipts
            .retain(|(session, _), _| session != &f.session);
    }
    let mut proof = crate::public_session::v2::proof::Proof::bounded(32 << 20);
    let mut safe = crate::public_session::budget::Budget::new(
        32 << 20,
        crate::PublicSessionLimit::HeaderBytes,
    );
    let state = store.state.lock().unwrap();
    assert!(super::public_pending(&state, &f.session, 64, &mut safe, &mut proof).is_err());
}
#[tokio::test]
async fn legacy_queue_never_acquires_source_membership_from_controls() {
    let store = crate::MemoryStore::new();
    let f = crate::queue_contract::fixture(&store).await;
    store.mutate_queue(f.enqueue.clone()).await.unwrap();
    store.state.lock().unwrap().queue_publications = crate::queue::source::Ledger::default();
    let authority = crate::queue_contract::claim(&store, &f).await;
    let context =
        crabber_core::SessionContext::new(f.session.clone(), "bound-workspace", "/bound/workspace");
    assert!(matches!(
        store
            .inspect_owned_queue_preparation(
                &context,
                &authority,
                store.session_revision(&f.session).await.unwrap()
            )
            .await,
        Err(crate::StoreError::HostUnsupported)
    ));
    let cancel = crate::queue_contract::mutation(
        &store,
        &f,
        QueueAction::Cancel {
            queue_id: f.queue.clone(),
        },
    )
    .await;
    assert!(matches!(
        store.mutate_queue(cancel).await.unwrap(),
        QueueOutcome::Applied { .. }
    ));
    let state = store.state.lock().unwrap();
    let scope = crate::queue::source::Scope {
        session: f.session,
        queue: f.queue,
    };
    assert!(
        !state
            .queue_publications
            .protocol(
                &scope,
                &mut crate::public_session::v2::proof::Proof::bounded(32 << 20)
            )
            .unwrap()
    );
}

#[tokio::test]
async fn journal_context_precharges_metadata_before_validation_or_clone() {
    use crate::public_session::v2::proof::{Proof, ReadError};
    let store = crate::MemoryStore::new();
    let f = crate::queue_contract::fixture(&store).await;
    store.mutate_queue(f.enqueue.clone()).await.unwrap();
    {
        let mut state = store.state.lock().unwrap();
        let mut measured = Proof::bounded(32 << 20);
        super::protocol::context(&state, &f.session, &mut measured).unwrap();
        super::protocol::context(&state, &f.session, &mut Proof::bounded(measured.used())).unwrap();
        let mut shared = Proof::bounded(measured.used() + 17);
        shared.charge(18).unwrap();
        assert!(matches!(
            super::protocol::context(&state, &f.session, &mut shared),
            Err(ReadError::Budget(_))
        ));
        state
            .management
            .get_mut(&f.session)
            .unwrap()
            .defaults
            .provider_id = Some("x".repeat(1 << 20));
        assert!(matches!(
            super::protocol::context(&state, &f.session, &mut Proof::bounded(1024)),
            Err(ReadError::Budget(_))
        ));
        assert!(matches!(
            super::protocol::context(&state, &f.session, &mut Proof::bounded(32 << 20)),
            Err(ReadError::Domain(_))
        ));
    }
    assert!(
        store
            .lookup_queue(&f.session, &f.enqueue.id)
            .await
            .unwrap()
            .is_some()
    );
    assert!(store.queue_session_snapshot(&f.session).is_err());
}
