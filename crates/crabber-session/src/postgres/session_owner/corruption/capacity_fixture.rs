//! Committed capacity fixture; public API retains the actual boundary oracles.
use super::*;

pub(super) async fn fill(store: &PostgresStore) -> (ReserveSessionOwner, SessionOwnerAuthority) {
    let first = session_owner_contract::reservation(SessionId::new());
    let SessionOwnerOutcome::Applied { receipt, authority } = store
        .reserve_session_owner(first.clone(), None)
        .await
        .unwrap()
    else {
        panic!("first capacity reservation must apply");
    };
    assert_eq!(receipt.owner.state, SessionOwnerState::ReservedUnstarted);
    let authority = authority.unwrap();
    let mut expected = vec![first.clone()];
    let mut tx = crate::postgres::transaction::begin(&store.pool)
        .await
        .unwrap();
    crate::postgres::lock_session(&mut tx, &first.binding.session_id)
        .await
        .unwrap();
    // Only fixture interiors bypass repeated public preflight/commit boundaries.
    // Each row still uses the real record, revision and protected source writers.
    for _ in 0..1022 {
        let mut request = first.clone();
        request.id = SessionOwnerOperationId::new();
        request.binding.id = SessionOwnerId::new();
        let digest = request.fingerprint().unwrap();
        let record = Record::reserve(&request, None, store.clock.now()).unwrap();
        drop(record.grant().unwrap());
        let receipt = record.receipt(request.id.clone(), digest, SessionOwnerOperation::Reserve);
        let publication = publish_revision(&mut tx, &first.binding.session_id, true)
            .await
            .unwrap();
        reads::publish(
            &mut tx,
            &Audit {
                version: 2,
                receipt,
                record,
                publication: Some(publication),
            },
            true,
        )
        .await
        .unwrap();
        expected.push(request);
    }
    tx.commit().await.unwrap();
    verify(store, &expected).await;
    let mut last = first.clone();
    last.id = SessionOwnerOperationId::new();
    last.binding.id = SessionOwnerId::new();
    let SessionOwnerOutcome::Applied { receipt, .. } = store
        .reserve_session_owner(last.clone(), None)
        .await
        .unwrap()
    else {
        panic!("public reservation 1024 must apply");
    };
    assert_eq!(receipt.owner.state, SessionOwnerState::ReservedUnstarted);
    expected.push(last);
    let mut overflow = first.clone();
    overflow.id = SessionOwnerOperationId::new();
    overflow.binding.id = SessionOwnerId::new();
    assert!(matches!(
        store.reserve_session_owner(overflow, None).await,
        Err(Error::Capacity)
    ));
    verify(store, &expected).await;
    (first, authority)
}

async fn verify(store: &PostgresStore, expected: &[ReserveSessionOwner]) {
    assert!(matches!(expected.len(), 1023 | 1024));
    let session = &expected[0].binding.session_id;
    let mut tx = crate::postgres::transaction::begin(&store.pool)
        .await
        .unwrap();
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        .execute(&mut *tx)
        .await
        .unwrap();
    reads::scope(&mut tx, session).await.unwrap();
    let exists: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM sessions WHERE id=$1)")
        .bind(&session.0)
        .fetch_one(&mut *tx)
        .await
        .unwrap();
    assert!(
        !exists,
        "root reservations must keep absent-session provenance"
    );
    let mut ids: Vec<_> = expected.iter().map(|r| r.binding.id.0.clone()).collect();
    ids.sort();
    ids.dedup();
    assert_eq!(ids.len(), expected.len());
    for table in [
        "session_owner_current",
        "session_owner_audits",
        "session_owner_publications",
        "session_owner_publication_cohorts",
    ] {
        let mut actual: Vec<String> = sqlx::query_scalar(&format!(
            "SELECT owner_id FROM {table} WHERE session_id=$1 LIMIT 1025"
        ))
        .bind(&session.0)
        .fetch_all(&mut *tx)
        .await
        .unwrap();
        actual.sort();
        assert_eq!(actual, ids, "committed identities in {table}");
    }
    for (index, request) in expected.iter().enumerate() {
        let mut proof = crate::public_session::v2::proof::Proof::bounded(32 << 20);
        let record = reads::current_with_proof(&mut tx, session, &request.binding.id, &mut proof)
            .await
            .unwrap();
        assert_eq!(record.snapshot.binding, request.binding);
        assert_eq!(record.origin, request.id);
        assert_eq!(record.snapshot.state, SessionOwnerState::ReservedUnstarted);
        assert_eq!(record.snapshot.termination, None);
        assert_eq!(
            record.snapshot.target.generation,
            crabber_core::SessionOwnerGeneration::INITIAL
        );
        assert_eq!(record.sequence, 1);
        assert_eq!(record.owner, request.owner);
        let audit = reads::audit(&mut tx, session, &request.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(audit.version, 3);
        assert_eq!(audit.receipt.operation, SessionOwnerOperation::Reserve);
        assert_eq!(
            audit.receipt.semantic_digest,
            request.fingerprint().unwrap()
        );
        assert!(audit.record == record);
        let before = u64::try_from(index).unwrap();
        assert_eq!(
            audit.publication,
            Some(Publication {
                before: RevisionState::Absent(before),
                after: RevisionState::Absent(before + 1),
                public_change: true,
            })
        );
    }
    tx.commit().await.unwrap();
}
