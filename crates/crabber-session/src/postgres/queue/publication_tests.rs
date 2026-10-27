use crate::{
    Store,
    queue::{Audit, Record, decode, encode, source::Scope},
};
#[tokio::test]
async fn journal_original_generated_authority_is_independent_of_later_current() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};

    TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {
        crate::PostgresStore::migrate(&url).await.unwrap();
        let store = crate::PostgresStore::connect(&url).await.unwrap();
        let f = crate::queue_contract::fixture(&store).await;
        store.mutate_queue(f.enqueue.clone()).await.unwrap();
        crate::queue_contract::claim(&store, &f).await;
        let data: String = sqlx::query_scalar(
            "SELECT data_json FROM future_queue WHERE session_id=$1 AND queue_id=$2",
        )
        .bind(&f.session.0)
        .bind(&f.queue.0)
        .fetch_one(&store.pool)
        .await
        .unwrap();
        let record: Record = decode(&data).unwrap();
        let claim = record.claim.as_ref().unwrap().operation.clone();
        let original: String = sqlx::query_scalar(
            "SELECT data_json FROM future_queue_audit WHERE session_id=$1 AND operation_id=$2",
        )
        .bind(&f.session.0)
        .bind(&claim.0)
        .fetch_one(&store.pool)
        .await
        .unwrap();
        // A later corrupt current record must not invalidate the selected original.
        sqlx::query("UPDATE future_queue SET data_json='{}' WHERE session_id=$1 AND queue_id=$2")
            .bind(&f.session.0)
            .bind(&f.queue.0)
            .execute(&store.pool)
            .await
            .unwrap();
        assert!(
            store
                .lookup_queue(&f.session, &claim)
                .await
                .unwrap()
                .is_some()
        );
        assert!(store.retained_queue(&f.session, &f.queue).await.is_err());
        sqlx::query("UPDATE future_queue SET data_json=$3 WHERE session_id=$1 AND queue_id=$2")
            .bind(&f.session.0)
            .bind(&f.queue.0)
            .bind(data)
            .execute(&store.pool)
            .await
            .unwrap();
        let mut audit: Audit = decode(&original).unwrap();
        audit.authority.as_mut().unwrap().token.push_str("changed");
        // The generated authority isn't in the semantic request fingerprint.
        sqlx::query(
            "UPDATE future_queue_audit SET data_json=$3 WHERE session_id=$1 AND operation_id=$2",
        )
        .bind(&f.session.0)
        .bind(&claim.0)
        .bind(encode(&audit).unwrap())
        .execute(&store.pool)
        .await
        .unwrap();
        assert!(store.lookup_queue(&f.session, &claim).await.is_err());
        assert!(store.retained_queue(&f.session, &f.queue).await.is_err());
        sqlx::query(
            "UPDATE future_queue_audit SET data_json=$3 WHERE session_id=$1 AND operation_id=$2",
        )
        .bind(&f.session.0)
        .bind(&claim.0)
        .bind(original)
        .execute(&store.pool)
        .await
        .unwrap();
        assert!(
            store
                .lookup_queue(&f.session, &claim)
                .await
                .unwrap()
                .is_some()
        );
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn journal_identity_and_publications_are_append_only_and_share_proof() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};

    TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {

    crate::PostgresStore::migrate(&url).await.unwrap();
    let store = crate::PostgresStore::connect(&url).await.unwrap();
    let f = crate::queue_contract::fixture(&store).await;
    store.mutate_queue(f.enqueue.clone()).await.unwrap();
    for query in [
        "UPDATE future_queue SET source_protocol=0 WHERE session_id=$1",
        "DELETE FROM future_queue WHERE session_id=$1",
        "UPDATE queue_publication_sources SET data_json='{}' WHERE session_id=$1",
        "DELETE FROM queue_publication_sources WHERE session_id=$1",
        "UPDATE queue_publication_cohorts SET head_sequence=head_sequence+1 WHERE session_id=$1",
        "DELETE FROM queue_publication_cohorts WHERE session_id=$1",
    ] {
        assert!(
            sqlx::query(query)
                .bind(&f.session.0)
                .execute(&store.pool)
                .await
                .is_err(),
            "{query}"
        );
    }
    let mut tx = crate::postgres::transaction::begin(&store.pool)
        .await
        .unwrap();
    let scope = Scope {
        session: f.session.clone(),
        queue: f.queue.clone(),
    };
    let mut proof = crate::public_session::v2::proof::Proof::bounded(1);
    assert!(matches!(
        super::protocol::history(&mut tx, &scope, &mut proof).await,
        Err(crate::public_session::v2::proof::ReadError::Budget(_))
    ));
    tx.rollback().await.unwrap();
    let authority = crate::queue_contract::claim(&store, &f).await;
    let prepare = crate::queue_contract::mutation(
        &store,
        &f,
        crate::QueueAction::Prepare {
            authority: authority.clone(),
            prepared: Box::new(f.prepared.clone()),
        },
    )
    .await;
    store.mutate_queue(prepare).await.unwrap();
    let admit = crate::queue_contract::mutation(
        &store,
        &f,
        crate::QueueAction::Admit {
            authority,
            owner: "journal-noop".into(),
            lease: std::time::Duration::from_secs(30),
        },
    )
    .await;
    store.mutate_queue(admit).await.unwrap();
    let cancel = crate::queue_contract::mutation(
        &store,
        &f,
        crate::QueueAction::Cancel {
            queue_id: f.queue.clone(),
        },
    )
    .await;
    let cancelled = store.mutate_queue(cancel).await.unwrap();
    assert_eq!(
        cancelled.receipt().summary.state,
        crate::QueueState::Admitted
    );
    let count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM queue_publication_sources WHERE session_id=$1")
            .bind(&f.session.0)
            .fetch_one(&store.pool)
            .await
            .unwrap();
    assert_eq!(
        count, 5,
        "enqueue,claim,prepare,admit and successful no-op cancel are distinct publications"
    );
    let mut tx = crate::postgres::transaction::begin(&store.pool)
        .await
        .unwrap();
    let mut proof = crate::public_session::v2::proof::Proof::bounded(32 << 20);
    let before = proof.used();
    assert_eq!(
        super::protocol::history(&mut tx, &scope, &mut proof)
            .await
            .unwrap()
            .unwrap()
            .sequence,
        5
    );
    assert!(proof.used() > before);

    }).await.unwrap();
}
#[tokio::test]
async fn predecessor_queue_controls_never_enroll_originals() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};

    TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {

    crate::PostgresStore::migrate(&url).await.unwrap();
    let store = crate::PostgresStore::connect(&url).await.unwrap();
    let (f, record, audit) = crate::queue_contract::publications::legacy_rows(&store).await;
    let mut tx = crate::postgres::transaction::begin(&store.pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO future_queue(session_id,queue_id,sequence,state,prepared,summary_json,data_json,admission_key) VALUES($1,$2,1,'Queued',false,$3,$4,$5)").bind(&f.session.0).bind(&f.queue.0).bind(encode(&record.summary).unwrap()).bind(encode(&record).unwrap()).bind(record.input.admission.key.as_str()).execute(&mut *tx).await.unwrap();
    sqlx::query("INSERT INTO operation_receipts(session_id,operation_id,kind,semantic_digest,event_seq,data_json) VALUES($1,$2,'future_queue',$3,NULL,$4)").bind(&f.session.0).bind(&f.enqueue.id.0).bind(audit.receipt.semantic_digest.as_str()).bind(encode(&audit.receipt).unwrap()).execute(&mut *tx).await.unwrap();
    sqlx::query("INSERT INTO future_queue_audit(session_id,operation_id,queue_id,revision,data_json) VALUES($1,$2,$3,$4,$5)").bind(&f.session.0).bind(&f.enqueue.id.0).bind(&f.queue.0).bind(i64::try_from(audit.receipt.revision.get()).unwrap()).bind(encode(&audit).unwrap()).execute(&mut *tx).await.unwrap();
    sqlx::query("UPDATE sessions SET public_revision=$2 WHERE id=$1")
        .bind(&f.session.0)
        .bind(i64::try_from(audit.receipt.revision.get()).unwrap())
        .execute(&mut *tx)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert!(store.retained_queue(&f.session, &f.queue).await.is_ok());
    assert!(
        store
            .lookup_queue(&f.session, &f.enqueue.id)
            .await
            .unwrap()
            .is_some()
    );
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
        crate::QueueAction::Cancel {
            queue_id: f.queue.clone(),
        },
    )
    .await;
    store.mutate_queue(cancel).await.unwrap();
    let marker:i32=sqlx::query_scalar("SELECT CAST(source_protocol AS INTEGER) FROM future_queue WHERE session_id=$1 AND queue_id=$2").bind(&f.session.0).bind(&f.queue.0).fetch_one(&store.pool).await.unwrap();
    let sources: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM queue_publication_sources WHERE session_id=$1")
            .bind(&f.session.0)
            .fetch_one(&store.pool)
            .await
            .unwrap();
    assert_eq!((marker, sources), (0, 0));
    assert!(
        sqlx::query(
            "UPDATE future_queue SET source_protocol=1 WHERE session_id=$1 AND queue_id=$2"
        )
        .bind(&f.session.0)
        .bind(&f.queue.0)
        .execute(&store.pool)
        .await
        .is_err()
    );

    }).await.unwrap();
}

#[tokio::test]
async fn journal_context_precharges_workspace_and_preserves_historical_selection() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};

    use crate::public_session::v2::proof::{Proof, ReadError};
    TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {
        crate::PostgresStore::migrate(&url).await.unwrap();
        let store = crate::PostgresStore::connect(&url).await.unwrap();
        let f = crate::queue_contract::fixture(&store).await;
        store.mutate_queue(f.enqueue.clone()).await.unwrap();
        let mut tx = crate::postgres::transaction::begin(&store.pool)
            .await
            .unwrap();
        let mut measured = Proof::bounded(32 << 20);
        super::reads::current_with_proof(&mut tx, &f.session, &mut measured)
            .await
            .unwrap()
            .unwrap();
        super::reads::current_with_proof(&mut tx, &f.session, &mut Proof::bounded(measured.used()))
            .await
            .unwrap()
            .unwrap();
        let mut shared = Proof::bounded(measured.used() + 17);
        shared.charge(18).unwrap();
        assert!(matches!(
            super::reads::current_with_proof(&mut tx, &f.session, &mut shared).await,
            Err(ReadError::Budget(_))
        ));
        tx.rollback().await.unwrap();
        sqlx::query("UPDATE session_management SET workspace_id=$2 WHERE session_id=$1")
            .bind(&f.session.0)
            .bind("x".repeat(4096))
            .execute(&store.pool)
            .await
            .unwrap();
        let mut tx = crate::postgres::transaction::begin(&store.pool)
            .await
            .unwrap();
        assert!(matches!(
            super::reads::current_with_proof(&mut tx, &f.session, &mut Proof::bounded(1024)).await,
            Err(ReadError::Budget(_))
        ));
        assert!(matches!(
            super::reads::current_with_proof(&mut tx, &f.session, &mut Proof::bounded(32 << 20))
                .await,
            Err(ReadError::Domain(_))
        ));
        tx.rollback().await.unwrap();
        // Selected original publication lookup never needs later current metadata.
        assert!(
            store
                .lookup_queue(&f.session, &f.enqueue.id)
                .await
                .unwrap()
                .is_some()
        );
        assert!(store.queue_session_snapshot(&f.session).await.is_err());
        sqlx::query("UPDATE session_management SET workspace_id=$2 WHERE session_id=$1")
            .bind(&f.session.0)
            .bind("bound-workspace")
            .execute(&store.pool)
            .await
            .unwrap();
    })
    .await
    .unwrap();
}
#[tokio::test(flavor = "current_thread")]
async fn queue_proofs_issue_one_statement_per_logical_read() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};

    TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {
        crate::PostgresStore::migrate(&url).await.unwrap();
        let store = crate::PostgresStore::connect(&url).await.unwrap();
        let f = crate::queue_contract::fixture(&store).await;
        store.mutate_queue(f.enqueue.clone()).await.unwrap();
        let authority = crate::queue_contract::claim(&store, &f).await;
        let prepare = crate::queue_contract::mutation(
            &store,
            &f,
            crate::QueueAction::Prepare {
                authority: authority.clone(),
                prepared: Box::new(f.prepared.clone()),
            },
        )
        .await;
        store.mutate_queue(prepare).await.unwrap();
        let admit = crate::queue_contract::mutation(
            &store,
            &f,
            crate::QueueAction::Admit {
                authority,
                owner: "statement-bound".into(),
                lease: std::time::Duration::from_secs(30),
            },
        )
        .await;
        store.mutate_queue(admit).await.unwrap();
        let scope = Scope {
            session: f.session.clone(),
            queue: f.queue.clone(),
        };
        let mut tx = crate::postgres::transaction::begin(&store.pool)
            .await
            .unwrap();
        let statements = |captured: &crate::tracing_capture_tests::Capture| {
            captured
                .0
                .lock()
                .unwrap()
                .iter()
                .filter(|event| event.name == "event" && event.target == "sqlx::query")
                .count()
        };
        // Four publications. Each pass is one protocol probe, the cohort row with
        // its origin and head, one read per interior source, and per source one
        // audited leaf (history) or receipt (projection), plus one closing
        // check. The former split probes issued 43 and 38 statements here.
        let (captured, guard) = crate::tracing_capture_tests::capture();
        let mut proof = crate::public_session::v2::proof::Proof::bounded(32 << 20);
        let head = super::protocol::history(&mut tx, &scope, &mut proof)
            .await
            .unwrap()
            .unwrap();
        drop(guard);
        assert_eq!(head.sequence, 4);
        assert_eq!(statements(&captured), 11, "history statements");
        let (captured, guard) = crate::tracing_capture_tests::capture();
        let mut proof = crate::public_session::v2::proof::Proof::bounded(32 << 20);
        let head = super::protocol::projected(&mut tx, &scope, &mut proof)
            .await
            .unwrap()
            .unwrap();
        drop(guard);
        assert_eq!(head.sequence, 4);
        assert_eq!(statements(&captured), 11, "projection statements");
        tx.rollback().await.unwrap();
    })
    .await
    .unwrap();
}
