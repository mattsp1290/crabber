//! Genuine original calls with before/after-commit ACK faults; no synthetic grants.
use super::{
    Arc, PendingDriverTasks, PendingInitialProducer, PendingInitialProducerStatus, forwarding,
    initial_consumer_fixture,
};
use crate::Store;
use crate::native_enrollment::initial_consumer::{
    CallKind, ConsumerClose, DriverRetirement, InitialConsumption,
};
use crate::public_session::v2::proof::Proof;
use std::sync::atomic::{AtomicBool, AtomicU8};
#[tokio::test]
async fn sqlite_initial_driver_retirement_original_register_activate_and_paired_ack_faults() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("retirement.sqlite");
    crate::SqliteStore::migrate(&path).await.unwrap();
    let store = Arc::new(crate::SqliteStore::connect(&path).await.unwrap());
    for keyed in [false, true] {
        for (kind, mode, expected) in [
            (CallKind::Register, 1, 0),
            (CallKind::Register, 2, 1),
            (CallKind::Register, 4, 1),
            (CallKind::Activate, 1, 1),
            (CallKind::Activate, 2, 2),
            (CallKind::Conclude, 3, 2),
        ] {
            Box::pin(fault_case(store.clone(), keyed, (kind, mode, expected))).await;
        }
    }
}

async fn orphan_without_driver(
    store: &crate::SqliteStore,
    session: &crabber_core::SessionId,
    expected: &crate::native_enrollment::initial_consumer::Correlation,
) {
    let mut tx = store.begin_write().await.unwrap();
    // Deliberate rollback-only projection corruption. The protected native Pending
    // origin remains while the selected consumer assignment is actually erased.
    sqlx::query("DROP TRIGGER initial_consumer_assignments_delete")
        .execute(&mut *tx)
        .await
        .unwrap();
    let removed = sqlx::query("DELETE FROM initial_consumer_assignments WHERE session_id=$1")
        .bind(&session.0)
        .execute(&mut *tx)
        .await
        .unwrap()
        .rows_affected();
    let mut proof = crate::public_session::v2::proof::Proof::bounded(32 << 20);
    let selected = super::super::storage::load(&mut tx, expected, &mut proof)
        .await
        .unwrap();
    let view = selected.selection(&mut proof).unwrap();
    let rejected =
        super::super::retirement::absence::verify_route(&mut tx, &view, expected, &mut proof).await;
    tx.rollback().await.unwrap();
    assert_eq!(removed, 1);
    assert!(
        rejected.is_err(),
        "NoDriver cannot hide current-epoch native origin with missing assignment and no Driver"
    );
}

async fn older_driver_new_no_opened(
    store: Arc<crate::SqliteStore>,
    session: &crabber_core::SessionId,
) {
    let source = crabber_extension::LibraryScriptedSource::new(
        crabber::Selection {
            provider_id: "library".into(),
            model_id: "text".into(),
        },
        vec!["never entered new epoch".into()],
    )
    .unwrap();
    let revision = store.session_revision(session).await.unwrap();
    let bound = initial_consumer_fixture::bound(
        session,
        revision,
        &source,
        crate::AdmissionSelectionPolicy::Explicit,
    );
    let (context, original) = initial_consumer_fixture::observe(store.as_ref(), false, bound).await;
    let owner = PendingDriverTasks::default();
    let adapter = Arc::new(forwarding::Forward {
        inner: store.clone(),
        kind: CallKind::Register,
        armed: Arc::new(AtomicBool::new(false)),
        calls: std::sync::Mutex::new(Vec::new()),
        fail_first_open: AtomicBool::new(true),
        fault: AtomicU8::new(0),
        expire_clock: None,
    });
    let mut producer =
        PendingInitialProducer::register_scripted(&owner, adapter, context, original, source)
            .unwrap();
    producer.start().unwrap();
    let observed = producer.join().await.unwrap();
    let status = observed.status().clone();
    let failure = observed.observation_failed();
    owner.wait_for_supervision().await;
    let unopened:i64=sqlx::query_scalar("SELECT count(*) FROM initial_consumer_origins WHERE session_id=$1 AND origin_kind='no_opened_member'").bind(&session.0).fetch_one(&store.readers).await.unwrap();
    assert!(matches!(
        status,
        PendingInitialProducerStatus::ClosedNoPublication
    ));
    assert!(!failure);
    assert_eq!(
        unopened, 1,
        "actual older unrelated joined Driver does not block new no-opened member"
    );
}

async fn effect_counts(
    store: &crate::SqliteStore,
    session: &crabber_core::SessionId,
) -> (i64, i64, i64, i64) {
    sqlx::query_as("SELECT (SELECT count(*) FROM session_owner_audits WHERE session_id=$1 AND operation='Join' AND owner_id LIKE 'driver-owner-%'),(SELECT count(*) FROM initial_consumer_calls WHERE session_id=$1),(SELECT count(*) FROM initial_consumer_closes WHERE session_id=$1),(SELECT count(*) FROM pending_initial_closes WHERE session_id=$1)").bind(&session.0).fetch_one(&store.readers).await.unwrap()
}

async fn closed_native_counts(
    store: &crate::SqliteStore,
    session: &crabber_core::SessionId,
) -> (ConsumerClose, i64, i64, InitialConsumption) {
    let raw: String =
        sqlx::query_scalar("SELECT data_json FROM initial_consumer_closes WHERE session_id=$1")
            .bind(&session.0)
            .fetch_one(&store.readers)
            .await
            .unwrap();
    let close: ConsumerClose = serde_json::from_str(&raw).unwrap();
    let joined:i64=sqlx::query_scalar("SELECT count(*) FROM session_owner_audits WHERE session_id=$1 AND operation='Join' AND owner_id LIKE 'driver-owner-%'").bind(&session.0).fetch_one(&store.readers).await.unwrap();
    let activated: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM initial_consumer_calls WHERE session_id=$1 AND kind='activate'",
    )
    .bind(&session.0)
    .fetch_one(&store.readers)
    .await
    .unwrap();
    let run: String = sqlx::query_scalar("SELECT id FROM runs WHERE session_id=$1")
        .bind(&session.0)
        .fetch_one(&store.readers)
        .await
        .unwrap();
    let mut tx = store.begin_write().await.unwrap();
    let run = crabber_core::RunId(run);
    let class = Box::pin(super::super::classify_selected_run(
        &mut tx,
        session,
        &run,
        &mut Proof::bounded(32 << 20),
    ))
    .await
    .unwrap();
    let report = Box::pin(crate::sqlite::deletion::initial_report_fixture(
        &mut tx,
        session,
        &run,
        &mut Proof::bounded(32 << 20),
    ))
    .await;
    tx.rollback().await.unwrap();
    let report = report.unwrap_or_else(|_| panic!("actual partial Initial report failed"));
    assert_eq!(report.initial_consumption, class);
    assert_eq!(
        report.main_join,
        if class == InitialConsumption::CompletedSource {
            crate::deletion::history::Fact::Authenticated
        } else {
            crate::deletion::history::Fact::Unproven
        }
    );
    assert_eq!(
        report.resource_policy,
        crate::deletion::history::Fact::Unproven
    );
    assert!(!report.complete());
    (close, joined, activated, class)
}

async fn fault_case(store: Arc<crate::SqliteStore>, keyed: bool, case: (CallKind, u8, i32)) {
    let (kind, mode, expected) = case;
    let clock = Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::now_utc(),
    ));
    let store = Arc::new(store.as_ref().clone().with_clock(clock.clone()));
    let source = crabber_extension::LibraryScriptedSource::new(
        crabber::Selection {
            provider_id: "library".into(),
            model_id: "text".into(),
        },
        vec!["finite".into()],
    )
    .unwrap();
    let (context, original) =
        initial_consumer_fixture::prepared(store.as_ref(), keyed, &source).await;
    let session = context.session_id().clone();
    let owner = PendingDriverTasks::default();
    let adapter = Arc::new(forwarding::Forward {
        inner: store.clone(),
        kind,
        armed: Arc::new(AtomicBool::new(false)),
        calls: std::sync::Mutex::new(Vec::new()),
        fail_first_open: AtomicBool::new(false),
        fault: AtomicU8::new(mode),
        expire_clock: Some(clock.clone()),
    });
    let mut producer =
        PendingInitialProducer::register_scripted(&owner, adapter, context, original, source)
            .unwrap();
    producer.start().unwrap();
    let observed = producer.join().await.unwrap();
    let first_status = observed.status().clone();
    let error = format!("{:?}", observed.error());
    let failed = observed.observation_failed();
    let before_retry = if mode == 3 {
        Some(effect_counts(store.as_ref(), &session).await)
    } else {
        None
    };
    let mut retained_retry = observed.take_retry();
    let retry_present = retained_retry.is_some();
    let retried = if mode == 3 {
        match retained_retry.as_mut() {
            Some(retry) => Some(retry.retry().await),
            None => None,
        }
    } else {
        None
    };
    let status = match &retried {
        Some(Ok(status)) => status.clone(),
        _ => first_status.clone(),
    };
    owner.wait_for_supervision().await;
    let after_retry = if mode == 3 {
        Some(effect_counts(store.as_ref(), &session).await)
    } else {
        None
    };
    let (close, joined, activated, class) =
        Box::pin(closed_native_counts(store.as_ref(), &session)).await;

    if kind == CallKind::Conclude {
        older_driver_new_no_opened(store.clone(), &session).await;
    }
    if expected == 0 {
        Box::pin(orphan_without_driver(
            store.as_ref(),
            &session,
            &close.correlation,
        ))
        .await;
    }

    if mode == 3 {
        assert!(
            matches!(first_status, PendingInitialProducerStatus::Unresolved),
            "lost ACK remains explicitly unresolved before SAME retained retry"
        );
        assert!(retry_present);
        assert!(matches!(
            retried,
            Some(Ok(PendingInitialProducerStatus::ClosedAssignment))
        ));
        assert_eq!(
            before_retry, after_retry,
            "SAME Seal retry publishes no additional Join/member/close"
        );
    }
    assert!(
        matches!(status, PendingInitialProducerStatus::ClosedAssignment),
        "{keyed}/{expected}/{mode}: {status:?} {error}"
    );
    assert!(!failed, "{error}");
    verify_retirement(&close, expected, (joined, activated, class), (kind, mode));
}

fn verify_retirement(
    close: &ConsumerClose,
    expected: i32,
    counts: (i64, i64, InitialConsumption),
    call: (CallKind, u8),
) {
    let (joined, activated, consumption) = counts;
    let (kind, mode) = call;
    assert_eq!(
        consumption,
        if kind == CallKind::Conclude {
            InitialConsumption::CompletedSource
        } else {
            InitialConsumption::AuthenticatedClosedPrefix
        }
    );
    let class = match &close.retirement {
        DriverRetirement::NoDriver => 0,
        DriverRetirement::Unactivated { .. } => 1,
        DriverRetirement::Activated { .. } => 2,
    };
    assert_eq!(class, expected);
    assert_eq!(joined, i64::from(expected != 0));
    assert_eq!(
        activated,
        i64::from(kind == CallKind::Conclude || kind == CallKind::Activate && mode == 2)
    );
    if let Some(audit) = close.retirement.audit() {
        assert_eq!(
            audit.record.snapshot.state,
            crate::session_owner::SessionOwnerState::Joined
        );
        assert_eq!(audit.record.token, "");
        assert_eq!(audit.record.owned_join.is_some(), expected == 2);
    }
}
