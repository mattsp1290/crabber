use super::tests::{TEST_LOCK, test_url};
use super::*;
use crate::abandonment_contract::{FixtureStore, abandon_request, event, request};
use crabber_core::{
    AbandonAuthority, AbandonError, AbandonOutcome, AbandonRequest, EventKind, ManualClock,
};
use std::sync::atomic::Ordering;

#[async_trait]
impl FixtureStore for PostgresStore {
    async fn seed_run(&self, run: Run) {
        let mut tx = self.pool.begin().await.unwrap();
        load_run(&mut tx, &run.id, true).await.unwrap();
        save_run(&mut tx, &run).await.unwrap();
        tx.commit().await.unwrap();
    }
    async fn calls(&self, run: &RunId) -> Vec<ToolCallRecord> {
        sqlx::query("SELECT data FROM tool_calls WHERE run_id=$1 ORDER BY id")
            .bind(&run.0)
            .fetch_all(&self.pool)
            .await
            .unwrap()
            .into_iter()
            .map(|row| decode(row.get("data")).unwrap())
            .collect()
    }
    async fn unconsumed_inbox(&self, session: &SessionId) -> usize {
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM inbox WHERE session_id=$1 AND consumed_by_run IS NULL",
        )
        .bind(&session.0)
        .fetch_one(&self.pool)
        .await
        .unwrap();
        usize::try_from(count).unwrap()
    }
}

#[tokio::test]
async fn shared_abandonment_contract() {
    let Some(url) = test_url() else { return };
    let _guard = TEST_LOCK.lock().await;
    PostgresStore::migrate(&url).await.unwrap();
    let clock = Arc::new(ManualClock::new(OffsetDateTime::UNIX_EPOCH));
    let store = PostgresStore::connect(&url)
        .await
        .unwrap()
        .with_clock(clock.clone());
    crate::abandonment_contract::run_contract(store, clock).await;
}

#[tokio::test]
async fn process_helper() {
    let Ok(encoded) = std::env::var("CRABBER_ABANDON_CHILD_REQUEST") else {
        return;
    };
    let request: AbandonRequest = serde_json::from_str(&encoded).unwrap();
    let expected: AbandonOutcome =
        serde_json::from_str(&std::env::var("CRABBER_ABANDON_CHILD_OUTCOME").unwrap()).unwrap();
    let store = PostgresStore::connect(&test_url().expect("child database required"))
        .await
        .unwrap();
    assert_eq!(
        store
            .get_run(&request.expected.run_id)
            .await
            .unwrap()
            .unwrap(),
        expected.run
    );
    let messages = store
        .list_all_messages(&expected.run.session_id)
        .await
        .unwrap();
    let events = store
        .list_events(&expected.run.session_id, None, 100)
        .await
        .unwrap();
    let durable_parts: BTreeMap<crabber_core::PartId, Part> = sqlx::query(
        "SELECT parts.data FROM parts JOIN messages ON messages.id=parts.message_id WHERE messages.session_id=$1",
    ).bind(&expected.run.session_id.0).fetch_all(&store.pool).await.unwrap()
        .into_iter().map(|row| {let part: Part = decode(row.get("data")).unwrap(); (part.id.clone(), part)}).collect();
    let message_parts: BTreeMap<_, _> = messages
        .iter()
        .flat_map(|message| &message.parts)
        .map(|part| (part.id.clone(), part.clone()))
        .collect();
    assert_eq!(durable_parts, message_parts);

    assert!(
        store
            .list_unfinished_tool_calls(&expected.run.id)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(store.abandon_run(request).await.unwrap(), expected);
    assert_eq!(
        store
            .list_all_messages(&expected.run.session_id)
            .await
            .unwrap(),
        messages
    );
    assert_eq!(
        store
            .list_events(&expected.run.session_id, None, 100)
            .await
            .unwrap(),
        events
    );
    assert_eq!(
        events
            .iter()
            .filter(|e| e.kind == EventKind::RunSettled)
            .count(),
        1
    );
    println!(
        "fresh-process durable replay: run={} event={:?}, no unfinished tools and no duplicate messages/events",
        expected.run.id, expected.terminal_event.cursor
    );
    store.pool.close().await;
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn rollback_unknown_response_and_fresh_process_replay() {
    let Some(url) = test_url() else { return };
    let _guard = TEST_LOCK.lock().await;
    PostgresStore::migrate(&url).await.unwrap();
    let store = PostgresStore::connect(&url).await.unwrap();
    let admitted = store.admit_run(request()).await.unwrap();
    let execution = store.execution(admitted.fence.clone()).await.unwrap();
    let call = ToolCallRecord {
        id: ToolCallId::new(),
        run_id: admitted.run.id.clone(),
        name: "unavailable".into(),
        arguments: serde_json::json!({}),
        status: ToolCallStatus::Pending,
        retry_safe: false,
        result: None,
    };
    execution
        .create_tool_call(call, event(&admitted.run, EventKind::ToolCallPending))
        .await
        .unwrap();
    let request = abandon_request(&admitted, AbandonAuthority::HostStoppedOwner);
    let messages = store
        .list_all_messages(&admitted.run.session_id)
        .await
        .unwrap();
    let events = store
        .list_events(&admitted.run.session_id, None, 100)
        .await
        .unwrap();
    let calls = store.calls(&admitted.run.id).await;
    store.abandon_fault.store(1, Ordering::SeqCst);
    assert!(matches!(
        store.abandon_run(request.clone()).await,
        Err(AbandonError::Store(_))
    ));
    let fresh = PostgresStore::connect(&url).await.unwrap();
    assert_eq!(
        fresh.get_run(&admitted.run.id).await.unwrap().unwrap(),
        admitted.run
    );
    assert_eq!(
        fresh
            .list_all_messages(&admitted.run.session_id)
            .await
            .unwrap(),
        messages
    );
    assert_eq!(
        fresh
            .list_events(&admitted.run.session_id, None, 100)
            .await
            .unwrap(),
        events
    );
    assert_eq!(fresh.calls(&admitted.run.id).await, calls);
    store.abandon_fault.store(2, Ordering::SeqCst);
    assert!(matches!(
        store.abandon_run(request.clone()).await,
        Err(AbandonError::Store(_))
    ));
    let outcome = fresh.abandon_run(request.clone()).await.unwrap();
    assert_eq!(outcome.run.status, RunStatus::Interrupted);
    let exe = std::env::current_exe().unwrap();
    let child = std::process::Command::new(exe)
        .args([
            "--exact",
            "postgres::abandon_tests::process_helper",
            "--nocapture",
        ])
        .env(
            "CRABBER_ABANDON_CHILD_REQUEST",
            serde_json::to_string(&request).unwrap(),
        )
        .env(
            "CRABBER_ABANDON_CHILD_OUTCOME",
            serde_json::to_string(&outcome).unwrap(),
        )
        .output()
        .unwrap();
    print!("{}", String::from_utf8_lossy(&child.stdout));
    assert!(
        child.status.success(),
        "child: {} {}",
        String::from_utf8_lossy(&child.stdout),
        String::from_utf8_lossy(&child.stderr)
    );
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn independent_pools_renew_resume_and_abandon_serialize() {
    let Some(url) = test_url() else { return };
    let _guard = TEST_LOCK.lock().await;
    PostgresStore::migrate(&url).await.unwrap();
    let now = OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();
    let clock = Arc::new(ManualClock::new(now));
    let store = PostgresStore::connect(&url)
        .await
        .unwrap()
        .with_clock(clock.clone());
    let other = PostgresStore::connect(&url)
        .await
        .unwrap()
        .with_clock(clock.clone());
    // Every contender is on an independent pool. Hold the common row to force
    // overlap and assert the queued operation really waits, then release it.
    for mode in 0..4 {
        clock.set(now);
        let admitted = store.admit_run(request()).await.unwrap();
        let execution = other.execution(admitted.fence.clone()).await.unwrap();
        if mode != 0 {
            clock.set(admitted.run.lease_until);
        }
        let request = abandon_request(&admitted, AbandonAuthority::ExpiredLease);
        let mut held = store.pool.begin().await.unwrap();
        load_run(&mut held, &admitted.run.id, true).await.unwrap();
        let competing_store = other.clone();
        let competing_run = admitted.run.clone();
        let competing_request = request.clone();
        let contender = tokio::spawn(async move {
            match mode {
                0 => execution
                    .renew_lease(competing_run.lease_until + time::Duration::hours(1))
                    .await
                    .map(|()| None),
                1 => competing_store
                    .claim_expired_run(&competing_run.id, "replacement")
                    .await
                    .map(|_| None),
                2 => competing_store
                    .abandon_run(competing_request)
                    .await
                    .map(Some)
                    .map_err(|e| match e {
                        AbandonError::Store(e) => e,
                        _ => StoreError::Conflict,
                    }),
                _ => {
                    let mut altered = competing_request;
                    altered.authority = AbandonAuthority::HostStoppedOwner;
                    competing_store
                        .abandon_run(altered)
                        .await
                        .map(Some)
                        .map_err(|e| match e {
                            AbandonError::Store(e) => e,
                            _ => StoreError::Conflict,
                        })
                }
            }
        });
        // Wait until the contender is blocked on the run row, not just scheduled.
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                let count: i64 = sqlx::query_scalar("SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE '%runs%'")
                    .fetch_one(&store.pool).await.unwrap();
                if count > 0 {break;} tokio::task::yield_now().await;
            }
        }).await.unwrap();
        assert!(!contender.is_finished());
        let abandoning = store.clone();
        let attempted = request.clone();
        let abandonment = tokio::spawn(async move { abandoning.abandon_run(attempted).await });
        held.commit().await.unwrap();
        let first = contender.await.unwrap();
        let second = abandonment.await.unwrap();
        match mode {
            0 => {
                assert!(first.is_ok());
                assert_eq!(second.unwrap_err(), AbandonError::LiveLease);
            }
            1 => {
                assert!(first.is_ok());
                assert_eq!(second.unwrap_err(), AbandonError::StaleOwner);
            }
            2 => {
                assert_eq!(first.unwrap().unwrap(), second.unwrap());
            }
            _ => {
                assert!(first.unwrap().is_some());
                assert_eq!(second.unwrap_err(), AbandonError::StaleOwner);
            }
        }
        let events = store
            .list_events(&admitted.run.session_id, None, 100)
            .await
            .unwrap();
        assert_eq!(
            events
                .iter()
                .filter(|e| e.kind == EventKind::RunSettled)
                .count(),
            usize::from(mode >= 2)
        );
        if mode >= 1 {
            assert_eq!(
                other.execution(admitted.fence.clone()).await.err().unwrap(),
                StoreError::Conflict
            );
        }
    }
}

#[test]
fn required_url_is_enforced_in_fresh_process() {
    if std::env::var("CRABBER_ABANDON_MISSING_URL_CHILD").as_deref() == Ok("1") {
        test_url();
        return;
    }
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "postgres::abandon_tests::required_url_is_enforced_in_fresh_process",
            "--nocapture",
        ])
        .env("CRABBER_ABANDON_MISSING_URL_CHILD", "1")
        .env("CRABBER_REQUIRE_POSTGRES", "1")
        .env_remove("CRABBER_TEST_POSTGRES_URL")
        .output()
        .unwrap();
    println!(
        "required URL negative subprocess exit: {:?}",
        output.status.code()
    );
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("CRABBER_TEST_POSTGRES_URL is required")
    );
}

#[tokio::test]
async fn clock_is_read_under_lock_and_abandonment_wins_queued_recovery() {
    let Some(url) = test_url() else { return };
    let _guard = TEST_LOCK.lock().await;
    PostgresStore::migrate(&url).await.unwrap();
    let clock = Arc::new(ManualClock::new(
        OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap(),
    ));
    let store = PostgresStore::connect(&url)
        .await
        .unwrap()
        .with_clock(clock.clone());
    let other = PostgresStore::connect(&url)
        .await
        .unwrap()
        .with_clock(clock.clone());
    for resume in [false, true] {
        let admitted = store.admit_run(request()).await.unwrap();
        let execution = other.execution(admitted.fence.clone()).await.unwrap();
        let mut held = store.pool.begin().await.unwrap();
        load_run(&mut held, &admitted.run.id, true).await.unwrap();
        let abandoning = store.clone();
        let expected = abandon_request(&admitted, AbandonAuthority::ExpiredLease);
        let abandon = tokio::spawn(async move { abandoning.abandon_run(expected).await });
        wait_for_locks(&store, 1).await;
        // The request was submitted while the lease was live, but eligibility
        // must use the clock after waiting for ownership serialization.
        clock.set(admitted.run.lease_until);
        let recovering = other.clone();
        let id = admitted.run.id.clone();
        let until = clock.now() + time::Duration::hours(1);
        let recovery = tokio::spawn(async move {
            if resume {
                recovering
                    .claim_expired_run(&id, "resume")
                    .await
                    .map(|_| ())
            } else {
                execution.renew_lease(until).await
            }
        });
        wait_for_locks(&store, 2).await;
        held.commit().await.unwrap();
        assert_eq!(
            abandon.await.unwrap().unwrap().run.status,
            RunStatus::Interrupted
        );
        assert_eq!(recovery.await.unwrap().unwrap_err(), StoreError::Conflict);
        assert_eq!(
            store
                .list_events(&admitted.run.session_id, None, 100)
                .await
                .unwrap()
                .iter()
                .filter(|event| event.kind == EventKind::RunSettled)
                .count(),
            1
        );
    }
}

async fn wait_for_locks(store: &PostgresStore, required: i64) {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let count: i64 = sqlx::query_scalar("SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE '%runs%'")
                .fetch_one(&store.pool).await.unwrap();
            if count >= required {break;} tokio::task::yield_now().await;
        }
    }).await.unwrap();
}

#[tokio::test]
async fn denial_process_helper() {
    let Ok(encoded) = std::env::var("CRABBER_ABANDON_DENIAL_REQUEST") else {
        return;
    };
    let request: AbandonRequest = serde_json::from_str(&encoded).unwrap();
    let expected_run: Run =
        serde_json::from_str(&std::env::var("CRABBER_ABANDON_DENIAL_RUN").unwrap()).unwrap();
    let store = PostgresStore::connect(&test_url().expect("child database required"))
        .await
        .unwrap();
    // Bound a regression's lock wait so this fresh-process check fails promptly
    // even while its parent synchronously waits for this process to exit.
    sqlx::query("SET statement_timeout = '5s'")
        .execute(&store.pool)
        .await
        .unwrap();
    let expected_error = if request.expected_owner == expected_run.owner {
        AbandonError::LiveLease
    } else {
        AbandonError::StaleOwner
    };
    assert_eq!(
        store.abandon_run(request).await.unwrap_err(),
        expected_error
    );
    assert_eq!(
        store.get_run(&expected_run.id).await.unwrap().unwrap(),
        expected_run
    );
    println!(
        "fresh-process denial: {expected_error:?}, run={} unchanged and lock released",
        expected_run.id
    );
}

#[tokio::test]
async fn denials_and_precommit_failure_release_locks_before_return() {
    let Some(url) = test_url() else { return };
    let _guard = TEST_LOCK.lock().await;
    PostgresStore::migrate(&url).await.unwrap();
    let store = PostgresStore::connect(&url).await.unwrap();
    for mode in 0..3 {
        let admitted = store.admit_run(request()).await.unwrap();
        let mut attempted = abandon_request(&admitted, AbandonAuthority::ExpiredLease);
        if mode == 1 {
            attempted.expected_owner = "stale".into();
        }
        if mode == 2 {
            store.abandon_fault.store(1, Ordering::SeqCst);
            let mut stopped = attempted.clone();
            stopped.authority = AbandonAuthority::HostStoppedOwner;
            assert!(matches!(
                store.abandon_run(stopped).await,
                Err(AbandonError::Store(_))
            ));
            store.abandon_fault.store(0, Ordering::SeqCst);
        } else {
            let error = if mode == 0 {
                AbandonError::LiveLease
            } else {
                AbandonError::StaleOwner
            };
            assert_eq!(
                store.abandon_run(attempted.clone()).await.unwrap_err(),
                error
            );
        }
        let child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "postgres::abandon_tests::denial_process_helper",
                "--nocapture",
            ])
            .env(
                "CRABBER_ABANDON_DENIAL_REQUEST",
                serde_json::to_string(&attempted).unwrap(),
            )
            .env(
                "CRABBER_ABANDON_DENIAL_RUN",
                serde_json::to_string(&admitted.run).unwrap(),
            )
            .output()
            .unwrap();
        print!("{}", String::from_utf8_lossy(&child.stdout));
        assert!(
            child.status.success(),
            "{}",
            String::from_utf8_lossy(&child.stderr)
        );
    }
}
