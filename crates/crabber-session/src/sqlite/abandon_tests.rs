use super::records::{decode_row, load_run, save_run};
use super::*;
use crate::abandonment_contract::FixtureStore;
use async_trait::async_trait;
use crabber_core::{ManualClock, Run, RunId, SessionId, ToolCallRecord};
use time::OffsetDateTime;

#[async_trait]
impl FixtureStore for SqliteStore {
    async fn seed_run(&self, run: Run, _fixture_url: Option<&str>) {
        let mut tx = self.begin_write().await.unwrap();
        load_run(&mut tx, &run.id).await.unwrap();
        save_run(&mut tx, &run).await.unwrap();
        tx.commit().await.unwrap();
    }
    async fn calls(&self, run: &RunId, _fixture_url: Option<&str>) -> Vec<ToolCallRecord> {
        sqlx::query("SELECT data FROM tool_calls WHERE run_id=$1 ORDER BY id")
            .bind(&run.0)
            .fetch_all(&self.readers)
            .await
            .unwrap()
            .into_iter()
            .map(|row| decode_row(&row, "data").unwrap())
            .collect()
    }
    async fn unconsumed_inbox(&self, session: &SessionId, _fixture_url: Option<&str>) -> usize {
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM inbox WHERE session_id=$1 AND consumed_by_run IS NULL",
        )
        .bind(&session.0)
        .fetch_one(&self.readers)
        .await
        .unwrap();
        usize::try_from(count).unwrap()
    }
}

#[tokio::test]
async fn shared_abandonment_contract() {
    let (store, _, _directory) = super::tests::temp_store().await;
    let clock = Arc::new(ManualClock::new(OffsetDateTime::UNIX_EPOCH));
    let store = store.with_clock(clock.clone());
    crate::abandonment_contract::run_contract(store, clock).await;
}

#[tokio::test]
async fn untrusted_terminal_markers_are_never_replay_authority() {
    let (store, _, _directory) = super::tests::temp_store().await;
    let clock = Arc::new(ManualClock::new(OffsetDateTime::UNIX_EPOCH));
    let store = store.with_clock(clock.clone());
    crate::abandonment_contract::untrusted_terminal_contract(&store, &clock).await;
}

use super::{
    test_support::{child, close, raw, wait_writers},
    tests::temp_store,
};
use crate::abandonment_contract::{abandon_request, event, request};
use crate::*;
use crabber_core::*;
use sqlx::Row;
use std::{collections::BTreeMap, sync::atomic::Ordering};
#[tokio::test]
async fn process_helper() {
    let Ok(encoded) = std::env::var("CRABBER_SQLITE_ABANDON_CHILD_REQUEST") else {
        return;
    };
    let request: AbandonRequest = serde_json::from_str(&encoded).unwrap();
    let expected: AbandonOutcome =
        serde_json::from_str(&std::env::var("CRABBER_SQLITE_ABANDON_CHILD_OUTCOME").unwrap())
            .unwrap();
    let store = SqliteStore::connect(std::env::var("CRABBER_SQLITE_CHILD_PATH").unwrap())
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
    ).bind(&expected.run.session_id.0).fetch_all(&store.readers).await.unwrap()
        .into_iter().map(|row| {let part: Part = decode(row.get("data")).unwrap(); (part.id.clone(), part)}).collect();
    let message_parts: BTreeMap<_, _> = messages
        .iter()
        .flat_map(|message| &message.parts)
        .map(|part| (part.id.clone(), part.clone()))
        .collect();
    assert_eq!(durable_parts, message_parts);

    assert_eq!(
        store
            .list_unfinished_tool_calls(&expected.run.id)
            .await
            .unwrap(),
        [] as [crabber_core::ToolCallRecord; 0]
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
    close(&store).await;
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn rollback_unknown_response_and_fresh_process_replay() {
    let (store, path, _directory) = temp_store().await;
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
    let calls = store.calls(&admitted.run.id, None).await;
    store.abandon_fault.store(1, Ordering::SeqCst);
    assert!(matches!(
        store.abandon_run(request.clone()).await,
        Err(AbandonError::Store(_))
    ));
    let fresh = SqliteStore::connect(&path).await.unwrap();
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
    assert_eq!(fresh.calls(&admitted.run.id, None).await, calls);
    store.abandon_fault.store(2, Ordering::SeqCst);
    assert!(matches!(
        store.abandon_run(request.clone()).await,
        Err(AbandonError::Store(_))
    ));
    let outcome = fresh.abandon_run(request.clone()).await.unwrap();
    assert_eq!(outcome.run.status, RunStatus::Interrupted);
    child(
        &path,
        "sqlite::abandon_tests::process_helper",
        &[
            (
                "CRABBER_SQLITE_ABANDON_CHILD_REQUEST",
                serde_json::to_string(&request).unwrap(),
            ),
            (
                "CRABBER_SQLITE_ABANDON_CHILD_OUTCOME",
                serde_json::to_string(&outcome).unwrap(),
            ),
        ],
    )
    .wait()
    .await;
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn independent_pools_renew_resume_and_abandon_serialize() {
    let (_initial, path, _directory) = temp_store().await;
    let now = OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();
    let clock = Arc::new(ManualClock::new(now));
    let store = SqliteStore::connect(&path)
        .await
        .unwrap()
        .with_clock(clock.clone());
    let other = SqliteStore::connect(&path)
        .await
        .unwrap()
        .with_clock(clock.clone());
    // Every contender is on an independent pool. Hold the database write lock to force
    // overlap and assert the queued operation really waits, then release it.
    for mode in 0..4 {
        clock.set(now);
        let admitted = store.admit_run(request()).await.unwrap();
        let execution = other.execution(admitted.fence.clone()).await.unwrap();
        if mode != 0 {
            clock.set(admitted.run.lease_until);
        }
        let request = abandon_request(&admitted, AbandonAuthority::ExpiredLease);
        let mut holder = raw(&path).await;
        let held = holder.begin_with("BEGIN IMMEDIATE").await.unwrap();
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
        wait_writers(&other, 1).await;
        assert!(!contender.is_finished());
        let abandoning = other.clone();
        let attempted = request.clone();
        let abandonment = tokio::spawn(async move { abandoning.abandon_run(attempted).await });
        wait_writers(&other, 2).await;
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

#[tokio::test]
async fn clock_is_read_under_lock_and_abandonment_wins_queued_recovery() {
    let (_initial, path, _directory) = temp_store().await;
    let clock = Arc::new(ManualClock::new(
        OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap(),
    ));
    let store = SqliteStore::connect(&path)
        .await
        .unwrap()
        .with_clock(clock.clone());
    let other = SqliteStore::connect(&path)
        .await
        .unwrap()
        .with_clock(clock.clone());
    for resume in [false, true] {
        let admitted = store.admit_run(request()).await.unwrap();
        let execution = other.execution(admitted.fence.clone()).await.unwrap();
        let mut holder = raw(&path).await;
        let held = holder.begin_with("BEGIN IMMEDIATE").await.unwrap();
        let abandoning = other.clone();
        let expected = abandon_request(&admitted, AbandonAuthority::ExpiredLease);
        let abandon = tokio::spawn(async move { abandoning.abandon_run(expected).await });
        wait_writers(&other, 1).await;
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
        wait_writers(&other, 2).await;
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

#[tokio::test]
async fn denial_process_helper() {
    let Ok(encoded) = std::env::var("CRABBER_SQLITE_ABANDON_DENIAL_REQUEST") else {
        return;
    };
    let request: AbandonRequest = serde_json::from_str(&encoded).unwrap();
    let expected_run: Run =
        serde_json::from_str(&std::env::var("CRABBER_SQLITE_ABANDON_DENIAL_RUN").unwrap()).unwrap();
    let store = SqliteStore::connect(std::env::var("CRABBER_SQLITE_CHILD_PATH").unwrap())
        .await
        .unwrap();
    let path = std::env::var("CRABBER_SQLITE_CHILD_PATH").unwrap();
    let mut connection = raw(Path::new(&path)).await;
    connection
        .begin_with("BEGIN IMMEDIATE")
        .await
        .unwrap()
        .rollback()
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
    let (store, path, _directory) = temp_store().await;
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
        child(
            &path,
            "sqlite::abandon_tests::denial_process_helper",
            &[
                (
                    "CRABBER_SQLITE_ABANDON_DENIAL_REQUEST",
                    serde_json::to_string(&attempted).unwrap(),
                ),
                (
                    "CRABBER_SQLITE_ABANDON_DENIAL_RUN",
                    serde_json::to_string(&admitted.run).unwrap(),
                ),
            ],
        )
        .wait()
        .await;
    }
}
