//! T4.1: three independently connected OS processes contend on one WAL database.
use super::{
    snapshot_tests::{assistant, event},
    test_support::*,
    tests::*,
    *,
};
use crate::abandonment_contract::FixtureStore;
use crate::*;
use crabber_core::*;
use time::OffsetDateTime;

const ITERATIONS: usize = 200;

#[tokio::test]
async fn writer_process_helper() {
    let Ok(encoded) = std::env::var("CRABBER_SQLITE_CONTESTED_RUN") else {
        return;
    };
    let original: Run = serde_json::from_str(&encoded).unwrap();
    let session = SessionId(std::env::var("CRABBER_SQLITE_WRITER_SESSION").unwrap());
    let store = SqliteStore::connect(std::env::var("CRABBER_SQLITE_CHILD_PATH").unwrap())
        .await
        .unwrap();
    let ready = std::env::var("CRABBER_SQLITE_WRITER_READY").unwrap();
    let gate = std::env::var("CRABBER_SQLITE_WRITER_GATE").unwrap();
    std::fs::write(ready, b"ready").unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        while !Path::new(&gate).exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let mut inputs = request(&session);
    inputs.session_id = Some(session.clone());
    let mut admitted = store.admit_run(inputs.clone()).await.unwrap();
    for i in 0..ITERATIONS {
        if i > 0 && i % 20 == 0 {
            store
                .execution(admitted.fence.clone())
                .await
                .unwrap()
                .settle_run(
                    RunStatus::Completed,
                    None,
                    Usage::default(),
                    event(&admitted, EventKind::RunSettled),
                )
                .await
                .unwrap();
            inputs.user_message = input(&session, "next run");
            admitted = store.admit_run(inputs.clone()).await.unwrap();
        }
        let execution = store.execution(admitted.fence.clone()).await.unwrap();
        execution
            .append_message(assistant(
                &admitted,
                &format!("writer {} iteration {i}", session.0),
            ))
            .await
            .unwrap();
        let mut appended = event(&admitted, EventKind::ExtensionNotice);
        appended.payload = serde_json::json!({"iteration":i});
        execution.append_event(appended).await.unwrap();
        execution
            .renew_lease(store.clock.now() + time::Duration::hours(1))
            .await
            .unwrap();
        match store
            .claim_expired_run(&original.id, &format!("writer-{}", session.0))
            .await
        {
            Ok(fence) => {
                let owned = store.get_run(&original.id).await.unwrap().unwrap();
                store
                    .abandon_run(AbandonRequest {
                        expected: fence,
                        expected_owner: owned.owner,
                        authority: AbandonAuthority::HostStoppedOwner,
                    })
                    .await
                    .unwrap();
            }
            Err(StoreError::Conflict) => (),
            other => panic!("unexpected recovery outcome: {other:?}"),
        }
        // All losing processes exercise a stale abandonment against the same run.
        assert!(matches!(
            store
                .abandon_run(AbandonRequest {
                    expected: RunFence {
                        run_id: original.id.clone(),
                        claim_token: original.claim_token.clone()
                    },
                    expected_owner: original.owner.clone(),
                    authority: AbandonAuthority::ExpiredLease,
                })
                .await,
            Err(AbandonError::StaleOwner)
        ));
    }
    store
        .execution(admitted.fence.clone())
        .await
        .unwrap()
        .settle_run(
            RunStatus::Completed,
            None,
            Usage::default(),
            event(&admitted, EventKind::RunSettled),
        )
        .await
        .unwrap();
    close(&store).await;
}

#[tokio::test]
async fn multi_process_writers_serialize_and_keep_fences() {
    let (store, path, directory) = temp_store().await;
    let contested = store.admit_run(request(&SessionId::new())).await.unwrap();
    let mut expired = contested.run.clone();
    expired.lease_until = OffsetDateTime::now_utc() - time::Duration::seconds(1);
    store.seed_run(expired.clone(), None).await;
    let gate = directory.path().join("writers.go");
    let sessions: Vec<_> = (0..3).map(|_| SessionId::new()).collect();
    for session in &sessions {
        let seeded = store.admit_run(request(session)).await.unwrap();
        store
            .execution(seeded.fence.clone())
            .await
            .unwrap()
            .settle_run(
                RunStatus::Completed,
                None,
                Usage::default(),
                event(&seeded, EventKind::RunSettled),
            )
            .await
            .unwrap();
    }
    let markers: Vec<_> = (0..3)
        .map(|i| directory.path().join(format!("writer-{i}.ready")))
        .collect();
    let mut children: Vec<_> = sessions
        .iter()
        .zip(&markers)
        .map(|(session, ready)| {
            child(
                &path,
                "sqlite::process_tests::writer_process_helper",
                &[
                    (
                        "CRABBER_SQLITE_CONTESTED_RUN",
                        serde_json::to_string(&expired).unwrap(),
                    ),
                    ("CRABBER_SQLITE_WRITER_SESSION", session.0.clone()),
                    (
                        "CRABBER_SQLITE_WRITER_READY",
                        ready.to_str().unwrap().into(),
                    ),
                    ("CRABBER_SQLITE_WRITER_GATE", gate.to_str().unwrap().into()),
                ],
            )
        })
        .collect();
    for (ready, process) in markers.iter().zip(&mut children) {
        marker(ready, process).await;
    }
    std::fs::write(gate, b"go").unwrap();
    for process in &mut children {
        process.wait().await;
    }
    for session in &sessions {
        assert_writer_history(&store, session).await;
    }
    let terminal = store
        .list_events(&contested.session.id, None, 100)
        .await
        .unwrap();
    assert_eq!(
        terminal
            .iter()
            .filter(|e| e.kind == EventKind::RunSettled)
            .count(),
        1
    );
    let commits: i64 =
        sqlx::query_scalar("SELECT count(*) FROM abandonment_commits WHERE run_id=$1")
            .bind(&contested.run.id.0)
            .fetch_one(&store.readers)
            .await
            .unwrap();
    assert_eq!(commits, 1);
    assert_eq!(
        store
            .get_run(&contested.run.id)
            .await
            .unwrap()
            .unwrap()
            .status,
        RunStatus::Interrupted
    );
    assert_eq!(
        store.execution(contested.fence).await.err().unwrap(),
        StoreError::Conflict
    );
}

async fn assert_writer_history(store: &SqliteStore, session: &SessionId) {
    let events = store.list_events(session, None, 1000).await.unwrap();
    assert!(
        events
            .windows(2)
            .all(|pair| pair[0].cursor.unwrap() < pair[1].cursor.unwrap())
    );
    let writes: Vec<_> = events
        .iter()
        .filter(|e| e.kind == EventKind::ExtensionNotice)
        .collect();
    assert_eq!(writes.len(), ITERATIONS);
    for (i, event) in writes.into_iter().enumerate() {
        assert_eq!(event.payload, serde_json::json!({"iteration":i}));
    }
    assert_eq!(
        store.list_all_messages(session).await.unwrap().len(),
        ITERATIONS + ITERATIONS / 20 + 1
    );
}
