//! Each crash boundary runs even when the separate projection integration fails.
use super::*;
async fn admission_boundary(kind: BackendKind, mode: &str) {
    let (dir, session) = handoff();
    let backend = kind.prepare(&dir.0).await;
    let store = backend.connect().await;
    fs::write(dir.0.join("text-only"), "yes").unwrap();
    fs::write(dir.0.join("short-admission-lease"), "yes").unwrap();
    finish(spawn(&backend, &dir.0, "commit-loss")).await;
    let receipt = original(&dir.0);
    expired(store.as_ref(), &receipt).await;
    let mut process = spawn(&backend, &dir.0, mode);
    if mode == "unstarted-before-begin" || mode == "unstarted-after-begin" {
        wait(&dir.0.join(if mode == "unstarted-before-begin" {
            "claimed"
        } else {
            "begun"
        }))
        .await;
        process.0.kill().unwrap();
        process.0.wait().unwrap();
    } else {
        finish(process).await;
    }
    assert_eq!(count(&dir.0.join("ledger"), "provider"), 0);
    assert!(
        store
            .latest_execution_attempt(&receipt.run_id)
            .await
            .unwrap()
            .is_none()
    );
    expired(store.as_ref(), &receipt).await;
    if mode == "unstarted-begin-loss" || mode == "unstarted-after-begin" {
        assert_eq!(
            store
                .load_admission_execution(&session, &options().key)
                .await
                .unwrap()
                .unwrap()
                .state,
            AdmissionExecutionState::Started
        );
        finish(spawn(&backend, &dir.0, "unstarted-native-absent")).await;
    } else {
        unstarted(store.as_ref(), &session).await;
        finish(spawn(&backend, &dir.0, "unstarted-complete-control")).await;
    }
    let events = store.list_events(&session, None, 1000).await.unwrap();
    finish(spawn(&backend, &dir.0, "unstarted-replay-control")).await;
    assert_eq!(
        events,
        store.list_events(&session, None, 1000).await.unwrap()
    );
    evidence(&backend, &dir.0, &session, 1, 0).await;
}
async fn provider_started(kind: BackendKind) {
    let (dir, session) = handoff();
    let backend = kind.prepare(&dir.0).await;
    let store = backend.connect().await;
    fs::write(dir.0.join("text-only"), "yes").unwrap();
    let mut process = spawn(&backend, &dir.0, "unstarted-provider-loss");
    wait(&dir.0.join("executing")).await;
    process.0.kill().unwrap();
    process.0.wait().unwrap();
    let receipt = original(&dir.0);
    expired(store.as_ref(), &receipt).await;
    finish(spawn(&backend, &dir.0, "unstarted-started-refuse")).await;
    evidence(&backend, &dir.0, &session, 1, 0).await;
}

#[cfg(feature = "sqlite")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqlite_claim_ack_loss() {
    admission_boundary(BackendKind::Sqlite, "unstarted-claim-loss").await;
}
#[cfg(feature = "postgres")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn postgres_claim_ack_loss() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};

    TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {
        crabber::session::PostgresStore::migrate(&url)
            .await
            .unwrap();
        admission_boundary(BackendKind::Postgres(url), "unstarted-claim-loss").await;
    })
    .await
    .unwrap();
}

#[cfg(feature = "sqlite")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqlite_claim_worker_death_before_admission_begin() {
    admission_boundary(BackendKind::Sqlite, "unstarted-before-begin").await;
}
#[cfg(feature = "postgres")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn postgres_claim_worker_death_before_admission_begin() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};

    TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {
        crabber::session::PostgresStore::migrate(&url)
            .await
            .unwrap();
        admission_boundary(BackendKind::Postgres(url), "unstarted-before-begin").await;
    })
    .await
    .unwrap();
}

#[cfg(feature = "sqlite")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqlite_admission_begin_failure() {
    admission_boundary(BackendKind::Sqlite, "unstarted-begin-fail").await;
}
#[cfg(feature = "postgres")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn postgres_admission_begin_failure() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};

    TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {
        crabber::session::PostgresStore::migrate(&url)
            .await
            .unwrap();
        admission_boundary(BackendKind::Postgres(url), "unstarted-begin-fail").await;
    })
    .await
    .unwrap();
}

#[cfg(feature = "sqlite")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqlite_admission_begin_ack_loss() {
    admission_boundary(BackendKind::Sqlite, "unstarted-begin-loss").await;
}
#[cfg(feature = "postgres")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn postgres_admission_begin_ack_loss() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};

    TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {
        crabber::session::PostgresStore::migrate(&url)
            .await
            .unwrap();
        admission_boundary(BackendKind::Postgres(url), "unstarted-begin-loss").await;
    })
    .await
    .unwrap();
}

#[cfg(feature = "sqlite")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqlite_worker_death_after_admission_begin() {
    admission_boundary(BackendKind::Sqlite, "unstarted-after-begin").await;
}
#[cfg(feature = "postgres")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn postgres_worker_death_after_admission_begin() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};

    TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {
        crabber::session::PostgresStore::migrate(&url)
            .await
            .unwrap();
        admission_boundary(BackendKind::Postgres(url), "unstarted-after-begin").await;
    })
    .await
    .unwrap();
}

#[cfg(feature = "sqlite")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqlite_started_provider_refuses_reexecution() {
    provider_started(BackendKind::Sqlite).await;
}
#[cfg(feature = "postgres")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn postgres_started_provider_refuses_reexecution() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};

    TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {
        crabber::session::PostgresStore::migrate(&url)
            .await
            .unwrap();
        provider_started(BackendKind::Postgres(url)).await;
    })
    .await
    .unwrap();
}
