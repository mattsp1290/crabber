//! Independent OS processes exercise the public facade against a real database.
use super::{config, options};
use async_trait::async_trait;
use crabber::{
    Admission, AdmissionKey, AdmissionReceipt, Agent, FakeProvider, RuntimeError, StreamDelta,
    core::{
        EpochId, EventCursor, EventRecord, Message, Role, Run, RunFence, RunId, RunStatus, Session,
        SessionId, ToolCallId, ToolCallRecord,
    },
    providers::{DeltaStream, ModelRequest, ProviderError, Resolver, Selection, Streamer},
    session::{
        AdmitOutcome, AdmitRequest, ExecutionStore, InboxKind, KeyedAdmitOutcome,
        KeyedAdmitRequest, PostgresStore, Store, StoreError,
    },
};
use std::{
    collections::BTreeMap,
    fs,
    io::Write,
    path::{Path, PathBuf},
    process::{Child, Command},
    sync::Arc,
    time::{Duration, Instant},
};

fn append(path: &Path, entry: &str) {
    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .unwrap();
    writeln!(file, "{entry}").unwrap();
    file.sync_all().unwrap();
}
fn count(path: &Path, entry: &str) -> usize {
    fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .filter(|line| *line == entry)
        .count()
}
async fn wait(path: &Path) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while !path.exists() {
        assert!(Instant::now() < deadline, "process handshake timed out");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}
mod fault_store;
use fault_store::FaultStore;

mod provider;
use provider::agent;

mod child;

struct ChildGuard(Child);
impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn spawn(dir: &Path, mode: &str) -> ChildGuard {
    ChildGuard(
        Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "process::child::facade_child", "--nocapture"])
            .env("CRABBER_RECEIPT_CHILD", mode)
            .env("CRABBER_RECEIPT_HANDOFF", dir)
            .spawn()
            .unwrap(),
    )
}
async fn finish(mut child: ChildGuard) {
    let deadline = Instant::now() + Duration::from_secs(40);
    loop {
        if let Some(status) = child.0.try_wait().unwrap() {
            assert!(status.success(), "child failed");
            return;
        }
        if Instant::now() >= deadline {
            child.0.kill().unwrap();
            child.0.wait().unwrap();
            panic!("child timed out")
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}
struct Handoff(PathBuf);
impl Drop for Handoff {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
fn handoff() -> (Handoff, SessionId) {
    let session = SessionId::new();
    let path = std::env::temp_dir().join(format!("crabber-receipt-{session}"));
    fs::create_dir(&path).unwrap();
    fs::write(
        path.join("session.json"),
        serde_json::to_vec(&session).unwrap(),
    )
    .unwrap();
    (Handoff(path), session)
}
async fn evidence(url: &str, dir: &Path, session: &SessionId, executions: usize, tools: usize) {
    let store = PostgresStore::connect(url).await.unwrap();
    let receipt = store
        .lookup_admission(session, &options().key)
        .await
        .unwrap()
        .unwrap();
    let messages = store.list_all_messages(session).await.unwrap();
    let users = messages.iter().filter(|m| m.role == Role::User).count();
    let pool = sqlx::PgPool::connect(url).await.unwrap();
    let runs: i64 = sqlx::query_scalar("SELECT count(*) FROM runs WHERE session_id=$1")
        .bind(&session.0)
        .fetch_one(&pool)
        .await
        .unwrap();
    let receipts: i64 =
        sqlx::query_scalar("SELECT count(*) FROM admission_receipts WHERE session_id=$1")
            .bind(&session.0)
            .fetch_one(&pool)
            .await
            .unwrap();
    let schema: i32 = sqlx::query_scalar("SELECT max(version) FROM schema_version")
        .fetch_one(&pool)
        .await
        .unwrap();
    let provider_count = count(&dir.join("ledger"), "provider");
    let tool_count = count(&dir.join("ledger"), "tool");
    assert_eq!(
        (runs, receipts, users, provider_count, tool_count),
        (1, 1, 1, executions, tools)
    );
    let original: AdmissionReceipt =
        serde_json::from_slice(&fs::read(dir.join("committed.json")).unwrap()).unwrap();
    assert_eq!(receipt, original);
    for entry in fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().is_some_and(|ext| ext == "json")
            && path.file_name().unwrap() != "session.json"
        {
            let child: AdmissionReceipt = serde_json::from_slice(&fs::read(path).unwrap()).unwrap();
            assert_eq!(child, receipt);
        }
    }
    let sha = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .unwrap();
    assert!(sha.status.success());
    println!(
        "source={} store=postgres schema={schema} provider=fake/scripted session={session} receipt={} run={} user_message={} receipts={receipts} runs={runs} user_messages={users} provider_requests={provider_count} tool_effects={tool_count} count_assertions=5",
        String::from_utf8(sha.stdout).unwrap().trim(),
        receipt.run_id,
        receipt.run_id,
        receipt.user_message_id
    );
    pool.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn postgres_public_facade_fault_restart_journey() {
    let Ok(url) = std::env::var("CRABBER_TEST_POSTGRES_URL") else {
        assert!(
            std::env::var("CRABBER_REQUIRE_POSTGRES").as_deref() != Ok("1"),
            "CRABBER_TEST_POSTGRES_URL required"
        );
        eprintln!("skipping live facade journey: CRABBER_TEST_POSTGRES_URL absent");
        return;
    };
    PostgresStore::migrate(&url).await.unwrap();
    // Failed before admission: no session/receipt, exact retry can start once.
    let (dir, session) = handoff();
    finish(spawn(&dir.0, "precommit")).await;
    finish(spawn(&dir.0, "absent")).await;
    finish(spawn(&dir.0, "normal")).await;
    finish(spawn(&dir.0, "reconcile")).await;
    evidence(&url, &dir.0, &session, 2, 1).await;
    // Lookup can be absent while the original call is still about to commit.
    let (dir, session) = handoff();
    let delayed = spawn(&dir.0, "delayed");
    wait(&dir.0.join("waiting")).await;
    finish(spawn(&dir.0, "absent")).await;
    fs::write(dir.0.join("release"), "go").unwrap();
    let retry = spawn(&dir.0, "normal");
    finish(delayed).await;
    finish(retry).await;
    evidence(&url, &dir.0, &session, 2, 1).await;
    // Simultaneous fresh independent-process starts, with the same retained key.
    let (dir, session) = handoff();
    let first = spawn(&dir.0, "race-a");
    let second = spawn(&dir.0, "race-b");
    wait(&dir.0.join("race-a")).await;
    wait(&dir.0.join("race-b")).await;
    fs::write(dir.0.join("go"), "go").unwrap();
    finish(first).await;
    finish(second).await;
    finish(spawn(&dir.0, "reconcile")).await;
    evidence(&url, &dir.0, &session, 2, 1).await;
    // Real commit succeeded, Store reply was lost before a handle could be spawned.
    let (dir, session) = handoff();
    finish(spawn(&dir.0, "commit-loss")).await;
    finish(spawn(&dir.0, "reconcile")).await;
    finish(spawn(&dir.0, "recover")).await;
    finish(spawn(&dir.0, "reconcile")).await;
    evidence(&url, &dir.0, &session, 0, 0).await;
    // Public host response is withheld after provider execution begins. Another
    // process reconciles while the worker is live; after release it finishes once.
    let (dir, session) = handoff();
    let original = spawn(&dir.0, "execution-loss");
    wait(&dir.0.join("executing")).await;
    finish(spawn(&dir.0, "reconcile")).await;
    assert_eq!(count(&dir.0.join("ledger"), "provider"), 1);
    fs::write(dir.0.join("execute-release"), "go").unwrap();
    finish(original).await;
    assert!(dir.0.join("response-discarded").exists());
    assert!(!dir.0.join("execution-loss.json").exists());
    finish(spawn(&dir.0, "reconcile")).await;
    evidence(&url, &dir.0, &session, 2, 1).await;
}
