//! A host journal is separate from Crabber's transaction, not a two-store commit.
//! These cuts await Completed before losing the receipt/ACK. Started-worker loss
//! and ambiguous external effects retain their separate, stricter process tests.
use super::*;
use crabber::{AdmissionOptions, core::SessionRevision};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct HostRequest {
    session: SessionId,
    text: String,
    options: AdmissionOptions,
    configuration: serde_json::Value,
    trace: TraceContext,
    // The public legacy keyed facade has no caller If-Match argument.
    expected_revision: Option<SessionRevision>,
}
impl HostRequest {
    fn original(dir: &Path, session: SessionId) -> Self {
        Self {
            session,
            text: "process input".into(),
            options: options(),
            configuration: configuration(),
            trace: read(&dir.join("context.json")),
            expected_revision: None,
        }
    }
    fn digest(&self) -> String {
        format!("{:x}", Sha256::digest(serde_json::to_vec(self).unwrap()))
    }
}

fn configuration() -> serde_json::Value {
    let config = config();
    serde_json::json!({
        "selection": {"provider_id": config.selection.provider_id, "model_id": config.selection.model_id},
        "workspace_id": config.workspace_id, "directory": config.directory,
        "title": config.title, "system_prompt": config.system_prompt,
        "max_output_tokens": config.max_output_tokens,
    })
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
struct HostReceipt {
    request_digest: String,
    receipt: AdmissionReceipt,
}
fn read<T: serde::de::DeserializeOwned>(path: &Path) -> T {
    serde_json::from_slice(&fs::read(path).unwrap()).unwrap()
}

// One sequential host coordinator per fixture; restart happens only after the
// parent reaps the previous process. No distributed journal CAS is claimed.
fn durable(path: &Path, bytes: &[u8]) {
    if path.exists() {
        assert_eq!(fs::read(path).unwrap(), bytes);
        return;
    }
    let temporary = path.with_extension("next");
    let mut file = fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temporary)
        .unwrap();
    file.write_all(bytes).unwrap();
    file.sync_all().unwrap();
    fs::rename(&temporary, path).unwrap();
    fs::File::open(path.parent().unwrap())
        .unwrap()
        .sync_all()
        .unwrap();
}
fn save<T: Serialize>(dir: &Path, name: &str, value: &T) {
    durable(&dir.join(name), &serde_json::to_vec(value).unwrap());
}

async fn cut(dir: &Path, mode: &str, boundary: &str) {
    if mode == boundary {
        // Every preceding renamed journal file and its directory is durable
        // before the parent can observe this separately durable handshake.
        save(dir, "boundary", &boundary);
        wait(&dir.join("never-release")).await;
        panic!("parent must kill this original process at its boundary");
    }
}

#[tokio::test]
async fn host_outbox_child() {
    let Some((backend, dir, mode)) = Backend::child() else {
        return;
    };
    let original: HostRequest = read(&dir.join("original-http-request"));
    assert_eq!(original.configuration, configuration());
    assert_eq!(original.options, options());
    assert_eq!(original.text, "process input");
    assert_eq!(original.expected_revision, None);
    assert_eq!(
        original.session,
        read::<SessionId>(&dir.join("session.json"))
    );
    assert_eq!(
        original.trace,
        read::<TraceContext>(&dir.join("context.json"))
    );
    cut(&dir, &mode, "before-intent").await;
    save(&dir, "host-intent", &original);
    assert_eq!(read::<HostRequest>(&dir.join("host-intent")), original);
    cut(&dir, &mode, "after-intent").await;
    cut(&dir, &mode, "before-receipt").await;
    let receipt = admit_or_reconcile(&backend, &dir, &original).await;
    cut(&dir, &mode, "after-receipt").await;
    let mapping = HostReceipt {
        request_digest: original.digest(),
        receipt,
    };
    save(&dir, "host-receipt", &mapping);
    cut(&dir, &mode, "before-ack").await;
    // This distinct durable host record acknowledges receipt correlation only;
    // it does not commit any Crabber work or grant another execution.
    save(&dir, "host-ack", &mapping);
    cut(&dir, &mode, "after-ack").await;
    save(&dir, "caller-response", &mapping);
}

async fn admit_or_reconcile(
    backend: &Backend,
    dir: &Path,
    request: &HostRequest,
) -> AdmissionReceipt {
    let store = backend.connect().await;
    let existing = store
        .lookup_admission(&request.session, &request.options.key)
        .await
        .unwrap();
    let factories = count(&dir.join("ledger"), "factory");
    let wrapped = Arc::new(FaultStore {
        inner: store.clone(),
        failure: backend.failure(),
        mode: "normal".into(),
        dir: dir.to_owned(),
    });
    let agent = agent(wrapped, dir, false, Arc::default(), false);
    let admission = agent
        .prompt_keyed_with_context(
            request.session.clone(),
            request.text.clone(),
            request.options.clone(),
            Some(request.trace.clone()),
        )
        .await
        .unwrap();
    let receipt = admission.receipt().clone();
    if let Some(expected) = existing {
        assert_eq!(receipt, expected);
        assert!(matches!(admission, Admission::Replayed(_)));
        assert_eq!(count(&dir.join("ledger"), "factory"), factories);
    } else {
        let Admission::Started { handle, .. } = admission else {
            panic!("new durable host intent must obtain the sole worker")
        };
        assert_eq!(handle.done().await.unwrap().status, RunStatus::Completed);
    }
    assert_eq!(receipt.session_id, request.session);
    assert_eq!(receipt.fingerprint, request.options.fingerprint);
    assert_eq!(
        store
            .lookup_admission(&request.session, &request.options.key)
            .await
            .unwrap(),
        Some(receipt.clone())
    );
    assert_eq!(
        store
            .get_run(&receipt.run_id)
            .await
            .unwrap()
            .unwrap()
            .status,
        RunStatus::Completed
    );
    agent.close_extensions().await.unwrap();
    receipt
}

fn worker(backend: &Backend, dir: &Path, mode: &str) -> ChildGuard {
    let mut command = Command::new(std::env::current_exe().unwrap());
    command.args([
        "--exact",
        "process::host_outbox::host_outbox_child",
        "--nocapture",
    ]);
    backend.child_environment(&mut command, dir, mode);
    ChildGuard(command.spawn().unwrap())
}

async fn crash(backend: &Backend, dir: &Path, boundary: &str) {
    let mut original = worker(backend, dir, boundary);
    wait(&dir.join("boundary")).await;
    assert_eq!(read::<String>(&dir.join("boundary")), boundary);
    original.0.kill().unwrap();
    assert!(!original.0.wait().unwrap().success());
    assert!(!dir.join("caller-response").exists());
    fs::remove_file(dir.join("boundary")).unwrap();
    fs::File::open(dir).unwrap().sync_all().unwrap();
}

async fn inspect_cut(backend: &Backend, dir: &Path, request: &HostRequest, boundary: &str) {
    let has_receipt = matches!(boundary, "after-receipt" | "before-ack" | "after-ack");
    assert_eq!(
        dir.join("host-intent").exists(),
        boundary != "before-intent"
    );
    assert_eq!(
        dir.join("host-receipt").exists(),
        matches!(boundary, "before-ack" | "after-ack")
    );
    assert_eq!(dir.join("host-ack").exists(), boundary == "after-ack");
    let store = backend.connect().await;
    let receipt = store
        .lookup_admission(&request.session, &request.options.key)
        .await
        .unwrap();
    assert_eq!(receipt.is_some(), has_receipt);
    assert_eq!(
        count(&dir.join("ledger"), "provider"),
        usize::from(has_receipt) * 2
    );
    assert_eq!(count(&dir.join("ledger"), "tool"), usize::from(has_receipt));
    if let Some(receipt) = receipt {
        assert_eq!(
            store
                .get_run(&receipt.run_id)
                .await
                .unwrap()
                .unwrap()
                .status,
            RunStatus::Completed
        );
    } else {
        assert!(store.get_session(&request.session).await.unwrap().is_none());
    }
}

async fn completed_evidence(backend: &Backend, dir: &Path, request: &HostRequest) {
    let intent: HostRequest = read(&dir.join("host-intent"));
    assert_eq!(intent, *request);
    let mapping: HostReceipt = read(&dir.join("host-receipt"));
    let ack: HostReceipt = read(&dir.join("host-ack"));
    assert_eq!(ack, mapping);
    assert_eq!(mapping.request_digest, request.digest());
    let store = backend.connect().await;
    assert_eq!(
        store
            .lookup_admission(&request.session, &request.options.key)
            .await
            .unwrap(),
        Some(mapping.receipt.clone())
    );
    let raw = backend.evidence(&request.session).await;
    assert_eq!((raw.runs, raw.receipts, raw.capsules), (1, 1, 1));
    let messages = store.list_all_messages(&request.session).await.unwrap();
    let users: Vec<_> = messages
        .iter()
        .filter(|message| message.role == Role::User)
        .collect();
    assert_eq!(users.len(), 1);
    assert_eq!(users[0].id, mapping.receipt.user_message_id);
    assert_eq!(
        users[0].parts[0].content,
        crabber::core::ContentBlock::Text {
            text: request.text.clone()
        }
    );
    assert_eq!(count(&dir.join("ledger"), "provider"), 2);
    assert_eq!(count(&dir.join("ledger"), "tool"), 1);
    assert_eq!(count(&dir.join("ledger"), "factory"), 1);
    let before = store
        .list_events(&request.session, None, 1000)
        .await
        .unwrap();
    let revision = store.session_revision(&request.session).await.unwrap();
    let run = store.get_run(&mapping.receipt.run_id).await.unwrap();
    let host_ack = fs::read(dir.join("host-ack")).unwrap();
    finish(worker(backend, dir, "reconcile")).await;
    assert_eq!(fs::read(dir.join("host-ack")).unwrap(), host_ack);
    assert_eq!(
        store.list_all_messages(&request.session).await.unwrap(),
        messages
    );
    assert_eq!(
        store
            .list_events(&request.session, None, 1000)
            .await
            .unwrap(),
        before
    );
    assert_eq!(
        store.session_revision(&request.session).await.unwrap(),
        revision
    );
    assert_eq!(store.get_run(&mapping.receipt.run_id).await.unwrap(), run);
    assert_eq!(count(&dir.join("ledger"), "provider"), 2);
    assert_eq!(count(&dir.join("ledger"), "tool"), 1);
    assert_eq!(count(&dir.join("ledger"), "factory"), 1);
}

async fn journey(kind: BackendKind) {
    for boundary in [
        "before-intent",
        "after-intent",
        "before-receipt",
        "after-receipt",
        "before-ack",
        "after-ack",
        "completed-control",
    ] {
        let (dir, session) = handoff();
        let backend = kind.prepare(&dir.0).await;
        let request = HostRequest::original(&dir.0, session);
        // This immutable client envelope is the test driver's retry input, not
        // the host's accepted intent or a substitute for its separate journal.
        save(&dir.0, "original-http-request", &request);
        if boundary == "completed-control" {
            finish(worker(&backend, &dir.0, boundary)).await;
        } else {
            crash(&backend, &dir.0, boundary).await;
            inspect_cut(&backend, &dir.0, &request, boundary).await;
            // The original process is reaped. Before-intent is an explicit exact
            // client resubmission, not discovery of an accepted host operation.
            finish(worker(&backend, &dir.0, "reconcile")).await;
        }
        completed_evidence(&backend, &dir.0, &request).await;
    }
}

#[cfg(feature = "sqlite")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqlite_separate_host_outbox_crash_boundaries() {
    journey(BackendKind::Sqlite).await;
}

#[cfg(feature = "postgres")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn postgres_separate_host_outbox_crash_boundaries() {
    let _guard = POSTGRES_JOURNEY_LOCK.lock().await;
    let Ok(url) = std::env::var("CRABBER_TEST_POSTGRES_URL") else {
        assert_ne!(
            std::env::var("CRABBER_REQUIRE_POSTGRES").as_deref(),
            Ok("1"),
            "CRABBER_TEST_POSTGRES_URL required"
        );
        eprintln!("skipping host outbox journey: CRABBER_TEST_POSTGRES_URL absent");
        return;
    };
    crabber::session::PostgresStore::migrate(&url)
        .await
        .unwrap();
    journey(BackendKind::Postgres(url)).await;
}
