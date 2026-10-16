//! A dead controller cannot acknowledge its retained resources as joined.
use super::*;
use std::path::Path;

#[derive(serde::Serialize, serde::Deserialize)]
struct Wire {
    receipt: crabber_core::AdmissionReceipt,
    incomplete: PreparationReconciliation,
    child: crabber_core::SessionOwnerId,
}
fn child(path: &Path, wire: &Path, expired: bool, role: &str) {
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "tests::preparation_resources::successor::process::sqlite_runtime_successor_process_helper",
            "--nocapture",
        ])
        .env("CRABBER_SUCCESSOR_PROCESS_DB", path)
        .env("CRABBER_SUCCESSOR_PROCESS_WIRE", wire)
        .env("CRABBER_SUCCESSOR_PROCESS_ROLE", role)
        .env("CRABBER_SUCCESSOR_PROCESS_EXPIRED", expired.to_string())
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "role={role} expired={expired}: {}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}
#[test]
fn sqlite_runtime_successor_actual_process_restart() {
    for expired in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("restart.sqlite");
        let wire = directory.path().join("metadata.json");
        child(&path, &wire, expired, "original");
        child(&path, &wire, expired, "cold");
    }
}
#[tokio::test]
async fn sqlite_runtime_successor_process_helper() {
    let Ok(path) = std::env::var("CRABBER_SUCCESSOR_PROCESS_DB") else {
        return;
    };
    let role = std::env::var("CRABBER_SUCCESSOR_PROCESS_ROLE").unwrap();
    let wire_path = std::env::var("CRABBER_SUCCESSOR_PROCESS_WIRE").unwrap();
    let expired = std::env::var("CRABBER_SUCCESSOR_PROCESS_EXPIRED").unwrap() == "true";
    crabber_session::SqliteStore::migrate(&path).await.unwrap();
    let clock = Arc::new(ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH
            + time::Duration::seconds(if role == "cold" { 62 } else { 0 }),
    ));
    let store = Arc::new(
        crabber_session::SqliteStore::connect(path)
            .await
            .unwrap()
            .with_clock(clock.clone()),
    );
    let gate = Arc::new(SettlementGate::default());
    gate.release.notify_one();
    let wrapped = Arc::new(DelayedTerminalStore {
        inner: store.clone(),
        gate: gate.clone(),
    });
    let mut h = Harness::on_store_with_clock(wrapped, store, gate, Some(clock.clone()));
    if role == "original" {
        let wire = original(&h, &clock, expired).await;
        std::fs::write(wire_path, serde_json::to_vec(&wire).unwrap()).unwrap();
        // Skip every Rust destructor, including the live Runtime registry.
        std::process::exit(0);
    }
    assert!(std::fs::metadata(&wire_path).unwrap().len() < 64 * 1024);
    let wire: Wire = serde_json::from_slice(&std::fs::read(wire_path).unwrap()).unwrap();
    h.input.session_id = Some(wire.receipt.session_id.clone());
    cold(&h, wire, expired).await;
}
async fn original(h: &Harness, clock: &ManualClock, expired: bool) -> Wire {
    let receipt = initial(h, clock, expired).await;
    h.gate.preparation_fault.store(2, Ordering::SeqCst);
    let error = if expired {
        h.runtime
            .resume_execution(&receipt.run_id)
            .await
            .err()
            .unwrap()
    } else {
        h.runtime
            .recover_admission(h.input.clone(), options())
            .await
            .err()
            .unwrap()
    };
    let incomplete = error.preparation_reconciliation().unwrap().clone();
    assert_eq!(h.provider.calls.load(Ordering::SeqCst), 2);
    assert_eq!(h.fake.requests().len(), 0);
    assert!(h.gate.successor_request.lock().unwrap().is_none());
    let root = h
        .memory
        .lookup_session_owner(&incomplete.session_id, &incomplete.reserve_id)
        .await
        .unwrap()
        .unwrap();
    let link = PreparationResourceLink {
        session_id: incomplete.session_id.clone(),
        root: incomplete.owner.clone(),
        root_reserve: incomplete.reserve_id.clone(),
        root_begin: incomplete.begin_id.clone(),
        root_digest: root.semantic_digest,
    };
    let child = link.identities().unwrap().owner;
    assert_eq!(
        h.memory
            .get_session_owner(&incomplete.session_id, &child)
            .await
            .unwrap()
            .state,
        SessionOwnerState::Active
    );
    Wire {
        receipt,
        incomplete,
        child,
    }
}
async fn cold(h: &Harness, wire: Wire, expired: bool) {
    let handle = start(h, &wire.receipt, expired).await;
    tokio::time::timeout(Duration::from_secs(7), handle.done())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(captured(h).claim.issuance, 3);
    assert_eq!(h.fake.requests().len(), 1);
    assert_eq!(h.provider.calls.load(Ordering::SeqCst), 1);
    cleanup(h, expired).await;
    assert_eq!(
        h.memory
            .get_session_owner(&wire.receipt.session_id, &wire.child)
            .await
            .unwrap()
            .state,
        SessionOwnerState::Active
    );
    let pending = h
        .runtime
        .lookup_preparation_operation(
            &wire.receipt.session_id,
            &if expired {
                PreparationKey::ExpiredRun {
                    run_id: wire.receipt.run_id,
                    issuance: 1,
                }
            } else {
                PreparationKey::AdmissionRecovery {
                    key: options().key,
                    issuance: 1,
                }
            },
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(pending, wire.incomplete);
}
