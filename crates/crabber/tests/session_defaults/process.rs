//! Fresh-process keyed reconciliation never consults changed defaults or factories.
use super::{fixture::*, store::TestStore};
use crabber::{
    Admission, AdmissionOptions,
    core::{AdmissionReceipt, SessionId},
    session::Store,
};
use crabber_test_support::ChildDatabase;
use serde::{Deserialize, Serialize};
use std::{
    process::Command,
    sync::{Arc, atomic::Ordering},
    time::Duration,
};
#[derive(Serialize, Deserialize)]
struct Cold {
    session: SessionId,
    key: crabber::AdmissionKey,
    receipt: AdmissionReceipt,
    sqlite_path: Option<std::path::PathBuf>,
}
impl Cold {
    fn options(&self) -> AdmissionOptions {
        AdmissionOptions {
            key: self.key.clone(),
            fingerprint: crabber::InputFingerprint::new("a".repeat(64)).unwrap(),
            behavior_fingerprint: crabber::InputFingerprint::new("b".repeat(64)).unwrap(),
        }
    }
}
pub(super) async fn run(native: Arc<dyn Store>, database: ChildDatabase<'_>) {
    let sqlite_path = match database {
        ChildDatabase::Sqlite(path) => Some(path.to_owned()),
        #[cfg(feature = "postgres")]
        ChildDatabase::Postgres(_) => None,
    };
    let session = create(
        native.as_ref(),
        defaults(Some("cold-original"), Some("cold-original-model")),
    )
    .await;
    let wrapped = Arc::new(TestStore::new(native.clone()));
    wrapped.lose_ack.store(true, Ordering::SeqCst);
    let probe = Probe::new(0);
    let host = agent(wrapped, &probe);
    let key = options();
    let Err(error) = host
        .prompt_session_keyed(
            session.clone(),
            "cold lost acknowledgement",
            key.clone(),
            None,
        )
        .await
    else {
        panic!("lost native acknowledgement");
    };
    let preparation = error.preparation_reconciliation().unwrap().clone();
    let receipt = native
        .lookup_admission(&session, &key.key)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(probe.fake.requests().len(), 0);
    assert_eq!(probe.resolutions.load(Ordering::SeqCst), 0);
    update(
        native.as_ref(),
        &session,
        defaults(Some("cold-changed"), Some("cold-changed-model")),
    )
    .await;
    super::recovery::reconcile_retained(&host, &preparation, &probe).await;
    host.close_extensions().await.unwrap();
    let fixture = Cold {
        session: session.clone(),
        key: key.key,
        receipt: receipt.clone(),
        sqlite_path,
    };
    let mut command = Command::new(std::env::current_exe().unwrap());
    database.configure(&mut command, "CRABBER_SQLITE_CHILD_PATH");
    let mut child = command
        .args(["--exact", "default_selection_process_helper", "--nocapture"])
        .env(
            "CRABBER_DEFAULT_SELECTION_CHILD",
            serde_json::to_string(&fixture).unwrap(),
        )
        .spawn()
        .unwrap();
    let status = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if let Some(status) = child.try_wait().unwrap() {
                break status;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    if status.is_err() {
        let _ = child.kill();
        let _ = child.wait();
    }
    assert!(status.unwrap().success());
    assert_eq!(
        native
            .get_run(&receipt.run_id)
            .await
            .unwrap()
            .unwrap()
            .status,
        crabber::core::RunStatus::Completed
    );
    assert_eq!(
        native
            .lookup_admission(&session, &fixture.key)
            .await
            .unwrap(),
        Some(receipt)
    );
}
pub(super) async fn child() {
    let Ok(encoded) = std::env::var("CRABBER_DEFAULT_SELECTION_CHILD") else {
        return;
    };
    let fixture: Cold = serde_json::from_str(&encoded).unwrap();
    let native = connect(&fixture).await;
    let wrapped = Arc::new(TestStore::new(native.clone()));
    wrapped.unsupported.store(true, Ordering::SeqCst);
    let probe = Probe::new(1);
    let host = agent_with_selection(
        wrapped.clone(),
        &probe,
        selection("cold-new-host", "cold-new-host-model"),
    );
    host.close_extensions().await.unwrap();
    assert!(
        matches!(host.prompt_session_keyed(fixture.session.clone(),"cold lost acknowledgement",fixture.options(),None).await.unwrap(),Admission::Replayed(saved) if saved==fixture.receipt)
    );
    assert_eq!(wrapped.summary_reads.load(Ordering::SeqCst), 0);
    assert_eq!(probe.factories.load(Ordering::SeqCst), 0);
    assert_eq!(probe.resolutions.load(Ordering::SeqCst), 0);
    assert_eq!(probe.fake.requests().len(), 0);
    tokio::time::sleep(Duration::from_millis(30)).await;
    let restored = agent_with_selection(
        wrapped.clone(),
        &probe,
        selection("cold-new-host", "cold-new-host-model"),
    );
    let Admission::Started { receipt, handle } = restored
        .recover_session_admission(
            fixture.session.clone(),
            "cold lost acknowledgement",
            fixture.options(),
            None,
        )
        .await
        .unwrap()
    else {
        panic!("cold Unstarted recovery")
    };
    assert_eq!(receipt, fixture.receipt);
    handle.done().await.unwrap();
    assert_eq!(wrapped.summary_reads.load(Ordering::SeqCst), 0);
    assert_eq!(probe.resolutions.load(Ordering::SeqCst), 1);
    assert_eq!(probe.fake.requests().len(), 1);
    assert_eq!(
        probe.fake.requests()[0].selection,
        selection("cold-original", "cold-original-model")
    );
}
async fn connect(fixture: &Cold) -> Arc<dyn Store> {
    if let Some(path) = &fixture.sqlite_path {
        #[cfg(feature = "sqlite")]
        return Arc::new(crabber::session::SqliteStore::connect(path).await.unwrap());
        #[cfg(not(feature = "sqlite"))]
        panic!("SQLite child feature required: {}", path.display());
    }
    #[cfg(feature = "postgres")]
    return Arc::new(
        crabber::session::PostgresStore::connect(
            &crabber_test_support::postgres::TestDatabase::attached_url()
                .await
                .unwrap(),
        )
        .await
        .unwrap(),
    );
    #[cfg(not(feature = "postgres"))]
    panic!("PostgreSQL child feature required");
}
