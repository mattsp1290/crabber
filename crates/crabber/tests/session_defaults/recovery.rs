//! Text and typed retries retain original default selection after unknown ACK.
use super::{fixture::*, store::TestStore};
use crabber::{
    Admission, AdmissionOptions, Agent, RuntimeError,
    core::{InputEnvelope, SessionId},
    session::Store,
};
use std::sync::{Arc, atomic::Ordering};
struct Pending {
    session: SessionId,
    key: AdmissionOptions,
    input: Option<InputEnvelope>,
}
impl Pending {
    async fn admit(&self, host: &Agent) -> Result<Admission, RuntimeError> {
        if let Some(input) = &self.input {
            host.prompt_session_input_keyed(
                self.session.clone(),
                input.clone(),
                self.key.clone(),
                None,
            )
            .await
        } else {
            host.prompt_session_keyed(
                self.session.clone(),
                "lost acknowledgement",
                self.key.clone(),
                None,
            )
            .await
        }
    }
    async fn recover(&self, host: &Agent) -> Result<Admission, RuntimeError> {
        if let Some(input) = &self.input {
            host.recover_session_input_admission(
                self.session.clone(),
                input.clone(),
                self.key.clone(),
                None,
            )
            .await
        } else {
            host.recover_session_admission(
                self.session.clone(),
                "lost acknowledgement",
                self.key.clone(),
                None,
            )
            .await
        }
    }
}
pub(super) async fn run(native: Arc<dyn Store>, typed: bool) {
    let session = create(native.as_ref(), defaults(None, None)).await;
    let pending = Pending {
        input: if typed {
            Some(
                InputEnvelope::text(
                    "lost acknowledgement",
                    native.session_revision(&session).await.unwrap(),
                )
                .unwrap(),
            )
        } else {
            None
        },
        session,
        key: options(),
    };
    let wrapped = Arc::new(TestStore::new(native.clone()));
    wrapped.lose_ack.store(true, Ordering::SeqCst);
    let probe = Probe::new(1);
    let host = agent(wrapped.clone(), &probe);
    let Err(error) = pending.admit(&host).await else {
        panic!("lost admission acknowledgment");
    };
    let preparation = error.preparation_reconciliation().unwrap().clone();
    assert_eq!(preparation.session_id, pending.session);
    let receipt = native
        .lookup_admission(&pending.session, &pending.key.key)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(probe.fake.requests().len(), 0);
    assert_eq!(probe.resolutions.load(Ordering::SeqCst), 0);
    update(
        native.as_ref(),
        &pending.session,
        defaults(Some("new-provider"), Some("new-model")),
    )
    .await;
    reconcile_retained(&host, &preparation, &probe).await;
    host.close_extensions().await.unwrap();
    let factories = probe.factories.load(Ordering::SeqCst);
    let reads = wrapped.summary_reads.load(Ordering::SeqCst);
    assert_eq!(
        host.lookup_preparation_operation(
            &pending.session,
            &crabber::session::session_owner::preparation::PreparationKey::Admission(
                pending.key.key.clone()
            )
        )
        .await
        .unwrap(),
        Some(preparation)
    );
    assert!(
        matches!(pending.admit(&host).await.unwrap(),Admission::Replayed(saved) if saved==receipt)
    );
    assert_eq!(wrapped.summary_reads.load(Ordering::SeqCst), reads);
    assert_eq!(probe.factories.load(Ordering::SeqCst), factories);
    tokio::time::sleep(std::time::Duration::from_millis(30)).await;
    let restarted = agent_with_selection(
        wrapped.clone(),
        &probe,
        selection("changed-host-provider", "changed-host-model"),
    );
    restarted.close_extensions().await.unwrap();
    wrapped.unsupported.store(true, Ordering::SeqCst);
    assert!(
        matches!(pending.admit(&restarted).await.unwrap(),Admission::Replayed(saved) if saved==receipt)
    );
    assert_eq!(wrapped.summary_reads.load(Ordering::SeqCst), reads);
    assert_eq!(probe.factories.load(Ordering::SeqCst), factories);
    assert_eq!(probe.fake.requests().len(), 0);
    let restarted = agent_with_selection(
        wrapped.clone(),
        &probe,
        selection("changed-host-provider", "changed-host-model"),
    );
    let Admission::Started {
        receipt: saved,
        handle,
    } = pending.recover(&restarted).await.unwrap()
    else {
        panic!("fresh recovery")
    };
    assert_eq!(saved, receipt);
    handle.done().await.unwrap();
    assert_eq!(probe.fake.requests().len(), 1);
    assert_eq!(
        probe.fake.requests()[0].selection,
        selection("host-provider", "host-model")
    );
}

pub(super) async fn reconcile_retained(
    host: &Agent,
    preparation: &crabber::session::session_owner::preparation::PreparationReconciliation,
    probe: &Probe,
) {
    let before = (
        probe.factories.load(Ordering::SeqCst),
        probe.resolutions.load(Ordering::SeqCst),
        probe.fake.requests().len(),
    );
    let progress = host
        .retry_preparation_completion(preparation)
        .await
        .unwrap();
    assert!(progress.replayed);
    assert_eq!(
        before,
        (
            probe.factories.load(Ordering::SeqCst),
            probe.resolutions.load(Ordering::SeqCst),
            probe.fake.requests().len(),
        )
    );
    assert_eq!(
        host.retained_preparation_status(&preparation.session_id),
        []
    );
}
