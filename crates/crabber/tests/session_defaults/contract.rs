use super::{fixture::*, store::TestStore};
use crabber::{
    Admission, ExecutionContext, RuntimeError, SessionContext,
    core::{InputEnvelope, SessionId},
    session::{AdmissionSelectionPolicy, Store},
};
use std::sync::{Arc, atomic::Ordering};

struct TextRetry {
    session: SessionId,
    key: crabber::AdmissionOptions,
    receipt: crabber::core::AdmissionReceipt,
}
struct TypedRetry {
    input: InputEnvelope,
    key: crabber::AdmissionOptions,
    receipt: crabber::core::AdmissionReceipt,
}
pub(super) async fn run(native: Arc<dyn Store>) {
    let wrapped = Arc::new(TestStore::new(native.clone()));
    let probe = Probe::new(12);
    let host = agent(wrapped.clone(), &probe);
    choices(native.as_ref(), &host, &probe).await;
    let text = keyed_changes(native.as_ref(), &wrapped, &host, &probe).await;
    let typed = typed_request(native.as_ref(), &host, &text.session).await;
    closed_replay(&wrapped, &host, &probe, text, typed).await;
    wrapped.unsupported.store(false, Ordering::SeqCst);
    super::contexts::run(native.clone()).await;
    lazy_session(native.clone()).await;
    super::resume::run(native.clone()).await;
    super::races::run(native).await;
}
async fn choices(native: &dyn Store, agent: &crabber::Agent, probe: &Probe) {
    for (stored, expected) in [
        (
            defaults(Some("managed-provider"), Some("managed-model")),
            selection("managed-provider", "managed-model"),
        ),
        (
            defaults(None, Some("model-only")),
            selection("host-provider", "model-only"),
        ),
        (
            defaults(Some("provider-only"), None),
            selection("provider-only", "host-model"),
        ),
        (
            defaults(None, None),
            selection("host-provider", "host-model"),
        ),
    ] {
        let session = create(native, stored).await;
        let handle = agent
            .prompt_session(session.clone(), "choose defaults", None)
            .await
            .unwrap();
        let run = handle.run_id().clone();
        handle.done().await.unwrap();
        assert_eq!(probe.fake.requests().last().unwrap().selection, expected);
        let binding = native
            .execution_binding(&session, &run)
            .await
            .unwrap()
            .unwrap()
            .capsule;
        assert_eq!(binding.provider.provider_id(), expected.provider_id);
        assert_eq!(binding.provider.model_id(), expected.model_id);
        assert!(matches!(
            binding.request.selection_policy,
            AdmissionSelectionPolicy::SessionDefaults { .. }
        ));
    }
}
async fn keyed_changes(
    native: &dyn Store,
    wrapped: &TestStore,
    agent: &crabber::Agent,
    probe: &Probe,
) -> TextRetry {
    let session = create(native, defaults(Some("stored"), Some("old"))).await;
    let key = options();
    let Admission::Started { receipt, handle } = agent
        .prompt_session_keyed(session.clone(), "keyed", key.clone(), None)
        .await
        .unwrap()
    else {
        panic!("fresh")
    };
    handle.done().await.unwrap();
    assert_eq!(
        probe.fake.requests().last().unwrap().selection,
        selection("stored", "old")
    );
    update(native, &session, defaults(Some("changed"), Some("new"))).await;
    let count = probe.fake.requests().len();
    let factories = probe.factories.load(Ordering::SeqCst);
    let reads = wrapped.summary_reads.load(Ordering::SeqCst);
    assert!(
        matches!(agent.prompt_session_keyed(session.clone(),"keyed",key.clone(),None).await.unwrap(),Admission::Replayed(saved) if saved==receipt)
    );
    assert_eq!(wrapped.summary_reads.load(Ordering::SeqCst), reads);
    assert_eq!(probe.fake.requests().len(), count);
    assert_eq!(probe.factories.load(Ordering::SeqCst), factories);
    // Explicit context cannot substitute for the original default-selection policy.
    let context = ExecutionContext::new(
        SessionContext::new(session.clone(), "defaults-workspace", "/defaults"),
        selection("stored", "old"),
    );
    assert!(matches!(
        agent
            .prompt_keyed_in(&context, "keyed", key.clone(), None)
            .await,
        Err(RuntimeError::Store(
            crabber::session::StoreError::AdmissionConflict
        ))
    ));
    let handle = agent
        .prompt_session(session.clone(), "new ordinary admission", None)
        .await
        .unwrap();
    handle.done().await.unwrap();
    assert_eq!(
        probe.fake.requests().last().unwrap().selection,
        selection("changed", "new")
    );
    let handle = agent
        .prompt_in(&context, "explicit override", None)
        .await
        .unwrap();
    handle.done().await.unwrap();
    assert_eq!(
        probe.fake.requests().last().unwrap().selection,
        selection("stored", "old")
    );
    TextRetry {
        session,
        key,
        receipt,
    }
}
async fn typed_request(
    native: &dyn Store,
    agent: &crabber::Agent,
    session: &SessionId,
) -> TypedRetry {
    let revision = native.session_revision(session).await.unwrap();
    let input = InputEnvelope::text("typed defaults", revision).unwrap();
    let typed_key = options();
    let Admission::Started {
        receipt: typed_receipt,
        handle,
    } = agent
        .prompt_session_input_keyed(session.clone(), input.clone(), typed_key.clone(), None)
        .await
        .unwrap()
    else {
        panic!("fresh typed")
    };
    handle.done().await.unwrap();
    let binding = native
        .execution_binding(session, &typed_receipt.run_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(binding.capsule.input_envelope, Some(input.clone()));
    update(native, session, defaults(None, Some("later"))).await;
    assert!(matches!(
        agent
            .prompt_session_input(session.clone(), input.clone(), None)
            .await,
        Err(RuntimeError::Store(
            crabber::session::StoreError::AdmissionRevisionConflict
        ))
    ));
    TypedRetry {
        input,
        key: typed_key,
        receipt: typed_receipt,
    }
}
async fn closed_replay(
    wrapped: &Arc<TestStore>,
    agent: &crabber::Agent,
    probe: &Probe,
    text: TextRetry,
    typed: TypedRetry,
) {
    agent.close_extensions().await.unwrap();
    wrapped.unsupported.store(true, Ordering::SeqCst);
    let reads = wrapped.summary_reads.load(Ordering::SeqCst);
    let count = probe.fake.requests().len();
    let factories = probe.factories.load(Ordering::SeqCst);
    assert!(
        matches!(agent.prompt_session_keyed(text.session.clone(),"keyed",text.key,None).await.unwrap(),Admission::Replayed(saved) if saved==text.receipt)
    );
    assert!(
        matches!(agent.prompt_session_input_keyed(text.session,typed.input,typed.key,None).await.unwrap(),Admission::Replayed(saved) if saved==typed.receipt)
    );
    assert_eq!(wrapped.summary_reads.load(Ordering::SeqCst), reads);
    assert_eq!(probe.fake.requests().len(), count);
    assert_eq!(probe.factories.load(Ordering::SeqCst), factories);
    let fresh = SessionId::new();
    let cold = super::fixture::agent(wrapped.clone(), probe);
    assert!(matches!(
        cold.prompt_session_keyed(fresh, "unsupported", options(), None)
            .await,
        Err(RuntimeError::Management(
            crabber::ManagementError::Unsupported
        ))
    ));
    assert_eq!(probe.factories.load(Ordering::SeqCst), factories);
}
async fn lazy_session(native: Arc<dyn Store>) {
    let probe = Probe::new(3);
    let host = agent(native.clone(), &probe);
    let session = SessionId::new();
    let Admission::Started { handle, .. } = host
        .prompt_session_keyed(session.clone(), "lazy create", options(), None)
        .await
        .unwrap()
    else {
        panic!("fresh lazy")
    };
    handle.done().await.unwrap();
    let summary = native.session_summary(&session).await.unwrap();
    assert_eq!(summary.defaults, defaults(None, None));
    let handle = host
        .prompt_session(session.clone(), "existing lazy", None)
        .await
        .unwrap();
    handle.done().await.unwrap();
    assert!(
        probe
            .fake
            .requests()
            .iter()
            .all(|request| request.selection == selection("host-provider", "host-model"))
    );
    let changed = agent_with_selection(
        native,
        &probe,
        selection("new-host-provider", "new-host-model"),
    );
    let handle = changed
        .prompt_session(session, "new host fallback", None)
        .await
        .unwrap();
    handle.done().await.unwrap();
    assert_eq!(
        probe.fake.requests().last().unwrap().selection,
        selection("new-host-provider", "new-host-model")
    );
}
