//! One configured host consumes defaults for two explicitly admitted workspaces.
use super::{fixture::*, store::TestStore};
use crabber::{
    Admission, RuntimeError, SessionContext,
    core::{ControlId, InputEnvelope, SessionId, SessionRevision},
    session::{ManageSession, SessionChange, Store},
};
use std::sync::{Arc, atomic::Ordering};
pub(super) async fn run(native: Arc<dyn Store>) {
    let a = SessionContext::new(SessionId::new(), "workspace-a", "/host/a");
    let b = SessionContext::new(SessionId::new(), "workspace-b", "/host/b");
    for (context, defaults) in [
        (&a, defaults(Some("workspace-a-provider"), None)),
        (&b, defaults(None, Some("workspace-b-model"))),
    ] {
        native
            .manage_session(ManageSession {
                id: ControlId::new(),
                session_id: context.session_id().clone(),
                expected_revision: SessionRevision::INITIAL,
                change: SessionChange::Create {
                    workspace_id: context.workspace_id().into(),
                    directory: context.directory().into(),
                    title: "scoped defaults".into(),
                    defaults,
                },
            })
            .await
            .unwrap();
    }
    let wrapped = Arc::new(TestStore::new(native.clone()));
    let probe = Probe::new(4);
    let host = agent(wrapped.clone(), &probe);
    let handle = host
        .prompt_session_in(&a, "workspace A", None)
        .await
        .unwrap();
    let run = handle.run_id().clone();
    handle.done().await.unwrap();
    assert_eq!(
        probe.fake.requests()[0].selection,
        selection("workspace-a-provider", "host-model")
    );
    let binding = native
        .execution_binding(a.session_id(), &run)
        .await
        .unwrap()
        .unwrap()
        .capsule;
    assert_eq!(binding.request.workspace_id, a.workspace_id());
    assert_eq!(binding.request.directory, a.directory());
    let key = options();
    let input = InputEnvelope::text(
        "workspace B",
        native.session_revision(b.session_id()).await.unwrap(),
    )
    .unwrap();
    let Admission::Started { receipt, handle } = host
        .prompt_session_input_keyed_in(&b, input.clone(), key.clone(), None)
        .await
        .unwrap()
    else {
        panic!("fresh workspace B")
    };
    handle.done().await.unwrap();
    assert_eq!(
        probe.fake.requests()[1].selection,
        selection("host-provider", "workspace-b-model")
    );
    let binding = native
        .execution_binding(b.session_id(), &receipt.run_id)
        .await
        .unwrap()
        .unwrap()
        .capsule;
    assert_eq!(binding.request.workspace_id, b.workspace_id());
    assert_eq!(binding.request.directory, b.directory());
    // A stored summary never authorizes substituting a different supplied root.
    let wrong = SessionContext::new(a.session_id().clone(), "workspace-a", "/forged/root");
    let factories = probe.factories.load(Ordering::SeqCst);
    assert!(matches!(
        host.prompt_session_in(&wrong, "wrong root", None).await,
        Err(RuntimeError::Store(
            crabber::session::StoreError::SessionIdentityMismatch
        ))
    ));
    assert_eq!(probe.fake.requests().len(), 2);
    assert_eq!(probe.factories.load(Ordering::SeqCst), factories);
    host.close_extensions().await.unwrap();
    wrapped.unsupported.store(true, Ordering::SeqCst);
    let reads = wrapped.summary_reads.load(Ordering::SeqCst);
    assert!(
        matches!(host.prompt_session_input_keyed_in(&b,input,key,None).await.unwrap(),Admission::Replayed(saved) if saved==receipt)
    );
    assert_eq!(wrapped.summary_reads.load(Ordering::SeqCst), reads);
    assert_eq!(probe.fake.requests().len(), 2);
}
