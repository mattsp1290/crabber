//! Whole deletion shares one finite allowance with both readiness and purge.
use super::{Error, observe};
use crabber::{
    SessionContext,
    core::{ControlId, EventKind, InputEnvelope, RunId, SessionId, SessionRevision},
    extension::LibraryScriptedSource,
    session::{
        DeleteSession, DeletionBlocker, DeletionOutcome, ManageSession, SessionChange,
        SessionDefaults, SessionDisposition, Store,
        session_owner::owned_scope::{FixedMemoryInitialHost, FixedMemoryInitialInput},
    },
};
use std::{collections::BTreeSet, time::Duration};

pub(super) async fn run() -> Result<(), Error> {
    let host = FixedMemoryInitialHost::new();
    let session = SessionId::new();
    host.store()
        .manage_session(ManageSession {
            id: ControlId::new(),
            session_id: session.clone(),
            expected_revision: SessionRevision::INITIAL,
            change: SessionChange::Create {
                workspace_id: "workspace".into(),
                directory: "/workspace".into(),
                title: "fixed readiness".into(),
                defaults: SessionDefaults::default(),
            },
        })
        .await?;
    // Match the genuine two-source fixture: both originals complete and physically
    // join. No synthetic row, added payload padding or reduced proof cap is used.
    for _ in 0..2 {
        complete(&host, &session).await?;
    }
    let store = host.store();
    let before_session = store.get_session(&session).await?;
    let before_messages = store.list_all_messages(&session).await?;
    let before_events = store.list_events(&session, None, 1000).await?;
    let before_state = store.public_state(&session).await?;
    let revision = store.session_revision(&session).await?;
    let runs: BTreeSet<RunId> = before_events
        .iter()
        .filter(|event| event.kind == EventKind::RunStarted)
        .map(|event| event.run_id.clone())
        .collect();
    assert_eq!(runs.len(), 2);
    let request = DeleteSession {
        id: ControlId::new(),
        session_id: session.clone(),
        expected_revision: revision,
    };
    // Each public operation has its normal 32MiB allowance. Deletion's one
    // allowance covers read AND purge; a fresh operation is not a reset inside it.
    for _ in 0..2 {
        assert_eq!(
            store.delete_session(request.clone()).await?,
            DeletionOutcome::Blocked(DeletionBlocker::ProofLimit)
        );
        assert!(
            store
                .lookup_session_deletion(&session, &request.id)
                .await?
                .is_none()
        );
        assert_eq!(
            store.session_disposition(&session).await?,
            SessionDisposition::Present { revision }
        );
        assert_eq!(store.session_revision(&session).await?, revision);
        assert_eq!(store.get_session(&session).await?, before_session);
        assert_eq!(store.list_all_messages(&session).await?, before_messages);
        assert_eq!(
            store.list_events(&session, None, 1000).await?,
            before_events
        );
        assert_eq!(store.public_state(&session).await?, before_state);
    }
    Ok(())
}
async fn complete(host: &FixedMemoryInitialHost, session: &SessionId) -> Result<(), Error> {
    let revision = host.store().session_revision(session).await?;
    let mut task = host.register_scripted(
        FixedMemoryInitialInput {
            context: SessionContext::new(session.clone(), "workspace", "/workspace"),
            title: "fixed readiness".into(),
            input: InputEnvelope::text("genuine value input", revision)?,
            key: None,
        },
        LibraryScriptedSource::new(
            crabber::Selection {
                provider_id: "library".into(),
                model_id: "text".into(),
            },
            vec!["actual closed source".into()],
        )?,
    )?;
    task.start()?;
    let mut retry = None;
    observe(&mut task, &mut retry).await?;
    drop(task);
    tokio::time::timeout(Duration::from_secs(5), host.wait_for_supervision()).await?;
    Ok(())
}
