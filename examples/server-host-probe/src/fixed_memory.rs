//! Public fixed intake, actual cleanup observation, guarded deletion and replay.
use super::{Error, selection};
use crabber::{
    SessionContext,
    core::{
        AdmissionKey, ContentBlock, ControlId, InputEnvelope, Role, SessionId, SessionRevision,
    },
    extension::LibraryScriptedSource,
    session::{
        DeleteSession, DeletionOutcome, ManageSession, SessionChange, SessionDefaults, Store,
        session_owner::owned_scope::{
            FixedMemoryInitialHost, FixedMemoryInitialInput, FixedMemoryInitialTask,
            LocallyJoinedInitialSeal, OwnedMainDisposition, PendingInitialProducerStatus,
        },
    },
};
use std::time::Duration;

pub(crate) async fn run() -> Result<usize, Error> {
    let host = FixedMemoryInitialHost::new();
    let clone = host.clone();
    session(&host, false).await?;
    session(&clone, true).await?;
    parked_stop(&host).await?;
    Ok(2)
}

async fn managed(
    host: &FixedMemoryInitialHost,
) -> Result<(SessionContext, SessionRevision), Error> {
    let create = ManageSession {
        id: ControlId::new(),
        session_id: SessionId::new(),
        expected_revision: SessionRevision::INITIAL,
        change: SessionChange::Create {
            workspace_id: "fixed-probe".into(),
            directory: "/fixed-probe".into(),
            title: "public fixed intake".into(),
            defaults: SessionDefaults::default(),
        },
    };
    let created = host.store().manage_session(create.clone()).await?;
    assert_eq!(host.store().manage_session(create.clone()).await?, created);
    Ok((
        SessionContext::new(create.session_id, "fixed-probe", "/fixed-probe"),
        created.revision,
    ))
}

async fn session(host: &FixedMemoryInitialHost, keyed: bool) -> Result<(), Error> {
    let (context, revision) = managed(host).await?;
    let session = context.session_id().clone();
    let source =
        LibraryScriptedSource::new(selection(), vec!["finite public fixed response".into()])?;
    let mut task = host.register_scripted(
        FixedMemoryInitialInput {
            context,
            title: "public fixed intake".into(),
            input: InputEnvelope::text("fixed input", revision)?,
            key: keyed.then(|| AdmissionKey::new("fixed-probe-input").unwrap()),
        },
        source,
    )?;
    assert!(task.control().status().is_none());
    assert_eq!(host.store().session_revision(&session).await?, revision);
    task.start()?;
    let mut retry = None;
    observe(&mut task, &mut retry).await?;
    drop(task);
    tokio::time::timeout(Duration::from_secs(5), host.wait_for_supervision()).await?;
    let messages = host.store().list_all_messages(&session).await?;
    assert!(
        messages
            .iter()
            .any(|message| message.role == Role::Assistant
                && message.parts.iter().any(|part| matches!(&part.content,
            ContentBlock::Text { text } if text == "finite public fixed response")))
    );
    let delete = DeleteSession {
        id: ControlId::new(),
        session_id: session.clone(),
        expected_revision: host.store().session_revision(&session).await?,
    };
    let DeletionOutcome::Applied(receipt) = host.store().delete_session(delete.clone()).await?
    else {
        panic!("completed fixed intake must permit guarded deletion")
    };
    assert_eq!(
        host.store().delete_session(delete).await?,
        DeletionOutcome::Replayed(receipt)
    );
    assert!(host.store().get_session(&session).await?.is_none());
    Ok(())
}

async fn observe(
    task: &mut FixedMemoryInitialTask,
    retry: &mut Option<LocallyJoinedInitialSeal>,
) -> Result<(), Error> {
    let observed = tokio::time::timeout(Duration::from_secs(30), task.join()).await??;
    *retry = observed.take_retry();
    assert_eq!(observed.disposition(), OwnedMainDisposition::Returned);
    assert!(!observed.observation_failed());
    assert!(observed.error().is_none(), "{:?}", observed.error());
    if let Some(original) = retry.as_mut() {
        // Only a genuinely returned original can be reconciled. The normal
        // credential-free path does not inject a lost acknowledgement.
        assert_eq!(
            original.retry().await?,
            PendingInitialProducerStatus::ClosedAssignment
        );
    } else {
        assert_eq!(
            observed.status(),
            Some(&PendingInitialProducerStatus::ClosedAssignment)
        );
    }
    Ok(())
}

// This third task never completes a Run and is not counted in fixed_sessions.
async fn parked_stop(host: &FixedMemoryInitialHost) -> Result<(), Error> {
    let (context, revision) = managed(host).await?;
    let session = context.session_id().clone();
    let mut task = host.register_scripted(
        FixedMemoryInitialInput {
            context,
            title: "parked stop".into(),
            input: InputEnvelope::text("must not execute", revision)?,
            key: None,
        },
        LibraryScriptedSource::new(selection(), vec!["must not execute".into()])?,
    )?;
    task.control().request_stop();
    task.start()?;
    let observed = tokio::time::timeout(Duration::from_secs(5), task.join()).await??;
    assert_eq!(observed.disposition(), OwnedMainDisposition::NoDispatch);
    assert!(observed.status().is_none());
    assert!(!observed.observation_failed());
    assert!(observed.error().is_none());
    assert!(observed.take_retry().is_none());
    drop(task);
    tokio::time::timeout(Duration::from_secs(5), host.wait_for_supervision()).await?;
    assert_eq!(host.store().session_revision(&session).await?, revision);
    assert_eq!(
        host.store().list_all_messages(&session).await?,
        Vec::<crabber::core::Message>::new()
    );
    assert_eq!(
        host.store().list_unfinished_runs().await?,
        Vec::<crabber::core::Run>::new()
    );
    Ok(())
}
