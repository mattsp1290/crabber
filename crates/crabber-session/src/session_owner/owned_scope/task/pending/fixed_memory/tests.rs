//! Genuine public closed host journeys; generic producers are tested separately.
use super::*;
use crate::{ManageSession, SessionChange, SessionDefaults, Store};
use crabber_core::{ControlId, SessionId, SessionRevision};
use std::time::Duration;
const NO_MESSAGES: [crabber_core::Message; 0] = [];
const NO_RUNS: [crabber_core::Run; 0] = [];

async fn managed(host: &FixedMemoryInitialHost) -> (SessionContext, SessionRevision) {
    let session = SessionId::new();
    let receipt = host
        .store()
        .manage_session(ManageSession {
            id: ControlId::new(),
            session_id: session.clone(),
            expected_revision: SessionRevision::INITIAL,
            change: SessionChange::Create {
                workspace_id: "fixed-workspace".into(),
                directory: "/fixed-workspace".into(),
                title: "fixed initial".into(),
                defaults: SessionDefaults::default(),
            },
        })
        .await
        .unwrap();
    (
        SessionContext::new(session, "fixed-workspace", "/fixed-workspace"),
        receipt.revision,
    )
}
fn source() -> LibraryScriptedSource {
    LibraryScriptedSource::new(
        crabber::Selection {
            provider_id: "library".into(),
            model_id: "finite-text".into(),
        },
        vec!["actual fixed host response".into()],
    )
    .unwrap()
}
fn values(
    context: SessionContext,
    revision: SessionRevision,
    keyed: bool,
) -> FixedMemoryInitialInput {
    FixedMemoryInitialInput {
        context,
        title: "fixed initial".into(),
        input: InputEnvelope::text("hello fixed source", revision).unwrap(),
        key: keyed.then(|| AdmissionKey::new("fixed-input").unwrap()),
    }
}

#[tokio::test]
async fn fixed_host_runs_both_admission_kinds_and_shared_root_sessions() {
    let host = FixedMemoryInitialHost::new();
    let other_handle = host.clone();
    let mut sessions = Vec::new();
    for (index, keyed) in [false, true].into_iter().enumerate() {
        let selected = if index == 0 { &host } else { &other_handle };
        let (context, revision) = managed(selected).await;
        let session = context.session_id().clone();
        let mut task = selected
            .register_scripted(values(context, revision, keyed), source())
            .unwrap();
        assert!(
            task.control().status().is_none(),
            "parked controller has not created a producer"
        );
        assert_eq!(
            selected.store().session_revision(&session).await.unwrap(),
            revision
        );
        task.start().unwrap();
        // Bound the whole 11/12-call journey by its 30s preparation lease;
        // each native call keeps its independent production 5s bound.
        let observed = tokio::time::timeout(Duration::from_secs(30), task.join())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(observed.disposition(), OwnedMainDisposition::Returned);
        assert!(matches!(
            observed.status(),
            Some(PendingInitialProducerStatus::ClosedAssignment)
        ));
        assert!(!observed.observation_failed());
        assert!(observed.error().is_none(), "{:?}", observed.error());
        assert!(observed.take_retry().is_none());
        drop(task);
        sessions.push(session);
    }
    tokio::time::timeout(Duration::from_secs(5), host.wait_for_supervision())
        .await
        .unwrap();
    for session in sessions {
        let report = host.store().fixed_initial_fixture_report(&session).unwrap();
        assert_eq!(
            report.initial_consumption,
            crate::native_enrollment::initial_consumer::InitialConsumption::CompletedSource
        );
        assert_eq!(
            report.main_join,
            crate::deletion::history::Fact::Authenticated
        );
        assert_eq!(
            report.resource_policy,
            crate::deletion::history::Fact::Authenticated
        );
        assert!(report.complete());
        let messages = host.store().list_all_messages(&session).await.unwrap();
        assert!(
            messages
                .iter()
                .any(|message| message.role == crabber_core::Role::Assistant)
        );
        let revision = host.store().session_revision(&session).await.unwrap();
        let request = crate::DeleteSession {
            id: ControlId::new(),
            session_id: session.clone(),
            expected_revision: revision,
        };
        let outcome = host.store().delete_session(request.clone()).await.unwrap();
        let crate::DeletionOutcome::Applied(receipt) = outcome else {
            panic!("completed fixed source must delete");
        };
        assert_eq!(receipt.final_revision, revision.next().unwrap());
        assert!(host.store().get_session(&session).await.unwrap().is_none());
        assert_eq!(
            host.store().delete_session(request).await.unwrap(),
            crate::DeletionOutcome::Replayed(receipt)
        );
    }
}

#[tokio::test]
async fn parked_fixed_stop_and_drop_perform_no_preparation_io() {
    let host = FixedMemoryInitialHost::new();
    for drop_without_start in [false, true] {
        let (context, revision) = managed(&host).await;
        let session = context.session_id().clone();
        let mut task = host
            .register_scripted(values(context, revision, false), source())
            .unwrap();
        if drop_without_start {
            drop(task);
        } else {
            task.control().request_stop();
            task.start().unwrap();
            let observation = tokio::time::timeout(Duration::from_secs(5), task.join())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(observation.disposition(), OwnedMainDisposition::NoDispatch);
            assert!(observation.status().is_none());
            assert!(observation.error().is_none());
            drop(task);
        }
        tokio::time::timeout(Duration::from_secs(5), host.wait_for_supervision())
            .await
            .unwrap();
        assert_eq!(
            host.store().session_revision(&session).await.unwrap(),
            revision
        );
        assert_eq!(
            host.store().list_all_messages(&session).await.unwrap(),
            NO_MESSAGES
        );
        assert_eq!(host.store().list_unfinished_runs().await.unwrap(), NO_RUNS);
    }
}

#[tokio::test]
async fn fixed_host_refuses_missing_managed_session_without_adopting_it() {
    let host = FixedMemoryInitialHost::new();
    let session = SessionId::new();
    let context = SessionContext::new(session.clone(), "workspace", "/workspace");
    let mut task = host
        .register_scripted(values(context, SessionRevision::INITIAL, false), source())
        .unwrap();
    task.start().unwrap();
    let observed = tokio::time::timeout(Duration::from_secs(5), task.join())
        .await
        .unwrap()
        .unwrap();
    assert!(observed.status().is_none());
    assert!(observed.error().is_some());
    drop(task);
    tokio::time::timeout(Duration::from_secs(5), host.wait_for_supervision())
        .await
        .unwrap();
    assert!(host.store().get_session(&session).await.unwrap().is_none());
    assert_eq!(host.store().list_unfinished_runs().await.unwrap(), NO_RUNS);
}

mod lifetime;

mod substitution;
