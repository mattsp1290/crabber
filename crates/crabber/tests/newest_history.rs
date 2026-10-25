//! Real public Agent turns and pull-driven replacement-window qualification.
use crabber::{
    PublicResyncReason, PublicSessionError, PublicSessionLimitV2, PublicSessionLimitsV2,
    PublicSessionOutcomeV2, PublicSessionPageV2, PublicSessionPollV2, PublicSessionRequestV2,
    PublicSessionSubscriptionV2,
    core::SessionId,
    session::{SnapshotLimit, SnapshotLimits, Store},
};
#[path = "newest_history/provider.rs"]
mod provider;
use std::sync::Arc;
fn limits() -> PublicSessionLimitsV2 {
    PublicSessionLimitsV2 {
        header_bytes: 2 * 1024 * 1024,
        proof_bytes: 32 * 1024 * 1024,
        approval_interactions: 64,
        tool_requirements: 64,
        queue_entries: 64,
        registered_owners: 1024,
        run_controls: 64,
        history: SnapshotLimits {
            messages: 100,
            tool_calls: 100,
            parts: 1024,
            text_bytes: 1024 * 1024,
            encoded_bytes: 2 * 1024 * 1024,
        },
    }
}
fn request(session: &SessionId) -> PublicSessionRequestV2 {
    PublicSessionRequestV2 {
        session_id: session.clone(),
        limits: limits(),
        continuation: None,
    }
}
fn page(outcome: PublicSessionOutcomeV2) -> Box<PublicSessionPageV2> {
    let PublicSessionOutcomeV2::Page(page) = outcome else {
        panic!("complete window")
    };
    page
}
fn assert_latest(page: &PublicSessionPageV2, turn: usize) {
    assert_eq!(page.captured_revision, page.header.revision);
    assert_eq!(page.captured_revision, page.history.captured_revision);
    assert_eq!(page.history.messages.len(), 100);
    assert!(page.history.omitted_older_messages);
    assert!(page.history.continuation.is_none());
    let texts: Vec<_> = page
        .history
        .messages
        .iter()
        .map(|m| serde_json::to_string(&m.parts).unwrap())
        .collect();
    assert!(texts[texts.len() - 2].contains(&format!("history-turn-{turn}")));
    assert!(texts.last().unwrap().contains("done"));
    assert!(texts[0].contains(&format!("history-turn-{}", turn - 49)));
    assert!(!texts.iter().any(|s| s.contains("history-turn-0\"")));
    assert_eq!(page.history.read_usage.messages, 100);
}
async fn run(native: Arc<dyn Store>) {
    let provider = crabber::FakeProvider::scripted(
        (0..53)
            .map(|_| {
                vec![
                    crabber::StreamDelta::TextDelta("done".into()),
                    crabber::StreamDelta::Completed,
                ]
            })
            .collect(),
    );
    let agent = crabber::Agent::builder()
        .store(native.clone())
        .provider(Arc::new(provider::LongContext(provider)))
        .behavior_version("newest-history-test.v1")
        .config(crabber::AgentConfig::new(crabber::Selection {
            provider_id: "fake".into(),
            model_id: "scripted".into(),
        }))
        .build()
        .unwrap();
    let session = SessionId::new();
    native
        .manage_session(crabber::session::ManageSession {
            id: crabber::core::ControlId::new(),
            session_id: session.clone(),
            expected_revision: crabber::core::SessionRevision::INITIAL,
            change: crabber::session::SessionChange::Create {
                workspace_id: "default".into(),
                directory: ".".into(),
                title: "newest".into(),
                defaults: crabber::session::SessionDefaults::default(),
            },
        })
        .await
        .unwrap();
    for turn in 0..52 {
        agent
            .prompt_session(session.clone(), format!("history-turn-{turn}"), None)
            .await
            .unwrap()
            .done()
            .await
            .unwrap();
    }
    let oldest = page(
        agent
            .public_session_snapshot_v2(request(&session))
            .await
            .unwrap(),
    );
    assert!(!oldest.history.omitted_older_messages);
    assert!(
        serde_json::to_string(&oldest.history.messages[0])
            .unwrap()
            .contains("history-turn-0")
    );
    let token = oldest.history.continuation.unwrap();
    let latest = page(
        agent
            .public_session_newest_snapshot_v2(request(&session))
            .await
            .unwrap(),
    );
    assert_latest(&latest, 51);
    let mut invalid = request(&session);
    invalid.continuation = Some(token);
    assert_eq!(
        agent
            .public_session_newest_snapshot_v2(invalid)
            .await
            .unwrap_err(),
        PublicSessionError::InvalidRequest
    );
    polling(&agent, &session).await;
    agent.close_extensions().await.unwrap();
    assert_latest(
        &page(
            agent
                .public_session_newest_snapshot_v2(request(&session))
                .await
                .unwrap(),
        ),
        52,
    );
}
async fn polling(agent: &crabber::Agent, session: &SessionId) {
    let mut poll = PublicSessionSubscriptionV2::newest(session.clone(), limits());
    let PublicSessionPollV2::Page {
        page: initial,
        reset: true,
    } = agent.poll_public_session_v2(&mut poll).await.unwrap()
    else {
        panic!("initial replacement")
    };
    assert_latest(&initial, 51);
    assert!(matches!(
        agent.poll_public_session_v2(&mut poll).await.unwrap(),
        PublicSessionPollV2::Unchanged { .. }
    ));
    agent
        .prompt_session(session.clone(), "history-turn-52", None)
        .await
        .unwrap()
        .done()
        .await
        .unwrap();
    assert!(matches!(
        agent.poll_public_session_v2(&mut poll).await.unwrap(),
        PublicSessionPollV2::Resync {
            reason: PublicResyncReason::RevisionChanged
        }
    ));
    let PublicSessionPollV2::Page {
        page: updated,
        reset: true,
    } = agent.poll_public_session_v2(&mut poll).await.unwrap()
    else {
        panic!("updated replacement")
    };
    assert_latest(&updated, 52);
    assert_ne!(initial.captured_revision, updated.captured_revision);
    poll.overflow();
    assert!(matches!(
        agent.poll_public_session_v2(&mut poll).await.unwrap(),
        PublicSessionPollV2::Resync {
            reason: PublicResyncReason::Overflow
        }
    ));
    let PublicSessionPollV2::Page {
        page: reset,
        reset: true,
    } = agent.poll_public_session_v2(&mut poll).await.unwrap()
    else {
        panic!("overflow replacement")
    };
    assert_latest(&reset, 52);
    assert_eq!(updated.captured_revision, reset.captured_revision);
    let mut small = limits();
    small.history.encoded_bytes = 1;
    poll.set_limits(small);
    assert!(matches!(
        agent.poll_public_session_v2(&mut poll).await.unwrap(),
        PublicSessionPollV2::Limited {
            limit: PublicSessionLimitV2::History(SnapshotLimit::EncodedBytes)
        }
    ));
    poll.set_limits(limits());
    assert!(matches!(
        agent.poll_public_session_v2(&mut poll).await.unwrap(),
        PublicSessionPollV2::Page { reset: true, .. }
    ));
}
#[tokio::test]
async fn memory_newest_history() {
    run(Arc::new(crabber::session::MemoryStore::new())).await;
}
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_newest_history() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("history.db");
    crabber::session::SqliteStore::migrate(&path).await.unwrap();
    run(Arc::new(
        crabber::session::SqliteStore::connect(path).await.unwrap(),
    ))
    .await;
}
#[cfg(feature = "postgres")]
#[test]
fn postgres_newest_history() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};
    TestDatabase::run_with_runtime(
        DatabaseMode::Current,
        tokio::runtime::Builder::new_current_thread(),
        async move |url| {
            crabber::session::PostgresStore::migrate(&url)
                .await
                .unwrap();
            run(Arc::new(
                crabber::session::PostgresStore::connect(&url)
                    .await
                    .unwrap(),
            ))
            .await;
        },
    )
    .unwrap();
}
