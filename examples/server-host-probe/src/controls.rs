//! Pure public reads and controls, including after local host close.
use super::{Error, selection};
use crabber::{
    Agent, AgentConfig,
    core::{ControlId, SessionId},
    session::{
        DeleteSession, DeletionBlocker, DeletionOutcome, ForkBoundary, ForkOutcome, ForkSession,
        PublicSessionLimitsV2, PublicSessionOutcomeV2, PublicSessionRequestV2, SnapshotLimits,
        Store,
    },
};
use std::sync::Arc;
pub(crate) async fn run(store: Arc<dyn Store>, source: SessionId) -> Result<(), Error> {
    let provider = crabber::FakeProvider::scripted(vec![]);
    let host = Agent::builder()
        .behavior_version("pure-probe.v1")
        .store(store.clone())
        .provider(Arc::new(provider.clone()))
        .config(AgentConfig::new(selection()))
        .build()?;
    host.close_extensions().await?;
    let summary = host.session_summary(&source).await?;
    let messages = store.list_all_messages(&source).await?;
    let request = ForkSession {
        id: ControlId::new(),
        source_session_id: source.clone(),
        workspace_id: summary.workspace_id,
        directory: summary.directory,
        expected_source_revision: summary.revision,
        boundary: ForkBoundary::AfterMessage {
            message_id: messages.last().unwrap().id.clone(),
        },
        target_session_id: SessionId::new(),
        title: "nonexecuting imported history".into(),
        defaults: summary.defaults,
        limits: SnapshotLimits {
            messages: 64,
            tool_calls: 64,
            parts: 256,
            text_bytes: 1 << 20,
            encoded_bytes: 2 << 20,
        },
        max_proof_bytes: 32 << 20,
    };
    let ForkOutcome::Applied(fork) = host.fork_session(request.clone()).await? else {
        panic!("closed history prefix")
    };
    assert!(
        matches!(host.fork_session(request).await?,ForkOutcome::Replayed(actual) if actual==fork)
    );
    let mut continuation = None;
    let mut imported = 0;
    let mut copied = 0;
    let mut pages = 0;
    loop {
        let PublicSessionOutcomeV2::Page(page) = host
            .public_session_snapshot_v2(PublicSessionRequestV2 {
                session_id: fork.target.session_id.clone(),
                limits: limits(),
                continuation,
            })
            .await?
        else {
            panic!("complete bounded public page")
        };
        assert_eq!(page.captured_revision, fork.target.revision);
        assert_eq!(page.header.revision, fork.target.revision);
        assert_eq!(page.history.tool_calls, []);
        imported += page.history.imported_calls.len();
        copied += page.history.messages.len();
        pages += 1;
        continuation = page.history.continuation;
        if continuation.is_none() {
            break;
        }
    }
    assert!(pages > 1);
    assert_eq!(imported, 1);
    assert_eq!(copied, messages.len());
    let request = DeleteSession {
        id: ControlId::new(),
        session_id: fork.target.session_id.clone(),
        expected_revision: fork.target.revision,
    };
    let DeletionOutcome::Applied(receipt) = host.delete_session(request.clone()).await? else {
        panic!("quiet protected fork")
    };
    assert_eq!(
        host.delete_session(request).await?,
        DeletionOutcome::Replayed(receipt.clone())
    );
    assert_eq!(
        host.lookup_session_deletion(&receipt.session_id, &receipt.operation_id)
            .await?,
        Some(receipt)
    );
    assert!(store.get_session(&source).await?.is_some());
    assert!(matches!(
        host.delete_session(DeleteSession {
            id: ControlId::new(),
            session_id: source.clone(),
            expected_revision: store.session_revision(&source).await?
        })
        .await?,
        DeletionOutcome::Blocked(DeletionBlocker::OwnershipUnknown)
    ));
    assert_eq!(provider.requests().len(), 0);
    Ok(())
}
fn limits() -> PublicSessionLimitsV2 {
    PublicSessionLimitsV2 {
        header_bytes: 65536,
        proof_bytes: 32 << 20,
        approval_interactions: 64,
        tool_requirements: 64,
        queue_entries: 64,
        registered_owners: 1024,
        run_controls: 64,
        history: SnapshotLimits {
            messages: 1,
            tool_calls: 1,
            parts: 256,
            text_bytes: 65536,
            encoded_bytes: 131_072,
        },
    }
}
