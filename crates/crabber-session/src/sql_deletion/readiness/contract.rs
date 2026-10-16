//! Genuine generic owner metadata is insufficient to certify owned dispatch.
use crate::session_owner::{
    MutateSessionOwner, SessionOwnerCommand, SessionOwnerOutcome, SessionOwnerTermination,
};
use crate::{SessionSummary, Store};
use crabber_core::SessionOwnerOperationId;
pub(crate) async fn generic_joined(store: &impl Store, summary: &SessionSummary) {
    let mut request = crate::session_owner_contract::reservation(summary.session_id.clone());
    request.binding.workspace_id = summary.workspace_id.clone();
    request.binding.directory = summary.directory.clone();
    let SessionOwnerOutcome::Applied {
        receipt,
        authority: Some(authority),
    } = store.reserve_session_owner(request, None).await.unwrap()
    else {
        panic!("reserved")
    };
    for command in [
        SessionOwnerCommand::Begin,
        SessionOwnerCommand::Join {
            termination: SessionOwnerTermination::Returned,
        },
    ] {
        store
            .mutate_session_owner(
                MutateSessionOwner {
                    id: SessionOwnerOperationId::new(),
                    session_id: summary.session_id.clone(),
                    target: receipt.owner.target.clone(),
                    command,
                },
                Some(authority.clone()),
            )
            .await
            .unwrap();
    }
}

/// Native protected output whose actual local cleanup has been observed.
pub(crate) async fn closed_fork(store: &impl Store) -> crate::ForkReceipt {
    let mut fixture = crate::fork_pending_contract::fixture(store).await;
    tokio::spawn(async {}).await.unwrap();
    let outcome = store
        .record_phase_cleanup(crate::PhaseCleanup {
            session_id: fixture.request.source_session_id.clone(),
            run_id: fixture.run,
            id: crabber_core::ControlId::new(),
            expected_revision: fixture.request.expected_source_revision,
            target: fixture.target,
            result: crate::PhaseQuiescenceResult::Complete,
            claim: crate::PhaseResolverClaim {
                resolver_id: "scope-fixture".into(),
                authority_claim: "observed local source cleanup join".into(),
            },
        })
        .await
        .unwrap();
    let crate::PhaseControlOutcome::Applied(cleaned) = outcome else {
        panic!("fresh cleanup");
    };
    fixture.request.expected_source_revision = cleaned.revision;
    let crate::ForkOutcome::Applied(receipt) = store.fork_session(fixture.request).await.unwrap()
    else {
        panic!("closed native fork");
    };
    assert_eq!(receipt.imported_calls, 1);
    receipt
}
