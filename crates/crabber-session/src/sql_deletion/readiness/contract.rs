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
