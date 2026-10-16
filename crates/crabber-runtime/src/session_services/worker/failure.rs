//! Failed actual joins revoke ingress; they never produce a successful Join.
use super::super::entry::{Entry, Status};
use crabber_session::{
    Store,
    session_owner::{
        MutateSessionOwner, SessionOwnerAuthority, SessionOwnerCommand, SessionOwnerError,
        SessionOwnerResolver, owned_scope::OwnedScopeActivation,
    },
};

pub(super) async fn record(
    store: &dyn Store,
    entry: &Entry,
    authority: &SessionOwnerAuthority,
    activation: &OwnedScopeActivation,
) {
    entry.set_status(Status::Uncertain);
    let request = MutateSessionOwner {
        id: crabber_core::SessionOwnerOperationId(format!(
            "service-uncertain-{}",
            activation.operation_id().0
        )),
        session_id: authority.session_id().clone(),
        target: authority.target().clone(),
        command: SessionOwnerCommand::MarkUncertain {
            claim: SessionOwnerResolver {
                resolver_id: "crabber.runtime.session-services".into(),
                authority_claim: "observed actual dependency main or resource cleanup failure"
                    .into(),
            },
        },
    };
    // Retry this same observation key after a lost ACK. Neither the task nor
    // its cleanup is invoked again, and failure still grants no cleanup credit.
    for _ in 0..2 {
        match store
            .mutate_session_owner(request.clone(), Some(authority.clone()))
            .await
        {
            Err(SessionOwnerError::UnknownStoreOutcome) => {}
            Err(SessionOwnerError::Deleted) => {
                entry.set_status(Status::Deleted);
                break;
            }
            _ => break,
        }
    }
}
