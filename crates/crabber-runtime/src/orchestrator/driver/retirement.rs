//! Retries retain the exact physical observations and original authority.
use super::RuntimeError;
use crabber_session::{
    Store,
    session_owner::{
        SessionOwnerCommand, SessionOwnerError, SessionOwnerOperation, SessionOwnerOutcome,
        SessionOwnerState, owned_scope::JoinedOwnedScope,
    },
};
use std::time::Duration;

pub(in crate::orchestrator) async fn join(
    store: &dyn Store,
    grant: &super::registration::Grant,
    proof: JoinedOwnedScope,
) -> Result<(), RuntimeError> {
    let native = proof.request()?;
    let fingerprint = proof.fingerprint()?;
    let SessionOwnerCommand::Join { termination } = native.command else {
        return Err(RuntimeError::PreparationRecoveryRequired);
    };
    let mut retry_wait = Duration::from_millis(50);
    loop {
        match super::store_call(async {
            store
                .join_owned_scope(proof.clone(), grant.authority().clone())
                .await
        })
        .await
        {
            Ok(outcome) => match outcome {
                SessionOwnerOutcome::Applied {
                    receipt,
                    authority: None,
                }
                | SessionOwnerOutcome::Replayed(receipt)
                    if receipt.operation == SessionOwnerOperation::Join
                        && receipt.owner.state == SessionOwnerState::Joined
                        && receipt.owner.binding == grant.begin().owner.binding
                        && receipt.owner.created_at == grant.begin().owner.created_at
                        && receipt.owner.termination == Some(termination)
                        && receipt.session_id == native.session_id
                        && receipt.operation_id == native.id
                        && receipt.owner.target == native.target
                        && receipt.semantic_digest == fingerprint =>
                {
                    return Ok(());
                }
                // A malformed acknowledgement cannot release the original proof,
                // even when the native transaction actually committed its Join.
                _ => {}
            },
            Err(SessionOwnerError::UnknownStoreOutcome) => {}
            Err(error) => return Err(error.into()),
        }
        tokio::time::sleep(retry_wait).await;
        retry_wait = (retry_wait * 2).min(Duration::from_secs(1));
    }
}
