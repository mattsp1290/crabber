//! Exact original root/child/activation leaves; no current eligibility shortcut.
use crate::native_enrollment::pending_initial::prefix;
use crate::transaction::Transaction;
use crate::{
    public_session::v2::proof::Proof,
    session_owner::{
        SessionOwnerError as Error,
        audit::Audit,
        preparation::{JoinedPreparation, PreparationCommitError},
    },
};
use sqlx::Sqlite;

pub(in crate::sqlite) async fn selected(
    tx: &mut Transaction<'_, Sqlite>,
    original: &JoinedPreparation,
    selected: Option<&crate::native_enrollment::caller::initial::SelectedOriginal<'_>>,
    proof: &mut Proof,
) -> Result<[Audit; 5], PreparationCommitError> {
    let ids = prefix::select_selected(original, selected, proof)?;
    let session = &original
        .observed_grant()
        .original_reservation()
        .owner
        .binding
        .session_id;
    let mut values = Vec::with_capacity(5);
    for id in [
        &ids.root_reserve,
        &ids.root_begin,
        &ids.child_reserve,
        &ids.child_begin,
        &ids.activated,
    ] {
        let audit = super::super::reads::audit_with_proof(tx, session, id, proof)
            .await?
            .ok_or(Error::UnknownStoreOutcome)?;
        // selected() deliberately permits honest legacy metadata. This producer
        // requires an actual independently protected publication, never that fallback.
        if audit.version != 3 {
            return Err(Error::Unsupported.into());
        }
        super::super::protocol::selected(tx, &audit, proof)
            .await
            .map_err(|error| crate::session_owner::preparation::inspection::read_error(&error))?;
        values.push(audit);
    }
    values
        .try_into()
        .map_err(|_| Error::UnknownStoreOutcome.into())
}
