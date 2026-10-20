//! Public Raw replay guards precede receipts; metadata never adopts Pending.
use super::{Error, proof, unknown};
use crate::postgres::transaction::Transaction;
use crate::public_session::v2::proof::Proof;
use crabber_core::{RunId, SessionId};
use sqlx::Postgres;
pub(super) async fn run(
    tx: &mut Transaction<'_, Postgres>,
    session: &SessionId,
    id: &RunId,
    proof: &mut Proof,
) -> Result<(), Error> {
    proof.measure(&(session, id)).map_err(unknown)?;
    let exists: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM runs WHERE session_id=$1 AND id=$2)")
            .bind(&session.0)
            .bind(&id.0)
            .fetch_one(&mut **tx)
            .await
            .map_err(unknown)?;
    if !exists {
        return Ok(());
    } // Preserve the original missing-source error path.
    let origin = super::super::super::native_enrollment::original(tx, session, id, proof)
        .await
        .map_err(unknown)?;
    match origin {
        crate::native_enrollment::Status::Legacy
        | crate::native_enrollment::Status::Unclassified => Ok(()),
        crate::native_enrollment::Status::Pending => Err(Error::Unsupported),
    }
}
pub(super) async fn activation(
    tx: &mut Transaction<'_, Postgres>,
    request: &crate::session_owner::owned_scope::OwnedScopeActivation,
    proof: &mut Proof,
) -> Result<(), Error> {
    // Use the actual protected source even when a Raw retry omits its parent.
    let Some(begin) = proof::audit(tx, request.session_id(), &request.source.begin, proof).await?
    else {
        return Ok(());
    };
    let id = begin
        .record
        .driver
        .as_deref()
        .map(crate::session_owner::driver::DriverLink::run_id)
        .or_else(|| {
            begin
                .record
                .successor_driver
                .as_deref()
                .map(crate::session_owner::driver::SuccessorDriverLink::run_id)
        });
    if let Some(id) = id {
        run(tx, request.session_id(), id, proof).await?;
    }
    Ok(())
}
