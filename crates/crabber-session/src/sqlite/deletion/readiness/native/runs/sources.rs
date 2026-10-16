//! Original admission carriers authenticate source history, never resource joins.
mod admission;
mod execution;
mod unkeyed;
use crate::native_parent::RunIssuance;
use crate::public_session::v2::proof::Proof;
use crate::sql_deletion::readiness::Result;
use crabber_core::{RunId, SessionId};
use sqlx::{Sqlite, Transaction};
pub(super) async fn verify(
    tx: &mut Transaction<'_, Sqlite>,
    session: &SessionId,
    run: &RunId,
    initial: Option<&RunIssuance>,
    proof: &mut Proof,
) -> Result<bool> {
    let owned = admission::verify(tx, session, run, initial, proof).await?;
    let execution = execution::verify(tx, session, run, owned.as_ref(), proof).await?;
    Ok(owned.is_some() && execution)
}

#[cfg(test)]
mod tests;
