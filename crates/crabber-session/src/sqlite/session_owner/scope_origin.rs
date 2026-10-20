//! Positive scope birth is independent of the current owner roster.
mod source;
mod write;
use super::{Audit, Error, SessionId, unknown};
use crate::public_session::v2::proof::{Failure, Proof};
use crate::session_owner::publication::scope::{Origin, Source};
use crate::transaction::Transaction;
use sqlx::{Row, Sqlite};
pub(in crate::sqlite) use write::created;
pub(super) use write::{context_protocol, record_absent};
fn failure(error: &Failure) -> Error {
    match error {
        Failure::Limited(_) => Error::Capacity,
        Failure::Error(_) => Error::UnknownStoreOutcome,
    }
}
pub(super) async fn require(
    tx: &mut Transaction<'_, Sqlite>,
    session: &SessionId,
    workspace: &str,
    directory: &str,
    proof: &mut Proof,
) -> Result<(), Error> {
    proof.charge(1).map_err(|error| failure(&error))?;
    let legacy: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM session_owner_current WHERE session_id=$1 AND source_protocol=0)")
        .bind(&session.0).fetch_one(&mut **tx).await.map_err(unknown)?;
    if legacy {
        return Err(Error::Unsupported);
    }
    let origin = load(tx, session, proof).await?;
    source::verify(tx, &origin, proof).await?;
    if origin.workspace != workspace || origin.directory != directory {
        return Err(Error::IdentityMismatch);
    }
    Ok(())
}
pub(in crate::sqlite) async fn verify_if_present(
    tx: &mut Transaction<'_, Sqlite>,
    session: &SessionId,
    proof: &mut Proof,
) -> Result<(), Error> {
    let present: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM session_owner_scope_origins WHERE session_id=$1 UNION ALL SELECT 1 FROM sessions WHERE id=$1 AND owner_scope_protocol=1 UNION ALL SELECT 1 FROM session_owner_contexts WHERE session_id=$1 AND scope_protocol=1)",
    )
    .bind(&session.0)
    .fetch_one(&mut **tx)
    .await
    .map_err(unknown)?;
    proof.charge(1).map_err(|error| failure(&error))?;
    if present {
        let origin = load(tx, session, proof).await?;
        source::verify(tx, &origin, proof).await?;
    }
    Ok(())
}
async fn load(
    tx: &mut Transaction<'_, Sqlite>,
    session: &SessionId,
    proof: &mut Proof,
) -> Result<Origin, Error> {
    let row = sqlx::query(MARKERS)
        .bind(&session.0)
        .fetch_one(&mut **tx)
        .await
        .map_err(unknown)?;
    proof.charge(24).map_err(|error| failure(&error))?;
    let session_marker: Option<i64> = row.try_get("session_marker").map_err(unknown)?;
    let context_marker: Option<i64> = row.try_get("context_marker").map_err(unknown)?;
    if session_marker == Some(0) || context_marker == Some(0) {
        return Err(Error::Unsupported);
    }
    if session_marker
        .into_iter()
        .chain(context_marker)
        .any(|marker| marker != 1)
        || (session_marker.is_none() && context_marker.is_none())
    {
        return Err(Error::UnknownStoreOutcome);
    }
    let bytes: Option<i64> = row.try_get("bytes").map_err(unknown)?;
    let bytes = usize::try_from(bytes.ok_or(Error::UnknownStoreOutcome)?).map_err(unknown)?;
    proof.charge(bytes).map_err(|error| failure(&error))?;
    if bytes > 32768 {
        return Err(Error::UnknownStoreOutcome);
    }
    let raw: String =
        sqlx::query_scalar("SELECT data_json FROM session_owner_scope_origins WHERE session_id=$1")
            .bind(&session.0)
            .fetch_one(&mut **tx)
            .await
            .map_err(unknown)?;
    let origin: Origin = serde_json::from_str(&raw).map_err(unknown)?;
    if origin.session != *session {
        return Err(Error::UnknownStoreOutcome);
    }
    Ok(origin)
}
const MARKERS: &str = "SELECT (SELECT CAST(owner_scope_protocol AS BIGINT) FROM sessions WHERE id=$1) AS session_marker,(SELECT CAST(scope_protocol AS BIGINT) FROM session_owner_contexts WHERE session_id=$1) AS context_marker,(SELECT CAST(length(CAST(data_json AS BLOB)) AS BIGINT) FROM session_owner_scope_origins WHERE session_id=$1) AS bytes";

#[cfg(test)]
mod tests;
