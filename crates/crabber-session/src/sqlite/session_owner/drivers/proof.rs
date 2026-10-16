//! Selected actual proof leaves are charged before transfer/validation.
use super::super::{Audit, Error, Record, public, reads, unknown};
use crate::public_session::v2::proof::Proof;
use crabber_core::{SessionId, SessionOwnerId, SessionOwnerOperationId};
use sqlx::{Sqlite, Transaction};
pub(super) async fn charge(
    tx: &mut Transaction<'_, Sqlite>,
    sql: &str,
    session: &str,
    id: &str,
    proof: &mut Proof,
) -> Result<(), Error> {
    let bytes: Option<i64> = sqlx::query_scalar(sql)
        .bind(session)
        .bind(id)
        .fetch_optional(&mut **tx)
        .await
        .map_err(unknown)?;
    if let Some(bytes) = bytes {
        proof
            .charge(usize::try_from(bytes).map_err(unknown)?)
            .map_err(unknown)?;
    }
    Ok(())
}
pub(super) async fn audit(
    tx: &mut Transaction<'_, Sqlite>,
    session: &SessionId,
    id: &SessionOwnerOperationId,
    proof: &mut Proof,
) -> Result<Option<Audit>, Error> {
    charge(tx, AUDIT, &session.0, &id.0, proof).await?;
    reads::audit(tx, session, id).await
}
pub(super) async fn current(
    tx: &mut Transaction<'_, Sqlite>,
    session: &SessionId,
    id: &SessionOwnerId,
    proof: &mut Proof,
) -> Result<Record, Error> {
    public::preflight(tx, session, &id.0, proof)
        .await
        .map_err(unknown)?;
    reads::current(tx, session, id).await
}
pub(super) async fn membership(
    tx: &mut Transaction<'_, Sqlite>,
    session: &SessionId,
    proof: &mut Proof,
) -> Result<(), Error> {
    let bytes: i64 = sqlx::query_scalar(MEMBERSHIP)
        .bind(&session.0)
        .fetch_one(&mut **tx)
        .await
        .map_err(unknown)?;
    proof
        .charge(usize::try_from(bytes).map_err(unknown)?)
        .map_err(unknown)?;
    reads::scope(tx, session).await
}
const AUDIT: &str = "SELECT CAST(length(CAST(data_json AS BLOB))+length(CAST(owner_id AS BLOB))+length(CAST(operation_id AS BLOB))+length(CAST(operation AS BLOB))+length(CAST(state AS BLOB))+8 AS BIGINT) FROM session_owner_audits WHERE session_id=$1 AND operation_id=$2";
const MEMBERSHIP: &str = "SELECT CAST(8+COALESCE((SELECT SUM(length(CAST(owner_id AS BLOB))) FROM session_owner_current WHERE session_id=$1),0)+COALESCE((SELECT SUM(length(CAST(operation_id AS BLOB))+length(CAST(owner_id AS BLOB))+8) FROM session_owner_audits WHERE session_id=$1),0) AS BIGINT)";
