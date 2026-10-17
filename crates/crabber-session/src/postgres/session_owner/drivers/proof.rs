//! Selected actual proof leaves are charged before transfer/validation.
use super::super::{Audit, Error, Record, public, reads, unknown};
use crate::postgres::transaction::Transaction;
use crate::public_session::v2::proof::{Proof, ReadError};
use crabber_core::{SessionId, SessionOwnerId, SessionOwnerOperationId};
use sqlx::Postgres;
pub(super) async fn charge(
    tx: &mut Transaction<'_, Postgres>,
    sql: &str,
    session: &str,
    id: &str,
    proof: &mut Proof,
) -> Result<(), Error> {
    read_charge(tx, sql, session, id, proof)
        .await
        .map_err(|error| error.into_domain_or(Error::UnknownStoreOutcome))
}
pub(super) async fn read_charge(
    tx: &mut Transaction<'_, Postgres>,
    sql: &str,
    session: &str,
    id: &str,
    proof: &mut Proof,
) -> Result<(), ReadError<Error>> {
    let bytes: Option<i64> = sqlx::query_scalar(sql)
        .bind(session)
        .bind(id)
        .fetch_optional(&mut **tx)
        .await
        .map_err(unknown)?;
    if let Some(bytes) = bytes {
        proof
            .charge(usize::try_from(bytes).map_err(unknown)?)
            .map_err(ReadError::Budget)?;
    }
    Ok(())
}
pub(super) async fn audit(
    tx: &mut Transaction<'_, Postgres>,
    session: &SessionId,
    id: &SessionOwnerOperationId,
    proof: &mut Proof,
) -> Result<Option<Audit>, Error> {
    read_audit(tx, session, id, proof)
        .await
        .map_err(|error| error.into_domain_or(Error::UnknownStoreOutcome))
}
pub(super) async fn read_audit(
    tx: &mut Transaction<'_, Postgres>,
    session: &SessionId,
    id: &SessionOwnerOperationId,
    proof: &mut Proof,
) -> Result<Option<Audit>, ReadError<Error>> {
    read_charge(tx, AUDIT, &session.0, &id.0, proof).await?;
    let audit = reads::audit(tx, session, id).await?;
    if let Some(audit) = &audit {
        super::super::protocol::selected(tx, audit, proof).await?;
    }
    Ok(audit)
}
pub(super) async fn current(
    tx: &mut Transaction<'_, Postgres>,
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
    tx: &mut Transaction<'_, Postgres>,
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
const AUDIT: &str = "SELECT CAST(octet_length(data_json)+octet_length(owner_id)+octet_length(operation_id)+octet_length(operation)+octet_length(state)+8 AS BIGINT) FROM session_owner_audits WHERE session_id=$1 AND operation_id=$2";
const MEMBERSHIP: &str = "SELECT CAST(8+COALESCE((SELECT SUM(octet_length(owner_id)) FROM session_owner_current WHERE session_id=$1),0)+COALESCE((SELECT SUM(octet_length(operation_id)+octet_length(owner_id)+8) FROM session_owner_audits WHERE session_id=$1),0) AS BIGINT)";
