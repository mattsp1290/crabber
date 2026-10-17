use crate::postgres::transaction::Transaction;
use crate::public_session::v2::proof::{Proof, ReadError};
use crate::{StoreError, run_facts::RunFacts};
use crabber_core::{RunId, SessionId};
use sqlx::{Postgres, Row};
pub(super) async fn read(
    tx: &mut Transaction<'_, Postgres>,
    session: &SessionId,
    run: &RunId,
    proof: &mut Proof,
) -> Result<RunFacts, ReadError<StoreError>> {
    crate::public_run::validate_session(session)?;
    crate::attempt::validate_identifier(&run.0).map_err(|_| StoreError::AdmissionConflict)?;
    super::read_proof::charge(
        tx,
        sqlx::query_scalar(COST).bind(&run.0).bind(&session.0),
        proof,
    )
    .await
    .map_err(ReadError::Budget)?;
    let row = sqlx::query(FACTS)
        .bind(&run.0)
        .bind(&session.0)
        .fetch_optional(&mut **tx)
        .await
        .map_err(super::db)?
        .ok_or(StoreError::NotFound)?;
    let text = row
        .try_get::<Option<String>, _>("facts")
        .map_err(super::db)?
        .ok_or(StoreError::AdmissionConflict)?;
    let facts: RunFacts = serde_json::from_str(&text).map_err(|_| StoreError::AdmissionConflict)?;
    facts.validate(session, run)?;
    Ok(facts)
}
pub(super) const COST: &str = r"SELECT CAST(octet_length(facts) AS BIGINT) FROM (SELECT CASE WHEN jsonb_typeof(data->'id')='string' AND jsonb_typeof(data->'session_id')='string' AND jsonb_typeof(data->'status')='string' AND data->>'id'=id AND data->>'session_id'=session_id AND data->>'status'=status AND octet_length(id)<=128 AND octet_length(session_id)<=512 AND octet_length(status)<=32 AND octet_length(CAST(data->'created_at' AS TEXT))<=128 THEN json_build_object('session_id',session_id,'run_id',id,'status',status,'created_at',data->'created_at')::text END AS facts FROM runs WHERE id=$1 AND session_id=$2) bounded_facts";
const FACTS: &str = r"SELECT CASE WHEN jsonb_typeof(data->'id')='string' AND jsonb_typeof(data->'session_id')='string' AND jsonb_typeof(data->'status')='string' AND data->>'id'=id AND data->>'session_id'=session_id AND data->>'status'=status AND octet_length(id)<=128 AND octet_length(session_id)<=512 AND octet_length(status)<=32 AND octet_length(CAST(data->'created_at' AS TEXT))<=128 THEN json_build_object('session_id',session_id,'run_id',id,'status',status,'created_at',data->'created_at')::text END AS facts FROM runs WHERE id=$1 AND session_id=$2";
