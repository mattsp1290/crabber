use crate::public_session::v2::proof::{Proof, ReadError};
use crate::{StoreError, run_facts::RunFacts};
use crabber_core::{RunId, SessionId};
use sqlx::{Row, Sqlite, Transaction};
pub(super) async fn read(
    tx: &mut Transaction<'_, Sqlite>,
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
pub(super) const COST: &str = r"SELECT CAST(length(CAST(facts AS BLOB)) AS BIGINT) FROM (SELECT CASE WHEN json_type(data,'$.id')='text' AND json_type(data,'$.session_id')='text' AND json_type(data,'$.status')='text' AND json_extract(data,'$.id')=id AND json_extract(data,'$.session_id')=session_id AND json_extract(data,'$.status')=status AND length(CAST(id AS BLOB))<=128 AND length(CAST(session_id AS BLOB))<=512 AND length(CAST(status AS BLOB))<=32 AND length(CAST(json_extract(data,'$.created_at') AS BLOB))<=128 THEN json_object('session_id',session_id,'run_id',id,'status',status,'created_at',json_extract(data,'$.created_at')) END AS facts FROM runs WHERE id=$1 AND session_id=$2) bounded_facts";
const FACTS: &str = r"SELECT CASE WHEN json_type(data,'$.id')='text' AND json_type(data,'$.session_id')='text' AND json_type(data,'$.status')='text' AND json_extract(data,'$.id')=id AND json_extract(data,'$.session_id')=session_id AND json_extract(data,'$.status')=status AND length(CAST(id AS BLOB))<=128 AND length(CAST(session_id AS BLOB))<=512 AND length(CAST(status AS BLOB))<=32 AND length(CAST(json_extract(data,'$.created_at') AS BLOB))<=128 THEN json_object('session_id',session_id,'run_id',id,'status',status,'created_at',json_extract(data,'$.created_at')) END AS facts FROM runs WHERE id=$1 AND session_id=$2";
