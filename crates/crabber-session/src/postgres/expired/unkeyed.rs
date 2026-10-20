//! Bounded explicit Witness2 source; selected original audits never grant work.
use super::Error;
use crate::postgres::transaction::Transaction;
use crate::session_owner::preparation::{admission::Witness, expired::unkeyed::Original};
use crabber_core::{RunId, SessionId};
use sqlx::{Postgres, Row};
pub(in crate::postgres) async fn read_with_proof(
    tx: &mut Transaction<'_, Postgres>,
    session: &SessionId,
    run: &RunId,
    proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<Option<Box<Original>>, Error> {
    super::super::read_proof::charge(tx, sqlx::query_scalar("SELECT CAST(octet_length(w.data_json)+COALESCE(octet_length(s.data::text),0)+octet_length(w.root_join) AS BIGINT) FROM preparation_admission_witnesses w LEFT JOIN sessions s ON s.id=w.session_id WHERE w.session_id=$1 AND w.run_id=$2").bind(&session.0).bind(&run.0), proof).await.map_err(unknown)?;
    let Some(row) = sqlx::query(SOURCE)
        .bind(&session.0)
        .bind(&run.0)
        .fetch_optional(&mut **tx)
        .await
        .map_err(unknown)?
    else {
        return Ok(None);
    };
    let witness: Witness = serde_json::from_str(
        &row.try_get::<Option<String>, _>("witness")
            .map_err(unknown)?
            .ok_or(Error::UnknownStoreOutcome)?,
    )
    .map_err(unknown)?;
    witness.validate()?;
    if witness.session_id() != session
        || witness.run_id() != run
        || row
            .try_get::<Option<String>, _>("root_join")
            .map_err(unknown)?
            .as_deref()
            != Some(&witness.join_id()?.0)
    {
        return Err(Error::UnknownStoreOutcome);
    }
    if !witness.is_unkeyed() {
        return Ok(None);
    }
    proof.charge(1).map_err(unknown)?;
    let mixed: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM admission_executions WHERE run_id=$1 UNION ALL SELECT 1 FROM admission_execution_sources WHERE run_id=$1)")
        .bind(&run.0).fetch_one(&mut **tx).await.map_err(unknown)?;
    if mixed {
        return Err(Error::UnknownStoreOutcome);
    }
    let current: crabber_core::Session = serde_json::from_str(
        &row.try_get::<Option<String>, _>("session")
            .map_err(unknown)?
            .ok_or(Error::UnknownStoreOutcome)?,
    )
    .map_err(unknown)?;
    if current.id != *session {
        return Err(Error::UnknownStoreOutcome);
    }
    let initial =
        super::super::admission_journal::issuance_with_proof(tx, session, run, Some(1), proof)
            .await
            .map_err(unknown)?;
    let mut audits = Vec::with_capacity(5);
    for id in witness.audit_ids()? {
        audits.push(
            super::super::session_owner::source_audit_with_proof(tx, session, &id, proof)
                .await?
                .ok_or(Error::UnknownStoreOutcome)?,
        );
    }
    let original = Box::new(Original {
        session: current,
        witness,
        initial,
        audits: audits.try_into().map_err(|_| Error::UnknownStoreOutcome)?,
    });
    original.digest()?;
    Ok(Some(original))
}
fn unknown(_: impl std::fmt::Debug) -> Error {
    Error::UnknownStoreOutcome
}
const SOURCE: &str = "SELECT CASE WHEN octet_length(w.data_json)<=8192 THEN w.data_json END AS witness,CASE WHEN octet_length(s.data::text)<=16384 THEN s.data::text END AS session,CASE WHEN octet_length(w.root_join)<=128 THEN w.root_join END AS root_join FROM preparation_admission_witnesses w LEFT JOIN sessions s ON s.id=w.session_id WHERE w.session_id=$1 AND w.run_id=$2";

#[cfg(test)]
#[path = "unkeyed/tests.rs"]
mod tests;
