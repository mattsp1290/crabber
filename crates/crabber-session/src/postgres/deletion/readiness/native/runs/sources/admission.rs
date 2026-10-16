//! Immutable initial issuer dictates exact per-Run witness membership.
use super::super::super::super::super::super as backend;
use crate::native_parent::RunIssuance;
use crate::public_session::v2::proof::Proof;
use crate::session_owner::preparation::admission::{SourceAudits, Witness};
use crate::sql_deletion::readiness::{Result, unknown};
use crabber_core::{RunId, Session, SessionId};
use sqlx::{Postgres, Row, Transaction};
pub(super) async fn verify(
    tx: &mut Transaction<'_, Postgres>,
    session: &SessionId,
    run: &RunId,
    initial: Option<&RunIssuance>,
    proof: &mut Proof,
) -> Result<bool> {
    backend::read_proof::charge(tx, sqlx::query_scalar(COST).bind(&run.0), proof).await?;
    let row = sqlx::query(WITNESS)
        .bind(&run.0)
        .fetch_optional(&mut **tx)
        .await
        .map_err(unknown)?;
    let Some(row) = row else {
        if initial.is_some_and(|i| i.ownership_handoff.is_some()) {
            return Err(unknown("missing owned admission witness"));
        }
        return Ok(false);
    };
    let witness: Witness = serde_json::from_str(
        &row.try_get::<Option<String>, _>("record")
            .map_err(unknown)?
            .ok_or_else(|| unknown("witness carrier bound"))?,
    )
    .map_err(unknown)?;
    witness.validate().map_err(unknown)?;
    if witness.session_id() != session
        || witness.run_id() != run
        || row
            .try_get::<Option<String>, _>("session")
            .map_err(unknown)?
            .as_deref()
            != Some(&session.0)
        || row
            .try_get::<Option<String>, _>("root_join")
            .map_err(unknown)?
            .as_deref()
            != Some(&witness.join_id().map_err(unknown)?.0)
    {
        return Err(unknown("owned witness routing"));
    }
    let initial = initial.ok_or_else(|| unknown("owned initial issuer"))?;
    backend::read_proof::charge(tx, sqlx::query_scalar(SESSION_COST).bind(&session.0), proof)
        .await?;
    let raw: Option<String> = sqlx::query_scalar(SESSION)
        .bind(&session.0)
        .fetch_one(&mut **tx)
        .await
        .map_err(unknown)?;
    let current: Session =
        serde_json::from_str(&raw.ok_or_else(|| unknown("session carrier bound"))?)
            .map_err(unknown)?;
    let mut audits = Vec::with_capacity(5);
    for id in witness.audit_ids().map_err(unknown)? {
        backend::read_proof::charge(
            tx,
            sqlx::query_scalar(AUDIT_COST).bind(&session.0).bind(&id.0),
            proof,
        )
        .await?;
        audits.push(
            backend::session_owner::source_audit(tx, session, &id)
                .await
                .map_err(unknown)?
                .ok_or_else(|| unknown("original admission audit"))?,
        );
    }
    let audits = audits
        .try_into()
        .map_err(|_| unknown("admission audit count"))?;
    witness
        .verify_sources(&current, initial, &SourceAudits::from_array(&audits))
        .map_err(unknown)?;
    Ok(true)
}
const COST: &str = "SELECT CAST(octet_length(data_json)+octet_length(session_id)+octet_length(root_join) AS BIGINT) FROM preparation_admission_witnesses WHERE run_id=$1";
const WITNESS: &str = "SELECT CASE WHEN octet_length(data_json)<=8192 THEN data_json END AS record,CASE WHEN octet_length(session_id)<=512 THEN session_id END AS session,CASE WHEN octet_length(root_join)<=128 THEN root_join END AS root_join FROM preparation_admission_witnesses WHERE run_id=$1";
const SESSION_COST: &str =
    "SELECT CAST(octet_length(data::text) AS BIGINT) FROM sessions WHERE id=$1";
const SESSION: &str = "SELECT CASE WHEN octet_length(data::text)<=16384 THEN data::text END FROM sessions WHERE id=$1";
const AUDIT_COST: &str = "SELECT CAST(octet_length(data_json)+octet_length(owner_id)+octet_length(operation_id)+octet_length(operation)+octet_length(state)+8 AS BIGINT) FROM session_owner_audits WHERE session_id=$1 AND operation_id=$2";
