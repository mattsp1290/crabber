//! Original Raw birth and first-grant poison, never positive main membership.
use crate::native_enrollment::{Origin, SOURCE_BYTES, Status, Unclassified};
use crate::native_parent::{RunIssuance, source_protocol::JournalSource};
use crate::postgres::transaction::Transaction;
use crate::public_session::v2::proof::{Failure, Proof, ReadError, unknown};
use crabber_core::{RunId, SessionId};
use sqlx::{Postgres, Row};

pub(super) async fn birth(
    tx: &mut Transaction<'_, Postgres>,
    issuer: &RunIssuance,
    source: &JournalSource,
    proof: &mut Proof,
) -> Result<(), Failure> {
    let (origin, unclassified) =
        Origin::raw_birth(issuer, source, proof).map_err(ReadError::public)?;
    let digest = origin.digest(proof).map_err(ReadError::public)?;
    let original = encode(&origin, proof)?;
    let poison = encode(&unclassified, proof)?;
    sqlx::query(
        "INSERT INTO native_execution_origins
         (session_id,run_id,first_issuer_digest,origin_digest,unclassified_run_id,data)
         VALUES($1,$2,$3,$4,$2,$5)",
    )
    .bind(&origin.session_id.0)
    .bind(&origin.run_id.0)
    .bind(origin.first_issuer_digest.as_str())
    .bind(digest.as_str())
    .bind(original)
    .execute(&mut **tx)
    .await
    .map_err(unknown)?;
    sqlx::query(
        "INSERT INTO native_execution_unclassified(session_id,run_id,origin_digest,data)
         VALUES($1,$2,$3,$4)",
    )
    .bind(&origin.session_id.0)
    .bind(&origin.run_id.0)
    .bind(digest.as_str())
    .bind(poison)
    .execute(&mut **tx)
    .await
    .map_err(unknown)?;
    Ok(())
}

fn encode(value: &impl serde::Serialize, proof: &mut Proof) -> Result<String, Failure> {
    proof.measure(value)?;
    if crate::snapshot::encoded_bytes(value, SOURCE_BYTES).is_none() {
        return Err(unknown("execution enrollment source cap"));
    }
    serde_json::to_string(value).map_err(unknown)
}

/// The immutable first source is selected exactly, independently of later heads.
pub(super) async fn original(
    tx: &mut Transaction<'_, Postgres>,
    session: &SessionId,
    run: &RunId,
    proof: &mut Proof,
) -> Result<Status, Failure> {
    crate::native_parent::source_protocol::validate_scope(session, run).map_err(unknown)?;
    proof.measure(&(session, run))?;
    let row = fetch(tx, session, run, proof).await?;
    let Some(selected) = decode(&row, session, run, proof)? else {
        return Ok(Status::Legacy);
    };
    let first = super::native_parent::source_protocol::source(tx, session, run, 1, proof)
        .await
        .map_err(ReadError::public)?
        .ok_or_else(|| unknown("execution enrollment first source"))?;
    let issuer = super::native_parent::source_protocol::original(tx, session, run, 1, proof)
        .await
        .map_err(ReadError::public)?
        .ok_or_else(|| unknown("execution enrollment first issuer"))?;
    selected
        .origin
        .verify_original(&selected.poison, &first, &issuer, proof)
        .map_err(ReadError::public)?;
    Ok(Status::Unclassified)
}

async fn fetch(
    tx: &mut Transaction<'_, Postgres>,
    session: &SessionId,
    run: &RunId,
    proof: &mut Proof,
) -> Result<sqlx::postgres::PgRow, Failure> {
    super::read_proof::charge(
        tx,
        sqlx::query_scalar(
            "SELECT CAST(32+COALESCE(octet_length(r.execution_enrollment_origin_id),0)
             +COALESCE(octet_length(o.session_id)+octet_length(o.first_issuer_digest)
             +octet_length(o.origin_digest)+octet_length(o.birth_kind)+octet_length(o.data),0)
             +COALESCE(octet_length(u.session_id)+octet_length(u.origin_digest)
             +octet_length(u.data),0) AS BIGINT)
             FROM runs r LEFT JOIN native_execution_origins o ON o.run_id=r.id
             LEFT JOIN native_execution_unclassified u ON u.run_id=r.id
             WHERE r.session_id=$1 AND r.id=$2",
        )
        .bind(&session.0)
        .bind(&run.0),
        proof,
    )
    .await?;
    sqlx::query(
        "SELECT CAST(r.execution_enrollment_protocol AS INTEGER) AS protocol,
         r.execution_enrollment_origin_id IS NOT NULL AS has_origin_id,
         o.run_id IS NOT NULL AS has_origin,u.run_id IS NOT NULL AS has_poison,
         CASE WHEN octet_length(r.execution_enrollment_origin_id)<=128
              THEN r.execution_enrollment_origin_id END AS origin_id,
         CASE WHEN octet_length(o.session_id)<=512 THEN o.session_id END AS origin_scope,
         CAST(o.protocol_version AS INTEGER) AS origin_version,o.first_sequence,
         CASE WHEN octet_length(o.birth_kind)<=16 THEN o.birth_kind END AS birth_kind,
         CASE WHEN octet_length(o.first_issuer_digest)<=64 THEN o.first_issuer_digest END AS issuer,
         CASE WHEN octet_length(o.origin_digest)<=64 THEN o.origin_digest END AS origin_digest,
         CASE WHEN octet_length(o.data)<=4096 THEN o.data END AS original,
         CASE WHEN octet_length(u.session_id)<=512 THEN u.session_id END AS poison_scope,
         CASE WHEN octet_length(u.origin_digest)<=64 THEN u.origin_digest END AS poison_digest,
         CASE WHEN octet_length(u.data)<=4096 THEN u.data END AS poison
         FROM runs r LEFT JOIN native_execution_origins o ON o.run_id=r.id
         LEFT JOIN native_execution_unclassified u ON u.run_id=r.id
         WHERE r.session_id=$1 AND r.id=$2",
    )
    .bind(&session.0)
    .bind(&run.0)
    .fetch_one(&mut **tx)
    .await
    .map_err(unknown)
}

struct Selected {
    origin: Origin,
    poison: Unclassified,
}
fn decode(
    row: &sqlx::postgres::PgRow,
    session: &SessionId,
    run: &RunId,
    proof: &mut Proof,
) -> Result<Option<Selected>, Failure> {
    let protocol: i32 = row.try_get("protocol").map_err(unknown)?;
    let origin_id: Option<String> = row.try_get("origin_id").map_err(unknown)?;
    let original: Option<String> = row.try_get("original").map_err(unknown)?;
    let poison: Option<String> = row.try_get("poison").map_err(unknown)?;
    if protocol == 0 {
        let has_origin: bool = row.try_get("has_origin").map_err(unknown)?;
        let has_poison: bool = row.try_get("has_poison").map_err(unknown)?;
        return if !row.try_get::<bool, _>("has_origin_id").map_err(unknown)?
            && origin_id.is_none()
            && !has_origin
            && !has_poison
        {
            Ok(None)
        } else {
            Err(unknown("legacy execution enrollment source"))
        };
    }
    let origin: Origin = serde_json::from_str(
        &original.ok_or_else(|| unknown("missing execution enrollment origin"))?,
    )
    .map_err(unknown)?;
    let poison: Unclassified = serde_json::from_str(
        &poison.ok_or_else(|| unknown("missing execution enrollment poison"))?,
    )
    .map_err(unknown)?;
    if protocol != 1
        || origin_id.as_deref() != Some(run.0.as_str())
        || row.try_get::<String, _>("birth_kind").map_err(unknown)? != "raw"
        || row.try_get::<String, _>("origin_scope").map_err(unknown)? != session.0
        || row.try_get::<String, _>("poison_scope").map_err(unknown)? != session.0
        || row.try_get::<i32, _>("origin_version").map_err(unknown)? != 1
        || row.try_get::<i64, _>("first_sequence").map_err(unknown)? != 1
        || origin.session_id != *session
        || origin.run_id != *run
        || origin.first_issuer_digest.as_str()
            != row.try_get::<String, _>("issuer").map_err(unknown)?
        || origin.digest(proof).map_err(ReadError::public)?.as_str()
            != row.try_get::<String, _>("origin_digest").map_err(unknown)?
        || poison.origin_digest.as_str()
            != row.try_get::<String, _>("poison_digest").map_err(unknown)?
    {
        return Err(unknown("execution enrollment original routing"));
    }
    Ok(Some(Selected { origin, poison }))
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod migration_tests;
