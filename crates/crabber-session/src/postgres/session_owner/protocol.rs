//! Coherent selected owner sources. Historical equality is checked in SQL;
//! only original/head source bodies materialize in the shared client allowance.
mod write;
use super::{Audit, Error, SessionId, SessionOwnerId, unknown};
use crate::postgres::transaction::Transaction;
use crate::{
    public_session::v2::proof::{Proof, ReadError},
    session_owner::publication::Coverage,
};
use sqlx::{Postgres, Row};
pub(super) use write::{prepare, publish};
type Read = ReadError<Error>;
fn domain(error: impl std::fmt::Debug) -> Read {
    ReadError::Domain(unknown(error))
}
/// Authenticate one original projection without consulting any later owner head.
/// Legacy metadata remains readable but supplies no publication authority.
pub(in crate::postgres) async fn selected(
    tx: &mut Transaction<'_, Postgres>,
    audit: &Audit,
    proof: &mut Proof,
) -> Result<(), Read> {
    let session = &audit.receipt.session_id;
    let id = &audit.record.snapshot.target.id;
    proof.charge(8).map_err(ReadError::Budget)?;
    let protocol: Option<i64> = sqlx::query_scalar("SELECT CAST(source_protocol AS BIGINT) FROM session_owner_current WHERE session_id=$1 AND owner_id=$2")
        .bind(&session.0).bind(&id.0).fetch_optional(&mut **tx).await.map_err(domain)?;
    proof.charge(1).map_err(ReadError::Budget)?;
    let retained: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM session_owner_publication_cohorts WHERE session_id=$1 AND owner_id=$2 UNION ALL SELECT 1 FROM session_owner_publications WHERE session_id=$1 AND owner_id=$2)")
        .bind(&session.0).bind(&id.0).fetch_one(&mut **tx).await.map_err(domain)?;
    if audit.version < 3 && protocol == Some(0) && !retained {
        return Ok(());
    }
    let original = source(
        tx,
        session,
        id,
        i64::try_from(audit.record.sequence).map_err(domain)?,
        proof,
    )
    .await?;
    if crate::session_owner::publication::digest(audit).map_err(domain)?
        != crate::session_owner::publication::digest(&original).map_err(domain)?
    {
        return Err(domain("selected owner publication differs"));
    }
    Ok(())
}
/// The selected record must itself be the actual protected native publication.
pub(super) async fn require_record(
    tx: &mut Transaction<'_, Postgres>,
    record: &super::Record,
    proof: &mut Proof,
) -> Result<(), Read> {
    let session = &record.snapshot.binding.session_id;
    let id = &record.snapshot.target.id;
    require(tx, session, id, proof).await?;
    let original = source(
        tx,
        session,
        id,
        i64::try_from(record.sequence).map_err(domain)?,
        proof,
    )
    .await?;
    if original.record != *record {
        return Err(domain("selected owner record differs from publication"));
    }
    Ok(())
}
pub(super) async fn require(
    tx: &mut Transaction<'_, Postgres>,
    session: &SessionId,
    id: &SessionOwnerId,
    proof: &mut Proof,
) -> Result<(), Read> {
    match verify(tx, session, id, proof).await? {
        Coverage::Verified => Ok(()),
        Coverage::Legacy => Err(ReadError::Domain(Error::Unsupported)),
    }
}
pub(super) async fn verify(
    tx: &mut Transaction<'_, Postgres>,
    session: &SessionId,
    id: &SessionOwnerId,
    proof: &mut Proof,
) -> Result<Coverage, Read> {
    let protocol: Option<i64> = sqlx::query_scalar("SELECT CAST(source_protocol AS BIGINT) FROM session_owner_current WHERE session_id=$1 AND owner_id=$2")
        .bind(&session.0).bind(&id.0).fetch_optional(&mut **tx).await.map_err(domain)?;
    proof.charge(8).map_err(ReadError::Budget)?;
    let protocol = protocol.ok_or(ReadError::Domain(Error::NotFound))?;
    if protocol == 0 {
        let source: bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM session_owner_publication_cohorts WHERE session_id=$1 AND owner_id=$2 UNION ALL SELECT 1 FROM session_owner_publications WHERE session_id=$1 AND owner_id=$2 UNION ALL SELECT 1 FROM session_owner_audits WHERE session_id=$1 AND owner_id=$2 AND data_json LIKE '{\"version\":3,%')")
            .bind(&session.0).bind(&id.0).fetch_one(&mut **tx).await.map_err(domain)?;
        proof.charge(1).map_err(ReadError::Budget)?;
        return if source {
            Err(domain("legacy owner advertised a new source"))
        } else {
            Ok(Coverage::Legacy)
        };
    }
    if protocol != 1 {
        return Err(domain("owner protocol"));
    }
    let head = head(tx, session, id, proof).await?;
    let coherent: bool = sqlx::query_scalar(COHERENT)
        .bind(&session.0)
        .bind(&id.0)
        .bind(head.sequence)
        .fetch_one(&mut **tx)
        .await
        .map_err(domain)?;
    proof.charge(1).map_err(ReadError::Budget)?;
    if !coherent {
        return Err(domain("owner publication membership"));
    }
    let origin = source(tx, session, id, 1, proof).await?;
    let latest = source(tx, session, id, head.sequence, proof).await?;
    if origin.receipt.operation_id.0 != head.origin
        || crate::session_owner::publication::digest(&origin)
            .map_err(domain)?
            .as_str()
            != head.origin_digest
        || crate::session_owner::publication::digest(&latest)
            .map_err(domain)?
            .as_str()
            != head.digest
    {
        return Err(domain("owner publication head/origin"));
    }
    crate::session_owner::audit::verify_current(&latest.record, &origin, &latest)
        .map_err(domain)?;
    Ok(Coverage::Verified)
}
struct Head {
    origin: String,
    origin_digest: String,
    sequence: i64,
    digest: String,
}
async fn head(
    tx: &mut Transaction<'_, Postgres>,
    session: &SessionId,
    id: &SessionOwnerId,
    proof: &mut Proof,
) -> Result<Head, Read> {
    let bytes: Option<i64> = sqlx::query_scalar(HEAD_COST)
        .bind(&session.0)
        .bind(&id.0)
        .fetch_optional(&mut **tx)
        .await
        .map_err(domain)?;
    proof
        .charge(
            usize::try_from(bytes.ok_or_else(|| domain("owner cohort absent"))?).map_err(domain)?,
        )
        .map_err(ReadError::Budget)?;
    let row = sqlx::query(HEAD)
        .bind(&session.0)
        .bind(&id.0)
        .fetch_one(&mut **tx)
        .await
        .map_err(domain)?;
    let origin: Option<String> = row.try_get("origin").map_err(domain)?;
    let original: Option<String> = row.try_get("origin_digest").map_err(domain)?;
    let digest: Option<String> = row.try_get("head_digest").map_err(domain)?;
    let sequence: i64 = row.try_get("head_sequence").map_err(domain)?;
    let version: i64 = row.try_get("protocol_version").map_err(domain)?;
    if sequence < 1 || version != 1 {
        return Err(domain("owner cohort version/head"));
    }
    Ok(Head {
        origin: origin.ok_or_else(|| domain("owner origin bound"))?,
        origin_digest: original.ok_or_else(|| domain("owner origin digest"))?,
        sequence,
        digest: digest.ok_or_else(|| domain("owner head digest"))?,
    })
}
async fn source(
    tx: &mut Transaction<'_, Postgres>,
    session: &SessionId,
    id: &SessionOwnerId,
    sequence: i64,
    proof: &mut Proof,
) -> Result<Audit, Read> {
    let bytes: Option<i64> = sqlx::query_scalar(SOURCE_COST)
        .bind(&session.0)
        .bind(&id.0)
        .bind(sequence)
        .fetch_optional(&mut **tx)
        .await
        .map_err(domain)?;
    proof
        .charge(
            usize::try_from(bytes.ok_or_else(|| domain("owner original source absent"))?)
                .map_err(domain)?,
        )
        .map_err(ReadError::Budget)?;
    let row = sqlx::query(SOURCE)
        .bind(&session.0)
        .bind(&id.0)
        .bind(sequence)
        .fetch_one(&mut **tx)
        .await
        .map_err(domain)?;
    let text: Option<String> = row.try_get("data").map_err(domain)?;
    let operation: Option<String> = row.try_get("operation").map_err(domain)?;
    let digest: Option<String> = row.try_get("digest").map_err(domain)?;
    let audit: Audit =
        serde_json::from_str(&text.ok_or_else(|| domain("owner source bound"))?).map_err(domain)?;
    audit.validate().map_err(domain)?;
    if audit.version != 3
        || audit.receipt.session_id != *session
        || audit.record.snapshot.binding.session_id != *session
        || audit.record.snapshot.target.id != *id
        || audit.record.sequence != u64::try_from(sequence).map_err(domain)?
        || Some(&audit.receipt.operation_id.0) != operation.as_ref()
        || Some(
            crate::session_owner::publication::digest(&audit)
                .map_err(domain)?
                .as_str(),
        ) != digest.as_deref()
    {
        return Err(domain("owner source routing/digest"));
    }
    Ok(audit)
}
const COHERENT: &str = "SELECT (SELECT COUNT(*) FROM session_owner_publications WHERE session_id=$1 AND owner_id=$2)=$3 AND (SELECT COUNT(*) FROM session_owner_audits WHERE session_id=$1 AND owner_id=$2)=$3 AND NOT EXISTS(SELECT 1 FROM session_owner_publications p WHERE p.session_id=$1 AND p.owner_id=$2 AND NOT EXISTS(SELECT 1 FROM session_owner_audits a WHERE a.session_id=p.session_id AND a.owner_id=p.owner_id AND a.operation_id=p.operation_id AND a.sequence=p.sequence AND a.data_json=p.data_json AND a.operation=p.operation AND a.state=p.state)) AND NOT EXISTS(SELECT 1 FROM session_owner_audits a WHERE a.session_id=$1 AND a.owner_id=$2 AND NOT EXISTS(SELECT 1 FROM session_owner_publications p WHERE p.session_id=a.session_id AND p.owner_id=a.owner_id AND p.operation_id=a.operation_id AND p.sequence=a.sequence AND p.data_json=a.data_json AND p.operation=a.operation AND p.state=a.state)) AND (SELECT MAX(sequence) FROM session_owner_publications WHERE session_id=$1 AND owner_id=$2)=$3";
const HEAD_COST: &str = "SELECT CAST(octet_length(origin_id)+octet_length(origin_digest)+octet_length(head_digest)+16 AS BIGINT) FROM session_owner_publication_cohorts WHERE session_id=$1 AND owner_id=$2";
const HEAD: &str = "SELECT CASE WHEN octet_length(origin_id)<=128 THEN origin_id END AS origin,CASE WHEN octet_length(origin_digest)=64 THEN origin_digest END AS origin_digest,CAST(head_sequence AS BIGINT) AS head_sequence,CASE WHEN octet_length(head_digest)=64 THEN head_digest END AS head_digest,CAST(protocol_version AS BIGINT) AS protocol_version FROM session_owner_publication_cohorts WHERE session_id=$1 AND owner_id=$2";
const SOURCE_COST: &str = "SELECT CAST(octet_length(data_json)+octet_length(operation_id)+octet_length(audit_digest) AS BIGINT) FROM session_owner_publications WHERE session_id=$1 AND owner_id=$2 AND sequence=$3";
const SOURCE: &str = "SELECT CASE WHEN octet_length(data_json)<=32768 THEN data_json END AS data,CASE WHEN octet_length(operation_id)<=128 THEN operation_id END AS operation,CASE WHEN octet_length(audit_digest)=64 THEN audit_digest END AS digest FROM session_owner_publications WHERE session_id=$1 AND owner_id=$2 AND sequence=$3";

#[cfg(test)]
pub(super) mod tests;
