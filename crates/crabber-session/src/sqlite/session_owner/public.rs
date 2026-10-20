//! Bounded owner membership proofs in the caller's coherent read transaction.
use super::{SessionId, reads};
use crate::public_session::{
    budget::Budget,
    v2::proof::{Failure, Proof, owner, unknown},
};
use crate::session_owner::SessionOwnerState;
use crate::transaction::Transaction;
use crate::{PublicRegisteredOwner, PublicSessionLimitV2};
use sqlx::{Row, Sqlite};
#[path = "public/deletion.rs"]
pub(in crate::sqlite) mod deletion;
#[cfg(test)]
#[path = "public/deletion_tests.rs"]
mod deletion_tests;
enum Purpose {
    Public,
    Deletion,
}

/// Every current identity must have an immutable Reserve and conversely. Then
/// authenticate independently latest evidence before applying the Joined filter.
/// Paging historical joined identities can exhaust `ProofBytes`, never an unbounded map.
pub(in crate::sqlite) async fn registered(
    tx: &mut Transaction<'_, Sqlite>,
    session: &SessionId,
    limit: u16,
    safe: &mut Budget,
    proof: &mut Proof,
) -> Result<Vec<PublicRegisteredOwner>, Failure> {
    scan(tx, session, limit, safe, proof, Purpose::Public)
        .await
        .map(|owners| owners.live)
}

pub(in crate::sqlite) async fn deletion_registered(
    tx: &mut Transaction<'_, Sqlite>,
    session: &SessionId,
    safe: &mut Budget,
    proof: &mut Proof,
) -> Result<crate::session_owner::coverage::CheckedOwners, Failure> {
    if !proof.active() {
        return Err(unknown("detached coverage requires bounded proof"));
    }
    scan(tx, session, u16::MAX, safe, proof, Purpose::Deletion).await
}

async fn scan(
    tx: &mut Transaction<'_, Sqlite>,
    session: &SessionId,
    limit: u16,
    safe: &mut Budget,
    proof: &mut Proof,
    purpose: Purpose,
) -> Result<crate::session_owner::coverage::CheckedOwners, Failure> {
    reads::scope(tx, session).await.map_err(unknown)?;
    let mut result = Vec::new();
    let mut unclassified = false;
    let mut after = String::new();
    loop {
        let rows = sqlx::query(IDS)
            .bind(&session.0)
            .bind(&after)
            .fetch_all(&mut **tx)
            .await
            .map_err(unknown)?;
        if rows.is_empty() {
            break;
        }
        // All routing carriers in this page already transferred, even when
        // an authenticated count sentinel returns before its remaining rows.
        for row in &rows {
            let bytes = usize::try_from(row.try_get::<i64, _>("bytes").map_err(unknown)?)
                .map_err(unknown)?;
            proof.charge(bytes)?;
        }
        for row in &rows {
            let id = row
                .try_get::<Option<String>, _>("id")
                .map_err(unknown)?
                .as_ref()
                .ok_or_else(|| unknown("owner routing"))?
                .clone();
            if id <= after {
                return Err(unknown("owner keyset"));
            }
            preflight(tx, session, &id, proof).await?;
            let record = reads::current(tx, session, &crabber_core::SessionOwnerId(id.clone()))
                .await
                .map_err(unknown)?;
            if record.snapshot.state == SessionOwnerState::Joined {
                if matches!(purpose, Purpose::Deletion) {
                    unclassified |= !deletion::classified(tx, &record, proof).await?;
                }
            } else {
                if result.len() >= usize::from(limit) {
                    return Err(Failure::Limited(PublicSessionLimitV2::RegisteredOwners));
                }
                result.push(owner(&record.snapshot, safe)?);
            }
            after = id;
        }
    }
    Ok(crate::session_owner::coverage::CheckedOwners {
        live: result,
        unclassified,
    })
}
/// Current/origin/context carriers, plus exact optional cancellation sources,
/// and repeated routing fields fetched by `reads::current`.
/// Length-only selection happens before ANY of their private bodies transfer.
pub(super) async fn preflight(
    tx: &mut Transaction<'_, Sqlite>,
    session: &SessionId,
    id: &str,
    proof: &mut Proof,
) -> Result<(), Failure> {
    let current: i64 = sqlx::query_scalar(CURRENT_COST)
        .bind(&session.0)
        .bind(id)
        .fetch_one(&mut **tx)
        .await
        .map_err(unknown)?;
    let current = usize::try_from(current).map_err(unknown)?;
    proof.charge(current)?;
    if current > 32768 {
        return Err(unknown("owner current bound"));
    }
    let row = sqlx::query(COST)
        .bind(&session.0)
        .bind(id)
        .fetch_one(&mut **tx)
        .await
        .map_err(unknown)?;
    for field in [
        "latest_bytes",
        "origin_bytes",
        "context_bytes",
        "first_bytes",
    ] {
        let bytes = row
            .try_get::<Option<i64>, _>(field)
            .map_err(unknown)?
            .ok_or_else(|| unknown("owner source absent"))?;
        proof.charge(usize::try_from(bytes).map_err(unknown)?)?;
    }
    for field in [
        "request_bytes",
        "observed_bytes",
        "resource_reserve_bytes",
        "resource_begin_bytes",
        "scope_reserve_bytes",
        "scope_begin_bytes",
        "scope_activation_bytes",
    ] {
        if let Some(bytes) = row.try_get::<Option<i64>, _>(field).map_err(unknown)? {
            proof.charge(usize::try_from(bytes).map_err(unknown)?)?;
        }
    }
    super::protocol::verify(tx, session, &crabber_core::SessionOwnerId(id.into()), proof)
        .await
        .map_err(crate::public_session::v2::proof::ReadError::public)?;
    Ok(())
}
const IDS: &str = r"SELECT length(CAST(owner_id AS BLOB)) AS bytes,CASE WHEN length(CAST(owner_id AS BLOB))<=128 THEN owner_id END AS id FROM session_owner_current WHERE session_id=$1 AND owner_id>$2 ORDER BY owner_id LIMIT 32";
const CURRENT_COST: &str = "SELECT CAST(length(CAST(data_json AS BLOB)) AS BIGINT) FROM session_owner_current WHERE session_id=$1 AND owner_id=$2";
const COST: &str = r"SELECT length(CAST(c.data_json AS BLOB)) AS current_bytes,length(CAST(l.data_json AS BLOB))+length(CAST(l.owner_id AS BLOB))+length(CAST(l.operation_id AS BLOB))+length(CAST(l.operation AS BLOB))+length(CAST(l.state AS BLOB)) AS latest_bytes,
 length(CAST(o.data_json AS BLOB))+length(CAST(o.owner_id AS BLOB))+length(CAST(o.operation_id AS BLOB))+length(CAST(o.operation AS BLOB))+length(CAST(o.state AS BLOB)) AS origin_bytes,
 length(CAST(x.data_json AS BLOB)) AS context_bytes,
 length(CAST(f.data_json AS BLOB))+length(CAST(f.owner_id AS BLOB))+length(CAST(f.operation_id AS BLOB))+length(CAST(f.operation AS BLOB))+length(CAST(f.state AS BLOB)) AS first_bytes,
 length(CAST(r.data_json AS BLOB))+length(CAST(r.owner_id AS BLOB))+length(CAST(r.operation_id AS BLOB))+length(CAST(r.operation AS BLOB))+length(CAST(r.state AS BLOB)) AS request_bytes,
 length(CAST(v.data_json AS BLOB))+length(CAST(v.owner_id AS BLOB))+length(CAST(v.operation_id AS BLOB))+length(CAST(v.operation AS BLOB))+length(CAST(v.state AS BLOB)) AS observed_bytes
,
 CAST(length(CAST(rr.data_json AS BLOB))+length(CAST(rr.owner_id AS BLOB))+length(CAST(rr.operation_id AS BLOB))+length(CAST(rr.operation AS BLOB))+length(CAST(rr.state AS BLOB)) AS INTEGER) AS resource_reserve_bytes,
 CAST(length(CAST(rb.data_json AS BLOB))+length(CAST(rb.owner_id AS BLOB))+length(CAST(rb.operation_id AS BLOB))+length(CAST(rb.operation AS BLOB))+length(CAST(rb.state AS BLOB)) AS INTEGER) AS resource_begin_bytes,
 CAST(length(CAST(sr.data_json AS BLOB))+length(CAST(sr.owner_id AS BLOB))+length(CAST(sr.operation_id AS BLOB))+length(CAST(sr.operation AS BLOB))+length(CAST(sr.state AS BLOB)) AS INTEGER) AS scope_reserve_bytes,
 CAST(length(CAST(sb.data_json AS BLOB))+length(CAST(sb.owner_id AS BLOB))+length(CAST(sb.operation_id AS BLOB))+length(CAST(sb.operation AS BLOB))+length(CAST(sb.state AS BLOB)) AS INTEGER) AS scope_begin_bytes,
 CAST(length(CAST(sa.data_json AS BLOB))+length(CAST(sa.owner_id AS BLOB))+length(CAST(sa.operation_id AS BLOB))+length(CAST(sa.operation AS BLOB))+length(CAST(sa.state AS BLOB)) AS INTEGER) AS scope_activation_bytes FROM session_owner_current c
 LEFT JOIN session_owner_audits l ON l.session_id=c.session_id AND l.owner_id=c.owner_id AND l.sequence=(SELECT MAX(a.sequence) FROM session_owner_audits a WHERE a.session_id=c.session_id AND a.owner_id=c.owner_id)
 LEFT JOIN session_owner_audits o ON o.session_id=c.session_id AND o.operation_id=json_extract(c.data_json,'$.origin')
 LEFT JOIN session_owner_contexts x ON x.session_id=c.session_id
 LEFT JOIN session_owner_audits f ON f.session_id=c.session_id AND f.ordinal=(SELECT MIN(a.ordinal) FROM session_owner_audits a WHERE a.session_id=c.session_id AND a.operation='Reserve')
 LEFT JOIN session_owner_audits r ON r.session_id=c.session_id AND r.operation_id=json_extract(c.data_json,'$.cancellation.request.id')
 LEFT JOIN session_owner_audits v ON v.session_id=c.session_id AND v.operation_id=json_extract(c.data_json,'$.cancellation.observed.request.id')

 LEFT JOIN session_owner_audits rr ON rr.session_id=c.session_id AND rr.operation_id=COALESCE(json_extract(c.data_json,'$.resource.root_reserve'),json_extract(c.data_json,'$.service.root.root_reserve'))
 LEFT JOIN session_owner_audits rb ON rb.session_id=c.session_id AND rb.operation_id=COALESCE(json_extract(c.data_json,'$.resource.root_begin'),json_extract(c.data_json,'$.service.root.root_begin'))
 LEFT JOIN session_owner_audits sr ON sr.session_id=c.session_id AND sr.operation_id=COALESCE(json_extract(c.data_json,'$.owned_scope.source.reserve'),json_extract(c.data_json,'$.owned_join.activation.source.reserve'))
 LEFT JOIN session_owner_audits sb ON sb.session_id=c.session_id AND sb.operation_id=COALESCE(json_extract(c.data_json,'$.owned_scope.source.begin'),json_extract(c.data_json,'$.owned_join.activation.source.begin'))
 LEFT JOIN session_owner_audits sa ON sa.session_id=c.session_id AND sa.operation_id=COALESCE(json_extract(c.data_json,'$.owned_scope.id'),json_extract(c.data_json,'$.owned_join.activation.id'))
 WHERE c.session_id=$1 AND c.owner_id=$2";

#[cfg(test)]
#[path = "public_tests.rs"]
mod tests;
