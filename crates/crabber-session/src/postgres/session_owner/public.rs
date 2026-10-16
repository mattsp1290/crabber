//! Bounded owner membership proofs in the caller's coherent read transaction.
use super::{SessionId, reads};
use crate::public_session::{
    budget::Budget,
    v2::proof::{Failure, Proof, owner, unknown},
};
use crate::session_owner::SessionOwnerState;
use crate::{PublicRegisteredOwner, PublicSessionLimitV2};
use sqlx::{Postgres, Row, Transaction};

/// Every current identity must have an immutable Reserve and conversely. Then
/// authenticate independently latest evidence before applying the Joined filter.
/// Paging historical joined identities can exhaust `ProofBytes`, never an unbounded map.
pub(in crate::postgres) async fn registered(
    tx: &mut Transaction<'_, Postgres>,
    session: &SessionId,
    limit: u16,
    safe: &mut Budget,
    proof: &mut Proof,
) -> Result<Vec<PublicRegisteredOwner>, Failure> {
    reads::scope(tx, session).await.map_err(unknown)?;
    let mut result = Vec::new();
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
            if record.snapshot.state != SessionOwnerState::Joined {
                if result.len() >= usize::from(limit) {
                    return Err(Failure::Limited(PublicSessionLimitV2::RegisteredOwners));
                }
                result.push(owner(&record.snapshot, safe)?);
            }
            after = id;
        }
    }
    Ok(result)
}
/// The five carriers and repeated routing fields fetched by `reads::current`.
/// Length-only selection happens before ANY of their private bodies transfer.
async fn preflight(
    tx: &mut Transaction<'_, Postgres>,
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
    Ok(())
}
const IDS: &str = r"SELECT CAST(octet_length(owner_id) AS BIGINT) AS bytes,CASE WHEN octet_length(owner_id)<=128 THEN owner_id END AS id FROM session_owner_current WHERE session_id=$1 AND owner_id>$2 ORDER BY owner_id LIMIT 32";
const CURRENT_COST: &str = "SELECT CAST(octet_length(data_json) AS BIGINT) FROM session_owner_current WHERE session_id=$1 AND owner_id=$2";
const COST: &str = r"SELECT CAST(octet_length(c.data_json) AS BIGINT) AS current_bytes,CAST(octet_length(l.data_json)+octet_length(l.owner_id)+octet_length(l.operation_id)+octet_length(l.operation)+octet_length(l.state) AS BIGINT) AS latest_bytes,
 CAST(octet_length(o.data_json)+octet_length(o.owner_id)+octet_length(o.operation_id)+octet_length(o.operation)+octet_length(o.state) AS BIGINT) AS origin_bytes,
 CAST(octet_length(x.data_json) AS BIGINT) AS context_bytes,
 CAST(octet_length(f.data_json)+octet_length(f.owner_id)+octet_length(f.operation_id)+octet_length(f.operation)+octet_length(f.state) AS BIGINT) AS first_bytes
 FROM session_owner_current c
 LEFT JOIN session_owner_audits l ON l.session_id=c.session_id AND l.owner_id=c.owner_id AND l.sequence=(SELECT MAX(a.sequence) FROM session_owner_audits a WHERE a.session_id=c.session_id AND a.owner_id=c.owner_id)
 LEFT JOIN session_owner_audits o ON o.session_id=c.session_id AND o.operation_id=c.data_json::json->>'origin'
 LEFT JOIN session_owner_contexts x ON x.session_id=c.session_id
 LEFT JOIN session_owner_audits f ON f.session_id=c.session_id AND f.ordinal=(SELECT MIN(a.ordinal) FROM session_owner_audits a WHERE a.session_id=c.session_id AND a.operation='Reserve')
 WHERE c.session_id=$1 AND c.owner_id=$2";

#[cfg(test)]
#[path = "public_tests.rs"]
mod tests;
