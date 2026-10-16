//! Scalar source classification; no historical private carrier crosses the adapter.
use super::super::{Error, Postgres, SessionId, Transaction, unknown};

// A relational Renew label is only a selection hint. Bound its original carrier
// before parsing and corroborate original operation/publication and routing.
// JSON (not JSONB) permits PostgreSQL to fail closed on escaped NUL text.
const SOURCES: &str = r"WITH sources AS (
    SELECT ordinal, operation_id, owner_id, operation,
      CASE WHEN operation<>'Renew' OR octet_length(data_json)>32768 THEN false
      ELSE COALESCE(data_json::json #>> '{receipt,operation}'='Renew'
        AND data_json::json #>> '{receipt,session_id}'=session_id
        AND data_json::json #>> '{receipt,operation_id}'=operation_id
        AND data_json::json #>> '{record,snapshot,binding,id}'=owner_id
        AND data_json::json #>> '{record,snapshot,target,id}'=owner_id
        AND data_json::json #>> '{receipt,owner,binding,id}'=owner_id
        AND data_json::json #>> '{receipt,owner,target,id}'=owner_id
        AND data_json::json #>> '{record,snapshot,binding,session_id}'=session_id
        AND data_json::json #>> '{receipt,owner,binding,session_id}'=session_id
        AND data_json::json #>> '{record,snapshot,state}'=state
        AND data_json::json #>> '{receipt,owner,state}'=state
        AND state IN ('Active','ReservedUnstarted')
        AND data_json::json #>> '{record,sequence}'=CAST(sequence AS TEXT)
        AND ((data_json::json #>> '{version}'='1' AND data_json::json #>> '{record,version}'='1' AND data_json::json #>> '{publication}' IS NULL) OR (data_json::json #>> '{version}'='2' AND data_json::json #>> '{record,version}'='2' AND data_json::json #>> '{publication,public_change}'='false')), false) END AS private
    FROM session_owner_audits WHERE session_id=$1
)";

/// Private Renew never advances the immutable publication clock. A corrupt
/// purported Renew refuses the read instead of silently concealing a publication.
pub(in super::super) async fn publication_count(
    tx: &mut Transaction<'_, Postgres>,
    session: &SessionId,
) -> Result<u64, Error> {
    let query = format!(
        "{SOURCES} SELECT CASE WHEN EXISTS(SELECT 1 FROM sources WHERE operation='Renew' AND NOT private) THEN NULL ELSE (SELECT COUNT(*) FROM sources WHERE NOT private) END"
    );
    let count: Option<i64> = sqlx::query_scalar(&query)
        .bind(&session.0)
        .fetch_one(&mut **tx)
        .await
        .map_err(unknown)?;
    u64::try_from(count.ok_or(Error::UnknownStoreOutcome)?).map_err(unknown)
}

/// Independent immutable ordering rejects foreign publications even if an older
/// historical audit was erased and a scalar publication count happens to match.
pub(in super::super) async fn preparation_order(
    tx: &mut Transaction<'_, Postgres>,
    request: &crate::session_owner::preparation::ReservePreparation,
) -> Result<(), Error> {
    let query = format!(
        "{SOURCES} SELECT EXISTS(SELECT 1 FROM sources WHERE ordinal>(SELECT ordinal FROM sources WHERE operation_id=$2) AND NOT private AND owner_id<>$3)"
    );
    let foreign: bool = sqlx::query_scalar(&query)
        .bind(&request.owner.binding.session_id.0)
        .bind(&request.owner.id.0)
        .bind(&request.owner.binding.id.0)
        .fetch_one(&mut **tx)
        .await
        .map_err(unknown)?;
    if foreign {
        Err(Error::RevisionConflict)
    } else {
        Ok(())
    }
}
