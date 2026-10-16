//! Scalar source classification; no historical private carrier crosses the adapter.
use super::super::{Error, SessionId, Sqlite, Transaction, unknown};

// A relational Renew label is only a selection hint. Bound its original carrier
// before parsing and corroborate original operation/publication and routing.
// JSON (not JSONB) permits PostgreSQL to fail closed on escaped NUL text.
const SOURCES: &str = r"WITH sources AS (
    SELECT ordinal, operation_id, owner_id, operation,
      CASE WHEN operation<>'Renew' OR length(CAST(data_json AS BLOB))>32768 THEN false
      ELSE COALESCE(json_extract(data_json, '$.receipt.operation')='Renew'
        AND json_extract(data_json, '$.receipt.session_id')=session_id
        AND json_extract(data_json, '$.receipt.operation_id')=operation_id
        AND json_extract(data_json, '$.record.snapshot.binding.id')=owner_id
        AND json_extract(data_json, '$.record.snapshot.target.id')=owner_id
        AND json_extract(data_json, '$.receipt.owner.binding.id')=owner_id
        AND json_extract(data_json, '$.receipt.owner.target.id')=owner_id
        AND json_extract(data_json, '$.record.snapshot.binding.session_id')=session_id
        AND json_extract(data_json, '$.receipt.owner.binding.session_id')=session_id
        AND json_extract(data_json, '$.record.snapshot.state')=state
        AND json_extract(data_json, '$.receipt.owner.state')=state
        AND state IN ('Active','ReservedUnstarted')
        AND json_extract(data_json, '$.record.sequence')=sequence
        AND ((json_extract(data_json, '$.version')=1 AND json_extract(data_json, '$.record.version')=1 AND json_extract(data_json, '$.publication') IS NULL) OR (json_extract(data_json, '$.version')=2 AND json_extract(data_json, '$.record.version')=2 AND json_extract(data_json, '$.publication.public_change')=0)), false) END AS private
    FROM session_owner_audits WHERE session_id=$1
)";

/// Private Renew never advances the immutable publication clock. A corrupt
/// purported Renew refuses the read instead of silently concealing a publication.
pub(in super::super) async fn publication_count(
    tx: &mut Transaction<'_, Sqlite>,
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
    tx: &mut Transaction<'_, Sqlite>,
    grant: &crate::session_owner::preparation::PreparationGrant,
) -> Result<(), Error> {
    let request = grant.original_reservation();
    let ids = super::services::service_ids(
        tx,
        &request.owner.binding.session_id,
        &request.owner.binding.id,
    )
    .await?;
    let query = format!(
        "{SOURCES} SELECT EXISTS(SELECT 1 FROM sources WHERE ordinal>(SELECT ordinal FROM sources WHERE operation_id=$2) AND NOT private AND owner_id<>$3 AND owner_id<>$4 AND owner_id<>$5 AND owner_id<>$6 AND owner_id<>$7)"
    );
    let foreign: bool = sqlx::query_scalar(&query)
        .bind(&request.owner.binding.session_id.0)
        .bind(&request.owner.id.0)
        .bind(&request.owner.binding.id.0)
        .bind(
            grant
                .retained_resources_authority()
                .map_or(request.owner.binding.id.0.as_str(), |authority| {
                    authority.target().id.0.as_str()
                }),
        )
        .bind(
            ids.first()
                .map_or(request.owner.binding.id.0.as_str(), |id| id.0.as_str()),
        )
        .bind(
            ids.get(1)
                .map_or(request.owner.binding.id.0.as_str(), |id| id.0.as_str()),
        )
        .bind(
            ids.get(2)
                .map_or(request.owner.binding.id.0.as_str(), |id| id.0.as_str()),
        )
        .fetch_one(&mut **tx)
        .await
        .map_err(unknown)?;
    if foreign {
        Err(Error::RevisionConflict)
    } else {
        Ok(())
    }
}
