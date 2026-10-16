//! Classify independently latest preparation sources before native grant.
//! Indexed state is a routing hint; it cannot conceal an Active source body.
use super::super::{Error, SessionId, Sqlite, Transaction, unknown};
pub(in super::super) async fn has_preparation(
    tx: &mut Transaction<'_, Sqlite>,
    session: &SessionId,
) -> Result<bool, Error> {
    sqlx::query_scalar(BLOCKED)
        .bind(&session.0)
        .fetch_one(&mut **tx)
        .await
        .map_err(unknown)
}
const BLOCKED: &str = "SELECT EXISTS(SELECT 1 FROM session_owner_audits a WHERE a.session_id=$1 AND NOT EXISTS(SELECT 1 FROM session_owner_audits later WHERE later.session_id=a.session_id AND later.owner_id=a.owner_id AND later.sequence>a.sequence) AND CASE WHEN length(CAST(a.data_json AS BLOB))>32768 THEN true ELSE NOT COALESCE(json_extract(a.data_json, '$.receipt.operation')=a.operation AND json_extract(a.data_json, '$.receipt.operation_id')=a.operation_id AND json_extract(a.data_json, '$.receipt.session_id')=a.session_id AND json_extract(a.data_json, '$.record.snapshot.binding.session_id')=a.session_id AND json_extract(a.data_json, '$.receipt.owner.binding.session_id')=a.session_id AND json_extract(a.data_json, '$.record.snapshot.binding.id')=a.owner_id AND json_extract(a.data_json, '$.record.snapshot.target.id')=a.owner_id AND json_extract(a.data_json, '$.receipt.owner.binding.id')=a.owner_id AND json_extract(a.data_json, '$.receipt.owner.target.id')=a.owner_id AND json_extract(a.data_json, '$.record.snapshot.state')=a.state AND json_extract(a.data_json, '$.receipt.owner.state')=a.state AND json_extract(a.data_json, '$.record.sequence')=a.sequence,false) OR ((json_type(a.data_json,'$.record.preparation') IS NOT NULL OR EXISTS(SELECT 1 FROM session_owner_audits original WHERE original.session_id=a.session_id AND original.owner_id=a.owner_id AND original.operation='Reserve' AND CASE WHEN length(CAST(original.data_json AS BLOB))>32768 THEN true ELSE COALESCE(json_extract(original.data_json,'$.receipt.owner.binding.kind')='Preparation' OR json_type(original.data_json,'$.record.preparation') IS NOT NULL,true) END)) AND (a.state<>'Joined' OR a.operation NOT IN ('Join','ResolveJoined'))) END)";
