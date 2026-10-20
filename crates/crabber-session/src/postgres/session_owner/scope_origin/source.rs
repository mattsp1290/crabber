//! Original scope sources materialize under the caller's existing allowance.
use super::{Error, Origin, Postgres, Proof, Row, Source, Transaction, failure, unknown};
pub(super) async fn verify(
    tx: &mut Transaction<'_, Postgres>,
    origin: &Origin,
    proof: &mut Proof,
) -> Result<(), Error> {
    match &origin.source {
        Source::Absent {
            operation, owner, ..
        } => {
            super::super::protocol::require(tx, &origin.session, owner, proof)
                .await
                .map_err(|error| error.into_domain_or(Error::Capacity))?;
            let audit =
                super::super::reads::audit_with_proof(tx, &origin.session, operation, proof)
                    .await?
                    .ok_or(Error::UnknownStoreOutcome)?;
            origin.verify_absent(&audit)
        }
        Source::Managed { operation, .. } | Source::Fork { operation, .. } => {
            let source = created(tx, &origin.session, operation, proof).await?;
            origin.verify_created(
                &source.kind,
                &source.digest,
                source.event,
                &source.receipt,
                &source.audit,
            )
        }
    }
}

pub(super) struct Created {
    pub kind: String,
    pub digest: String,
    pub event: Option<i64>,
    pub receipt: String,
    pub audit: String,
}
pub(super) async fn created(
    tx: &mut Transaction<'_, Postgres>,
    session: &super::SessionId,
    operation: &crabber_core::ControlId,
    proof: &mut Proof,
) -> Result<Created, Error> {
    let costs = sqlx::query(COSTS)
        .bind(&session.0)
        .bind(&operation.0)
        .fetch_one(&mut **tx)
        .await
        .map_err(unknown)?;
    for field in ["kind_bytes", "digest_bytes", "receipt_bytes", "audit_bytes"] {
        let bytes: Option<i64> = costs.try_get(field).map_err(unknown)?;
        proof.charge(8).map_err(|error| failure(&error))?;
        proof
            .charge(usize::try_from(bytes.ok_or(Error::UnknownStoreOutcome)?).map_err(unknown)?)
            .map_err(|error| failure(&error))?;
    }
    proof.charge(8).map_err(|error| failure(&error))?;
    let row = sqlx::query(SOURCE)
        .bind(&session.0)
        .bind(&operation.0)
        .fetch_one(&mut **tx)
        .await
        .map_err(unknown)?;
    let get = |name| -> Result<String, Error> {
        row.try_get::<Option<String>, _>(name)
            .map_err(unknown)?
            .ok_or(Error::UnknownStoreOutcome)
    };
    Ok(Created {
        kind: get("kind")?,
        digest: get("digest")?,
        event: row.try_get("event_seq").map_err(unknown)?,
        receipt: get("receipt")?,
        audit: get("audit")?,
    })
}
const COSTS: &str = "SELECT CAST(octet_length(r.kind) AS BIGINT) AS kind_bytes,CAST(octet_length(r.semantic_digest) AS BIGINT) AS digest_bytes,CAST(octet_length(r.data_json) AS BIGINT) AS receipt_bytes,CAST(octet_length(CASE WHEN r.kind='session_management' THEN m.data_json WHEN r.kind='session_fork' THEN f.data_json END) AS BIGINT) AS audit_bytes FROM operation_receipts r LEFT JOIN session_management_audit m ON m.session_id=r.session_id AND m.operation_id=r.operation_id LEFT JOIN session_fork_audit f ON f.session_id=r.session_id AND f.operation_id=r.operation_id WHERE r.session_id=$1 AND r.operation_id=$2";
const SOURCE: &str = "SELECT CASE WHEN octet_length(r.kind)<=32 THEN r.kind END AS kind,CASE WHEN octet_length(r.semantic_digest)<=64 THEN r.semantic_digest END AS digest,CASE WHEN octet_length(r.data_json)<=32768 THEN r.data_json END AS receipt,CASE WHEN octet_length(CASE WHEN r.kind='session_management' THEN m.data_json WHEN r.kind='session_fork' THEN f.data_json END)<=32768 THEN CASE WHEN r.kind='session_management' THEN m.data_json WHEN r.kind='session_fork' THEN f.data_json END END AS audit,r.event_seq FROM operation_receipts r LEFT JOIN session_management_audit m ON m.session_id=r.session_id AND m.operation_id=r.operation_id LEFT JOIN session_fork_audit f ON f.session_id=r.session_id AND f.operation_id=r.operation_id WHERE r.session_id=$1 AND r.operation_id=$2";
