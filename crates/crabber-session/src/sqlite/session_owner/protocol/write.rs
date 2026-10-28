//! The actual owner write alone can establish a cohort; later writes cannot promote legacy state.
use super::{Audit, Coverage, Error, Proof, SessionId, SessionOwnerId, Sqlite, Transaction};
use crate::session_owner::SessionOwnerOperation;
pub(in super::super) async fn prepare(
    tx: &mut Transaction<'_, Sqlite>,
    audit: &Audit,
    fresh: bool,
    proof: &mut Proof,
) -> Result<bool, crate::public_session::v2::proof::ReadError<Error>> {
    if fresh {
        if audit.receipt.operation != SessionOwnerOperation::Reserve
            || audit.record.sequence != 1
            || audit.record.version != 2
        {
            return Err(Error::InvalidRequest.into());
        }
        return Ok(true);
    }
    let binding = &audit.record.snapshot.binding;
    match super::verify(tx, &binding.session_id, &binding.id, proof)
        .await
        .map_err(crate::public_session::v2::proof::ReadError::capacity_fallback)?
    {
        Coverage::Verified => Ok(true),
        Coverage::Legacy => Ok(false),
    }
}
pub(in super::super) async fn publish(
    tx: &mut Transaction<'_, Sqlite>,
    audit: &Audit,
    text: &str,
    fresh: bool,
) -> Result<(), Error> {
    let session: &SessionId = &audit.receipt.session_id;
    let id: &SessionOwnerId = &audit.record.snapshot.target.id;
    let digest = crate::session_owner::publication::digest(audit)?;
    let sequence = i64::try_from(audit.record.sequence).map_err(super::super::unknown)?;
    if fresh {
        sqlx::query("INSERT INTO session_owner_publication_cohorts(session_id,owner_id,origin_id,origin_digest,head_sequence,head_digest) VALUES($1,$2,$3,$4,1,$4)")
            .bind(&session.0).bind(&id.0).bind(&audit.receipt.operation_id.0).bind(digest.as_str()).execute(&mut **tx).await.map_err(super::super::unknown)?;
    }
    sqlx::query("INSERT INTO session_owner_publications(session_id,owner_id,operation_id,sequence,audit_digest,data_json,operation,state) VALUES($1,$2,$3,$4,$5,$6,$7,$8)")
        .bind(&session.0).bind(&id.0).bind(&audit.receipt.operation_id.0).bind(sequence).bind(digest.as_str()).bind(text).bind(super::super::operation(audit.receipt.operation)).bind(super::super::state(audit.record.snapshot.state)).execute(&mut **tx).await.map_err(super::super::unknown)?;
    if !fresh {
        let changed=sqlx::query("UPDATE session_owner_publication_cohorts SET head_sequence=$1,head_digest=$2 WHERE session_id=$3 AND owner_id=$4 AND head_sequence=$5")
            .bind(sequence).bind(digest.as_str()).bind(&session.0).bind(&id.0).bind(sequence-1).execute(&mut **tx).await.map_err(super::super::unknown)?;
        if changed.rows_affected() != 1 {
            return Err(Error::UnknownStoreOutcome);
        }
    }
    Ok(())
}
