//! Atomic immutable protocol writes. The native Session gate stays held through COMMIT.
use super::storage::encode;
use crate::native_enrollment::pending_initial::{
    Assignment, CloseRecord, EpochKind, PublicationKind, RecordDelta,
};
use crate::public_session::v2::proof::{Failure, Proof, unknown};
use crate::transaction::Transaction;
use sqlx::Sqlite;

pub(super) async fn persist(
    tx: &mut Transaction<'_, Sqlite>,
    delta: &RecordDelta,
    proof: &mut Proof,
) -> Result<(), Failure> {
    bootstrap(tx, delta, proof).await?;
    if let Some(record) = &delta.assignment {
        assignment(tx, record, proof).await?;
    }
    if let (Some(publication), Some(head)) = (&delta.publication, &delta.head) {
        // Closing publication+head precede the absorbing close row. No subsequent
        // mutable/immutable operation can publish after that exact close exists.
        let epoch_digest = match (&delta.epoch, &delta.assignment, &delta.close) {
            (Some(epoch), _, _) => {
                proof.measure(epoch)?;
                epoch.digest()?
            }
            (_, Some(assignment), _) => assignment.epoch_digest.clone(),
            (_, _, Some(close)) => close.epoch_digest.clone(),
            _ => return Err(unknown("Pending publication record")),
        };
        let data = encode(publication, proof)?;
        sqlx::query("INSERT INTO pending_initial_publications(session_id,source_key,epoch_id,epoch_digest,sequence,kind,publication_digest,data_json) VALUES($1,$2,$3,$4,$5,$6,$7,$8)")
   .bind(&publication.key.session_id.0).bind(publication.key.identity.as_str()).bind(&publication.epoch.0).bind(epoch_digest.as_str()).bind(i64::try_from(publication.sequence).map_err(unknown)?).bind(kind(publication.kind)).bind(publication.digest.as_str()).bind(data).execute(&mut **tx).await.map_err(unknown)?;
        let data = encode(head, proof)?;
        let affected = if head.sequence == 1 {
            sqlx::query("INSERT INTO pending_initial_heads(session_id,source_key,epoch_id,epoch_digest,sequence,publication_digest,data_json) VALUES($1,$2,$3,$4,$5,$6,$7)")
    .bind(&head.key.session_id.0).bind(head.key.identity.as_str()).bind(&head.epoch.0).bind(epoch_digest.as_str()).bind(1_i64).bind(head.digest.as_str()).bind(data).execute(&mut **tx).await.map_err(unknown)?.rows_affected()
        } else {
            sqlx::query("UPDATE pending_initial_heads SET sequence=$4,publication_digest=$5,data_json=$6 WHERE session_id=$1 AND source_key=$2 AND epoch_id=$3 AND sequence=$7 AND publication_digest=$8")
    .bind(&head.key.session_id.0).bind(head.key.identity.as_str()).bind(&head.epoch.0).bind(i64::try_from(head.sequence).map_err(unknown)?).bind(head.digest.as_str()).bind(data).bind(i64::try_from(head.sequence-1).map_err(unknown)?).bind(publication.previous_digest.as_ref().ok_or_else(||unknown("Pending predecessor"))?.as_str()).execute(&mut **tx).await.map_err(unknown)?.rows_affected()
        };
        if affected != 1 {
            return Err(unknown("Pending head CAS"));
        }
    } else {
        return Err(unknown("Pending missing publication head"));
    }
    if let Some(record) = &delta.close {
        close(tx, record, proof).await?;
    }
    Ok(())
}
async fn bootstrap(
    tx: &mut Transaction<'_, Sqlite>,
    delta: &RecordDelta,
    proof: &mut Proof,
) -> Result<(), Failure> {
    if let Some(record) = &delta.epoch {
        let data = encode(record, proof)?;
        let digest = record.digest()?;
        let c = &record.correlation;
        let opened = record.kind == EpochKind::Opened;
        sqlx::query("INSERT INTO pending_initial_epochs(session_id,source_key,epoch_id,epoch_digest,kind,complete_call,required_complete,required_close,data_json) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)")
   .bind(&c.key.session_id.0).bind(c.key.identity.as_str()).bind(&c.identity.epoch.0).bind(digest.as_str()).bind(if opened{"Opened"}else{"SealedBeforeOpen"}).bind(&c.identity.complete.0).bind(opened.then_some(c.identity.complete.0.as_str())).bind((!opened).then_some(c.identity.seal.0.as_str())).bind(data).execute(&mut **tx).await.map_err(unknown)?;
    }
    if let Some(record) = &delta.slot {
        proof.measure(record)?;
        sqlx::query("INSERT INTO pending_initial_cohorts(session_id,source_key,epoch_id,epoch_digest) VALUES($1,$2,$3,$4)")
   .bind(&record.key.session_id.0).bind(record.key.identity.as_str()).bind(&record.epoch.0).bind(record.epoch_digest.as_str()).execute(&mut **tx).await.map_err(unknown)?;
        let data = encode(record, proof)?;
        sqlx::query("INSERT INTO pending_initial_source_slots(session_id,source_key,epoch_id,epoch_digest,data_json) VALUES($1,$2,$3,$4,$5)")
   .bind(&record.key.session_id.0).bind(record.key.identity.as_str()).bind(&record.epoch.0).bind(record.epoch_digest.as_str()).bind(data).execute(&mut **tx).await.map_err(unknown)?;
    }
    if let Some(record) = &delta.registered {
        let data = encode(record, proof)?;
        sqlx::query("INSERT INTO pending_initial_registered_completes(session_id,source_key,epoch_id,epoch_digest,call_id,data_json) VALUES($1,$2,$3,$4,$5,$6)")
   .bind(&record.key.session_id.0).bind(record.key.identity.as_str()).bind(&record.epoch.0).bind(record.epoch_digest.as_str()).bind(&record.call.0).bind(data).execute(&mut **tx).await.map_err(unknown)?;
    }
    Ok(())
}
async fn assignment(
    tx: &mut Transaction<'_, Sqlite>,
    record: &Assignment,
    proof: &mut Proof,
) -> Result<(), Failure> {
    let data = encode(record, proof)?;
    let c = &record.correlation;
    sqlx::query("INSERT INTO pending_initial_assignments(session_id,source_key,epoch_id,epoch_digest,call_id,run_id,data_json) VALUES($1,$2,$3,$4,$5,$6,$7)")
  .bind(&c.key.session_id.0).bind(c.key.identity.as_str()).bind(&c.identity.epoch.0).bind(record.epoch_digest.as_str()).bind(&record.call.0).bind(&record.run_id.0).bind(data).execute(&mut **tx).await.map_err(unknown)?;
    Ok(())
}
async fn close(
    tx: &mut Transaction<'_, Sqlite>,
    record: &CloseRecord,
    proof: &mut Proof,
) -> Result<(), Failure> {
    let data = encode(record, proof)?;
    let c = &record.correlation;
    sqlx::query("INSERT INTO pending_initial_closes(session_id,source_key,epoch_id,epoch_digest,seal_id,data_json) VALUES($1,$2,$3,$4,$5,$6)")
  .bind(&c.key.session_id.0).bind(c.key.identity.as_str()).bind(&c.identity.epoch.0).bind(record.epoch_digest.as_str()).bind(&c.identity.seal.0).bind(data).execute(&mut **tx).await.map_err(unknown)?;
    Ok(())
}
fn kind(kind: PublicationKind) -> &'static str {
    match kind {
        PublicationKind::Opened => "Opened",
        PublicationKind::Assigned => "Assigned",
        PublicationKind::Closed => "Closed",
    }
}
