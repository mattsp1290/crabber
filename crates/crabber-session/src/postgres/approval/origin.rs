//! Original suspension anchors presentation and Store-clock deadlines.
use super::{Error, approval};
use crate::public_session::v2::proof::{ApprovalReadError, Proof};
use sqlx::{Postgres, Transaction};

#[cfg(test)]
pub(in super::super) async fn verify(
    tx: &mut Transaction<'_, Postgres>,
    record: &approval::ApprovalRecord,
) -> Result<crate::run_facts::RunFacts, Error> {
    verify_with_proof(tx, record, &mut Proof::legacy())
        .await
        .map_err(|error| error.into_domain_or(Error::UnknownStoreOutcome))
}
pub(in super::super) async fn verify_with_proof(
    tx: &mut Transaction<'_, Postgres>,
    record: &approval::ApprovalRecord,
    proof: &mut Proof,
) -> Result<crate::run_facts::RunFacts, ApprovalReadError> {
    let suspension = super::receipt_with_proof(tx, &record.session_id, &record.batch.id, proof)
        .await?
        .ok_or(Error::UnknownStoreOutcome)?;
    let key = approval::derived_id("suspend-attempt", &record.batch.id);
    let native =
        super::super::attempt::host_receipt_with_proof(tx, &record.session_id, &key, proof)
            .await?
            .ok_or(Error::UnknownStoreOutcome)?;
    let run = super::super::run_facts::read(tx, &record.session_id, &record.run_id, proof).await?;
    let audit = super::super::read_proof::event(
        tx,
        &record.session_id,
        suspension.event_cursor,
        false,
        proof,
    )
    .await?;
    super::super::continuation_input::verify(
        tx,
        crate::continuation_input::Input::ApprovalSuspension(&record.batch),
        crate::continuation_input::Receipt::Approval(&suspension),
        &audit,
        proof,
    )
    .await
    .map_err(|e| e.map_domain(|_| Error::UnknownStoreOutcome))?;
    let native_audit =
        super::super::read_proof::event(tx, &record.session_id, native.event_cursor, false, proof)
            .await?;
    let source =
        super::super::read_proof::event(tx, &record.session_id, native.event_cursor, true, proof)
            .await?;
    record.verify_origin_at(
        &crate::approval::OriginRunFacts {
            session_id: &run.session_id,
            run_id: &run.run_id,
            created_at: run.created_at,
        },
        &suspension,
        &audit,
        &native,
        &native_audit,
        &source,
    )?;
    Ok(run)
}
