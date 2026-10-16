//! Exact original preparation and native control leaves for one retained claim.
use super::super::super::super::super as backend;
use crate::native_parent::{CompletionOperation, RunIssuance, completion_history};
use crate::public_session::v2::proof::Proof;
use crate::session_owner::{audit::Audit, preparation::PreparationKey};
use crate::sql_deletion::readiness::{Result, unknown};
use crabber_core::{EventCursor, SessionId, SessionOwnerOperationId};
use sqlx::{Sqlite, Transaction};
pub(super) async fn source(
    tx: &mut Transaction<'_, Sqlite>,
    previous: Option<&RunIssuance>,
    current: &RunIssuance,
    proof: &mut Proof,
) -> Result<Option<EventCursor>> {
    let Some(source) = &current.completion_source else {
        return Ok(None);
    };
    if matches!(
        source.native_operation,
        CompletionOperation::AdmissionRecovery | CompletionOperation::ExpiredRunResume
    ) {
        // Their historical native-domain verifiers have not been qualified.
        return Ok(None);
    }
    let previous = previous.ok_or_else(|| unknown("claim predecessor"))?;
    let session = &current.state.session_id;
    let join = audit(tx, session, &source.root_join_id, proof).await?;
    let reserve = audit(tx, session, &join.record.origin, proof).await?;
    let origin = reserve
        .record
        .preparation
        .as_ref()
        .ok_or_else(|| unknown("claim preparation origin"))?;
    let ids =
        crate::session_owner::preparation::identity::ids(&origin.key, session).map_err(unknown)?;
    let begin = audit(tx, session, &ids.begin, proof).await?;
    let cursor = match &origin.key {
        PreparationKey::Approval(id) => {
            let receipt = backend::approval::receipt_with_proof(tx, session, id, proof)
                .await?
                .ok_or_else(|| unknown("original approval claim receipt"))?;
            let event =
                backend::read_proof::control_event(tx, session, receipt.event_cursor, proof)
                    .await?;
            verify(
                previous,
                current,
                &reserve,
                &begin,
                &join,
                &completion_history::Control::Approval(&receipt, &event),
            )?;
            receipt.event_cursor
        }
        PreparationKey::ToolContinuation(id) => {
            let (receipt, _) =
                backend::tool_continuation::read::receipt_with_proof(tx, session, id, proof)
                    .await?
                    .ok_or_else(|| unknown("original tool claim receipt"))?;
            // Independently validate indexed/serialized cursor and run routing;
            // the ordinary receipt reader normalizes its event cursor.
            let event =
                backend::read_proof::control_event(tx, session, receipt.event_cursor, proof)
                    .await?;
            verify(
                previous,
                current,
                &reserve,
                &begin,
                &join,
                &completion_history::Control::Tool(&receipt, &event),
            )?;
            receipt.event_cursor
        }
        _ => return Err(unknown("native claim preparation key")),
    };
    Ok(Some(cursor))
}
fn verify(
    previous: &RunIssuance,
    current: &RunIssuance,
    reserve: &Audit,
    begin: &Audit,
    join: &Audit,
    control: &completion_history::Control<'_>,
) -> Result<()> {
    if !completion_history::verify_control(previous, current, reserve, begin, join, control)
        .map_err(unknown)?
    {
        return Err(unknown("unverified retained control source"));
    }
    Ok(())
}
async fn audit(
    tx: &mut Transaction<'_, Sqlite>,
    session: &SessionId,
    id: &SessionOwnerOperationId,
    proof: &mut Proof,
) -> Result<Audit> {
    backend::read_proof::charge(
        tx,
        sqlx::query_scalar(AUDIT_COST).bind(&session.0).bind(&id.0),
        proof,
    )
    .await?;
    backend::session_owner::source_audit(tx, session, id)
        .await
        .map_err(unknown)?
        .ok_or_else(|| unknown("original claim preparation audit"))
}
const AUDIT_COST: &str = "SELECT CAST(length(CAST(data_json AS BLOB))+length(CAST(owner_id AS BLOB))+length(CAST(operation_id AS BLOB))+length(CAST(operation AS BLOB))+length(CAST(state AS BLOB))+8 AS INTEGER) FROM session_owner_audits WHERE session_id=$1 AND operation_id=$2";

#[cfg(test)]
mod tests;
