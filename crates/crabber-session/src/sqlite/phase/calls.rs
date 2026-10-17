use super::super::{db, records};
use crate::{PhaseError as Error, phase::sql};
use crabber_core::{Run, ToolCallId, ToolCallRecord, ToolCallStatus};
use sqlx::{Row, Sqlite, Transaction};

pub(in super::super) async fn load(
    tx: &mut Transaction<'_, Sqlite>,
    run: &Run,
    id: &ToolCallId,
) -> Result<ToolCallRecord, Error> {
    let row = sqlx::query("SELECT CASE WHEN length(CAST(status AS BLOB))<=64 THEN status END AS status,CASE WHEN length(CAST(data AS BLOB))<=4194304 THEN data END AS record FROM tool_calls WHERE run_id=$1 AND id=$2").bind(&run.id.0).bind(&id.0).fetch_optional(&mut **tx).await.map_err(db).map_err(sql::unknown)?.ok_or(Error::NotFound)?;
    let call: ToolCallRecord = serde_json::from_str(
        &row.get::<Option<String>, _>("record")
            .ok_or(Error::UnknownStoreOutcome)?,
    )
    .map_err(|_| Error::UnknownStoreOutcome)?;
    if call.run_id != run.id
        || call.id != *id
        || row.get::<Option<String>, _>("status").as_deref()
            != Some(records::call_status(call.status))
    {
        return Err(Error::UnknownStoreOutcome);
    }
    Ok(call)
}
pub(in super::super) async fn load_with_proof(
    tx: &mut Transaction<'_, Sqlite>,
    run: &Run,
    id: &ToolCallId,
    proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<ToolCallRecord, Error> {
    super::super::read_proof::charge(tx, sqlx::query_scalar("SELECT CAST(length(CAST(data AS BLOB))+length(CAST(status AS BLOB)) AS BIGINT) FROM tool_calls WHERE run_id=$1 AND id=$2").bind(&run.id.0).bind(&id.0), proof)
        .await.map_err(|_| Error::UnknownStoreOutcome)?;
    load(tx, run, id).await
}
pub(super) async fn begin(
    tx: &mut Transaction<'_, Sqlite>,
    run: &Run,
    id: &ToolCallId,
    start: Option<&crate::PhaseStart>,
    proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<Option<crabber_core::EventRecord>, Error> {
    let mut call = load_with_proof(tx, run, id, proof).await?;
    let mut running_event = None;
    if call.status != ToolCallStatus::Pending || call.result.is_some() {
        return Err(Error::InvalidTransition);
    }
    if let Some(start) = start {
        let mut event = start.materialize(run, &call)?;
        proof
            .measure(&event)
            .map_err(|_| Error::UnknownStoreOutcome)?;
        event.cursor = Some(
            records::insert_event_captured(tx, run, &event)
                .await
                .map_err(sql::unknown)?,
        );
        running_event = Some(event);
    }
    call.status = ToolCallStatus::Running;
    records::save_call(tx, id, &call)
        .await
        .map_err(sql::unknown)?;
    Ok(running_event)
}

pub(super) async fn settle(
    tx: &mut Transaction<'_, Sqlite>,
    run: &Run,
    settlement: &crate::PhaseSettlement,
) -> Result<(), Error> {
    let call = load(tx, run, &settlement.completion.command.target().call_id).await?;
    let records = settlement.materialize(run, &call)?;
    super::super::tool_result::apply(tx, run, call, records).await
}

pub(super) async fn resolve(
    tx: &mut Transaction<'_, Sqlite>,
    run: &Run,
    resolution: &crate::PhaseResolution,
) -> Result<(), Error> {
    let call = load(tx, run, &resolution.resolution.command.target().call_id).await?;
    if let Some(records) = resolution.materialize(run, &call)? {
        super::super::tool_result::apply(tx, run, call, records).await?;
    }
    Ok(())
}
