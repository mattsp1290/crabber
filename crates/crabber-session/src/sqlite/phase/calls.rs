use super::super::{db, records};
use crate::{PhaseError as Error, phase::sql};
use crabber_core::{Run, ToolCallId, ToolCallRecord, ToolCallStatus};
use sqlx::{Row, Sqlite, Transaction};

pub(super) async fn load(
    tx: &mut Transaction<'_, Sqlite>,
    run: &Run,
    id: &ToolCallId,
) -> Result<ToolCallRecord, Error> {
    let row = sqlx::query("SELECT status,CASE WHEN length(CAST(data AS BLOB))<=4194304 THEN data END AS record FROM tool_calls WHERE run_id=$1 AND id=$2").bind(&run.id.0).bind(&id.0).fetch_optional(&mut **tx).await.map_err(db).map_err(sql::unknown)?.ok_or(Error::NotFound)?;
    let call: ToolCallRecord = serde_json::from_str(
        &row.get::<Option<String>, _>("record")
            .ok_or(Error::UnknownStoreOutcome)?,
    )
    .map_err(|_| Error::UnknownStoreOutcome)?;
    if call.run_id != run.id
        || call.id != *id
        || row.get::<String, _>("status") != records::call_status(call.status)
    {
        return Err(Error::UnknownStoreOutcome);
    }
    Ok(call)
}
pub(super) async fn begin(
    tx: &mut Transaction<'_, Sqlite>,
    run: &Run,
    id: &ToolCallId,
) -> Result<(), Error> {
    let mut call = load(tx, run, id).await?;
    if call.status != ToolCallStatus::Pending || call.result.is_some() {
        return Err(Error::InvalidTransition);
    }
    call.status = ToolCallStatus::Running;
    records::save_call(tx, id, &call)
        .await
        .map_err(sql::unknown)
}

pub(super) async fn settle(
    tx: &mut Transaction<'_, Sqlite>,
    run: &Run,
    settlement: &crate::PhaseSettlement,
) -> Result<(), Error> {
    let mut call = load(tx, run, &settlement.completion.command.target().call_id).await?;
    let records = settlement.materialize(run, &call)?;
    super::super::records::insert_message(tx, run, &records.message)
        .await
        .map_err(sql::unknown)?;
    super::super::records::insert_event(tx, run, &records.event)
        .await
        .map_err(sql::unknown)?;
    call.status = records.call_status;
    call.result = Some(records.result);
    super::super::records::save_call(tx, &call.id, &call)
        .await
        .map_err(sql::unknown)?;
    Ok(())
}
