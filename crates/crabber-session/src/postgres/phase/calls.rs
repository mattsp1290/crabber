use super::super::{call_status, db, json};
use crate::{PhaseError as Error, phase::sql, sql_snapshot as snapshot};
use crabber_core::{Run, ToolCallId, ToolCallRecord, ToolCallStatus};
use sqlx::{Postgres, Row, Transaction};

pub(super) async fn load(
    tx: &mut Transaction<'_, Postgres>,
    run: &Run,
    id: &ToolCallId,
) -> Result<ToolCallRecord, Error> {
    let row = sqlx::query("SELECT status,CASE WHEN octet_length(snapshot_record)<=4194304 THEN snapshot_record END AS record FROM tool_calls WHERE run_id=$1 AND id=$2 FOR UPDATE").bind(&run.id.0).bind(&id.0).fetch_optional(&mut **tx).await.map_err(db).map_err(sql::unknown)?.ok_or(Error::NotFound)?;
    let call: ToolCallRecord = serde_json::from_str(
        &row.get::<Option<String>, _>("record")
            .ok_or(Error::UnknownStoreOutcome)?,
    )
    .map_err(|_| Error::UnknownStoreOutcome)?;
    if call.run_id != run.id
        || call.id != *id
        || row.get::<String, _>("status") != call_status(call.status)
    {
        return Err(Error::UnknownStoreOutcome);
    }
    Ok(call)
}
pub(super) async fn begin(
    tx: &mut Transaction<'_, Postgres>,
    run: &Run,
    id: &ToolCallId,
) -> Result<(), Error> {
    let mut call = load(tx, run, id).await?;
    if call.status != ToolCallStatus::Pending || call.result.is_some() {
        return Err(Error::InvalidTransition);
    }
    call.status = ToolCallStatus::Running;
    let accounting = snapshot::call_record(&call).map_err(sql::unknown)?;
    sqlx::query("UPDATE tool_calls SET status=$2,data=$3,snapshot_record=$4,snapshot_parts=$5,snapshot_text=$6,snapshot_bytes=$7 WHERE id=$1")
        .bind(&id.0).bind(call_status(call.status)).bind(json(&call).map_err(sql::unknown)?)
        .bind(&accounting.record).bind(accounting.parts).bind(accounting.text).bind(accounting.bytes)
        .execute(&mut **tx).await.map_err(db).map_err(sql::unknown)?;
    Ok(())
}

pub(super) async fn settle(
    tx: &mut Transaction<'_, Postgres>,
    run: &Run,
    settlement: &crate::PhaseSettlement,
) -> Result<(), Error> {
    let mut call = load(tx, run, &settlement.completion.command.target().call_id).await?;
    let records = settlement.materialize(run, &call)?;
    super::super::insert_message(tx, run, &records.message)
        .await
        .map_err(sql::unknown)?;
    super::super::insert_event(tx, run, &records.event)
        .await
        .map_err(sql::unknown)?;
    call.status = records.call_status;
    call.result = Some(records.result);
    let accounting = snapshot::call_record(&call).map_err(sql::unknown)?;
    sqlx::query("UPDATE tool_calls SET status=$2,data=$3,snapshot_record=$4,snapshot_parts=$5,snapshot_text=$6,snapshot_bytes=$7 WHERE id=$1")
        .bind(&call.id.0).bind(call_status(call.status)).bind(json(&call).map_err(sql::unknown)?)
        .bind(&accounting.record).bind(accounting.parts).bind(accounting.text).bind(accounting.bytes)
        .execute(&mut **tx).await.map_err(db).map_err(sql::unknown)?;
    Ok(())
}
