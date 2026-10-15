//! Canonical logical tool results, independent of native phase transitions.
use super::{call_status, db, json};
use crate::sql_snapshot as snapshot;
use crate::{PhaseError as Error, phase::sql};
use crabber_core::{Run, ToolCallRecord};
use sqlx::{Postgres, Transaction};

pub(super) async fn apply(
    tx: &mut Transaction<'_, Postgres>,
    run: &Run,
    mut call: ToolCallRecord,
    records: crate::PhaseSettlementRecords,
) -> Result<(), Error> {
    super::insert_message(tx, run, &records.message)
        .await
        .map_err(sql::unknown)?;
    super::insert_event(tx, run, &records.event)
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
