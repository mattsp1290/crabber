//! Canonical logical tool results, independent of native phase transitions.
use crate::transaction::Transaction;
use crate::{PhaseError as Error, phase::sql};
use crabber_core::{Run, ToolCallRecord};
use sqlx::Sqlite;

pub(super) async fn apply(
    tx: &mut Transaction<'_, Sqlite>,
    run: &Run,
    mut call: ToolCallRecord,
    records: crate::PhaseSettlementRecords,
) -> Result<(), Error> {
    super::records::insert_message(tx, run, &records.message)
        .await
        .map_err(sql::unknown)?;
    super::records::insert_event(tx, run, &records.event)
        .await
        .map_err(sql::unknown)?;
    call.status = records.call_status;
    call.result = Some(records.result);
    super::records::save_call(tx, &call.id, &call)
        .await
        .map_err(sql::unknown)?;
    Ok(())
}
