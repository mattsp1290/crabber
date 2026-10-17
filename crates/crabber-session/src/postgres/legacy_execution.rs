//! Compatibility tool claims retain one proof through the actual native write.
use super::{PostgresExecution, StoreError, db};
use crate::native_enrollment::caller::{Caller, Fresh, LegacyTool};
use crate::public_session::v2::proof::Proof;
use crabber_core::{EventRecord, ToolCallId, ToolCallRecord, ToolCallStatus};
use sqlx::Row;
fn invalid(_: impl std::fmt::Debug) -> StoreError {
    StoreError::Validation("legacy tool admission proof unavailable".into())
}
impl PostgresExecution {
    pub(super) async fn claim_legacy_with_proof(
        &self,
        id: &ToolCallId,
        running: &EventRecord,
        proof: &mut Proof,
    ) -> Result<(), StoreError> {
        proof
            .measure(&(&self.fence, id, running))
            .map_err(invalid)?;
        let (mut tx, run) = self
            .store
            .run_transaction_with_proof(&self.fence.run_id, proof)
            .await?;
        let result = async {

            if run.claim_token != self.fence.claim_token || run.status.is_terminal()
                || run.lease_until <= self.store.clock.now() {
                return Err(StoreError::Conflict);
            }
            if super::admission_execution::load_record_with_proof(&mut tx, &run.id, proof)
                .await.map_err(|error| error.into_domain_or(invalid("admission proof")))?
                .is_some_and(|record| record.state == crate::AdmissionExecutionState::Unstarted) {
                return Err(StoreError::AdmissionRecoveryRequired);
            }
            super::phase::legacy_guard_with_proof(&mut tx, &run, id, proof).await?;
            super::read_proof::charge(&mut tx, sqlx::query_scalar(CALL_COST).bind(&id.0), proof)
                .await.map_err(invalid)?;
            let row = sqlx::query(CALL).bind(&id.0).fetch_optional(&mut *tx)
                .await.map_err(db)?.ok_or(StoreError::NotFound)?;
            let text = row.try_get::<Option<String>, _>("record").map_err(db)?
                .ok_or_else(|| invalid("oversized call"))?;
            let mut call: ToolCallRecord = serde_json::from_str(&text).map_err(invalid)?;
            if call.run_id != run.id || call.status != ToolCallStatus::Pending {
                return Err(StoreError::Conflict);
            }
            let cursor = super::insert_event_captured(&mut tx, &run, running).await?;
            call.status = ToolCallStatus::Running;
            let accounting = super::snapshot::call_record(&call)?;
            sqlx::query("UPDATE tool_calls SET status=$2,data=$3,snapshot_record=$4,snapshot_parts=$5,snapshot_text=$6,snapshot_bytes=$7 WHERE id=$1")
                .bind(&id.0).bind(super::call_status(call.status)).bind(super::json(&call)?)
                .bind(&accounting.record).bind(accounting.parts).bind(accounting.text).bind(accounting.bytes)
                .execute(&mut *tx).await.map_err(db)?;
            super::native_enrollment::record_raw_native(&mut tx, Caller::Raw,
                &Fresh::LegacyTool(LegacyTool { fence: &self.fence, run: &run, id,
                    requested: running, call: &call, cursor }), proof,
                #[cfg(test)] &self.store.native_raw_probe).await.map_err(invalid)?;
            Ok(())
        }.await;
        let committed = match result {
            Ok(()) => tx.commit().await.map_err(db),
            Err(error) => {
                tx.rollback().await.map_err(db)?;
                Err(error)
            }
        };
        #[cfg(test)]
        if committed.is_ok()
            && self
                .store
                .native_raw_probe
                .after_legacy(&self.fence.run_id, id)
        {
            return Err(invalid("lost original legacy claim ACK"));
        }
        committed
    }
}
const CALL_COST: &str =
    "SELECT CAST(octet_length(snapshot_record) AS BIGINT) FROM tool_calls WHERE id=$1";
const CALL: &str = "SELECT CASE WHEN octet_length(snapshot_record)<=33554432 THEN snapshot_record END AS record FROM tool_calls WHERE id=$1 FOR UPDATE";
