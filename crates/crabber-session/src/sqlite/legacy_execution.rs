//! Compatibility tool claims retain one proof through the actual native write.
use super::{SqliteExecution, StoreError, db};
use crate::native_enrollment::caller::{Caller, Fresh, LegacyTool};
use crate::public_session::v2::proof::Proof;
use crabber_core::{EventRecord, ToolCallId, ToolCallRecord, ToolCallStatus};
use sqlx::Row;
fn invalid(_: impl std::fmt::Debug) -> StoreError {
    StoreError::Validation("legacy tool admission proof unavailable".into())
}
impl SqliteExecution {
    pub(super) async fn claim_legacy_with_proof(
        &self,
        id: &ToolCallId,
        running: &EventRecord,
        proof: &mut Proof,
    ) -> Result<(), StoreError> {
        proof
            .measure(&(&self.fence, id, running))
            .map_err(invalid)?;
        let mut tx = self.store.begin_write().await?;
        let result = async {
            let run =
                super::records::load_run_with_proof(&mut tx, &self.fence.run_id, proof).await?;
            if run.claim_token != self.fence.claim_token
                || run.status.is_terminal()
                || run.lease_until <= self.store.clock.now()
            {
                return Err(StoreError::Conflict);
            }
            if super::admission_execution::load_record_with_proof(&mut tx, &run.id, proof)
                .await
                .map_err(|error| error.into_domain_or(invalid("admission proof")))?
                .is_some_and(|record| record.state == crate::AdmissionExecutionState::Unstarted)
            {
                return Err(StoreError::AdmissionRecoveryRequired);
            }
            super::phase::legacy_guard_with_proof(&mut tx, &run, id, proof).await?;
            super::read_proof::charge(
                &mut tx,
                sqlx::query_scalar(CALL_COST).bind(&id.0).bind(&run.id.0),
                proof,
            )
            .await
            .map_err(invalid)?;
            let row = sqlx::query(CALL)
                .bind(&id.0)
                .bind(&run.id.0)
                .fetch_optional(&mut *tx)
                .await
                .map_err(db)?
                .ok_or(StoreError::NotFound)?;
            let text = row
                .try_get::<Option<String>, _>("record")
                .map_err(db)?
                .ok_or_else(|| invalid("oversized call"))?;
            let mut call: ToolCallRecord = serde_json::from_str(&text).map_err(invalid)?;
            if call.run_id != run.id || call.status != ToolCallStatus::Pending {
                return Err(StoreError::Conflict);
            }
            let cursor = super::records::insert_event_captured(&mut tx, &run, running).await?;
            call.status = ToolCallStatus::Running;
            super::records::save_call(&mut tx, id, &call).await?;
            super::native_enrollment::record_raw_native(
                &mut tx,
                Caller::Raw,
                &Fresh::LegacyTool(LegacyTool {
                    fence: &self.fence,
                    run: &run,
                    id,
                    requested: running,
                    call: &call,
                    cursor,
                }),
                proof,
                #[cfg(test)]
                &self.store.native_raw_probe,
            )
            .await
            .map_err(invalid)?;
            Ok(())
        }
        .await;
        let committed = super::transactions::finish(tx, result, std::convert::identity).await;
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
    "SELECT CAST(length(CAST(data AS BLOB)) AS BIGINT) FROM tool_calls WHERE id=$1 AND run_id=$2";
const CALL: &str = "SELECT CASE WHEN length(CAST(data AS BLOB))<=33554432 THEN data END AS record FROM tool_calls WHERE id=$1 AND run_id=$2";
