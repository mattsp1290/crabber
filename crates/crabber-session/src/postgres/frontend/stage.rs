use super::{
    Error, FrontendOriginRecord, FrontendStagingReceipt, PostgresExecution, finish, read, unknown,
};
use crate::postgres::transaction::Transaction;
use crate::{AttemptAuthority, AttemptCleanup, AttemptState, FrontendCallStaging};
use crabber_core::{EventCursor, Run};
use sqlx::Postgres;
impl PostgresExecution {
    pub(in super::super) async fn stage_frontend(
        &self,
        request: FrontendCallStaging,
        authority: AttemptAuthority,
    ) -> Result<FrontendStagingReceipt, Error> {
        if request.run_id != self.fence.run_id {
            return Err(Error::NotFound);
        }
        let digest = request.fingerprint()?;
        let fault = super::super::tool_continuation::fault(&self.store);
        let mut tx = crate::postgres::transaction::begin(&self.store.pool)
            .await
            .map_err(unknown)?;
        let result = async {
            super::super::lock_session(&mut tx, &request.session_id).await.map_err(unknown)?;
            super::super::credential_access::check_origin(&mut tx, &request.session_id, &request.run_id,
                &mut crate::public_session::v2::proof::Proof::bounded(32 << 20))
                .await.map_err(|_| Error::UnknownStoreOutcome)?;
            if let Some(saved) = read::receipt(&mut tx, &request.session_id, &request.id).await? {
                if saved.run_id != request.run_id || saved.semantic_digest != digest { return Err(Error::SemanticConflict); }
                return Ok((saved, false));
            }
            let run = read::run(&mut tx, &request.session_id, &request.run_id, true).await?;
            if super::super::tool_continuation::revision(&mut tx, &run.session_id).await? != request.expected_revision { return Err(Error::RevisionConflict); }
            let now = self.store.clock.now();
            crate::attempt::verify_run(&run, &self.fence, now).map_err(|_|Error::StaleOwner)?;
            verify_parent(&mut tx, &run, &request, &authority, now).await?;
            let exists: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM tool_calls WHERE id=$1) OR EXISTS(SELECT 1 FROM frontend_tool_origins WHERE call_id=$1) OR EXISTS(SELECT 1 FROM tool_phases WHERE call_id=$1) OR EXISTS(SELECT 1 FROM imported_tool_calls WHERE id=$1)")
                .bind(&request.call.id.0).fetch_one(&mut *tx).await.map_err(unknown)?;
            if exists { return Err(Error::SemanticConflict); }
            read::binding(&mut tx, &run, &request).await?;
            insert_call(&mut tx, &request).await?;
            let event = request.materialize()?;
            let seq: i64 = sqlx::query_scalar("INSERT INTO events(session_id,run_id,data) VALUES($1,$2,$3) RETURNING seq")
                .bind(&run.session_id.0).bind(&run.id.0).bind(crate::public_state::encode_event(&event).map_err(unknown)?)
                .fetch_one(&mut *tx).await.map_err(unknown)?;
            let saved = crate::frontend_continuation::staging_receipt(&request, EventCursor(u64::try_from(seq).map_err(unknown)?))?;
            let origin = FrontendOriginRecord { request: request.clone(), receipt: saved.clone() };
            sqlx::query("INSERT INTO frontend_tool_origins(call_id,session_id,run_id,format_version,data_json) VALUES($1,$2,$3,1,$4)")
                .bind(&request.call.id.0).bind(&run.session_id.0).bind(&run.id.0).bind(serde_json::to_string(&origin).map_err(unknown)?)
                .execute(&mut *tx).await.map_err(unknown)?;
            super::super::bump_public_revision(&mut tx, &run.session_id).await.map_err(unknown)?;
            sqlx::query("INSERT INTO operation_receipts(session_id,operation_id,kind,semantic_digest,event_seq,data_json) VALUES($1,$2,'frontend_staging',$3,$4,$5)")
                .bind(&run.session_id.0).bind(&request.id.0).bind(digest.as_str()).bind(seq).bind(serde_json::to_string(&saved).map_err(unknown)?)
                .execute(&mut *tx).await.map_err(unknown)?;
            super::super::tool_continuation::before(fault)?;
            Ok((saved, true))
        }.await;
        super::super::tool_continuation::after(finish(tx, result).await, fault)
    }
}
async fn verify_parent(
    tx: &mut Transaction<'_, Postgres>,
    run: &Run,
    request: &FrontendCallStaging,
    authority: &AttemptAuthority,
    now: time::OffsetDateTime,
) -> Result<(), Error> {
    let (parent, _) = super::super::attempt::load_host_attempt(tx, &run.id, None, true)
        .await
        .map_err(unknown)?
        .ok_or(Error::NotFound)?;
    parent.validate().map_err(unknown)?;
    if parent.snapshot.target != request.parent
        || parent.snapshot.session_id != run.session_id
        || parent.snapshot.state != AttemptState::Started
        || parent.snapshot.cleanup != AttemptCleanup::Pending
        || parent.run_token != run.claim_token
        || parent.lease_until <= now
        || !authority.matches_store(&parent.snapshot, &parent.token, &parent.run_token)
    {
        return Err(Error::StaleOwner);
    }
    super::super::native_parent::verify_attempt(tx, run, &parent)
        .await
        .map_err(unknown)?;
    Ok(())
}
async fn insert_call(
    tx: &mut Transaction<'_, Postgres>,
    request: &FrontendCallStaging,
) -> Result<(), Error> {
    let accounting = crate::sql_snapshot::call_record(&request.call).map_err(unknown)?;
    sqlx::query("INSERT INTO tool_calls(id,run_id,status,data,snapshot_record,snapshot_parts,snapshot_text,snapshot_bytes) VALUES($1,$2,'pending',$3,$4,$5,$6,$7)")
        .bind(&request.call.id.0).bind(&request.run_id.0).bind(super::super::json(&request.call).map_err(unknown)?)
        .bind(accounting.record).bind(accounting.parts).bind(accounting.text).bind(accounting.bytes)
        .execute(&mut **tx).await.map_err(unknown)?;
    Ok(())
}
