//! Fork transactions copy only bounded closed presentation history.
use super::PostgresStore;
use crate::fork::{
    Audit,
    history::{Decision, ImportedHistory},
};
use crate::{ForkBoundary, ForkError as Error, ForkOutcome, ForkReceipt, ForkSession};
use crabber_core::{ControlId, Session, SessionId, SessionRevision};
use sqlx::{Postgres, Row, Transaction};
mod source;
fn unknown(_: impl std::fmt::Debug) -> Error {
    Error::UnknownStoreOutcome
}
async fn finish<T>(tx: Transaction<'_, Postgres>, result: Result<T, Error>) -> Result<T, Error> {
    match result {
        Ok(value) => {
            tx.commit().await.map_err(unknown)?;
            Ok(value)
        }
        Err(error) => {
            tx.rollback().await.map_err(unknown)?;
            Err(error)
        }
    }
}
async fn receipt(
    tx: &mut Transaction<'_, Postgres>,
    target: &SessionId,
    id: &ControlId,
) -> Result<Option<ForkReceipt>, Error> {
    let Some(row) = sqlx::query(RECEIPT)
        .bind(&target.0)
        .bind(&id.0)
        .fetch_optional(&mut **tx)
        .await
        .map_err(unknown)?
    else {
        return Ok(None);
    };
    if row.try_get::<String, _>("kind").map_err(unknown)? != "session_fork" {
        return Err(Error::SemanticConflict);
    }
    let saved: ForkReceipt = serde_json::from_str(
        &row.try_get::<Option<String>, _>("receipt")
            .map_err(unknown)?
            .ok_or(Error::UnknownStoreOutcome)?,
    )
    .map_err(unknown)?;
    let audit: Audit = serde_json::from_str(
        &row.try_get::<Option<String>, _>("audit")
            .map_err(unknown)?
            .ok_or(Error::UnknownStoreOutcome)?,
    )
    .map_err(unknown)?;
    audit.validate()?;
    if row
        .try_get::<Option<i64>, _>("event_seq")
        .map_err(unknown)?
        .is_some()
        || saved.operation_id != *id
        || saved.target.session_id != *target
        || saved != audit.receipt
        || saved.semantic_digest.as_str()
            != row
                .try_get::<String, _>("semantic_digest")
                .map_err(unknown)?
    {
        return Err(Error::UnknownStoreOutcome);
    }
    Ok(Some(saved))
}
impl PostgresStore {
    pub(super) async fn fork_receipt(
        &self,
        target: &SessionId,
        id: &ControlId,
    ) -> Result<Option<ForkReceipt>, Error> {
        crate::management::validate_scope(target, id).map_err(|_| Error::InvalidRequest)?;
        let mut tx = self.pool.begin().await.map_err(unknown)?;
        let result = receipt(&mut tx, target, id).await;
        finish(tx, result).await
    }
    pub(super) async fn fork(&self, request: ForkSession) -> Result<ForkOutcome, Error> {
        let digest = request.fingerprint()?;
        let mut tx = self.pool.begin().await.map_err(unknown)?;
        let result = async {
            let mut sessions=[&request.source_session_id,&request.target_session_id]; sessions.sort();
            for session in sessions { super::lock_session_scope(&mut tx,session).await.map_err(unknown)?; }
            super::deletion::ensure_live(&mut tx,&request.target_session_id).await.map_err(crate::deletion::fork_error)?;
            if let Some(saved) = receipt(&mut tx,&request.target_session_id,&request.id).await? {
                if saved.semantic_digest != digest { return Err(Error::SemanticConflict) }
                return Ok(ForkOutcome::Replayed(saved))
            }
            super::deletion::ensure_live(&mut tx,&request.source_session_id).await.map_err(crate::deletion::fork_error)?;
            let (source,_,revision) = super::management::current(&mut tx,&request.source_session_id).await.map_err(unknown)?.ok_or(Error::NotFound)?;
            if revision != request.expected_source_revision { return Err(Error::RevisionConflict) }
            if source.workspace_id != request.workspace_id || source.directory != request.directory { return Err(Error::SemanticConflict) }
            let used: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM sessions WHERE id=$1) OR EXISTS(SELECT 1 FROM session_owner_contexts WHERE session_id=$1) OR EXISTS(SELECT 1 FROM session_owner_current WHERE session_id=$1) OR EXISTS(SELECT 1 FROM session_owner_audits WHERE session_id=$1)").bind(&request.target_session_id.0).fetch_one(&mut *tx).await.map_err(unknown)?;
            if used { return Err(Error::TargetExists) }
            match source::prepare(&mut tx,&request).await {
                Err(source::Failure::Limited)=>Ok(ForkOutcome::ProofLimited),
                Err(source::Failure::RawLimited(limit)) | Ok(Decision::Limited(limit))=>Ok(ForkOutcome::Limited(limit)),
                Err(source::Failure::Error(error))=>Err(error),
                Ok(Decision::Unsafe(nearest_earlier)) => Ok(ForkOutcome::UnsafeBoundary { nearest_earlier }),
                Ok(Decision::Ready(history)) => publish(&mut tx,request,digest,history,self.clock.now()).await,
            }
        }.await;
        finish(tx, result).await
    }
}
async fn publish(
    tx: &mut Transaction<'_, Postgres>,
    request: ForkSession,
    digest: crabber_core::InputFingerprint,
    history: ImportedHistory,
    now: time::OffsetDateTime,
) -> Result<ForkOutcome, Error> {
    if crate::snapshot::encoded_bytes(&history, 32 * 1024 * 1024).is_none() {
        return Ok(ForkOutcome::Limited(crate::SnapshotLimit::EncodedBytes));
    }
    let session = Session {
        id: request.target_session_id.clone(),
        workspace_id: request.workspace_id.clone(),
        directory: request.directory.clone(),
        title: request.title.clone(),
        created_at: now,
        updated_at: now,
    };
    let metadata = crate::management::Metadata {
        archived: false,
        defaults: request.defaults.clone(),
    };
    let revision = SessionRevision::INITIAL.next().map_err(unknown)?;
    let saved = ForkReceipt {
        operation_id: request.id.clone(),
        semantic_digest: digest,
        source_session_id: request.source_session_id.clone(),
        source_revision: request.expected_source_revision,
        boundary: request.boundary.clone(),
        target: crate::management::summary(&session, metadata.clone(), revision),
        messages: history.messages.len(),
        imported_calls: history.calls.len(),
        history_digest: history.digest()?,
    };
    let audit = Audit {
        request,
        receipt: saved.clone(),
    };
    audit.validate()?;
    sqlx::query("INSERT INTO sessions(id,data,public_revision) VALUES($1,$2,$3)")
        .bind(&session.id.0)
        .bind(super::json(&session).map_err(unknown)?)
        .bind(i64::try_from(revision.get()).map_err(unknown)?)
        .execute(&mut **tx)
        .await
        .map_err(unknown)?;
    sqlx::query("INSERT INTO session_management(session_id,workspace_id,archived,format_version,data_json) VALUES($1,$2,$3,1,$4)").bind(&session.id.0).bind(&session.workspace_id).bind(false).bind(serde_json::to_string(&metadata).map_err(unknown)?).execute(&mut **tx).await.map_err(unknown)?;
    for message in &history.messages {
        insert_message(tx, message).await?;
    }
    for (index, call) in history.calls.iter().enumerate() {
        let encoded = serde_json::to_string(call).map_err(unknown)?;
        if encoded.len() > 8 * 1024 * 1024 {
            return Err(Error::UnknownStoreOutcome);
        }
        let exists: bool =
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM tool_calls WHERE id=$1)")
                .bind(&call.id.0)
                .fetch_one(&mut **tx)
                .await
                .map_err(unknown)?;
        if exists {
            return Err(Error::UnknownStoreOutcome);
        }
        let text = call.result.content.iter().fold(0usize, |n, block| {
            n.saturating_add(crate::snapshot::text_bytes(block))
        });
        sqlx::query("INSERT INTO imported_tool_calls(id,session_id,ordinal,data_json,snapshot_parts,snapshot_text,snapshot_bytes) VALUES($1,$2,$3,$4,0,$5,$6)").bind(&call.id.0).bind(&session.id.0).bind(i64::try_from(index+1).map_err(unknown)?).bind(&encoded).bind(i64::try_from(text).map_err(unknown)?).bind(i64::try_from(encoded.len()).map_err(unknown)?).execute(&mut **tx).await.map_err(unknown)?;
    }
    for (index, origin) in history.provenance.iter().enumerate() {
        let encoded = serde_json::to_string(origin).map_err(unknown)?;
        sqlx::query("INSERT INTO imported_message_provenance(message_id,session_id,ordinal,data_json,snapshot_bytes) VALUES($1,$2,$3,$4,$5)").bind(&origin.message_id.0).bind(&session.id.0).bind(i64::try_from(index+1).map_err(unknown)?).bind(&encoded).bind(i64::try_from(encoded.len()).map_err(unknown)?).execute(&mut **tx).await.map_err(unknown)?;
    }
    sqlx::query(
        "INSERT INTO session_fork_audit(session_id,operation_id,data_json) VALUES($1,$2,$3)",
    )
    .bind(&session.id.0)
    .bind(&saved.operation_id.0)
    .bind(serde_json::to_string(&audit).map_err(unknown)?)
    .execute(&mut **tx)
    .await
    .map_err(unknown)?;
    sqlx::query("INSERT INTO operation_receipts(session_id,operation_id,kind,semantic_digest,event_seq,data_json) VALUES($1,$2,'session_fork',$3,NULL,$4)").bind(&session.id.0).bind(&saved.operation_id.0).bind(saved.semantic_digest.as_str()).bind(serde_json::to_string(&saved).map_err(unknown)?).execute(&mut **tx).await.map_err(unknown)?;
    Ok(ForkOutcome::Applied(saved))
}
async fn insert_message(
    tx: &mut Transaction<'_, Postgres>,
    message: &crabber_core::Message,
) -> Result<(), Error> {
    let record = crate::sql_snapshot::message_record(message).map_err(unknown)?;
    sqlx::query("INSERT INTO messages(id,session_id,run_id,data,snapshot_record,snapshot_parts,snapshot_text,snapshot_bytes) VALUES($1,$2,NULL,$3,$7,$4,$5,$6)").bind(&message.id.0).bind(&message.session_id.0).bind(super::json(message).map_err(unknown)?).bind(record.parts).bind(record.text).bind(record.bytes).bind(&record.record).execute(&mut **tx).await.map_err(unknown)?;
    for part in &message.parts {
        super::insert_part(tx, part).await.map_err(unknown)?;
    }
    Ok(())
}

const RECEIPT: &str = "SELECT r.kind,r.semantic_digest,r.event_seq,CASE WHEN octet_length(r.data_json)<=32768 THEN r.data_json END AS receipt,CASE WHEN octet_length(a.data_json)<=32768 THEN a.data_json END AS audit FROM operation_receipts r LEFT JOIN session_fork_audit a ON a.session_id=r.session_id AND a.operation_id=r.operation_id WHERE r.session_id=$1 AND r.operation_id=$2";

#[cfg(test)]
mod tests;
