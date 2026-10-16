//! Bound and legacy admission share the same transaction-only write helpers.
use super::records::{decode_row, lookup_receipt};
use super::{SqliteStore, db, text};
use crate::public_session::v2::proof::{Proof, ReadError};

use crate::{
    AdmitOutcome, AdmitRequest, ExecutionBindingCapsule, KeyedAdmitOutcome, KeyedAdmitRequest,
    RetainedExecutionBinding, StoreError,
};
use crabber_core::{AdmissionReceipt, Message, RunId, Session, SessionId};
use sqlx::{Row, Sqlite, Transaction};

async fn finish<T>(
    tx: Transaction<'_, Sqlite>,
    result: Result<T, StoreError>,
) -> Result<T, StoreError> {
    match result {
        Ok(value) => {
            tx.commit().await.map_err(db)?;
            Ok(value)
        }
        Err(error) => {
            tx.rollback().await.map_err(db)?;
            Err(error)
        }
    }
}
impl SqliteStore {
    pub(super) async fn admit_keyed_transaction(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        keyed: KeyedAdmitRequest,
    ) -> Result<KeyedAdmitOutcome, StoreError> {
        self.admit_keyed_guarded(tx, keyed, crate::queue::AdmissionGuard::Ordinary)
            .await
    }
    pub(super) async fn admit_keyed_guarded(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        keyed: KeyedAdmitRequest,
        guard: crate::queue::AdmissionGuard<'_>,
    ) -> Result<KeyedAdmitOutcome, StoreError> {
        let digest = keyed.validated_digest()?;
        let request = &keyed.request;
        let session = request
            .session_id
            .as_ref()
            .ok_or(StoreError::AdmissionConflict)?;
        if let Some(row) = sqlx::query("SELECT data FROM sessions WHERE id=$1")
            .bind(&session.0)
            .fetch_optional(&mut **tx)
            .await
            .map_err(db)?
        {
            let existing: Session = decode_row(&row, "data")?;
            existing.ensure_identity(&request.workspace_id, &request.directory)?;
        }
        if let Some(receipt) = lookup_receipt(tx, session, &keyed.options.key).await? {
            if receipt.fingerprint != keyed.options.fingerprint || receipt.semantic_digest != digest
            {
                return Err(StoreError::AdmissionConflict);
            }
            return Ok(KeyedAdmitOutcome::Replayed(receipt));
        }
        if guard.is_ordinary() && sqlx::query_scalar::<_, bool>("SELECT EXISTS(SELECT 1 FROM future_queue WHERE session_id=$1 AND admission_key=$2)").bind(&session.0).bind(keyed.options.key.as_str()).fetch_one(&mut **tx).await.map_err(db)? { return Err(StoreError::AdmissionConflict); }
        let session_id = session.clone();
        let user_message_id = request.user_message.id.clone();
        let semantic_digest_version = if request.input_envelope.is_some() {
            2
        } else {
            1
        };
        let admitted = self
            .admit_transaction_guarded(tx, keyed.request, true, guard)
            .await?;
        let receipt = AdmissionReceipt {
            session_id,
            run_id: admitted.run.id.clone(),
            user_message_id,
            fingerprint: keyed.options.fingerprint,
            semantic_digest_version,
            semantic_digest: digest,
        };
        sqlx::query(
            "INSERT INTO \
                    admission_receipts(session_id,admission_key,run_id,user_message_id,data) \
                    VALUES($1,$2,$3,$4,$5)",
        )
        .bind(&receipt.session_id.0)
        .bind(keyed.options.key.as_str())
        .bind(&receipt.run_id.0)
        .bind(&receipt.user_message_id.0)
        .bind(text(&receipt)?)
        .execute(&mut **tx)
        .await
        .map_err(db)?;
        if let Some(capsule) = keyed.execution {
            let version =
                i32::try_from(capsule.version).map_err(|_| StoreError::AdmissionConflict)?;
            let record = crate::AdmissionExecutionRecord {
                receipt: receipt.clone(),
                key: keyed.options.key.clone(),
                capsule,
                state: crate::AdmissionExecutionState::Unstarted,
            };
            sqlx::query(
                    "INSERT INTO \
                        admission_executions(run_id,session_id,admission_key,capsule_version,start_state,data) \
                        VALUES($1,$2,$3,$4,'Unstarted',$5)",
                )
                .bind(&receipt.run_id.0)
                .bind(&receipt.session_id.0)
                .bind(keyed.options.key.as_str())
                .bind(version)
                .bind(record.encode()?)
                .execute(&mut **tx)
                .await
                .map_err(db)?;
        }
        Ok(KeyedAdmitOutcome::Started {
            receipt,
            admitted: Box::new(admitted),
        })
    }

    pub(super) async fn admit_with_binding(
        &self,
        request: AdmitRequest,
        binding: Option<ExecutionBindingCapsule>,
    ) -> Result<AdmitOutcome, StoreError> {
        #[cfg(test)]
        let is_bound = binding.is_some();
        if let Some(binding) = &binding {
            binding.validate(&request)?;
        }
        let mut tx = self.begin_write().await?;
        let user = request.user_message.clone();
        let result = async {
            let admitted = self.admit_transaction(&mut tx, request, false).await?;
            if let Some(capsule) = binding {
                save(
                    &mut tx,
                    &RetainedExecutionBinding::new(&admitted.run, &user, capsule),
                )
                .await?;
                #[cfg(test)]
                crate::binding_fault::trip(&self.binding_fault, 1)?;
            }
            Ok(admitted)
        }
        .await;
        let result = finish(tx, result).await?;
        #[cfg(test)]
        if is_bound {
            crate::binding_fault::trip(&self.binding_fault, 2)?;
        }
        Ok(result)
    }
    pub(super) async fn admit_keyed_with_binding(
        &self,
        request: KeyedAdmitRequest,
        binding: Option<ExecutionBindingCapsule>,
    ) -> Result<KeyedAdmitOutcome, StoreError> {
        #[cfg(test)]
        let is_bound = binding.is_some();
        if let Some(binding) = &binding {
            binding.validate_keyed(&request)?;
        } else {
            request.validated_digest()?;
        }
        let mut tx = self.begin_write().await?;
        let user = request.request.user_message.clone();
        let result = async {
            let outcome = self.admit_keyed_transaction(&mut tx, request).await?;
            if let Some(capsule) = binding {
                match &outcome {
                    KeyedAdmitOutcome::Started { admitted, .. } => {
                        save(
                            &mut tx,
                            &RetainedExecutionBinding::new(&admitted.run, &user, capsule),
                        )
                        .await?;
                        #[cfg(test)]
                        crate::binding_fault::trip(&self.binding_fault, 1)?;
                    }
                    KeyedAdmitOutcome::Replayed(receipt) => {
                        if read(&mut tx, &receipt.session_id, &receipt.run_id)
                            .await?
                            .is_none_or(|original| original.capsule != capsule)
                        {
                            return Err(StoreError::AdmissionConflict);
                        }
                    }
                }
            }
            Ok(outcome)
        }
        .await;
        let result = finish(tx, result).await?;
        #[cfg(test)]
        if is_bound && matches!(&result, KeyedAdmitOutcome::Started { .. }) {
            crate::binding_fault::trip(&self.binding_fault, 2)?;
        }
        Ok(result)
    }
    pub(super) async fn read_execution_binding(
        &self,
        session: &SessionId,
        run: &RunId,
    ) -> Result<Option<RetainedExecutionBinding>, StoreError> {
        let mut tx = self.readers.begin().await.map_err(db)?;

        let result = read(&mut tx, session, run).await;
        finish(tx, result).await
    }
}
pub(super) async fn save(
    tx: &mut Transaction<'_, Sqlite>,
    binding: &RetainedExecutionBinding,
) -> Result<(), StoreError> {
    sqlx::query(
        "INSERT INTO run_execution_bindings(run_id,capsule_version,data_json) VALUES($1,$2,$3)",
    )
    .bind(&binding.run_id.0)
    .bind(i32::try_from(binding.capsule.version).map_err(|_| StoreError::AdmissionConflict)?)
    .bind(binding.encode()?)
    .execute(&mut **tx)
    .await
    .map_err(db)?;
    Ok(())
}
pub(super) async fn read(
    tx: &mut Transaction<'_, Sqlite>,
    session: &SessionId,
    run: &RunId,
) -> Result<Option<RetainedExecutionBinding>, StoreError> {
    read_with_proof(tx, session, run, &mut Proof::legacy())
        .await
        .map_err(|error| error.into_domain_or(StoreError::AdmissionConflict))
}
pub(super) async fn read_with_proof(
    tx: &mut Transaction<'_, Sqlite>,
    session: &SessionId,
    run: &RunId,
    proof: &mut Proof,
) -> Result<Option<RetainedExecutionBinding>, ReadError<StoreError>> {
    super::read_proof::charge(
        tx,
        sqlx::query_scalar(BINDING_PROOF_COST)
            .bind(&run.0)
            .bind(&session.0),
        proof,
    )
    .await
    .map_err(ReadError::Budget)?;

    // The scope filter precedes private decoding. Load only immutable run fields;
    // a malformed worker token cannot invalidate original binding evidence.
    let row = sqlx::query("SELECT b.capsule_version, CASE WHEN length(CAST(b.data_json AS BLOB)) <= 16777216 THEN b.data_json END AS binding, CASE WHEN length(CAST((json_extract(r.data, '$.config_hash')) AS BLOB)) <= 64 THEN json_extract(r.data, '$.config_hash') END AS config, CASE WHEN length(CAST((json_extract(r.data, '$.plan_fingerprint')) AS BLOB)) <= 64 THEN json_extract(r.data, '$.plan_fingerprint') END AS plan, CASE WHEN length(CAST((json_extract(s.data, '$.workspace_id')) AS BLOB)) <= 512 THEN json_extract(s.data, '$.workspace_id') END AS workspace, CASE WHEN length(CAST((json_extract(s.data, '$.directory')) AS BLOB)) <= 16384 THEN json_extract(s.data, '$.directory') END AS directory FROM run_execution_bindings b JOIN runs r ON r.id=b.run_id JOIN sessions s ON s.id=r.session_id WHERE r.id=$1 AND r.session_id=$2")
        .bind(&run.0).bind(&session.0).fetch_optional(&mut **tx).await.map_err(db)?;
    let Some(row) = row else {
        return Ok(None);
    };
    let version: i32 = row.try_get("capsule_version").map_err(db)?;
    let encoded: Option<String> = row.try_get("binding").map_err(db)?;
    if !matches!(version, 2 | 3) {
        return Err(StoreError::AdmissionConflict.into());
    }
    let binding =
        RetainedExecutionBinding::decode(encoded.as_deref().ok_or(StoreError::AdmissionConflict)?)?;
    if u32::try_from(version).ok() != Some(binding.capsule.version) {
        return Err(StoreError::AdmissionConflict.into());
    }
    let count = binding
        .capsule
        .input_envelope
        .as_ref()
        .map_or(1, |input| input.messages().len());
    let messages = original_messages_with_proof(tx, session, run, count, proof).await?;
    let config: String = row
        .try_get::<Option<String>, _>("config")
        .map_err(db)?
        .ok_or(StoreError::AdmissionConflict)?;
    let plan: String = row
        .try_get::<Option<String>, _>("plan")
        .map_err(db)?
        .ok_or(StoreError::AdmissionConflict)?;
    let workspace: String = row
        .try_get::<Option<String>, _>("workspace")
        .map_err(db)?
        .ok_or(StoreError::AdmissionConflict)?;
    let directory: String = row
        .try_get::<Option<String>, _>("directory")
        .map_err(db)?
        .ok_or(StoreError::AdmissionConflict)?;
    binding.validate_messages(run, session, &config, &plan, &messages)?;
    if binding.capsule.request.workspace_id != workspace
        || binding.capsule.request.directory != directory
    {
        return Err(StoreError::SessionIdentityMismatch.into());
    }
    Ok(Some(binding))
}

/// Read the bounded original prefix in the caller's coherent transaction.
pub(super) async fn original_messages(
    tx: &mut Transaction<'_, Sqlite>,
    session: &SessionId,
    run: &RunId,
    count: usize,
) -> Result<Vec<Message>, StoreError> {
    if count == 0 || count > crabber_core::input::MAX_INPUT_MESSAGES {
        return Err(StoreError::AdmissionConflict);
    }
    let rows = sqlx::query("WITH originals AS (SELECT seq,data AS record FROM messages WHERE session_id=$1 AND run_id=$2 ORDER BY seq LIMIT $3) SELECT CASE WHEN sum(length(CAST(record AS BLOB))) OVER () <= 16777216 THEN record END AS record FROM originals ORDER BY seq")
        .bind(&session.0).bind(&run.0).bind(i64::try_from(count).map_err(|_| StoreError::AdmissionConflict)?)
        .fetch_all(&mut **tx).await.map_err(db)?;
    if rows.len() != count {
        return Err(StoreError::AdmissionConflict);
    }
    rows.iter()
        .map(|row| {
            let encoded = row
                .try_get::<Option<String>, _>("record")
                .map_err(db)?
                .ok_or(StoreError::AdmissionConflict)?;
            super::decode(&encoded)
        })
        .collect()
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod process_tests;

/// Charge the exact original-message prefix in this same transaction before transfer.
pub(super) async fn original_messages_with_proof(
    tx: &mut Transaction<'_, Sqlite>,
    session: &SessionId,
    run: &RunId,
    count: usize,
    proof: &mut Proof,
) -> Result<Vec<Message>, ReadError<StoreError>> {
    if count == 0 || count > crabber_core::input::MAX_INPUT_MESSAGES {
        return Err(StoreError::AdmissionConflict.into());
    }
    super::read_proof::charge(
        tx,
        sqlx::query_scalar(ORIGINAL_PROOF_COST)
            .bind(&session.0)
            .bind(&run.0)
            .bind(i64::try_from(count).map_err(|_| StoreError::AdmissionConflict)?),
        proof,
    )
    .await
    .map_err(ReadError::Budget)?;
    Ok(original_messages(tx, session, run, count).await?)
}
const BINDING_PROOF_COST: &str = r"SELECT CAST(length(CAST(b.data_json AS BLOB))+COALESCE(length(CAST(json_extract(r.data,'$.config_hash') AS BLOB)),0)+COALESCE(length(CAST(json_extract(r.data,'$.plan_fingerprint') AS BLOB)),0)+COALESCE(length(CAST(json_extract(s.data,'$.workspace_id') AS BLOB)),0)+COALESCE(length(CAST(json_extract(s.data,'$.directory') AS BLOB)),0) AS BIGINT) FROM run_execution_bindings b JOIN runs r ON r.id=b.run_id JOIN sessions s ON s.id=r.session_id WHERE r.id=$1 AND r.session_id=$2";
const ORIGINAL_PROOF_COST: &str = r"SELECT CAST(COALESCE(SUM(bytes),0) AS BIGINT) FROM (SELECT length(CAST(data AS BLOB)) AS bytes FROM messages WHERE session_id=$1 AND run_id=$2 ORDER BY seq LIMIT $3) originals";
