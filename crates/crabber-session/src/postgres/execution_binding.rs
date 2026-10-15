//! Bound and legacy admission share the same transaction-only write helpers.
use super::{PostgresStore, db};
use super::{decode, json, lookup_receipt};

use crate::{
    AdmitOutcome, AdmitRequest, ExecutionBindingCapsule, KeyedAdmitOutcome, KeyedAdmitRequest,
    RetainedExecutionBinding, StoreError,
};
use crabber_core::{AdmissionReceipt, Message, RunId, Session, SessionId};
use sqlx::{Postgres, Row, Transaction};

async fn finish<T>(
    tx: Transaction<'_, Postgres>,
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
impl PostgresStore {
    pub(super) async fn admit_keyed_transaction(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        keyed: KeyedAdmitRequest,
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
            let existing: Session = decode(row.get("data"))?;
            existing.ensure_identity(&request.workspace_id, &request.directory)?;
        }
        if let Some(receipt) = lookup_receipt(tx, session, &keyed.options.key).await? {
            if receipt.fingerprint != keyed.options.fingerprint || receipt.semantic_digest != digest
            {
                return Err(StoreError::AdmissionConflict);
            }
            return Ok(KeyedAdmitOutcome::Replayed(receipt));
        }
        let session_id = session.clone();
        let user_message_id = request.user_message.id.clone();
        let semantic_digest_version = if request.input_envelope.is_some() {
            2
        } else {
            1
        };
        let admitted = self.admit_transaction(tx, keyed.request, true).await?;
        let receipt = AdmissionReceipt {
            session_id,
            run_id: admitted.run.id.clone(),
            user_message_id,
            fingerprint: keyed.options.fingerprint,
            semantic_digest_version,
            semantic_digest: digest,
        };
        sqlx::query("INSERT INTO admission_receipts(session_id,admission_key,run_id,user_message_id,data) VALUES($1,$2,$3,$4,$5)")
            .bind(&receipt.session_id.0).bind(keyed.options.key.as_str()).bind(&receipt.run_id.0).bind(&receipt.user_message_id.0).bind(json(&receipt)?)
            .execute(&mut **tx).await.map_err(db)?;
        if let Some(capsule) = keyed.execution {
            let record = crate::AdmissionExecutionRecord {
                receipt: receipt.clone(),
                key: keyed.options.key.clone(),
                capsule,
                state: crate::AdmissionExecutionState::Unstarted,
            };
            if record.capsule.version == 2 {
                sqlx::query("INSERT INTO admission_executions(run_id,session_id,admission_key,capsule_version,start_state,data,data_json) VALUES($1,$2,$3,2,'Unstarted','{}'::jsonb,$4)")
                    .bind(&receipt.run_id.0).bind(&receipt.session_id.0).bind(keyed.options.key.as_str())
                    .bind(record.encode()?).execute(&mut **tx).await.map_err(db)?;
            } else {
                sqlx::query("INSERT INTO admission_executions(run_id,session_id,admission_key,capsule_version,start_state,data) VALUES($1,$2,$3,1,'Unstarted',$4)")
                    .bind(&receipt.run_id.0).bind(&receipt.session_id.0).bind(keyed.options.key.as_str())
                    .bind(json(&record)?).execute(&mut **tx).await.map_err(db)?;
            }
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
        let mut tx = self
            .session_transaction(
                request
                    .session_id
                    .as_ref()
                    .unwrap_or(&request.user_message.session_id),
            )
            .await?;
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
        let session = request.request.session_id.as_ref().ok_or_else(|| {
            StoreError::Validation("keyed admission requires a stable session ID".into())
        })?;
        let mut tx = self.session_transaction(session).await?;
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
        let mut tx = self.pool.begin().await.map_err(db)?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        let result = read(&mut tx, session, run).await;
        finish(tx, result).await
    }
}
async fn save(
    tx: &mut Transaction<'_, Postgres>,
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
async fn read(
    tx: &mut Transaction<'_, Postgres>,
    session: &SessionId,
    run: &RunId,
) -> Result<Option<RetainedExecutionBinding>, StoreError> {
    // The scope filter precedes private decoding. Load only immutable run fields;
    // a malformed worker token cannot invalidate original binding evidence.
    let row = sqlx::query("SELECT b.capsule_version, CASE WHEN octet_length(b.data_json) <= 16777216 THEN b.data_json END AS binding, CASE WHEN octet_length((r.data->>'config_hash')) <= 64 THEN r.data->>'config_hash' END AS config, CASE WHEN octet_length((r.data->>'plan_fingerprint')) <= 64 THEN r.data->>'plan_fingerprint' END AS plan, CASE WHEN octet_length((s.data->>'workspace_id')) <= 512 THEN s.data->>'workspace_id' END AS workspace, CASE WHEN octet_length((s.data->>'directory')) <= 16384 THEN s.data->>'directory' END AS directory FROM run_execution_bindings b JOIN runs r ON r.id=b.run_id JOIN sessions s ON s.id=r.session_id WHERE r.id=$1 AND r.session_id=$2")
        .bind(&run.0).bind(&session.0).fetch_optional(&mut **tx).await.map_err(db)?;
    let Some(row) = row else {
        return Ok(None);
    };
    let version: i32 = row.try_get("capsule_version").map_err(db)?;
    let encoded: Option<String> = row.try_get("binding").map_err(db)?;
    if !matches!(version, 2 | 3) {
        return Err(StoreError::AdmissionConflict);
    }
    let binding =
        RetainedExecutionBinding::decode(encoded.as_deref().ok_or(StoreError::AdmissionConflict)?)?;
    if u32::try_from(version).ok() != Some(binding.capsule.version) {
        return Err(StoreError::AdmissionConflict);
    }
    let count = binding
        .capsule
        .input_envelope
        .as_ref()
        .map_or(1, |input| input.messages().len());
    let messages = original_messages(tx, session, run, count).await?;
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
        return Err(StoreError::SessionIdentityMismatch);
    }
    Ok(Some(binding))
}

/// Read the bounded original prefix in the caller's coherent transaction.
pub(super) async fn original_messages(
    tx: &mut Transaction<'_, Postgres>,
    session: &SessionId,
    run: &RunId,
    count: usize,
) -> Result<Vec<Message>, StoreError> {
    if count == 0 || count > crabber_core::input::MAX_INPUT_MESSAGES {
        return Err(StoreError::AdmissionConflict);
    }
    let rows = sqlx::query("WITH originals AS (SELECT seq,snapshot_record AS record FROM messages WHERE session_id=$1 AND run_id=$2 ORDER BY seq LIMIT $3) SELECT CASE WHEN sum(octet_length(record)) OVER () <= 16777216 THEN record END AS record FROM originals ORDER BY seq")
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
            super::decode_record(&encoded)
        })
        .collect()
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod process_tests;
