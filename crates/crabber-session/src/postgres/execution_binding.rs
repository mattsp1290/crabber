//! Bound and legacy admission share the same transaction-only write helpers.
use super::json;
use super::{PostgresStore, db};
use crate::public_session::v2::proof::{Proof, ReadError};

use crate::{
    AdmitOutcome, AdmitRequest, ExecutionBindingCapsule, KeyedAdmitOutcome, KeyedAdmitRequest,
    RetainedExecutionBinding, StoreError,
};
use crabber_core::{AdmissionReceipt, Message, RunId, SessionId};
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
    pub(super) async fn finish_initial(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        input: crate::native_enrollment::caller::initial::Input<'_>,
        captured: crate::native_enrollment::caller::initial::CapturedAdmission,
        proof: &mut Proof,
    ) -> Result<AdmitOutcome, StoreError> {
        super::native_enrollment::record_raw_native(
            tx,
            crate::native_enrollment::caller::Caller::Raw,
            &crate::native_enrollment::caller::Fresh::Initial(
                crate::native_enrollment::caller::initial::Initial {
                    input,
                    result: &captured.admitted,
                    first: &captured.first,
                },
            ),
            proof,
            #[cfg(test)]
            &self.native_raw_probe,
        )
        .await
        .map_err(|_| StoreError::Conflict)?;
        Ok(captured.admitted)
    }
    pub(super) async fn admit_keyed_transaction(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        keyed: KeyedAdmitRequest,
    ) -> Result<KeyedAdmitOutcome, StoreError> {
        self.admit_keyed_guarded(tx, keyed, crate::queue::AdmissionGuard::Ordinary)
            .await
    }
    pub(super) async fn admit_keyed_guarded(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        keyed: KeyedAdmitRequest,
        guard: crate::queue::AdmissionGuard<'_>,
    ) -> Result<KeyedAdmitOutcome, StoreError> {
        self.admit_keyed_guarded_with_proof(
            tx,
            keyed,
            guard,
            &mut crate::public_session::v2::proof::Proof::bounded(32 << 20),
        )
        .await
    }
    pub(super) async fn admit_keyed_guarded_with_proof(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        keyed: KeyedAdmitRequest,
        guard: crate::queue::AdmissionGuard<'_>,
        read_proof: &mut crate::public_session::v2::proof::Proof,
    ) -> Result<KeyedAdmitOutcome, StoreError> {
        let captured = self
            .admit_keyed_captured(
                tx,
                &keyed,
                guard,
                crate::native_enrollment::caller::Caller::Raw,
                read_proof,
            )
            .await?;
        match captured {
            crate::native_enrollment::caller::initial::CapturedKeyedAdmission::Replayed(
                receipt,
            ) => Ok(KeyedAdmitOutcome::Replayed(receipt)),
            crate::native_enrollment::caller::initial::CapturedKeyedAdmission::Started {
                receipt,
                admitted,
            } => {
                let admitted = self
                    .finish_initial(
                        tx,
                        crate::native_enrollment::caller::initial::Input::Keyed {
                            request: &keyed,
                            receipt: &receipt,
                        },
                        *admitted,
                        read_proof,
                    )
                    .await?;
                Ok(KeyedAdmitOutcome::Started {
                    receipt,
                    admitted: Box::new(admitted),
                })
            }
        }
    }
    pub(super) async fn admit_keyed_captured(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        keyed: &KeyedAdmitRequest,
        guard: crate::queue::AdmissionGuard<'_>,
        caller: crate::native_enrollment::caller::Caller,
        read_proof: &mut Proof,
    ) -> Result<crate::native_enrollment::caller::initial::CapturedKeyedAdmission, StoreError> {
        let request = &keyed.request;
        let session = request
            .session_id
            .as_ref()
            .ok_or(StoreError::AdmissionConflict)?;
        if let Some(receipt) = super::lookup_receipt(tx, session, &keyed.options.key).await? {
            // Preserve historical replay-domain validation; no new fresh witness.
            if let Some((existing, _)) =
                super::admission_reads::session(tx, session, &mut Proof::legacy()).await?
            {
                existing.ensure_identity(&request.workspace_id, &request.directory)?;
            }
            let digest = keyed.validated_digest()?;
            if receipt.fingerprint != keyed.options.fingerprint || receipt.semantic_digest != digest
            {
                return Err(StoreError::AdmissionConflict);
            }
            return Ok(
                crate::native_enrollment::caller::initial::CapturedKeyedAdmission::Replayed(
                    receipt,
                ),
            );
        }
        read_proof
            .measure(keyed)
            .map_err(|_| StoreError::Conflict)?;
        let digest = keyed.validated_digest()?;
        if let Some((existing, _)) =
            super::admission_reads::session(tx, session, read_proof).await?
        {
            existing.ensure_identity(&request.workspace_id, &request.directory)?;
        }
        if guard.is_ordinary() && sqlx::query_scalar::<_, bool>("SELECT EXISTS(SELECT 1 FROM future_queue WHERE session_id=$1 AND admission_key=$2)").bind(&session.0).bind(keyed.options.key.as_str()).fetch_one(&mut **tx).await.map_err(db)? { return Err(StoreError::AdmissionConflict); }
        let session_id = session.clone();
        let user_message_id = request.user_message.id.clone();
        let semantic_digest_version = if request.input_envelope.is_some() {
            2
        } else {
            1
        };
        let captured = self
            .admit_transaction_captured(tx, &keyed.request, true, guard, caller, read_proof)
            .await?;
        let admitted = &captured.admitted;
        let receipt = AdmissionReceipt {
            session_id,
            run_id: admitted.run.id.clone(),
            user_message_id,
            fingerprint: keyed.options.fingerprint.clone(),
            semantic_digest_version,
            semantic_digest: digest,
        };
        sqlx::query("INSERT INTO admission_receipts(session_id,admission_key,run_id,user_message_id,data) VALUES($1,$2,$3,$4,$5)")
            .bind(&receipt.session_id.0).bind(keyed.options.key.as_str()).bind(&receipt.run_id.0).bind(&receipt.user_message_id.0).bind(json(&receipt)?)
            .execute(&mut **tx).await.map_err(db)?;
        if let Some(capsule) = &keyed.execution {
            let record = crate::AdmissionExecutionRecord {
                receipt: receipt.clone(),
                key: keyed.options.key.clone(),
                capsule: capsule.clone(),
                state: crate::AdmissionExecutionState::Unstarted,
            };
            if record.capsule.version == 2 {
                sqlx::query("INSERT INTO admission_executions(run_id,session_id,admission_key,capsule_version,start_state,data,data_json,source_protocol) VALUES($1,$2,$3,2,'Unstarted','{}'::jsonb,$4,1)")
                    .bind(&receipt.run_id.0).bind(&receipt.session_id.0).bind(keyed.options.key.as_str())
                    .bind(record.encode()?).execute(&mut **tx).await.map_err(db)?;
            } else {
                sqlx::query("INSERT INTO admission_executions(run_id,session_id,admission_key,capsule_version,start_state,data,source_protocol) VALUES($1,$2,$3,1,'Unstarted',$4,1)")
                    .bind(&receipt.run_id.0).bind(&receipt.session_id.0).bind(keyed.options.key.as_str())
                    .bind(json(&record)?).execute(&mut **tx).await.map_err(db)?;
            }
            super::admission_journal::init(tx, &record, &admitted.run, read_proof).await?;
        }
        read_proof
            .charge(std::mem::size_of::<
                crate::native_enrollment::caller::initial::CapturedAdmission,
            >())
            .map_err(|_| StoreError::Conflict)?;
        Ok(
            crate::native_enrollment::caller::initial::CapturedKeyedAdmission::Started {
                receipt,
                admitted: Box::new(captured),
            },
        )
    }

    pub(super) async fn admit_with_binding(
        &self,
        request: AdmitRequest,
        binding: Option<ExecutionBindingCapsule>,
    ) -> Result<AdmitOutcome, StoreError> {
        let mut proof = Proof::bounded(32 << 20);
        proof
            .measure(&(&request, &binding))
            .map_err(|_| StoreError::Conflict)?;
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
        let result = async {
            if binding.is_none() {
                return self
                    .admit_transaction_guarded_with_proof(
                        &mut tx,
                        &request,
                        false,
                        crate::queue::AdmissionGuard::Ordinary,
                        &mut proof,
                    )
                    .await;
            }

            let captured = self
                .admit_transaction_captured(
                    &mut tx,
                    &request,
                    false,
                    crate::queue::AdmissionGuard::Ordinary,
                    crate::native_enrollment::caller::Caller::Raw,
                    &mut proof,
                )
                .await?;
            let input = if let Some(capsule) = &binding {
                save(
                    &mut tx,
                    &RetainedExecutionBinding::new(
                        &captured.admitted.run,
                        &request.user_message,
                        capsule.clone(),
                    ),
                )
                .await?;
                #[cfg(test)]
                crate::binding_fault::trip(&self.binding_fault, 1)?;
                crate::native_enrollment::caller::initial::Input::Bound {
                    request: &request,
                    binding: capsule,
                }
            } else {
                crate::native_enrollment::caller::initial::Input::Ordinary(&request)
            };
            self.finish_initial(&mut tx, input, captured, &mut proof)
                .await
        }
        .await;
        let result = finish(tx, result).await?;
        #[cfg(test)]
        crate::binding_fault::trip(&self.binding_fault, 2)?;
        Ok(result)
    }
    pub(super) async fn admit_keyed_with_binding(
        &self,
        request: KeyedAdmitRequest,
        binding: Option<ExecutionBindingCapsule>,
    ) -> Result<KeyedAdmitOutcome, StoreError> {
        let mut proof = Proof::bounded(32 << 20);
        proof
            .measure(&(&request, &binding))
            .map_err(|_| StoreError::Conflict)?;
        if let Some(binding) = &binding {
            binding.validate_keyed(&request)?;
        } else {
            request.validated_digest()?;
        }
        let session = request.request.session_id.as_ref().ok_or_else(|| {
            StoreError::Validation("keyed admission requires a stable session ID".into())
        })?;
        let mut tx = self.session_transaction(session).await?;
        let result = async {
            match self
                .admit_keyed_captured(
                    &mut tx,
                    &request,
                    crate::queue::AdmissionGuard::Ordinary,
                    crate::native_enrollment::caller::Caller::Raw,
                    &mut proof,
                )
                .await?
            {
                crate::native_enrollment::caller::initial::CapturedKeyedAdmission::Replayed(
                    receipt,
                ) => {
                    if let Some(capsule) = &binding
                        && read_with_proof(
                            &mut tx,
                            &receipt.session_id,
                            &receipt.run_id,
                            &mut proof,
                        )
                        .await
                        .map_err(|e| e.into_domain_or(StoreError::AdmissionConflict))?
                        .is_none_or(|original| original.capsule != *capsule)
                    {
                        return Err(StoreError::AdmissionConflict);
                    }
                    Ok(KeyedAdmitOutcome::Replayed(receipt))
                }
                crate::native_enrollment::caller::initial::CapturedKeyedAdmission::Started {
                    receipt,
                    admitted,
                } => {
                    let input = if let Some(capsule) = &binding {
                        save(
                            &mut tx,
                            &RetainedExecutionBinding::new(
                                &admitted.admitted.run,
                                &request.request.user_message,
                                capsule.clone(),
                            ),
                        )
                        .await?;
                        #[cfg(test)]
                        crate::binding_fault::trip(&self.binding_fault, 1)?;
                        crate::native_enrollment::caller::initial::Input::BoundKeyed {
                            request: &request,
                            binding: capsule,
                            receipt: &receipt,
                        }
                    } else {
                        crate::native_enrollment::caller::initial::Input::Keyed {
                            request: &request,
                            receipt: &receipt,
                        }
                    };
                    let admitted = self
                        .finish_initial(&mut tx, input, *admitted, &mut proof)
                        .await?;
                    Ok(KeyedAdmitOutcome::Started {
                        receipt,
                        admitted: Box::new(admitted),
                    })
                }
            }
        }
        .await;
        let result = finish(tx, result).await?;
        #[cfg(test)]
        if matches!(&result, KeyedAdmitOutcome::Started { .. }) {
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
pub(super) async fn save(
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
pub(super) async fn read(
    tx: &mut Transaction<'_, Postgres>,
    session: &SessionId,
    run: &RunId,
) -> Result<Option<RetainedExecutionBinding>, StoreError> {
    read_with_proof(tx, session, run, &mut Proof::legacy())
        .await
        .map_err(|error| error.into_domain_or(StoreError::AdmissionConflict))
}
pub(super) async fn read_with_proof(
    tx: &mut Transaction<'_, Postgres>,
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
    let row = sqlx::query("SELECT b.capsule_version, CASE WHEN octet_length(b.data_json) <= 16777216 THEN b.data_json END AS binding, CASE WHEN octet_length((r.data->>'config_hash')) <= 64 THEN r.data->>'config_hash' END AS config, CASE WHEN octet_length((r.data->>'plan_fingerprint')) <= 64 THEN r.data->>'plan_fingerprint' END AS plan, CASE WHEN octet_length((s.data->>'workspace_id')) <= 512 THEN s.data->>'workspace_id' END AS workspace, CASE WHEN octet_length((s.data->>'directory')) <= 16384 THEN s.data->>'directory' END AS directory FROM run_execution_bindings b JOIN runs r ON r.id=b.run_id JOIN sessions s ON s.id=r.session_id WHERE r.id=$1 AND r.session_id=$2")
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

/// Charge the exact original-message prefix in this same transaction before transfer.
pub(super) async fn original_messages_with_proof(
    tx: &mut Transaction<'_, Postgres>,
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
const BINDING_PROOF_COST: &str = r"SELECT CAST(octet_length(b.data_json)+COALESCE(octet_length(r.data->>'config_hash'),0)+COALESCE(octet_length(r.data->>'plan_fingerprint'),0)+COALESCE(octet_length(s.data->>'workspace_id'),0)+COALESCE(octet_length(s.data->>'directory'),0) AS BIGINT) FROM run_execution_bindings b JOIN runs r ON r.id=b.run_id JOIN sessions s ON s.id=r.session_id WHERE r.id=$1 AND r.session_id=$2";
const ORIGINAL_PROOF_COST: &str = r"SELECT CAST(COALESCE(SUM(bytes),0) AS BIGINT) FROM (SELECT octet_length(snapshot_record) AS bytes FROM messages WHERE session_id=$1 AND run_id=$2 ORDER BY seq LIMIT $3) originals";
