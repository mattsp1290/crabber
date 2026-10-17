//! Durable approval records, controls and read-only receipt reconciliation.
use crate::public_session::v2::proof::{ApprovalReadError, Proof};
mod activation;
mod answer;
mod claim;
pub(super) mod current;
mod guards;
mod native_caller;
pub(super) mod origin;
mod original_plan;
pub(super) use original_plan::batch_with_proof as original_batch_with_proof;
pub(super) mod public_header;
pub(super) use public_header::read as public_header;
mod recover;
mod suspend;
mod terminal_history;
mod terminate;
pub(super) use terminate::cancel_waiting;
pub(super) mod write;
use super::{SqliteStore, db};
use crate::transaction::Transaction;
use crate::{
    ApprovalError as Error, ApprovalReceipt, ApprovalSnapshot, ContinuationTarget,
    approval::{self, sql},
};
use crabber_core::{ContinuationGeneration, ContinuationId, ControlId, Run, RunId, SessionId};
pub(super) use current::session as verify_session;
pub(super) use guards::{guard_attempt, release_terminal, retirement_context};
use sqlx::{Row, Sqlite};

const SNAPSHOT: &str = r"SELECT s.public_revision,CASE WHEN length(CAST(json_extract(s.data, '$.workspace_id') AS BLOB))<=512 THEN json_extract(s.data, '$.workspace_id') END AS workspace,CASE WHEN length(CAST(c.id AS BLOB))<=128 THEN c.id END AS id,c.generation,CASE WHEN length(CAST(c.state AS BLOB))<=32 THEN c.state END AS state,c.format_version AS format_version,
    CASE WHEN length(CAST(c.data_json AS BLOB))<=4194304 THEN c.data_json END AS data_json
FROM runs r JOIN sessions s ON s.id=r.session_id
LEFT JOIN continuations c ON c.id=(SELECT id FROM continuations WHERE session_id=$1 AND run_id=$2 ORDER BY generation DESC LIMIT 1)
WHERE r.session_id=$1 AND r.id=$2";
const RECEIPT: &str = r"SELECT CASE WHEN r.kind='approval' THEN 'approval' ELSE 'foreign' END AS kind,r.event_seq,
    CASE WHEN length(CAST(r.semantic_digest AS BLOB))<=64 THEN r.semantic_digest END AS semantic_digest,
    CASE WHEN length(CAST(r.data_json AS BLOB))<=32768 THEN r.data_json END AS data_json,
    CASE WHEN length(CAST(e.run_id AS BLOB))<=128 THEN e.run_id END AS event_run_id,
    CASE WHEN r.kind='approval' AND length(CAST(e.data AS BLOB))<=65536 THEN e.data END AS event_data
FROM operation_receipts r LEFT JOIN events e ON e.seq=r.event_seq AND e.session_id=r.session_id
WHERE r.session_id=$1 AND r.operation_id=$2";

impl SqliteStore {
    pub(super) async fn read_approval(
        &self,
        session: &SessionId,
        run: &RunId,
    ) -> Result<Option<ApprovalSnapshot>, Error> {
        let mut tx = crate::transaction::begin(&self.readers)
            .await
            .map_err(db)
            .map_err(sql::unknown)?;
        let result = load(&mut tx, session, run)
            .await
            .map(|saved| saved.map(|(record, revision)| record.snapshot(revision)));
        finish(tx, result).await
    }
    pub(super) async fn approval_receipt(
        &self,
        session: &SessionId,
        id: &ControlId,
    ) -> Result<Option<ApprovalReceipt>, Error> {
        crate::attempt::validate_identifier(&id.0).map_err(approval::attempt_error)?;
        let mut tx = crate::transaction::begin(&self.readers)
            .await
            .map_err(db)
            .map_err(sql::unknown)?;
        let result = receipt(&mut tx, session, id).await;
        finish(tx, result).await
    }
}
impl SqliteStore {
    async fn approval_transaction_with_proof(
        &self,
        session: &SessionId,
        run: &RunId,
        proof: &mut Proof,
    ) -> Result<(Transaction<'_, Sqlite>, Run), Error> {
        let mut tx = self.begin_write().await.map_err(sql::unknown)?;
        let exists: bool =
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM runs WHERE session_id=$1 AND id=$2)")
                .bind(&session.0)
                .bind(&run.0)
                .fetch_one(&mut *tx)
                .await
                .map_err(db)
                .map_err(sql::unknown)?;
        if !exists {
            return Err(Error::NotFound);
        }
        let saved = super::records::load_run_with_proof(&mut tx, run, proof)
            .await
            .map_err(sql::unknown)?;
        if saved.session_id != *session || saved.id != *run {
            return Err(Error::UnknownStoreOutcome);
        }
        Ok((tx, saved))
    }
    async fn approval_transaction(
        &self,
        session: &SessionId,
        run: &RunId,
    ) -> Result<(Transaction<'_, Sqlite>, Run), Error> {
        let mut tx = self.begin_write().await.map_err(sql::unknown)?;
        let result = async {
            let row = sqlx::query("SELECT data FROM runs WHERE session_id=$1 AND id=$2")
                .bind(&session.0)
                .bind(&run.0)
                .fetch_optional(&mut *tx)
                .await
                .map_err(db)
                .map_err(sql::unknown)?
                .ok_or(Error::NotFound)?;
            let saved: Run = super::records::decode_row(&row, "data").map_err(sql::unknown)?;
            if saved.session_id != *session || saved.id != *run {
                return Err(Error::UnknownStoreOutcome);
            }
            Ok(saved)
        }
        .await;
        match result {
            Ok(run) => Ok((tx, run)),
            Err(error) => {
                tx.rollback().await.map_err(db).map_err(sql::unknown)?;
                Err(error)
            }
        }
    }
}
pub(super) async fn load(
    tx: &mut Transaction<'_, Sqlite>,
    session: &SessionId,
    run: &RunId,
) -> Result<Option<(approval::ApprovalRecord, crabber_core::SessionRevision)>, Error> {
    load_inner(tx, session, run, true).await
}
async fn load_inner(
    tx: &mut Transaction<'_, Sqlite>,
    session: &SessionId,
    run: &RunId,
    validate_current: bool,
) -> Result<Option<(approval::ApprovalRecord, crabber_core::SessionRevision)>, Error> {
    load_inner_with_proof(tx, session, run, validate_current, &mut Proof::legacy())
        .await
        .map_err(|error| error.into_domain_or(Error::UnknownStoreOutcome))
}
pub(super) async fn load_with_proof(
    tx: &mut Transaction<'_, Sqlite>,
    session: &SessionId,
    run: &RunId,
    proof: &mut Proof,
) -> Result<Option<(approval::ApprovalRecord, crabber_core::SessionRevision)>, ApprovalReadError> {
    load_inner_with_proof(tx, session, run, true, proof).await
}
async fn load_inner_with_proof(
    tx: &mut Transaction<'_, Sqlite>,
    session: &SessionId,
    run: &RunId,
    validate_current: bool,
    proof: &mut Proof,
) -> Result<Option<(approval::ApprovalRecord, crabber_core::SessionRevision)>, ApprovalReadError> {
    let cost = format!(
        "SELECT CAST(COALESCE(length(CAST(workspace AS BLOB)),0)+COALESCE(length(CAST(id AS BLOB)),0)+COALESCE(length(CAST(state AS BLOB)),0)+COALESCE(length(CAST(data_json AS BLOB)),0) AS BIGINT) FROM ({SNAPSHOT}) bounded_record"
    );
    super::read_proof::charge(
        tx,
        sqlx::query_scalar(&cost).bind(&session.0).bind(&run.0),
        proof,
    )
    .await
    .map_err(ApprovalReadError::Budget)?;
    let row = sqlx::query(SNAPSHOT)
        .bind(&session.0)
        .bind(&run.0)
        .fetch_optional(&mut **tx)
        .await
        .map_err(db)
        .map_err(sql::unknown)?
        .ok_or(Error::NotFound)?;
    let revision = crate::attempt::sql::revision(row.get("public_revision"))
        .map_err(approval::attempt_error)?;
    let Some(id) = row.get::<Option<String>, _>("id") else {
        return if row.get::<Option<i64>, _>("generation").is_none() {
            if current::has_source(tx, session, Some(run)).await? {
                Err(Error::UnknownStoreOutcome.into())
            } else {
                Ok(None)
            }
        } else {
            Err(Error::UnknownStoreOutcome.into())
        };
    };
    let generation: i64 = row.get("generation");
    let target = ContinuationTarget {
        id: ContinuationId(id),
        generation: ContinuationGeneration::new(
            u64::try_from(generation).map_err(|_| Error::UnknownStoreOutcome)?,
        )
        .map_err(|_| Error::UnknownStoreOutcome)?,
    };
    let record = sql::decode_record(
        row.get("data_json"),
        session,
        run,
        &target,
        &row.get::<Option<String>, _>("state")
            .ok_or(Error::UnknownStoreOutcome)?,
        row.get("format_version"),
    )?;
    if row.get::<Option<String>, _>("workspace").as_deref() != Some(&record.workspace_id) {
        return Err(Error::UnknownStoreOutcome.into());
    }
    verify_questions_with_proof(tx, &record, proof).await?;
    let saved_run = origin::verify_with_proof(tx, &record, proof).await?;
    if validate_current {
        current::verify_status_with_proof(tx, saved_run.status, &record, false, proof).await?;
    }
    Ok(Some((record, revision)))
}
pub(super) async fn receipt(
    tx: &mut Transaction<'_, Sqlite>,
    session: &SessionId,
    id: &ControlId,
) -> Result<Option<ApprovalReceipt>, Error> {
    sqlx::query(RECEIPT)
        .bind(&session.0)
        .bind(&id.0)
        .fetch_optional(&mut **tx)
        .await
        .map_err(db)
        .map_err(sql::unknown)?
        .map(|row| {
            if row.get::<String, _>("kind") != "approval" {
                return Err(Error::SemanticConflict);
            }
            let mut event = serde_json::from_str(
                &row.get::<Option<String>, _>("event_data")
                    .ok_or(Error::UnknownStoreOutcome)?,
            )
            .map_err(|_| Error::UnknownStoreOutcome)?;
            let saved = sql::decode_receipt(
                row.get("data_json"),
                session,
                id,
                row.get::<Option<String>, _>("semantic_digest").as_deref(),
                row.get("event_seq"),
                &mut event,
            )?;
            if row.get::<Option<String>, _>("event_run_id").as_deref() != Some(&saved.run_id.0) {
                return Err(Error::UnknownStoreOutcome);
            }
            Ok(saved)
        })
        .transpose()
}
async fn verify_questions(
    tx: &mut Transaction<'_, Sqlite>,
    record: &approval::ApprovalRecord,
) -> Result<(), Error> {
    let rows = sqlx::query("SELECT CASE WHEN length(CAST(id AS BLOB))<=128 THEN id END AS id, CASE WHEN length(CAST(call_id AS BLOB))<=512 THEN call_id END AS call_id FROM continuation_interactions WHERE continuation_id=$1 AND run_id=$2 LIMIT 65")
        .bind(&record.batch.continuation.id.0).bind(&record.run_id.0)
        .fetch_all(&mut **tx).await.map_err(db).map_err(sql::unknown)?;
    let mut actual = std::collections::BTreeMap::new();
    for row in rows {
        let id = row
            .get::<Option<String>, _>("id")
            .ok_or(Error::UnknownStoreOutcome)?;
        let call = row
            .get::<Option<String>, _>("call_id")
            .ok_or(Error::UnknownStoreOutcome)?;
        actual.insert(id, call);
    }
    let expected: std::collections::BTreeMap<_, _> = record
        .batch
        .calls
        .iter()
        .filter_map(|call| {
            if let crate::CallApproval::Ask { id, .. } = &call.decision {
                Some((id.0.clone(), call.target.call_id.0.clone()))
            } else {
                None
            }
        })
        .collect();
    if actual != expected {
        return Err(Error::UnknownStoreOutcome);
    }
    Ok(())
}
async fn finish<T>(tx: Transaction<'_, Sqlite>, result: Result<T, Error>) -> Result<T, Error> {
    super::transactions::finish(tx, result, sql::unknown).await
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod compound_tests;

#[cfg(test)]
mod process_tests;

#[cfg(test)]
mod current_tests;

#[cfg(test)]
mod original_plan_tests;
pub(super) use write::replay_scope as replay_prepared;

pub(super) use claim::ready_with_proof as preparation_context;

pub(super) use recover::preparation_context as preparation_recovery_context;

#[cfg(test)]
mod origin_facts_tests;

pub(super) async fn receipt_with_proof(
    tx: &mut Transaction<'_, Sqlite>,
    session: &SessionId,
    id: &ControlId,
    proof: &mut Proof,
) -> Result<Option<ApprovalReceipt>, ApprovalReadError> {
    crate::attempt::validate_identifier(&id.0).map_err(|_| Error::InvalidRequest)?;
    super::read_proof::charge(
        tx,
        sqlx::query_scalar(RECEIPT_PROOF_COST)
            .bind(&session.0)
            .bind(&id.0),
        proof,
    )
    .await
    .map_err(ApprovalReadError::Budget)?;
    Ok(receipt(tx, session, id).await?)
}
async fn verify_questions_with_proof(
    tx: &mut Transaction<'_, Sqlite>,
    record: &approval::ApprovalRecord,
    proof: &mut Proof,
) -> Result<(), ApprovalReadError> {
    super::read_proof::charge(
        tx,
        sqlx::query_scalar(QUESTION_PROOF_COST)
            .bind(&record.batch.continuation.id.0)
            .bind(&record.run_id.0),
        proof,
    )
    .await
    .map_err(ApprovalReadError::Budget)?;
    Ok(verify_questions(tx, record).await?)
}
const RECEIPT_PROOF_COST: &str = r"SELECT CAST(COALESCE(length(CAST(r.data_json AS BLOB)),0)+COALESCE(length(CAST(r.semantic_digest AS BLOB)),0)+8+COALESCE(length(CAST(e.run_id AS BLOB)),0)+CASE WHEN r.kind='approval' THEN COALESCE(length(CAST(e.data AS BLOB)),0) ELSE 0 END AS BIGINT) FROM operation_receipts r LEFT JOIN events e ON e.session_id=r.session_id AND e.seq=r.event_seq WHERE r.session_id=$1 AND r.operation_id=$2";
const QUESTION_PROOF_COST: &str = r"SELECT CAST(COALESCE(SUM(bytes),0) AS BIGINT) FROM (SELECT length(CAST(id AS BLOB))+length(CAST(call_id AS BLOB)) AS bytes FROM continuation_interactions WHERE continuation_id=$1 AND run_id=$2 LIMIT 65) bounded_questions";

pub(super) use guards::guard_attempt_with_proof;

pub(super) use current::session_with_proof as verify_session_with_proof;
pub(super) use terminate::validate_terminal_phases_with_proof;
