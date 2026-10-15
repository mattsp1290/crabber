//! Durable approval records, controls and read-only receipt reconciliation.
mod answer;
mod claim;
mod guards;
mod recover;
mod suspend;
mod write;
use super::{SqliteStore, db};
use crate::{
    ApprovalError as Error, ApprovalReceipt, ApprovalSnapshot, ContinuationTarget,
    approval::{self, sql},
};
use crabber_core::{ContinuationGeneration, ContinuationId, ControlId, Run, RunId, SessionId};
pub(super) use guards::{guard_attempt, release_terminal};
use sqlx::{Row, Sqlite, Transaction};

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
        let mut tx = self
            .readers
            .begin()
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
        let mut tx = self
            .readers
            .begin()
            .await
            .map_err(db)
            .map_err(sql::unknown)?;
        let result = receipt(&mut tx, session, id).await;
        finish(tx, result).await
    }
}
impl SqliteStore {
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
async fn load(
    tx: &mut Transaction<'_, Sqlite>,
    session: &SessionId,
    run: &RunId,
) -> Result<Option<(approval::ApprovalRecord, crabber_core::SessionRevision)>, Error> {
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
            Ok(None)
        } else {
            Err(Error::UnknownStoreOutcome)
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
        return Err(Error::UnknownStoreOutcome);
    }
    verify_questions(tx, &record).await?;
    Ok(Some((record, revision)))
}
async fn receipt(
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
