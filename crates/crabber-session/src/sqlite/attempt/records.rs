use super::super::{db, records::micros};
use crate::{
    AttemptError as Error, AttemptMutation, AttemptReceipt, AttemptSnapshot, StoreError,
    attempt::{self, AttemptRecord, sql},
};
use crabber_core::{
    AttemptId, ControlId, EventCursor, InputFingerprint, Run, RunId, SessionId, SessionRevision,
};
use sqlx::{Row, Sqlite, Transaction};
use time::OffsetDateTime;

const RECORD: &str = r"SELECT
    s.public_revision,
    CASE WHEN length(CAST(s.id AS BLOB))<=512
        AND length(CAST(a.run_id AS BLOB))<=128
        AND length(CAST(a.id AS BLOB))<=128
        AND length(CAST(a.owner AS BLOB))<=512
        AND length(CAST(a.claim_token AS BLOB))<=512
        AND length(CAST(a.run_claim_token AS BLOB))<=512
        AND length(CAST(a.state AS BLOB))<=32
        AND length(CAST(a.cleanup_state AS BLOB))<=32
        THEN json_object('snapshot',json_object('session_id',s.id,'run_id',a.run_id,'target',json_object('id',a.id,'generation',a.generation),'state',a.state,'cleanup',a.cleanup_state),'owner',a.owner,'token',a.claim_token,'run_token',a.run_claim_token,'lease_until',a.lease_until)
    END AS record
FROM execution_attempts a
JOIN runs r ON r.id=a.run_id
JOIN sessions s ON s.id=r.session_id
WHERE a.run_id=$1
        AND (CAST($2 AS TEXT) IS NULL OR a.id=$2)
ORDER BY a.generation DESC
LIMIT 1";
const RECEIPT: &str = r"SELECT
    CASE WHEN r.kind='attempt'
        THEN 'attempt' ELSE 'foreign'
    END AS kind,
    CASE WHEN length(CAST(r.semantic_digest AS BLOB))<=64
        THEN r.semantic_digest
    END AS semantic_digest,r.event_seq,
    CASE WHEN length(CAST(e.run_id AS BLOB))<=128
        THEN e.run_id
    END AS event_run_id,
    CASE WHEN length(CAST(r.data_json AS BLOB))<=8192
        THEN r.data_json
    END AS data_json,
    CASE WHEN r.kind='attempt'
        AND length(CAST(e.data AS BLOB))<=32768
        THEN e.data
    END AS event_data
FROM operation_receipts r
LEFT JOIN events e ON e.seq=r.event_seq
        AND e.session_id=r.session_id
WHERE r.session_id=$1
        AND r.operation_id=$2";
const SAVE: &str = "UPDATE execution_attempts SET owner=$3,claim_token=$4,run_claim_token=$5,lease_until=$6,state=$7,cleanup_state=$8,outcome_json=$9 WHERE run_id=$1 AND id=$2 AND generation=$10";
const INSERT: &str = "INSERT INTO execution_attempts(run_id,id,owner,claim_token,run_claim_token,lease_until,state,cleanup_state,outcome_json,generation) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)";

pub(super) async fn load(
    tx: &mut Transaction<'_, Sqlite>,
    run: &RunId,
    id: Option<&AttemptId>,
    lock: bool,
) -> Result<Option<(AttemptRecord, SessionRevision)>, Error> {
    let _ = lock;
    let query = RECORD;
    sqlx::query(query)
        .bind(&run.0)
        .bind(id.map(|v| v.0.as_str()))
        .fetch_optional(&mut **tx)
        .await
        .map_err(db)
        .map_err(sql::unknown)?
        .map(|row| {
            Ok((
                sql::decode_record(row.get("record"))?,
                sql::revision(row.get("public_revision"))?,
            ))
        })
        .transpose()
}
pub(super) async fn receipt(
    tx: &mut Transaction<'_, Sqlite>,
    session: &SessionId,
    id: &ControlId,
) -> Result<Option<AttemptReceipt>, Error> {
    sqlx::query(RECEIPT)
        .bind(&session.0)
        .bind(&id.0)
        .fetch_optional(&mut **tx)
        .await
        .map_err(db)
        .map_err(sql::unknown)?
        .map(|row| {
            if row.get::<String, _>("kind") != "attempt" {
                return Err(Error::SemanticConflict);
            }
            let event = serde_json::from_str::<crabber_core::EventRecord>(
                &row.get::<Option<String>, _>("event_data")
                    .ok_or(Error::UnknownStoreOutcome)?,
            )
            .map_err(|_| Error::UnknownStoreOutcome)?;
            let result = sql::receipt(
                session,
                id,
                &row.get::<Option<String>, _>("semantic_digest")
                    .ok_or(Error::UnknownStoreOutcome)?,
                row.get("event_seq"),
                row.get("data_json"),
                &event,
            )?;
            if row.get::<Option<String>, _>("event_run_id").as_deref()
                != Some(&result.attempt.run_id.0)
            {
                return Err(Error::UnknownStoreOutcome);
            }
            Ok(result)
        })
        .transpose()
}
pub(super) async fn revision(
    tx: &mut Transaction<'_, Sqlite>,
    session: &SessionId,
) -> Result<SessionRevision, Error> {
    let value = sqlx::query_scalar("SELECT public_revision FROM sessions WHERE id=$1")
        .bind(&session.0)
        .fetch_optional(&mut **tx)
        .await
        .map_err(db)
        .map_err(sql::unknown)?
        .ok_or(Error::NotFound)?;
    sql::revision(value)
}
pub(super) async fn save(
    tx: &mut Transaction<'_, Sqlite>,
    record: &AttemptRecord,
    insert: bool,
) -> Result<(), Error> {
    record.validate()?;
    let outcome = if matches!(
        record.snapshot.state,
        crate::AttemptState::Unstarted | crate::AttemptState::Started
    ) {
        None
    } else {
        Some(
            serde_json::to_string(&record.snapshot.state)
                .map_err(|_| Error::UnknownStoreOutcome)?,
        )
    };
    let result = sqlx::query(if insert { INSERT } else { SAVE })
        .bind(&record.snapshot.run_id.0)
        .bind(&record.snapshot.target.id.0)
        .bind(&record.owner)
        .bind(&record.token)
        .bind(&record.run_token)
        .bind(micros(record.lease_until))
        .bind(sql::enum_text(record.snapshot.state)?)
        .bind(sql::enum_text(record.snapshot.cleanup)?)
        .bind(outcome)
        .bind(
            i64::try_from(record.snapshot.target.generation.get())
                .map_err(|_| Error::GenerationExhausted)?,
        )
        .execute(&mut **tx)
        .await
        .map_err(db)
        .map_err(sql::unknown)?;
    if result.rows_affected() != 1 {
        return Err(Error::UnknownStoreOutcome);
    }
    Ok(())
}
pub(super) async fn exists(
    tx: &mut Transaction<'_, Sqlite>,
    id: &AttemptId,
) -> Result<bool, Error> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM execution_attempts WHERE id=$1)")
        .bind(&id.0)
        .fetch_one(&mut **tx)
        .await
        .map_err(db)
        .map_err(sql::unknown)
}
pub(super) async fn quiet(
    tx: &mut Transaction<'_, Sqlite>,
    run: &RunId,
    id: &AttemptId,
) -> Result<bool, Error> {
    let phases: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM tool_phases WHERE run_id=$1 AND attempt_id=$2 AND (quiescence_state<>'Complete' OR state IN ('Claimed','Started','Uncertain')))")
        .bind(&run.0).bind(&id.0).fetch_one(&mut **tx).await.map_err(db).map_err(sql::unknown)?;
    let calls: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM tool_calls WHERE run_id=$1 AND json_extract(data,'$.status')='running')")
        .bind(&run.0).fetch_one(&mut **tx).await.map_err(db).map_err(sql::unknown)?;
    Ok(!phases && !calls)
}
pub(super) async fn publish(
    tx: &mut Transaction<'_, Sqlite>,
    run: &Run,
    request: &AttemptMutation,
    snapshot: &AttemptSnapshot,
    digest: InputFingerprint,
    revision: SessionRevision,
    now: OffsetDateTime,
) -> Result<AttemptReceipt, Error> {
    let event = attempt::event(run, request, snapshot, now)?;
    let seq: i64 = sqlx::query_scalar(
        "INSERT INTO events(session_id,run_id,data) VALUES($1,$2,$3) RETURNING seq",
    )
    .bind(&run.session_id.0)
    .bind(&run.id.0)
    .bind(serde_json::to_string(&event).map_err(|_| Error::InvalidRequest)?)
    .fetch_one(&mut **tx)
    .await
    .map_err(db)
    .map_err(sql::unknown)?;
    super::super::records::bump_public_revision(tx, &run.session_id)
        .await
        .map_err(sql::unknown)?;
    let receipt = AttemptReceipt {
        operation_id: request.id.clone(),
        semantic_digest: digest,
        revision,
        event_cursor: EventCursor(u64::try_from(seq).map_err(|_| Error::UnknownStoreOutcome)?),
        attempt: snapshot.clone(),
    };
    sqlx::query("INSERT INTO operation_receipts(session_id,operation_id,kind,semantic_digest,event_seq,data_json) VALUES($1,$2,'attempt',$3,$4,$5)")
        .bind(&run.session_id.0).bind(&request.id.0).bind(receipt.semantic_digest.as_str()).bind(seq)
        .bind(serde_json::to_string(&receipt).map_err(|_| Error::UnknownStoreOutcome)?)
        .execute(&mut **tx).await.map_err(db).map_err(sql::unknown)?;
    Ok(receipt)
}
pub(super) async fn renew(tx: &mut Transaction<'_, Sqlite>, run: &Run) -> Result<(), StoreError> {
    sqlx::query(
        "UPDATE execution_attempts SET lease_until=$3 WHERE run_id=$1 AND run_claim_token=$2",
    )
    .bind(&run.id.0)
    .bind(&run.claim_token)
    .bind(micros(run.lease_until))
    .execute(&mut **tx)
    .await
    .map_err(db)?;
    Ok(())
}
