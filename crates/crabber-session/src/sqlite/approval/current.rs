//! Bounded immutable audit selectors shared with public proof preflight.
use super::{Error, approval, sql};
use crate::approval::current::{CurrentProof, NativeSource, Release, Source};
use crate::public_session::budget::{Budget, Failure, unknown};
use crabber_core::{EventRecord, Run, RunId, SessionId};
use sqlx::{Row, Sqlite, Transaction};
#[derive(Clone, Copy)]
pub(in super::super) enum Select {
    Answers,
    Latest,
    Successor,
    Session,
    Native,
}
fn query(select: Select) -> &'static str {
    match select {
        Select::Answers => {
            r"WITH decoded AS (SELECT e.seq,e.session_id,e.run_id,e.data,e.data AS d FROM events e WHERE e.session_id=$1 AND (CAST($2 AS TEXT) IS NULL OR e.run_id=$2)), selected AS (SELECT * FROM decoded WHERE json_extract(d,'$.kind')='interaction_changed' AND json_extract(d,'$.payload.outcome.operation')='answered' AND json_extract(d,'$.payload.kind') IS NULL AND json_extract(d,'$.payload.outcome.continuation.id')=$3 AND CAST(json_extract(d,'$.payload.outcome.continuation.generation') AS TEXT)=$4 AND (CAST($3 AS TEXT) IS NULL OR TRUE) AND (CAST($4 AS TEXT) IS NULL OR TRUE) ORDER BY seq DESC LIMIT 65) SELECT s.seq,CAST(length(CAST(s.data AS BLOB))+COALESCE(length(CAST(r.data_json AS BLOB)),0)+256 AS BIGINT) AS bytes,CASE WHEN $5 AND length(CAST(s.data AS BLOB))<=65536 THEN CAST(s.data AS TEXT) END AS event,CASE WHEN $5 AND length(CAST(r.data_json AS BLOB))<=32768 AND r.kind='approval' AND r.event_seq=s.seq THEN r.data_json END AS receipt FROM selected s LEFT JOIN operation_receipts r ON r.session_id=s.session_id AND r.operation_id=json_extract(s.d,'$.correlation')"
        }
        Select::Latest => {
            r"WITH decoded AS (SELECT e.seq,e.session_id,e.run_id,e.data,e.data AS d FROM events e WHERE e.session_id=$1 AND (CAST($2 AS TEXT) IS NULL OR e.run_id=$2)), selected AS (SELECT * FROM decoded WHERE json_extract(d,'$.kind')='continuation_changed' AND json_extract(d,'$.payload.outcome.operation')<>'answered' AND json_extract(d,'$.payload.kind') IS NULL AND json_extract(d,'$.payload.outcome.continuation.id')=$3 AND CAST(json_extract(d,'$.payload.outcome.continuation.generation') AS TEXT)=$4 AND (CAST($3 AS TEXT) IS NULL OR TRUE) AND (CAST($4 AS TEXT) IS NULL OR TRUE) ORDER BY seq DESC LIMIT 1) SELECT s.seq,CAST(length(CAST(s.data AS BLOB))+COALESCE(length(CAST(r.data_json AS BLOB)),0)+256 AS BIGINT) AS bytes,CASE WHEN $5 AND length(CAST(s.data AS BLOB))<=65536 THEN CAST(s.data AS TEXT) END AS event,CASE WHEN $5 AND length(CAST(r.data_json AS BLOB))<=32768 AND r.kind='approval' AND r.event_seq=s.seq THEN r.data_json END AS receipt FROM selected s LEFT JOIN operation_receipts r ON r.session_id=s.session_id AND r.operation_id=json_extract(s.d,'$.correlation')"
        }
        Select::Successor => {
            r"WITH decoded AS (SELECT e.seq,e.session_id,e.run_id,e.data,e.data AS d FROM events e WHERE e.session_id=$1 AND (CAST($2 AS TEXT) IS NULL OR e.run_id=$2)), selected AS (SELECT * FROM decoded WHERE json_extract(d,'$.kind')='continuation_changed' AND json_extract(d,'$.payload.outcome.operation')='suspended' AND json_extract(d,'$.payload.kind') IS NULL AND CAST(json_extract(d,'$.payload.outcome.continuation.generation') AS TEXT)=$4 AND (CAST($3 AS TEXT) IS NULL OR TRUE) AND (CAST($4 AS TEXT) IS NULL OR TRUE) ORDER BY seq DESC LIMIT 1) SELECT s.seq,CAST(length(CAST(s.data AS BLOB))+COALESCE(length(CAST(r.data_json AS BLOB)),0)+256 AS BIGINT) AS bytes,CASE WHEN $5 AND length(CAST(s.data AS BLOB))<=65536 THEN CAST(s.data AS TEXT) END AS event,CASE WHEN $5 AND length(CAST(r.data_json AS BLOB))<=32768 AND r.kind='approval' AND r.event_seq=s.seq THEN r.data_json END AS receipt FROM selected s LEFT JOIN operation_receipts r ON r.session_id=s.session_id AND r.operation_id=json_extract(s.d,'$.correlation')"
        }
        Select::Session => {
            r"WITH decoded AS (SELECT e.seq,e.session_id,e.run_id,e.data,e.data AS d FROM events e WHERE e.session_id=$1 AND (CAST($2 AS TEXT) IS NULL OR e.run_id=$2)), selected AS (SELECT * FROM decoded WHERE json_extract(d,'$.kind')='continuation_changed' AND json_extract(d,'$.payload.outcome.operation')='suspended' AND json_extract(d,'$.payload.kind') IS NULL AND (CAST($3 AS TEXT) IS NULL OR TRUE) AND (CAST($4 AS TEXT) IS NULL OR TRUE) ORDER BY seq DESC LIMIT 1) SELECT s.seq,CAST(length(CAST(s.data AS BLOB))+COALESCE(length(CAST(r.data_json AS BLOB)),0)+256 AS BIGINT) AS bytes,CASE WHEN $5 AND length(CAST(s.data AS BLOB))<=65536 THEN CAST(s.data AS TEXT) END AS event,CASE WHEN $5 AND length(CAST(r.data_json AS BLOB))<=32768 AND r.kind='approval' AND r.event_seq=s.seq THEN r.data_json END AS receipt FROM selected s LEFT JOIN operation_receipts r ON r.session_id=s.session_id AND r.operation_id=json_extract(s.d,'$.correlation')"
        }
        Select::Native => {
            r"WITH decoded AS (SELECT e.seq,e.session_id,e.run_id,e.data,e.data AS d FROM events e WHERE e.session_id=$1 AND (CAST($2 AS TEXT) IS NULL OR e.run_id=$2)), selected AS (SELECT * FROM decoded WHERE json_extract(d,'$.kind')='execution_attempt_changed' AND CAST(json_extract(d,'$.payload.version') AS TEXT) IN ('2','3') AND json_extract(d,'$.payload.attempt.target.id')=$3 AND (CAST($3 AS TEXT) IS NULL OR TRUE) AND (CAST($4 AS TEXT) IS NULL OR TRUE) ORDER BY seq DESC LIMIT 1) SELECT s.seq,CAST(length(CAST(s.data AS BLOB))+COALESCE(length(CAST(r.data_json AS BLOB)),0)+256 AS BIGINT) AS bytes,CASE WHEN $5 AND length(CAST(s.data AS BLOB))<=65536 THEN CAST(s.data AS TEXT) END AS event,CASE WHEN $5 AND length(CAST(r.data_json AS BLOB))<=32768 AND r.kind='attempt' AND r.event_seq=s.seq THEN r.data_json END AS receipt FROM selected s LEFT JOIN operation_receipts r ON r.session_id=s.session_id AND r.operation_id=json_extract(s.d,'$.correlation')"
        }
    }
}
pub(in super::super) async fn rows(
    tx: &mut Transaction<'_, Sqlite>,
    record: &approval::ApprovalRecord,
    select: Select,
    materialize: bool,
) -> Result<Vec<sqlx::sqlite::SqliteRow>, Error> {
    let generation = if matches!(select, Select::Successor) {
        record
            .batch
            .continuation
            .generation
            .next()
            .map_err(|_| Error::UnknownStoreOutcome)?
            .get()
    } else {
        record.batch.continuation.generation.get()
    }
    .to_string();
    let id = if matches!(select, Select::Native) {
        &record.batch.next_attempt.id.0
    } else {
        &record.batch.continuation.id.0
    };
    sqlx::query(query(select))
        .bind(&record.session_id.0)
        .bind(Some(&record.run_id.0))
        .bind(Some(id))
        .bind(Some(generation))
        .bind(materialize)
        .fetch_all(&mut **tx)
        .await
        .map_err(super::super::db)
        .map_err(error)
}
fn event(row: &sqlx::sqlite::SqliteRow) -> Result<EventRecord, Error> {
    let text: String = row
        .try_get::<Option<String>, _>("event")
        .map_err(error)?
        .ok_or(Error::UnknownStoreOutcome)?;
    let mut event: EventRecord =
        serde_json::from_str(&text).map_err(|_| Error::UnknownStoreOutcome)?;
    let cursor = crabber_core::EventCursor(
        u64::try_from(row.try_get::<i64, _>("seq").map_err(error)?)
            .map_err(|_| Error::UnknownStoreOutcome)?,
    );
    if event.cursor.is_some_and(|v| v != cursor) {
        return Err(Error::UnknownStoreOutcome);
    }
    event.cursor = Some(cursor);
    Ok(event)
}
pub(in super::super) fn source(row: &sqlx::sqlite::SqliteRow) -> Result<Source, Error> {
    let receipt = serde_json::from_str(
        &row.try_get::<Option<String>, _>("receipt")
            .map_err(error)?
            .ok_or(Error::UnknownStoreOutcome)?,
    )
    .map_err(|_| Error::UnknownStoreOutcome)?;
    let source = Source {
        receipt,
        event: event(row)?,
    };
    source.verify()?;
    Ok(source)
}
pub(in super::super) async fn verify(
    tx: &mut Transaction<'_, Sqlite>,
    run: &Run,
    record: &approval::ApprovalRecord,
    releasing: bool,
) -> Result<(), Error> {
    let answers = rows(tx, record, Select::Answers, true).await?;
    if answers.len() > 64 {
        return Err(Error::UnknownStoreOutcome);
    }
    let answers = answers.iter().map(source).collect::<Result<_, _>>()?;
    let latest = rows(tx, record, Select::Latest, true).await?;
    let latest = source(latest.first().ok_or(Error::UnknownStoreOutcome)?)?;
    let release = if record.state == crate::ApprovalContinuationState::Released
        && record.termination.is_none()
    {
        if let Some(proof) = super::super::continuation_handoff::verify(
            tx,
            &record.session_id,
            &record.run_id,
            crate::continuation_handoff::Domain::Approval,
            &record.batch.continuation,
            &record.batch.next_attempt,
            &record.batch.expected_plan,
        )
        .await?
        {
            Some(Release::Handoff(proof))
        } else {
            let successor = rows(tx, record, Select::Successor, true).await?;
            if let Some(row) = successor.first() {
                let source = source(row)?;
                let target = approval::current::continuation(&source.receipt.outcome);
                let next = exact(tx, record, target).await?;
                super::origin::verify(tx, &next).await?;
                Some(Release::Successor {
                    pub_source: source,
                    attempt: next.batch.attempt,
                })
            } else {
                let native = rows(tx, record, Select::Native, true).await?;
                let row = native.first().ok_or(Error::UnknownStoreOutcome)?;
                let receipt = serde_json::from_str(
                    &row.try_get::<Option<String>, _>("receipt")
                        .map_err(error)?
                        .ok_or(Error::UnknownStoreOutcome)?,
                )
                .map_err(|_| Error::UnknownStoreOutcome)?;
                let latest = latest_attempt(tx, record).await?;
                Some(Release::Native(NativeSource {
                    receipt,
                    event: event(row)?,
                    latest,
                }))
            }
        }
    } else {
        None
    };
    let barrier = sqlx::query_scalar::<_, Option<String>>(BARRIER)
        .bind(&record.session_id.0)
        .fetch_optional(&mut **tx)
        .await
        .map_err(super::super::db)
        .map_err(error)?
        .flatten()
        .map(crabber_core::ContinuationId);
    record.verify_current(
        run,
        &CurrentProof {
            answers,
            latest,
            release,
        },
        barrier.as_ref(),
        releasing,
    )
}
const BARRIER: &str = "SELECT CASE WHEN length(CAST(continuation_id AS BLOB))<=128 THEN continuation_id END FROM continuation_barriers WHERE session_id=$1";
pub(in super::super) async fn exact(
    tx: &mut Transaction<'_, Sqlite>,
    record: &approval::ApprovalRecord,
    target: &crate::ContinuationTarget,
) -> Result<approval::ApprovalRecord, Error> {
    exact_scoped(tx, &record.session_id, &record.run_id, target).await
}
pub(in super::super) async fn exact_scoped(
    tx: &mut Transaction<'_, Sqlite>,
    session: &SessionId,
    run: &RunId,
    target: &crate::ContinuationTarget,
) -> Result<approval::ApprovalRecord, Error> {
    let row = sqlx::query(EXACT)
        .bind(&session.0)
        .bind(&run.0)
        .bind(&target.id.0)
        .bind(i64::try_from(target.generation.get()).map_err(|_| Error::UnknownStoreOutcome)?)
        .fetch_optional(&mut **tx)
        .await
        .map_err(super::super::db)
        .map_err(error)?
        .ok_or(Error::UnknownStoreOutcome)?;
    let next = sql::decode_record(
        row.try_get("data").map_err(error)?,
        session,
        run,
        target,
        &row.try_get::<Option<String>, _>("state")
            .map_err(error)?
            .ok_or(Error::UnknownStoreOutcome)?,
        row.try_get::<i64, _>("format_version").map_err(error)?,
    )?;
    if row
        .try_get::<Option<String>, _>("workspace")
        .map_err(error)?
        .as_deref()
        != Some(&next.workspace_id)
    {
        return Err(Error::UnknownStoreOutcome);
    }
    super::verify_questions(tx, &next).await?;
    Ok(next)
}
const EXACT: &str = "SELECT CASE WHEN length(CAST(c.data_json AS BLOB))<=4194304 THEN c.data_json END AS data,CASE WHEN length(CAST(c.state AS BLOB))<=32 THEN c.state END AS state,CAST(c.format_version AS BIGINT) AS format_version,CASE WHEN length(CAST(json_extract(s.data,'$.workspace_id') AS BLOB))<=512 THEN json_extract(s.data,'$.workspace_id') END AS workspace FROM continuations c JOIN sessions s ON s.id=c.session_id WHERE c.session_id=$1 AND c.run_id=$2 AND c.id=$3 AND c.generation=$4";
async fn latest_attempt(
    tx: &mut Transaction<'_, Sqlite>,
    record: &approval::ApprovalRecord,
) -> Result<crate::AttemptSnapshot, Error> {
    let text: Option<String> = sqlx::query_scalar(ATTEMPT)
        .bind(&record.session_id.0)
        .bind(&record.run_id.0)
        .fetch_optional(&mut **tx)
        .await
        .map_err(super::super::db)
        .map_err(error)?
        .flatten();
    serde_json::from_str(&text.ok_or(Error::UnknownStoreOutcome)?)
        .map_err(|_| Error::UnknownStoreOutcome)
}
const ATTEMPT: &str = "SELECT CASE WHEN length(CAST(value AS BLOB))<=8192 THEN value END FROM (SELECT json_object('session_id',$1,'run_id',run_id,'target',json_object('id',id,'generation',generation),'state',state,'cleanup',cleanup_state) AS value FROM execution_attempts WHERE run_id=$2 ORDER BY generation DESC LIMIT 1) bounded";

pub(in super::super) async fn has_source(
    tx: &mut Transaction<'_, Sqlite>,
    session: &SessionId,
    run: Option<&RunId>,
) -> Result<bool, Error> {
    Ok(!sqlx::query(query(Select::Session))
        .bind(&session.0)
        .bind(run.map(|id| &id.0))
        .bind(Option::<String>::None)
        .bind(Option::<String>::None)
        .bind(false)
        .fetch_all(&mut **tx)
        .await
        .map_err(super::super::db)
        .map_err(error)?
        .is_empty())
}
pub(in super::super) async fn session(
    tx: &mut Transaction<'_, Sqlite>,
    session: &SessionId,
) -> Result<(), Error> {
    let rows = sqlx::query(query(Select::Session))
        .bind(&session.0)
        .bind(Option::<String>::None)
        .bind(Option::<String>::None)
        .bind(Option::<String>::None)
        .bind(true)
        .fetch_all(&mut **tx)
        .await
        .map_err(super::super::db)
        .map_err(error)?;
    let Some(row) = rows.first() else {
        let exists:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM continuation_barriers WHERE session_id=$1) OR EXISTS(SELECT 1 FROM continuations WHERE session_id=$1)").bind(&session.0).fetch_one(&mut **tx).await.map_err(super::super::db).map_err(error)?;
        return if exists {
            Err(Error::UnknownStoreOutcome)
        } else {
            Ok(())
        };
    };
    let source = source(row)?;
    let (record, _) = super::load(tx, session, &source.receipt.run_id)
        .await?
        .ok_or(Error::UnknownStoreOutcome)?;
    if record.batch.continuation != *approval::current::continuation(&source.receipt.outcome) {
        return Err(Error::UnknownStoreOutcome);
    }
    Ok(())
}

fn error<T>(_: T) -> Error {
    Error::UnknownStoreOutcome
}

/// Charge the exact independently selected immutable carriers before the normal loader.
pub(in super::super) async fn preflight(
    tx: &mut Transaction<'_, Sqlite>,
    record: &approval::ApprovalRecord,
    proof: &mut Budget,
) -> Result<(), Failure> {
    for select in [Select::Answers, Select::Latest] {
        let rows = rows(tx, record, select, false).await.map_err(unknown)?;
        if matches!(select, Select::Answers) && rows.len() > 64 {
            return Err(unknown("duplicate answer evidence"));
        }
        charge_rows(&rows, proof)?;
    }
    proof.charge(128)?; // The separately re-read bounded barrier identifier.
    if record.state == crate::ApprovalContinuationState::Released && record.termination.is_none() {
        if super::super::continuation_handoff::preflight(tx, record, proof).await? {
            return Ok(());
        }
        let selected = rows(tx, record, Select::Successor, false)
            .await
            .map_err(unknown)?;
        charge_rows(&selected, proof)?;
        if selected.is_empty() {
            charge_rows(
                &rows(tx, record, Select::Native, false)
                    .await
                    .map_err(unknown)?,
                proof,
            )?;
            proof.charge(8192)?; // Exact safe native snapshot, statically capped before transfer.
        } else {
            let generation = i64::try_from(
                record
                    .batch
                    .continuation
                    .generation
                    .next()
                    .map_err(unknown)?
                    .get(),
            )
            .map_err(unknown)?;
            let row=sqlx::query("SELECT CAST(length(CAST(data_json AS BLOB)) AS BIGINT) AS bytes,CASE WHEN length(CAST(data_json AS BLOB))<=$4 AND length(CAST(data_json AS BLOB))<=4194304 THEN data_json END AS data FROM continuations WHERE session_id=$1 AND run_id=$2 AND generation=$3").bind(&record.session_id.0).bind(&record.run_id.0).bind(generation).bind(i64::try_from(proof.remaining()).map_err(unknown)?).fetch_optional(&mut **tx).await.map_err(unknown)?.ok_or_else(||unknown("missing successor"))?;
            let bytes = usize::try_from(row.try_get::<i64, _>("bytes").map_err(unknown)?)
                .map_err(unknown)?;
            proof.charge(bytes)?;
            let data = row
                .try_get::<Option<String>, _>("data")
                .map_err(unknown)?
                .ok_or_else(|| unknown("successor size"))?;
            let next: approval::ApprovalRecord = serde_json::from_str(&data).map_err(unknown)?;
            if next.questions.len() > 64 || next.batch.calls.len() > 64 {
                return Err(unknown("successor count"));
            }
            proof.charge(
                bytes
                    .checked_add(1024)
                    .ok_or_else(|| unknown("successor cost"))?,
            )?;
            super::public_header::origin_cost(tx, &next, proof).await?;
        }
    }
    Ok(())
}
fn charge_rows(rows: &[sqlx::sqlite::SqliteRow], proof: &mut Budget) -> Result<(), Failure> {
    for row in rows {
        proof.charge(
            usize::try_from(row.try_get::<i64, _>("bytes").map_err(unknown)?).map_err(unknown)?,
        )?;
    }
    Ok(())
}
