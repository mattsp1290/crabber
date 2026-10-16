//! Bounded immutable audit selectors shared with public proof preflight.
use super::{Error, approval, sql};
use crate::approval::current::{CurrentProof, NativeSource, Release, Source};
use crate::public_session::budget::{Budget, Failure, unknown};
use crate::public_session::v2::proof::{ApprovalReadError, Proof};
use crabber_core::{EventRecord, Run, RunId, SessionId};
use sqlx::{Postgres, Row, Transaction};
#[derive(Clone, Copy)]
pub(in super::super) enum Select {
    Answers,
    Latest,
    Successor,
    Session,
    Native,
}
// Wrapped events use canonical EventRecord field order: kind immediately precedes
// payload. Gate on that lexical prefix before any JSON operator traverses the
// carrier; unrelated public payloads may contain escaped NUL, valid in JSON text
// but rejected by PostgreSQL's JSON extraction. Receipt indexes are not authority.
fn query(select: Select) -> &'static str {
    match select {
        Select::Answers => {
            r#"WITH decoded AS (SELECT e.seq,e.session_id,e.run_id,e.data,(CASE WHEN e.data->>'encoding'='crabber.event.json-text.v1' THEN CASE WHEN split_part(e.data->>'record', ',"payload":', 1) ~ ',"kind":"(continuation_changed|interaction_changed|execution_attempt_changed)"$' THEN (e.data->>'record')::json ELSE '{}'::json END ELSE e.data::json END) AS d FROM events e WHERE e.session_id=$1 AND (CAST($2 AS TEXT) IS NULL OR e.run_id=$2)), selected AS (SELECT * FROM decoded WHERE d #>> '{kind}'='interaction_changed' AND d #>> '{payload,outcome,operation}'='answered' AND d #>> '{payload,kind}' IS NULL AND d #>> '{payload,outcome,continuation,id}'=$3 AND CAST(d #>> '{payload,outcome,continuation,generation}' AS TEXT)=$4 AND (CAST($3 AS TEXT) IS NULL OR TRUE) AND (CAST($4 AS TEXT) IS NULL OR TRUE) ORDER BY seq DESC LIMIT 65) SELECT s.seq,CAST(octet_length(s.data::text)+COALESCE(octet_length(r.data_json),0)+256 AS BIGINT) AS bytes,CASE WHEN $5 AND octet_length(s.data::text)<=65536 THEN CAST(s.data AS TEXT) END AS event,CASE WHEN $5 AND octet_length(r.data_json)<=32768 AND r.kind='approval' AND r.event_seq=s.seq THEN r.data_json END AS receipt FROM selected s LEFT JOIN operation_receipts r ON r.session_id=s.session_id AND r.operation_id=s.d #>> '{correlation}'"#
        }
        Select::Latest => {
            r#"WITH decoded AS (SELECT e.seq,e.session_id,e.run_id,e.data,(CASE WHEN e.data->>'encoding'='crabber.event.json-text.v1' THEN CASE WHEN split_part(e.data->>'record', ',"payload":', 1) ~ ',"kind":"(continuation_changed|interaction_changed|execution_attempt_changed)"$' THEN (e.data->>'record')::json ELSE '{}'::json END ELSE e.data::json END) AS d FROM events e WHERE e.session_id=$1 AND (CAST($2 AS TEXT) IS NULL OR e.run_id=$2)), selected AS (SELECT * FROM decoded WHERE d #>> '{kind}'='continuation_changed' AND d #>> '{payload,outcome,operation}'<>'answered' AND d #>> '{payload,kind}' IS NULL AND d #>> '{payload,outcome,continuation,id}'=$3 AND CAST(d #>> '{payload,outcome,continuation,generation}' AS TEXT)=$4 AND (CAST($3 AS TEXT) IS NULL OR TRUE) AND (CAST($4 AS TEXT) IS NULL OR TRUE) ORDER BY seq DESC LIMIT 1) SELECT s.seq,CAST(octet_length(s.data::text)+COALESCE(octet_length(r.data_json),0)+256 AS BIGINT) AS bytes,CASE WHEN $5 AND octet_length(s.data::text)<=65536 THEN CAST(s.data AS TEXT) END AS event,CASE WHEN $5 AND octet_length(r.data_json)<=32768 AND r.kind='approval' AND r.event_seq=s.seq THEN r.data_json END AS receipt FROM selected s LEFT JOIN operation_receipts r ON r.session_id=s.session_id AND r.operation_id=s.d #>> '{correlation}'"#
        }
        Select::Successor => {
            r#"WITH decoded AS (SELECT e.seq,e.session_id,e.run_id,e.data,(CASE WHEN e.data->>'encoding'='crabber.event.json-text.v1' THEN CASE WHEN split_part(e.data->>'record', ',"payload":', 1) ~ ',"kind":"(continuation_changed|interaction_changed|execution_attempt_changed)"$' THEN (e.data->>'record')::json ELSE '{}'::json END ELSE e.data::json END) AS d FROM events e WHERE e.session_id=$1 AND (CAST($2 AS TEXT) IS NULL OR e.run_id=$2)), selected AS (SELECT * FROM decoded WHERE d #>> '{kind}'='continuation_changed' AND d #>> '{payload,outcome,operation}'='suspended' AND d #>> '{payload,kind}' IS NULL AND CAST(d #>> '{payload,outcome,continuation,generation}' AS TEXT)=$4 AND (CAST($3 AS TEXT) IS NULL OR TRUE) AND (CAST($4 AS TEXT) IS NULL OR TRUE) ORDER BY seq DESC LIMIT 1) SELECT s.seq,CAST(octet_length(s.data::text)+COALESCE(octet_length(r.data_json),0)+256 AS BIGINT) AS bytes,CASE WHEN $5 AND octet_length(s.data::text)<=65536 THEN CAST(s.data AS TEXT) END AS event,CASE WHEN $5 AND octet_length(r.data_json)<=32768 AND r.kind='approval' AND r.event_seq=s.seq THEN r.data_json END AS receipt FROM selected s LEFT JOIN operation_receipts r ON r.session_id=s.session_id AND r.operation_id=s.d #>> '{correlation}'"#
        }
        Select::Session => {
            r#"WITH decoded AS (SELECT e.seq,e.session_id,e.run_id,e.data,(CASE WHEN e.data->>'encoding'='crabber.event.json-text.v1' THEN CASE WHEN split_part(e.data->>'record', ',"payload":', 1) ~ ',"kind":"(continuation_changed|interaction_changed|execution_attempt_changed)"$' THEN (e.data->>'record')::json ELSE '{}'::json END ELSE e.data::json END) AS d FROM events e WHERE e.session_id=$1 AND (CAST($2 AS TEXT) IS NULL OR e.run_id=$2)), selected AS (SELECT * FROM decoded WHERE d #>> '{kind}'='continuation_changed' AND d #>> '{payload,outcome,operation}'='suspended' AND d #>> '{payload,kind}' IS NULL AND (CAST($3 AS TEXT) IS NULL OR TRUE) AND (CAST($4 AS TEXT) IS NULL OR TRUE) ORDER BY seq DESC LIMIT 1) SELECT s.seq,CAST(octet_length(s.data::text)+COALESCE(octet_length(r.data_json),0)+256 AS BIGINT) AS bytes,CASE WHEN $5 AND octet_length(s.data::text)<=65536 THEN CAST(s.data AS TEXT) END AS event,CASE WHEN $5 AND octet_length(r.data_json)<=32768 AND r.kind='approval' AND r.event_seq=s.seq THEN r.data_json END AS receipt FROM selected s LEFT JOIN operation_receipts r ON r.session_id=s.session_id AND r.operation_id=s.d #>> '{correlation}'"#
        }
        Select::Native => {
            r#"WITH decoded AS (SELECT e.seq,e.session_id,e.run_id,e.data,(CASE WHEN e.data->>'encoding'='crabber.event.json-text.v1' THEN CASE WHEN split_part(e.data->>'record', ',"payload":', 1) ~ ',"kind":"(continuation_changed|interaction_changed|execution_attempt_changed)"$' THEN (e.data->>'record')::json ELSE '{}'::json END ELSE e.data::json END) AS d FROM events e WHERE e.session_id=$1 AND (CAST($2 AS TEXT) IS NULL OR e.run_id=$2)), selected AS (SELECT * FROM decoded WHERE d #>> '{kind}'='execution_attempt_changed' AND CAST(d #>> '{payload,version}' AS TEXT) IN ('2','3','4') AND d #>> '{payload,attempt,target,id}'=$3 AND (CAST($3 AS TEXT) IS NULL OR TRUE) AND (CAST($4 AS TEXT) IS NULL OR TRUE) ORDER BY seq DESC LIMIT 1) SELECT s.seq,CAST(octet_length(s.data::text)+COALESCE(octet_length(r.data_json),0)+256 AS BIGINT) AS bytes,CASE WHEN $5 AND octet_length(s.data::text)<=65536 THEN CAST(s.data AS TEXT) END AS event,CASE WHEN $5 AND octet_length(r.data_json)<=32768 AND r.kind='attempt' AND r.event_seq=s.seq THEN r.data_json END AS receipt FROM selected s LEFT JOIN operation_receipts r ON r.session_id=s.session_id AND r.operation_id=s.d #>> '{correlation}'"#
        }
    }
}
pub(in super::super) async fn rows(
    tx: &mut Transaction<'_, Postgres>,
    record: &approval::ApprovalRecord,
    select: Select,
    materialize: bool,
) -> Result<Vec<sqlx::postgres::PgRow>, Error> {
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
async fn rows_with_proof(
    tx: &mut Transaction<'_, Postgres>,
    record: &approval::ApprovalRecord,
    select: Select,
    proof: &mut Proof,
) -> Result<Vec<sqlx::postgres::PgRow>, ApprovalReadError> {
    if proof.active() {
        for row in rows(tx, record, select, false).await? {
            let bytes = usize::try_from(row.try_get::<i64, _>("bytes").map_err(error)?)
                .map_err(error)?
                .checked_sub(256)
                .ok_or(Error::UnknownStoreOutcome)?;
            proof.charge(bytes).map_err(ApprovalReadError::Budget)?;
        }
    }
    Ok(rows(tx, record, select, true).await?)
}
fn event(row: &sqlx::postgres::PgRow) -> Result<EventRecord, Error> {
    let text: String = row
        .try_get::<Option<String>, _>("event")
        .map_err(error)?
        .ok_or(Error::UnknownStoreOutcome)?;
    let mut event = crate::public_state::decode_event(sqlx::types::Json(
        serde_json::from_str(&text).map_err(|_| Error::UnknownStoreOutcome)?,
    ))
    .map_err(error)?;
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
pub(in super::super) fn source(row: &sqlx::postgres::PgRow) -> Result<Source, Error> {
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
    tx: &mut Transaction<'_, Postgres>,
    run: &Run,
    record: &approval::ApprovalRecord,
    releasing: bool,
) -> Result<(), Error> {
    verify_status(tx, run.status, record, releasing).await
}
pub(in super::super) async fn verify_status(
    tx: &mut Transaction<'_, Postgres>,
    status: crabber_core::RunStatus,
    record: &approval::ApprovalRecord,
    releasing: bool,
) -> Result<(), Error> {
    verify_status_with_proof(tx, status, record, releasing, &mut Proof::legacy())
        .await
        .map_err(|error| error.into_domain_or(Error::UnknownStoreOutcome))
}
pub(in super::super) async fn verify_status_with_proof(
    tx: &mut Transaction<'_, Postgres>,
    status: crabber_core::RunStatus,
    record: &approval::ApprovalRecord,
    releasing: bool,
    proof: &mut Proof,
) -> Result<(), ApprovalReadError> {
    let answers = rows_with_proof(tx, record, Select::Answers, proof).await?;
    if answers.len() > 64 {
        return Err(Error::UnknownStoreOutcome.into());
    }
    let answers = answers.iter().map(source).collect::<Result<_, _>>()?;
    let latest = rows_with_proof(tx, record, Select::Latest, proof).await?;
    let latest = source(latest.first().ok_or(Error::UnknownStoreOutcome)?)?;
    let release = if record.state == crate::ApprovalContinuationState::Released
        && record.termination.is_none()
    {
        if let Some(proof) = super::super::continuation_handoff::verify_with_proof(
            tx,
            &record.session_id,
            &record.run_id,
            &crate::continuation_handoff::ReadTarget {
                domain: crate::continuation_handoff::Domain::Approval,
                target: &record.batch.continuation,
                parent: &record.batch.next_attempt,
                plan: &record.batch.expected_plan,
            },
            proof,
        )
        .await?
        {
            Some(Release::Handoff(proof))
        } else {
            let successor = rows_with_proof(tx, record, Select::Successor, proof).await?;
            if let Some(row) = successor.first() {
                let source = source(row)?;
                let target = approval::current::continuation(&source.receipt.outcome);
                let next =
                    exact_scoped_with_proof(tx, &record.session_id, &record.run_id, target, proof)
                        .await?;
                super::origin::verify_with_proof(tx, &next, proof).await?;
                Some(Release::Successor {
                    pub_source: source,
                    attempt: next.batch.attempt,
                })
            } else {
                let native = rows_with_proof(tx, record, Select::Native, proof).await?;
                let row = native.first().ok_or(Error::UnknownStoreOutcome)?;
                let receipt = serde_json::from_str(
                    &row.try_get::<Option<String>, _>("receipt")
                        .map_err(error)?
                        .ok_or(Error::UnknownStoreOutcome)?,
                )
                .map_err(|_| Error::UnknownStoreOutcome)?;
                let latest = latest_attempt_with_proof(tx, record, proof).await?;
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
    super::super::read_proof::charge(tx, sqlx::query_scalar("SELECT CAST(COALESCE(octet_length(continuation_id),0) AS BIGINT) FROM continuation_barriers WHERE session_id=$1").bind(&record.session_id.0), proof).await.map_err(ApprovalReadError::Budget)?;
    let barrier = sqlx::query_scalar::<_, Option<String>>(BARRIER)
        .bind(&record.session_id.0)
        .fetch_optional(&mut **tx)
        .await
        .map_err(super::super::db)
        .map_err(error)?
        .flatten()
        .map(crabber_core::ContinuationId);
    Ok(record.verify_current_status(
        status,
        &CurrentProof {
            answers,
            latest,
            release,
        },
        barrier.as_ref(),
        releasing,
    )?)
}
const BARRIER: &str = "SELECT CASE WHEN octet_length(continuation_id)<=128 THEN continuation_id END FROM continuation_barriers WHERE session_id=$1";
#[cfg(test)]
pub(in super::super) async fn exact(
    tx: &mut Transaction<'_, Postgres>,
    record: &approval::ApprovalRecord,
    target: &crate::ContinuationTarget,
) -> Result<approval::ApprovalRecord, Error> {
    exact_scoped(tx, &record.session_id, &record.run_id, target).await
}
pub(in super::super) async fn exact_scoped(
    tx: &mut Transaction<'_, Postgres>,
    session: &SessionId,
    run: &RunId,
    target: &crate::ContinuationTarget,
) -> Result<approval::ApprovalRecord, Error> {
    exact_scoped_with_proof(tx, session, run, target, &mut Proof::legacy())
        .await
        .map_err(|error| error.into_domain_or(Error::UnknownStoreOutcome))
}
pub(in super::super) async fn exact_scoped_with_proof(
    tx: &mut Transaction<'_, Postgres>,
    session: &SessionId,
    run: &RunId,
    target: &crate::ContinuationTarget,
    proof: &mut Proof,
) -> Result<approval::ApprovalRecord, ApprovalReadError> {
    let cost = format!(
        "SELECT CAST(COALESCE(octet_length(data),0)+COALESCE(octet_length(state),0)+COALESCE(octet_length(workspace),0) AS BIGINT) FROM ({EXACT}) bounded_original"
    );
    super::super::read_proof::charge(
        tx,
        sqlx::query_scalar(&cost)
            .bind(&session.0)
            .bind(&run.0)
            .bind(&target.id.0)
            .bind(i64::try_from(target.generation.get()).map_err(|_| Error::UnknownStoreOutcome)?),
        proof,
    )
    .await
    .map_err(ApprovalReadError::Budget)?;
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
        return Err(Error::UnknownStoreOutcome.into());
    }
    super::verify_questions_with_proof(tx, &next, proof).await?;
    Ok(next)
}
const EXACT: &str = "SELECT CASE WHEN octet_length(c.data_json)<=4194304 THEN c.data_json END AS data,CASE WHEN octet_length(c.state)<=32 THEN c.state END AS state,CAST(c.format_version AS BIGINT) AS format_version,CASE WHEN octet_length(s.data->>'workspace_id')<=512 THEN s.data->>'workspace_id' END AS workspace FROM continuations c JOIN sessions s ON s.id=c.session_id WHERE c.session_id=$1 AND c.run_id=$2 AND c.id=$3 AND c.generation=$4";
async fn latest_attempt_with_proof(
    tx: &mut Transaction<'_, Postgres>,
    record: &approval::ApprovalRecord,
    proof: &mut Proof,
) -> Result<crate::AttemptSnapshot, ApprovalReadError> {
    let cost = format!(
        "SELECT CAST(COALESCE(octet_length(value),0) AS BIGINT) FROM ({ATTEMPT}) carrier(value)"
    );
    super::super::read_proof::charge(
        tx,
        sqlx::query_scalar(&cost)
            .bind(&record.session_id.0)
            .bind(&record.run_id.0),
        proof,
    )
    .await
    .map_err(ApprovalReadError::Budget)?;
    let text: Option<String> = sqlx::query_scalar(ATTEMPT)
        .bind(&record.session_id.0)
        .bind(&record.run_id.0)
        .fetch_optional(&mut **tx)
        .await
        .map_err(super::super::db)
        .map_err(error)?
        .flatten();
    Ok(
        serde_json::from_str(&text.ok_or(Error::UnknownStoreOutcome)?)
            .map_err(|_| Error::UnknownStoreOutcome)?,
    )
}
const ATTEMPT: &str = "SELECT CASE WHEN octet_length(value)<=8192 THEN value END FROM (SELECT json_build_object('session_id',$1,'run_id',run_id,'target',json_build_object('id',id,'generation',generation),'state',state,'cleanup',cleanup_state)::text AS value FROM execution_attempts WHERE run_id=$2 ORDER BY generation DESC LIMIT 1) bounded";

pub(in super::super) async fn has_source(
    tx: &mut Transaction<'_, Postgres>,
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
    tx: &mut Transaction<'_, Postgres>,
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
    tx: &mut Transaction<'_, Postgres>,
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
            let row=sqlx::query("SELECT CAST(octet_length(data_json) AS BIGINT) AS bytes,CASE WHEN octet_length(data_json)<=$4 AND octet_length(data_json)<=4194304 THEN data_json END AS data FROM continuations WHERE session_id=$1 AND run_id=$2 AND generation=$3").bind(&record.session_id.0).bind(&record.run_id.0).bind(generation).bind(i64::try_from(proof.remaining()).map_err(unknown)?).fetch_optional(&mut **tx).await.map_err(unknown)?.ok_or_else(||unknown("missing successor"))?;
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
fn charge_rows(rows: &[sqlx::postgres::PgRow], proof: &mut Budget) -> Result<(), Failure> {
    for row in rows {
        proof.charge(
            usize::try_from(row.try_get::<i64, _>("bytes").map_err(unknown)?).map_err(unknown)?,
        )?;
    }
    Ok(())
}

/// Page every current identity and authenticate its immutable origin before
/// applying state labels. A forged Released state never hides a live source.
pub(in super::super) async fn session_with_proof(
    tx: &mut Transaction<'_, Postgres>,
    session: &SessionId,
    proof: &mut Proof,
) -> Result<(), ApprovalReadError> {
    let mut after = String::new();
    let mut count = 0_usize;
    let mut active = false;
    loop {
        let rows=sqlx::query("SELECT CASE WHEN octet_length(id)<=128 THEN id END AS id,CASE WHEN octet_length(run_id)<=128 THEN run_id END AS run_id,generation,CAST(octet_length(id)+octet_length(run_id) AS BIGINT) AS bytes FROM continuations WHERE session_id=$1 AND id COLLATE \"C\">$2 ORDER BY id COLLATE \"C\" LIMIT 32").bind(&session.0).bind(&after).fetch_all(&mut **tx).await.map_err(error)?;
        if rows.is_empty() {
            break;
        }
        for row in &rows {
            proof
                .charge(
                    usize::try_from(row.try_get::<i64, _>("bytes").map_err(error)?)
                        .map_err(error)?,
                )
                .map_err(ApprovalReadError::Budget)?;
        }
        for row in rows {
            let id = row
                .try_get::<Option<String>, _>("id")
                .map_err(error)?
                .ok_or(Error::UnknownStoreOutcome)?;
            let run = RunId(
                row.try_get::<Option<String>, _>("run_id")
                    .map_err(error)?
                    .ok_or(Error::UnknownStoreOutcome)?,
            );
            let target = crate::ContinuationTarget {
                id: crabber_core::ContinuationId(id.clone()),
                generation: crabber_core::ContinuationGeneration::new(
                    u64::try_from(row.try_get::<i64, _>("generation").map_err(error)?)
                        .map_err(error)?,
                )
                .map_err(error)?,
            };
            let record = exact_scoped_with_proof(tx, session, &run, &target, proof).await?;
            let facts = super::origin::verify_with_proof(tx, &record, proof).await?;
            verify_status_with_proof(tx, facts.status, &record, false, proof).await?;
            if record.state != crate::ApprovalContinuationState::Released {
                if active {
                    return Err(Error::UnknownStoreOutcome.into());
                }
                active = true;
            }
            count = count.checked_add(1).ok_or(Error::UnknownStoreOutcome)?;
            after = id;
        }
    }
    let carrier = super::super::tool_continuation::current::CARRIER;
    let query = format!(
        "SELECT COUNT(*) FROM events e WHERE e.session_id=$1 AND {carrier}#>>'{{kind}}'='continuation_changed' AND {carrier}#>>'{{payload,kind}}' IS NULL AND {carrier}#>>'{{payload,outcome,operation}}'='suspended'"
    );
    let origins: i64 = sqlx::query_scalar(&query)
        .bind(&session.0)
        .fetch_one(&mut **tx)
        .await
        .map_err(error)?;
    if usize::try_from(origins).ok() != Some(count) {
        return Err(Error::UnknownStoreOutcome.into());
    }
    if count == 0 {
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM continuation_barriers WHERE session_id=$1)",
        )
        .bind(&session.0)
        .fetch_one(&mut **tx)
        .await
        .map_err(error)?;
        if exists {
            return Err(Error::UnknownStoreOutcome.into());
        }
    }
    Ok(())
}
