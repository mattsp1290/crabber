use super::super::db;
use crate::{
    PhaseError as Error, PhaseMutation, PhaseReceipt, PhaseSnapshot,
    phase::{self, PhaseRecord, sql},
};
use crabber_core::{
    ControlId, EventCursor, InputFingerprint, PhaseId, Run, RunId, SessionId, SessionRevision,
    ToolCallId,
};
use sqlx::{Postgres, Row, Transaction};
use time::OffsetDateTime;

const BOUNDS: &str = "octet_length(s.id)<=512 AND octet_length(p.run_id)<=128 AND octet_length(p.id)<=128 AND octet_length(p.call_id)<=512 AND octet_length(p.attempt_id)<=128 AND octet_length(p.state)<=32 AND octet_length(p.quiescence_state)<=32 AND octet_length(p.outcome_json)<=4198400";
const SNAPSHOT: &str = "json_build_object('session_id',s.id,'run_id',p.run_id,'target',json_build_object('id',p.id,'generation',p.generation,'call_id',p.call_id,'attempt',json_build_object('id',p.attempt_id,'generation',a.generation)),'call_binding',(p.outcome_json::json->>'call_binding'),'state',p.state,'quiescence',p.quiescence_state)";
const FROM: &str = "FROM tool_phases p JOIN execution_attempts a ON a.run_id=p.run_id AND a.id=p.attempt_id JOIN runs r ON r.id=p.run_id JOIN sessions s ON s.id=r.session_id WHERE p.run_id=$1 AND (CAST($2 AS TEXT) IS NULL OR p.id=$2) AND (CAST($3 AS TEXT) IS NULL OR p.call_id=$3) ORDER BY p.generation DESC LIMIT 1";
const RECEIPT: &str = "SELECT CASE WHEN r.kind='phase' THEN 'phase' ELSE 'foreign' END AS kind,CASE WHEN octet_length(r.semantic_digest)<=64 THEN r.semantic_digest END AS semantic_digest,r.event_seq,CASE WHEN octet_length(e.run_id)<=128 THEN e.run_id END AS event_run_id,CASE WHEN octet_length(r.data_json)<=8192 THEN r.data_json END AS data_json,CASE WHEN r.kind='phase' AND octet_length(e.data::text)<=32768 THEN e.data END AS event_data FROM operation_receipts r LEFT JOIN events e ON e.seq=r.event_seq AND e.session_id=r.session_id WHERE r.session_id=$1 AND r.operation_id=$2";
const SAVE: &str = "UPDATE tool_phases SET claim_token=$3,state=$4,quiescence_state=$5,outcome_json=$6 WHERE run_id=$1 AND id=$2 AND generation=$7 AND call_id=$8 AND attempt_id=$9";
const INSERT: &str = "INSERT INTO tool_phases(run_id,id,claim_token,state,quiescence_state,outcome_json,generation,call_id,attempt_id) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)";

async fn row(
    tx: &mut Transaction<'_, Postgres>,
    run: &RunId,
    id: Option<&PhaseId>,
    call: Option<&ToolCallId>,
    full: bool,
    lock: bool,
) -> Result<Option<(Option<String>, SessionRevision)>, Error> {
    let query = record_query(full, lock);
    sqlx::query(&query)
        .bind(&run.0)
        .bind(id.map(|v| v.0.as_str()))
        .bind(call.map(|v| v.0.as_str()))
        .fetch_optional(&mut **tx)
        .await
        .map_err(db)
        .map_err(sql::unknown)?
        .map(|row| {
            Ok((
                row.get("record"),
                sql::revision(row.get("public_revision"))?,
            ))
        })
        .transpose()
}
fn record_query(full: bool, lock: bool) -> String {
    let projection = if full {
        format!(
            "json_build_object('version',(p.outcome_json::json->'version'),'snapshot',{SNAPSHOT},'token',p.claim_token,'output',(p.outcome_json::json->'output'))"
        )
    } else {
        format!(
            "json_build_object('version',(p.outcome_json::json->'version'),'snapshot',{SNAPSHOT})"
        )
    };
    let private_bound = if full {
        " AND octet_length(p.claim_token)<=512"
    } else {
        ""
    };
    let projection = format!(
        "CASE WHEN octet_length(p.outcome_json::json->>'call_binding')<=64{private_bound} THEN {projection} END"
    );
    let projection = format!("({projection})::text");
    let mut query = format!(
        "SELECT s.public_revision, CASE WHEN {BOUNDS} THEN {projection} END AS record {FROM}"
    );
    if lock {
        query.push_str(" FOR UPDATE OF p");
    }
    query
}
pub(super) async fn metadata(
    tx: &mut Transaction<'_, Postgres>,
    run: &RunId,
    id: &PhaseId,
) -> Result<Option<(PhaseSnapshot, SessionRevision)>, Error> {
    row(tx, run, Some(id), None, false, false)
        .await?
        .map(|(data, revision)| Ok((sql::decode_metadata(data)?, revision)))
        .transpose()
}
pub(in super::super) async fn load(
    tx: &mut Transaction<'_, Postgres>,
    run: &RunId,
    id: &PhaseId,
) -> Result<Option<PhaseRecord>, Error> {
    row(tx, run, Some(id), None, true, true)
        .await?
        .map(|(data, _)| sql::decode_record(data))
        .transpose()
}
pub(in super::super) async fn latest(
    tx: &mut Transaction<'_, Postgres>,
    run: &RunId,
    call: &ToolCallId,
) -> Result<Option<PhaseSnapshot>, Error> {
    row(tx, run, None, Some(call), false, true)
        .await?
        .map(|(data, _)| sql::decode_metadata(data))
        .transpose()
}
pub(super) async fn latest_with_proof(
    tx: &mut Transaction<'_, Postgres>,
    run: &RunId,
    call: &ToolCallId,
    proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<Option<PhaseSnapshot>, Error> {
    super::super::read_proof::charge(tx, sqlx::query_scalar("SELECT CAST(SUM(octet_length(outcome_json)+4096) AS BIGINT) FROM tool_phases WHERE run_id=$1 AND call_id=$2").bind(&run.0).bind(&call.0), proof).await.map_err(|_| Error::UnknownStoreOutcome)?;
    latest(tx, run, call).await
}
pub(super) async fn receipt(
    tx: &mut Transaction<'_, Postgres>,
    session: &SessionId,
    id: &ControlId,
) -> Result<Option<PhaseReceipt>, Error> {
    sqlx::query(RECEIPT)
        .bind(&session.0)
        .bind(&id.0)
        .fetch_optional(&mut **tx)
        .await
        .map_err(db)
        .map_err(sql::unknown)?
        .map(|row| {
            if row.get::<String, _>("kind") != "phase" {
                return Err(Error::SemanticConflict);
            }
            let event = crate::public_state::decode_event(
                row.get::<Option<sqlx::types::Json<serde_json::Value>>, _>("event_data")
                    .ok_or(Error::UnknownStoreOutcome)?,
            )
            .map_err(sql::unknown)?;
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
                != Some(&result.phase.run_id.0)
            {
                return Err(Error::UnknownStoreOutcome);
            }
            Ok(result)
        })
        .transpose()
}
pub(super) async fn revision(
    tx: &mut Transaction<'_, Postgres>,
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
pub(super) async fn exists(
    tx: &mut Transaction<'_, Postgres>,
    id: &PhaseId,
) -> Result<bool, Error> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM tool_phases WHERE id=$1)")
        .bind(&id.0)
        .fetch_one(&mut **tx)
        .await
        .map_err(db)
        .map_err(sql::unknown)
}
pub(in super::super) async fn save(
    tx: &mut Transaction<'_, Postgres>,
    record: &PhaseRecord,
    insert: bool,
) -> Result<(), Error> {
    record.validate()?;
    let target = &record.snapshot.target;
    let result = sqlx::query(if insert { INSERT } else { SAVE })
        .bind(&record.snapshot.run_id.0)
        .bind(&target.id.0)
        .bind(&record.token)
        .bind(sql::enum_text(record.snapshot.state)?)
        .bind(sql::enum_text(record.snapshot.quiescence)?)
        .bind(sql::encode_outcome(record)?)
        .bind(i64::try_from(target.generation.get()).map_err(|_| Error::GenerationExhausted)?)
        .bind(&target.call_id.0)
        .bind(&target.attempt.id.0)
        .execute(&mut **tx)
        .await
        .map_err(db)
        .map_err(sql::unknown)?;
    if result.rows_affected() != 1 {
        return Err(Error::UnknownStoreOutcome);
    }
    Ok(())
}
pub(super) async fn publish(
    tx: &mut Transaction<'_, Postgres>,
    run: &Run,
    request: &PhaseMutation,
    snapshot: &PhaseSnapshot,
    digest: InputFingerprint,
    revision: SessionRevision,
    now: OffsetDateTime,
) -> Result<(PhaseReceipt, crabber_core::EventRecord), Error> {
    let mut event = phase::event(run, request, snapshot, now, digest.clone())?;
    let seq: i64 = sqlx::query_scalar(
        "INSERT INTO events(session_id,run_id,data) VALUES($1,$2,$3) RETURNING seq",
    )
    .bind(&run.session_id.0)
    .bind(&run.id.0)
    .bind(crate::public_state::encode_event(&event).map_err(sql::unknown)?)
    .fetch_one(&mut **tx)
    .await
    .map_err(db)
    .map_err(sql::unknown)?;
    super::super::bump_public_revision(tx, &run.session_id)
        .await
        .map_err(sql::unknown)?;
    let receipt = PhaseReceipt {
        operation: phase::operation(&request.command),
        resolver: phase::resolver(&request.command),
        operation_id: request.id.clone(),
        semantic_digest: digest,
        revision,
        event_cursor: EventCursor(u64::try_from(seq).map_err(|_| Error::UnknownStoreOutcome)?),
        phase: snapshot.clone(),
    };
    sqlx::query("INSERT INTO operation_receipts(session_id,operation_id,kind,semantic_digest,event_seq,data_json) VALUES($1,$2,'phase',$3,$4,$5)")
        .bind(&run.session_id.0).bind(&request.id.0).bind(receipt.semantic_digest.as_str()).bind(seq).bind(serde_json::to_string(&receipt).map_err(|_| Error::UnknownStoreOutcome)?)
        .execute(&mut **tx).await.map_err(db).map_err(sql::unknown)?;
    event.cursor = Some(receipt.event_cursor);
    Ok((receipt, event))
}

pub(super) async fn page(
    tx: &mut Transaction<'_, Postgres>,
    request: &crate::PhasePageRequest,
) -> Result<crate::PhasePage, Error> {
    let revision:Option<i64>=sqlx::query_scalar("SELECT s.public_revision FROM sessions s JOIN runs r ON r.session_id=s.id WHERE s.id=$1 AND r.id=$2")
        .bind(&request.session_id.0).bind(&request.run_id.0).fetch_optional(&mut **tx).await.map_err(db).map_err(sql::unknown)?;
    let revision = sql::revision(revision.ok_or(Error::NotFound)?)?;
    if request
        .expected_revision
        .is_some_and(|expected| expected != revision)
    {
        return Err(Error::RevisionConflict);
    }
    let query = page_query();
    let rows = sqlx::query(&query)
        .bind(&request.run_id.0)
        .bind(request.after.as_ref().map(|id| id.0.as_str()))
        .bind(i64::from(request.limit) + 1)
        .fetch_all(&mut **tx)
        .await
        .map_err(db)
        .map_err(sql::unknown)?;
    let phases = rows
        .into_iter()
        .map(|row| sql::decode_metadata(row.get("record")))
        .collect::<Result<Vec<_>, _>>()?;
    request.finish(revision, phases)
}

use crate::public_session::v2::proof::{Proof, ReadError};
pub(in super::super) async fn load_with_proof(
    tx: &mut Transaction<'_, Postgres>,
    run: &RunId,
    id: &PhaseId,
    proof: &mut Proof,
) -> Result<Option<PhaseRecord>, ReadError<Error>> {
    let record = record_query(true, false);
    let cost = format!(
        "SELECT CAST(GREATEST(COALESCE(octet_length(record),0),COALESCE(octet_length(p.outcome_json),0)+COALESCE(octet_length(p.claim_token),0)+COALESCE(octet_length(p.id),0)+COALESCE(octet_length(p.call_id),0)+COALESCE(octet_length(p.attempt_id),0)+COALESCE(octet_length(p.run_id),0)+COALESCE(octet_length(p.state),0)+COALESCE(octet_length(p.quiescence_state),0)) AS BIGINT) FROM ({record}) bounded_phase JOIN tool_phases p ON p.run_id=$1 AND p.id=$2"
    );
    super::super::read_proof::charge(
        tx,
        sqlx::query_scalar(&cost)
            .bind(&run.0)
            .bind(&id.0)
            .bind(Option::<&str>::None),
        proof,
    )
    .await
    .map_err(ReadError::Budget)?;
    Ok(row(tx, run, Some(id), None, true, false)
        .await?
        .map(|(data, _)| sql::decode_record(data))
        .transpose()?)
}
pub(in super::super) async fn receipt_with_proof(
    tx: &mut Transaction<'_, Postgres>,
    session: &SessionId,
    id: &ControlId,
    proof: &mut Proof,
) -> Result<Option<PhaseReceipt>, ReadError<Error>> {
    crate::attempt::validate_identifier(&id.0).map_err(|_| Error::InvalidRequest)?;
    super::super::read_proof::charge(
        tx,
        sqlx::query_scalar(RECEIPT_PROOF_COST)
            .bind(&session.0)
            .bind(&id.0),
        proof,
    )
    .await
    .map_err(ReadError::Budget)?;
    Ok(receipt(tx, session, id).await?)
}
const RECEIPT_PROOF_COST: &str = "SELECT CAST(COALESCE(octet_length(r.data_json),0)+COALESCE(octet_length(r.semantic_digest),0)+7+COALESCE(octet_length(e.run_id),0)+CASE WHEN r.kind='phase' THEN COALESCE(octet_length(e.data::text),0) ELSE 0 END AS BIGINT) FROM operation_receipts r LEFT JOIN events e ON e.session_id=r.session_id AND e.seq=r.event_seq WHERE r.session_id=$1 AND r.operation_id=$2";

fn page_query() -> String {
    format!(
        "SELECT CASE WHEN {BOUNDS} AND octet_length(p.outcome_json::json->>'call_binding')<=64 THEN (json_build_object('version',(p.outcome_json::json->'version'),'snapshot',{SNAPSHOT}))::text END AS record FROM tool_phases p LEFT JOIN execution_attempts a ON a.run_id=p.run_id AND a.id=p.attempt_id LEFT JOIN runs r ON r.id=p.run_id LEFT JOIN sessions s ON s.id=r.session_id WHERE p.run_id=$1 AND (CAST($2 AS TEXT) IS NULL OR p.id COLLATE \"C\">$2 COLLATE \"C\") ORDER BY p.id COLLATE \"C\" LIMIT $3"
    )
}
pub(super) async fn page_with_proof(
    tx: &mut Transaction<'_, Postgres>,
    request: &crate::PhasePageRequest,
    proof: &mut Proof,
) -> Result<crate::PhasePage, Error> {
    proof.charge(8).map_err(|_| Error::UnknownStoreOutcome)?;
    let query = page_query();
    let cost = format!(
        "SELECT CAST(SUM(octet_length(record)) AS BIGINT) FROM ({query}) selected_phase_page"
    );
    super::super::read_proof::charge(
        tx,
        sqlx::query_scalar(&cost)
            .bind(&request.run_id.0)
            .bind(request.after.as_ref().map(|id| id.0.as_str()))
            .bind(i64::from(request.limit) + 1),
        proof,
    )
    .await
    .map_err(|_| Error::UnknownStoreOutcome)?;
    page(tx, request).await
}

pub(super) async fn latest_metadata_with_proof(
    tx: &mut Transaction<'_, Postgres>,
    run: &RunId,
    call: &ToolCallId,
    proof: &mut Proof,
) -> Result<Option<PhaseSnapshot>, Error> {
    let query = record_query(false, false);
    let cost = format!("SELECT CAST(octet_length(record) AS BIGINT) FROM ({query}) selected_phase");
    super::super::read_proof::charge(
        tx,
        sqlx::query_scalar(&cost)
            .bind(&run.0)
            .bind(Option::<&str>::None)
            .bind(&call.0),
        proof,
    )
    .await
    .map_err(|_| Error::UnknownStoreOutcome)?;
    latest(tx, run, call).await
}
