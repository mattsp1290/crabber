//! Independently protected original input births under the enclosing transaction.
use crate::continuation_input::{Domain, Error, Input, Kind, Receipt, Scope, Source};
use crate::public_session::v2::proof::{Proof, ReadError};
use crate::transaction::Transaction;
use sqlx::{Row, Sqlite};
type Result<T> = std::result::Result<T, ReadError<Error>>;
fn bad(_: impl std::fmt::Debug) -> ReadError<Error> {
    ReadError::Domain(Error)
}
fn tables(domain: Domain) -> (&'static str, &'static str, &'static str) {
    match domain {
        Domain::Approval => (
            "continuations",
            "approval_input_sources",
            "approval_input_cohorts",
        ),
        Domain::Tool => (
            "tool_continuations",
            "tool_input_sources",
            "tool_input_cohorts",
        ),
    }
}
pub(super) async fn publish(
    tx: &mut Transaction<'_, Sqlite>,
    input: Input<'_>,
    receipt: Receipt<'_>,
    event: &crabber_core::EventRecord,
    proof: &mut Proof,
) -> Result<()> {
    let source = Source::new(input, receipt, event).map_err(bad)?;
    let scope = &source.scope;
    let (_, sources, cohorts) = tables(scope.domain);
    if source.kind == Kind::Answer && !protocol(tx, scope, proof).await? {
        return Ok(());
    }
    let raw = serde_json::to_string(&source).map_err(bad)?;
    let kind = match source.kind {
        Kind::Suspension => "suspension",
        Kind::Answer => "answer",
    };
    let sql = format!(
        "INSERT INTO {sources}(session_id,run_id,continuation_id,generation,operation_id,kind,subject,source_digest,event_seq,data_json) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)"
    );
    sqlx::query(&sql)
        .bind(&scope.session.0)
        .bind(&scope.run.0)
        .bind(&scope.continuation.id.0)
        .bind(i64::try_from(scope.continuation.generation.get()).map_err(bad)?)
        .bind(&source.operation.0)
        .bind(kind)
        .bind(&source.subject)
        .bind(source.digest.as_str())
        .bind(i64::try_from(source.cursor.0).map_err(bad)?)
        .bind(raw)
        .execute(&mut **tx)
        .await
        .map_err(bad)?;
    if source.kind == Kind::Suspension {
        let sql = format!(
            "INSERT INTO {cohorts}(session_id,run_id,continuation_id,generation,origin_operation,origin_digest) VALUES($1,$2,$3,$4,$5,$6)"
        );
        sqlx::query(&sql)
            .bind(&scope.session.0)
            .bind(&scope.run.0)
            .bind(&scope.continuation.id.0)
            .bind(i64::try_from(scope.continuation.generation.get()).map_err(bad)?)
            .bind(&source.operation.0)
            .bind(source.digest.as_str())
            .execute(&mut **tx)
            .await
            .map_err(bad)?;
    }
    Ok(())
}
pub(super) async fn protocol(
    tx: &mut Transaction<'_, Sqlite>,
    scope: &Scope,
    proof: &mut Proof,
) -> Result<bool> {
    scope.validate().map_err(bad)?;
    let (current, sources, cohorts) = tables(scope.domain);
    proof
        .measure(&(scope, 0_i64, 0_i64, 0_i64))
        .map_err(ReadError::Budget)?;
    let sql = format!(
        "SELECT CAST(c.input_source_protocol AS INTEGER) AS protocol,EXISTS(SELECT 1 FROM {cohorts} b WHERE b.session_id=$1 AND b.run_id=$2 AND b.continuation_id=$3 AND b.generation=$4) AS cohort,EXISTS(SELECT 1 FROM {sources} s WHERE s.session_id=$1 AND s.run_id=$2 AND s.continuation_id=$3 AND s.generation=$4) AS source FROM {current} c WHERE c.session_id=$1 AND c.run_id=$2 AND c.id=$3 AND c.generation=$4"
    );
    let row = sqlx::query(&sql)
        .bind(&scope.session.0)
        .bind(&scope.run.0)
        .bind(&scope.continuation.id.0)
        .bind(i64::try_from(scope.continuation.generation.get()).map_err(bad)?)
        .fetch_optional(&mut **tx)
        .await
        .map_err(bad)?
        .ok_or(Error)?;
    let protocol: i32 = row.try_get("protocol").map_err(bad)?;
    let cohort: bool = row.try_get("cohort").map_err(bad)?;
    let source: bool = row.try_get("source").map_err(bad)?;
    match (protocol, cohort, source) {
        (0, false, false) => Ok(false),
        (1, true, true) => {
            origin(tx, scope, proof).await?;
            Ok(true)
        }
        _ => Err(Error.into()),
    }
}
async fn origin(tx: &mut Transaction<'_, Sqlite>, scope: &Scope, proof: &mut Proof) -> Result<()> {
    let (_, _, cohorts) = tables(scope.domain);
    let cost = format!(
        "SELECT CAST(length(CAST(origin_operation AS BLOB))+length(CAST(origin_digest AS BLOB))+length(CAST(origin_kind AS BLOB))+length(CAST(origin_subject AS BLOB))+16 AS BIGINT) FROM {cohorts} WHERE session_id=$1 AND run_id=$2 AND continuation_id=$3 AND generation=$4"
    );
    charge_scope(tx, &cost, scope, proof).await?;
    let sql = format!(
        "SELECT CASE WHEN length(CAST(origin_operation AS BLOB))<=128 THEN origin_operation END AS operation,CASE WHEN length(CAST(origin_digest AS BLOB))=64 THEN origin_digest END AS digest,CASE WHEN origin_kind='suspension' THEN 'suspension' END AS origin_kind,CASE WHEN origin_subject='suspension' THEN 'suspension' END AS origin_subject,protocol_version FROM {cohorts} WHERE session_id=$1 AND run_id=$2 AND continuation_id=$3 AND generation=$4"
    );
    let row = sqlx::query(&sql)
        .bind(&scope.session.0)
        .bind(&scope.run.0)
        .bind(&scope.continuation.id.0)
        .bind(i64::try_from(scope.continuation.generation.get()).map_err(bad)?)
        .fetch_one(&mut **tx)
        .await
        .map_err(bad)?;
    let operation = crabber_core::ControlId(
        row.try_get::<Option<String>, _>("operation")
            .map_err(bad)?
            .ok_or(Error)?,
    );
    let digest = row
        .try_get::<Option<String>, _>("digest")
        .map_err(bad)?
        .ok_or(Error)?;
    if row.try_get::<i32, _>("protocol_version").map_err(bad)? != 1
        || row
            .try_get::<Option<String>, _>("origin_kind")
            .map_err(bad)?
            .as_deref()
            != Some("suspension")
        || row
            .try_get::<Option<String>, _>("origin_subject")
            .map_err(bad)?
            .as_deref()
            != Some("suspension")
    {
        return Err(Error.into());
    }
    let source = load(tx, scope, &operation, proof).await?;
    if source.kind != Kind::Suspension || source.digest.as_str() != digest {
        return Err(Error.into());
    }
    Ok(())
}
pub(super) async fn verify(
    tx: &mut Transaction<'_, Sqlite>,
    input: Input<'_>,
    receipt: Receipt<'_>,
    event: &crabber_core::EventRecord,
    proof: &mut Proof,
) -> Result<bool> {
    proof.measure(&input).map_err(ReadError::Budget)?;
    let desired = Source::new(input, receipt, event).map_err(bad)?;
    if !protocol(tx, &desired.scope, proof).await? {
        return Ok(false);
    }
    let source = load(tx, &desired.scope, &desired.operation, proof).await?;
    if source != desired {
        return Err(Error.into());
    }
    Ok(true)
}
async fn load(
    tx: &mut Transaction<'_, Sqlite>,
    scope: &Scope,
    operation: &crabber_core::ControlId,
    proof: &mut Proof,
) -> Result<Source> {
    let (_, sources, _) = tables(scope.domain);
    let cost = format!(
        "SELECT CAST(length(CAST(session_id AS BLOB))+length(CAST(run_id AS BLOB))+length(CAST(continuation_id AS BLOB))+length(CAST(operation_id AS BLOB))+length(CAST(kind AS BLOB))+length(CAST(subject AS BLOB))+length(CAST(source_digest AS BLOB))+length(CAST(data_json AS BLOB))+16 AS BIGINT) FROM {sources} WHERE session_id=$1 AND operation_id=$2"
    );
    super::read_proof::charge(
        tx,
        sqlx::query_scalar(&cost)
            .bind(&scope.session.0)
            .bind(&operation.0),
        proof,
    )
    .await
    .map_err(ReadError::Budget)?;
    let sql = format!(
        "SELECT CASE WHEN length(CAST(data_json AS BLOB))<=4096 THEN data_json END AS data,CASE WHEN length(CAST(kind AS BLOB))<=16 THEN kind END AS kind,CASE WHEN length(CAST(subject AS BLOB))<=128 THEN subject END AS subject,CASE WHEN length(CAST(source_digest AS BLOB))=64 THEN source_digest END AS digest,event_seq,CASE WHEN length(CAST(run_id AS BLOB))<=128 THEN run_id END AS run_id,CASE WHEN length(CAST(continuation_id AS BLOB))<=128 THEN continuation_id END AS continuation_id,generation,protocol_version FROM {sources} WHERE session_id=$1 AND operation_id=$2"
    );
    let row = sqlx::query(&sql)
        .bind(&scope.session.0)
        .bind(&operation.0)
        .fetch_optional(&mut **tx)
        .await
        .map_err(bad)?
        .ok_or(Error)?;
    let source: Source = serde_json::from_str(
        &row.try_get::<Option<String>, _>("data")
            .map_err(bad)?
            .ok_or(Error)?,
    )
    .map_err(bad)?;
    source.validate().map_err(bad)?;
    let kind = match source.kind {
        Kind::Suspension => "suspension",
        Kind::Answer => "answer",
    };
    if row
        .try_get::<Option<String>, _>("run_id")
        .map_err(bad)?
        .as_deref()
        != Some(&scope.run.0)
        || row
            .try_get::<Option<String>, _>("continuation_id")
            .map_err(bad)?
            .as_deref()
            != Some(&scope.continuation.id.0)
        || row.try_get::<i64, _>("generation").map_err(bad)?
            != i64::try_from(scope.continuation.generation.get()).map_err(bad)?
        || row.try_get::<i32, _>("protocol_version").map_err(bad)? != 1
        || source.scope != *scope
        || source.operation != *operation
        || row
            .try_get::<Option<String>, _>("kind")
            .map_err(bad)?
            .as_deref()
            != Some(kind)
        || row
            .try_get::<Option<String>, _>("subject")
            .map_err(bad)?
            .as_deref()
            != Some(&source.subject)
        || row
            .try_get::<Option<String>, _>("digest")
            .map_err(bad)?
            .as_deref()
            != Some(source.digest.as_str())
        || row.try_get::<i64, _>("event_seq").map_err(bad)?
            != i64::try_from(source.cursor.0).map_err(bad)?
    {
        return Err(Error.into());
    }
    Ok(source)
}
async fn charge_scope(
    tx: &mut Transaction<'_, Sqlite>,
    sql: &str,
    scope: &Scope,
    proof: &mut Proof,
) -> Result<()> {
    super::read_proof::charge(
        tx,
        sqlx::query_scalar(sql)
            .bind(&scope.session.0)
            .bind(&scope.run.0)
            .bind(&scope.continuation.id.0)
            .bind(i64::try_from(scope.continuation.generation.get()).map_err(bad)?),
        proof,
    )
    .await
    .map_err(ReadError::Budget)
}
pub(super) async fn subjects(
    tx: &mut Transaction<'_, Sqlite>,
    scope: &Scope,
    proof: &mut Proof,
) -> Result<Vec<(String, crabber_core::ControlId)>> {
    let (_, sources, _) = tables(scope.domain);
    let selected = format!(
        "SELECT subject,operation_id FROM {sources} WHERE session_id=$1 AND run_id=$2 AND continuation_id=$3 AND generation=$4 AND kind='answer' ORDER BY subject COLLATE BINARY LIMIT 65"
    );
    let cost = format!(
        "SELECT CAST(COALESCE(SUM(length(CAST(subject AS BLOB))+length(CAST(operation_id AS BLOB))),0) AS BIGINT) FROM ({selected}) bounded"
    );
    charge_scope(tx, &cost, scope, proof).await?;
    let sql = format!(
        "SELECT CASE WHEN length(CAST(subject AS BLOB))<=128 THEN subject END AS subject,CASE WHEN length(CAST(operation_id AS BLOB))<=128 THEN operation_id END AS operation_id FROM ({selected}) bounded"
    );
    let rows = sqlx::query(&sql)
        .bind(&scope.session.0)
        .bind(&scope.run.0)
        .bind(&scope.continuation.id.0)
        .bind(i64::try_from(scope.continuation.generation.get()).map_err(bad)?)
        .fetch_all(&mut **tx)
        .await
        .map_err(bad)?;
    if rows.len() > 64 {
        return Err(Error.into());
    }
    rows.into_iter()
        .map(|row| {
            let subject = row
                .try_get::<Option<String>, _>("subject")
                .map_err(bad)?
                .ok_or(Error)?;
            let operation = row
                .try_get::<Option<String>, _>("operation_id")
                .map_err(bad)?
                .ok_or(Error)?;
            crate::attempt::validate_identifier(&subject).map_err(bad)?;
            crate::attempt::validate_identifier(&operation).map_err(bad)?;
            Ok((subject, crabber_core::ControlId(operation)))
        })
        .collect()
}

pub(super) async fn accept(
    tx: &mut Transaction<'_, Sqlite>,
    input: Input<'_>,
    receipt: Receipt<'_>,
) -> Result<()> {
    let mut proof = Proof::bounded(32 << 20);
    proof.measure(&input).map_err(ReadError::Budget)?;
    proof.measure(&receipt).map_err(ReadError::Budget)?;
    let session = match receipt {
        Receipt::Approval(r) => &r.session_id,
        Receipt::Tool(r) => &r.session_id,
    };
    let event = match receipt {
        Receipt::Approval(_) => {
            super::read_proof::event(tx, session, receipt.cursor(), false, &mut proof).await
        }
        Receipt::Tool(_) => {
            super::read_proof::control_event(tx, session, receipt.cursor(), &mut proof).await
        }
    }
    .map_err(|e| e.map_domain(|_| Error))?;
    publish(tx, input, receipt, &event, &mut proof).await
}

pub(super) async fn answers(
    tx: &mut Transaction<'_, Sqlite>,
    scope: &Scope,
    inputs: &[Input<'_>],
    proof: &mut Proof,
) -> Result<()> {
    if !protocol(tx, scope, proof).await? {
        return Ok(());
    }
    if inputs.len() > 64 {
        return Err(Error.into());
    }
    let mut expected = Vec::with_capacity(inputs.len());
    for input in inputs {
        let (subject, operation) = input.answer_identity().map_err(bad)?;
        match input {
            Input::ApprovalAnswer(_) => {
                let receipt =
                    super::approval::receipt_with_proof(tx, &scope.session, operation, proof)
                        .await
                        .map_err(|e| e.map_domain(|_| Error))?
                        .ok_or(Error)?;
                let event = super::read_proof::event(
                    tx,
                    &scope.session,
                    receipt.event_cursor,
                    false,
                    proof,
                )
                .await
                .map_err(|e| e.map_domain(|_| Error))?;
                verify(tx, *input, Receipt::Approval(&receipt), &event, proof).await?;
            }
            Input::ToolAnswer(_) => {
                let (receipt, event) = super::tool_continuation::read::receipt_with_proof(
                    tx,
                    &scope.session,
                    operation,
                    proof,
                )
                .await
                .map_err(|e| e.map_domain(|_| Error))?
                .ok_or(Error)?;
                verify(tx, *input, Receipt::Tool(&receipt), &event, proof).await?;
            }
            _ => return Err(Error.into()),
        }
        expected.push((subject.0.clone(), operation.clone()));
    }
    expected.sort();
    if subjects(tx, scope, proof).await? != expected {
        return Err(Error.into());
    }
    Ok(())
}

/// Scalar anti-joins bound client transfer, not the database's identity scan work.
pub(super) async fn consistent(
    tx: &mut Transaction<'_, Sqlite>,
    domain: Domain,
    session: &crabber_core::SessionId,
    run: Option<&crabber_core::RunId>,
    proof: &mut Proof,
) -> Result<()> {
    proof
        .measure(&(session, run, 1_u8))
        .map_err(ReadError::Budget)?;
    let (current, _, cohorts) = tables(domain);
    let sql = format!(
        "SELECT EXISTS(SELECT 1 FROM {cohorts} b WHERE b.session_id=$1 AND (CAST($2 AS TEXT) IS NULL OR b.run_id=$2) AND NOT EXISTS(SELECT 1 FROM {current} c WHERE c.session_id=b.session_id AND c.run_id=b.run_id AND c.id=b.continuation_id AND c.generation=b.generation AND c.input_source_protocol=1)) OR EXISTS(SELECT 1 FROM {current} c WHERE c.session_id=$1 AND (CAST($2 AS TEXT) IS NULL OR c.run_id=$2) AND c.input_source_protocol=1 AND NOT EXISTS(SELECT 1 FROM {cohorts} b WHERE b.session_id=c.session_id AND b.run_id=c.run_id AND b.continuation_id=c.id AND b.generation=c.generation AND b.protocol_version=1))"
    );
    let invalid: bool = sqlx::query_scalar(&sql)
        .bind(&session.0)
        .bind(run.map(|r| r.0.as_str()))
        .fetch_one(&mut **tx)
        .await
        .map_err(bad)?;
    if invalid {
        return Err(Error.into());
    }
    Ok(())
}

#[cfg(test)]
#[path = "continuation_input/tests.rs"]
mod tests;

pub(super) async fn accept_tool_suspension(
    tx: &mut Transaction<'_, Sqlite>,
    batch: &crate::ToolContinuationSuspension,
    receipt: &crate::ToolContinuationReceipt,
) -> Result<()> {
    accept(tx, Input::ToolSuspension(batch), Receipt::Tool(receipt)).await
}
