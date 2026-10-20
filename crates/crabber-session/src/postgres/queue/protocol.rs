//! Selected queue publications and fresh full-prefix verification.
use super::super::db;
use super::selection::Originals;
use crate::public_session::v2::proof::{Failure, Proof, ReadError};
use crate::{
    StoreError,
    queue::{
        Audit, corrupt, decode,
        source::{Scope, Source},
    },
};
type Result<T> = std::result::Result<T, ReadError<StoreError>>;
pub(super) fn domain(error: ReadError<StoreError>) -> StoreError {
    error.into_domain_or(StoreError::Validation(
        "queue proof limit or source failure".into(),
    ))
}
use crate::postgres::transaction::Transaction;
use crabber_core::{ControlId, SessionId};
use sqlx::{Postgres, Row};
fn budget(error: Failure) -> ReadError<StoreError> {
    ReadError::Budget(error)
}
async fn charge(
    tx: &mut Transaction<'_, Postgres>,
    query: &str,
    session: &str,
    id: &str,
    proof: &mut Proof,
) -> Result<()> {
    let bytes: Option<i64> = sqlx::query_scalar(query)
        .bind(session)
        .bind(id)
        .fetch_optional(&mut **tx)
        .await
        .map_err(db)?
        .flatten();
    proof
        .charge(usize::try_from(bytes.unwrap_or(0)).map_err(|_| corrupt())?)
        .map_err(budget)
}
pub(super) async fn protocol(
    tx: &mut Transaction<'_, Postgres>,
    scope: &Scope,
    proof: &mut Proof,
) -> Result<bool> {
    proof.measure(scope).map_err(budget)?;
    let marker: Option<i32> = sqlx::query_scalar("SELECT CAST(source_protocol AS INTEGER) FROM future_queue WHERE session_id=$1 AND queue_id=$2").bind(&scope.session.0).bind(&scope.queue.0).fetch_optional(&mut **tx).await.map_err(db)?;
    let cohort: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM queue_publication_cohorts WHERE session_id=$1 AND queue_id=$2)").bind(&scope.session.0).bind(&scope.queue.0).fetch_one(&mut **tx).await.map_err(db)?;
    let source: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM queue_publication_sources WHERE session_id=$1 AND queue_id=$2)").bind(&scope.session.0).bind(&scope.queue.0).fetch_one(&mut **tx).await.map_err(db)?;
    match (marker, cohort, source) {
        (Some(0) | None, false, false) => Ok(false),
        (Some(1), true, true) => Ok(true),
        _ => Err(corrupt().into()),
    }
}
async fn source(
    tx: &mut Transaction<'_, Postgres>,
    session: &SessionId,
    operation: &ControlId,
    proof: &mut Proof,
) -> Result<Source> {
    let allowance = i64::try_from(proof.budget().remaining()).map_err(|_| corrupt())?;
    charge(tx, "SELECT CAST(octet_length(data_json)+octet_length(queue_id)+octet_length(operation_id)+octet_length(kind)+COALESCE(octet_length(previous_digest),0)+octet_length(source_digest) AS BIGINT) FROM queue_publication_sources WHERE session_id=$1 AND operation_id=$2", &session.0, &operation.0, proof).await?;
    let row = sqlx::query("SELECT protocol_version,queue_sequence,sequence,revision,CASE WHEN octet_length(queue_id)<=128 THEN queue_id END AS queue_id,CASE WHEN octet_length(operation_id)<=128 THEN operation_id END AS operation_id,CASE WHEN octet_length(kind)<=16 THEN kind END AS kind,CASE WHEN octet_length(previous_digest)<=64 THEN previous_digest END AS previous_digest,CASE WHEN octet_length(source_digest)<=64 THEN source_digest END AS source_digest,CASE WHEN octet_length(data_json)<=4096 AND octet_length(data_json)<=$3 THEN data_json END AS data FROM queue_publication_sources WHERE session_id=$1 AND operation_id=$2").bind(&session.0).bind(&operation.0).bind(allowance).fetch_optional(&mut **tx).await.map_err(db)?.ok_or_else(corrupt)?;
    let value: Source = decode(
        &row.try_get::<Option<String>, _>("data")
            .map_err(db)?
            .ok_or_else(corrupt)?,
    )?;
    value.validate()?;
    if value.scope.session != *session
        || value.operation != *operation
        || row.try_get::<i32, _>("protocol_version").map_err(db)? != 1
        || row.try_get::<i64, _>("queue_sequence").map_err(db)?
            != i64::try_from(value.queue_sequence).map_err(|_| corrupt())?
        || row.try_get::<i64, _>("sequence").map_err(db)?
            != i64::try_from(value.sequence).map_err(|_| corrupt())?
        || row.try_get::<i64, _>("revision").map_err(db)?
            != i64::try_from(value.revision.get()).map_err(|_| corrupt())?
        || row
            .try_get::<Option<String>, _>("queue_id")
            .map_err(db)?
            .as_deref()
            != Some(&value.scope.queue.0)
        || row
            .try_get::<Option<String>, _>("operation_id")
            .map_err(db)?
            .as_deref()
            != Some(&value.operation.0)
        || row
            .try_get::<Option<String>, _>("kind")
            .map_err(db)?
            .as_deref()
            != Some(value.kind.name())
        || row
            .try_get::<Option<String>, _>("previous_digest")
            .map_err(db)?
            .as_deref()
            != value
                .previous
                .as_ref()
                .map(crabber_core::InputFingerprint::as_str)
        || row
            .try_get::<Option<String>, _>("source_digest")
            .map_err(db)?
            .as_deref()
            != Some(value.digest.as_str())
    {
        return Err(corrupt().into());
    }
    Ok(value)
}
async fn indexed(
    tx: &mut Transaction<'_, Postgres>,
    scope: &Scope,
    sequence: u64,
    proof: &mut Proof,
) -> Result<Source> {
    let raw = i64::try_from(sequence).map_err(|_| corrupt())?;
    let bytes: Option<i64> = sqlx::query_scalar("SELECT CAST(octet_length(operation_id) AS BIGINT) FROM queue_publication_sources WHERE session_id=$1 AND queue_id=$2 AND sequence=$3").bind(&scope.session.0).bind(&scope.queue.0).bind(raw).fetch_optional(&mut **tx).await.map_err(db)?;
    proof
        .charge(usize::try_from(bytes.ok_or_else(corrupt)?).map_err(|_| corrupt())?)
        .map_err(budget)?;
    let operation: Option<String> = sqlx::query_scalar("SELECT CASE WHEN octet_length(operation_id)<=128 THEN operation_id END FROM queue_publication_sources WHERE session_id=$1 AND queue_id=$2 AND sequence=$3").bind(&scope.session.0).bind(&scope.queue.0).bind(raw).fetch_one(&mut **tx).await.map_err(db)?;
    let value = source(
        tx,
        &scope.session,
        &ControlId(operation.ok_or_else(corrupt)?),
        proof,
    )
    .await?;
    if value.scope != *scope || value.sequence != sequence {
        return Err(corrupt().into());
    }
    Ok(value)
}
async fn cohort(
    tx: &mut Transaction<'_, Postgres>,
    scope: &Scope,
    proof: &mut Proof,
) -> Result<(Source, Source)> {
    charge(tx, "SELECT CAST(octet_length(origin_operation)+octet_length(origin_digest)+octet_length(head_digest) AS BIGINT) FROM queue_publication_cohorts WHERE session_id=$1 AND queue_id=$2", &scope.session.0, &scope.queue.0, proof).await?;
    let row = sqlx::query("SELECT protocol_version,head_sequence,CASE WHEN octet_length(origin_operation)<=128 THEN origin_operation END AS origin_operation,CASE WHEN octet_length(origin_digest)<=64 THEN origin_digest END AS origin_digest,CASE WHEN octet_length(head_digest)<=64 THEN head_digest END AS head_digest FROM queue_publication_cohorts WHERE session_id=$1 AND queue_id=$2").bind(&scope.session.0).bind(&scope.queue.0).fetch_one(&mut **tx).await.map_err(db)?;
    let origin = indexed(tx, scope, 1, proof).await?;
    let head = indexed(
        tx,
        scope,
        u64::try_from(row.try_get::<i64, _>("head_sequence").map_err(db)?)
            .map_err(|_| corrupt())?,
        proof,
    )
    .await?;
    if row.try_get::<i32, _>("protocol_version").map_err(db)? != 1
        || row
            .try_get::<Option<String>, _>("origin_operation")
            .map_err(db)?
            .as_deref()
            != Some(&origin.operation.0)
        || row
            .try_get::<Option<String>, _>("origin_digest")
            .map_err(db)?
            .as_deref()
            != Some(origin.digest.as_str())
        || row
            .try_get::<Option<String>, _>("head_digest")
            .map_err(db)?
            .as_deref()
            != Some(head.digest.as_str())
    {
        return Err(corrupt().into());
    }
    Ok((origin, head))
}
/// Original selection deliberately does not consult the latest head or routing labels.
pub(in crate::postgres) async fn admission_prefix(
    tx: &mut Transaction<'_, Postgres>,
    session: &SessionId,
    operation: &ControlId,
    proof: &mut Proof,
) -> Result<Option<Vec<Audit>>> {
    let Some((_, audit)) = super::reads::audited_leaf(tx, session, operation, proof).await? else {
        let orphan: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM future_queue_audit WHERE session_id=$1 AND operation_id=$2 UNION ALL SELECT 1 FROM queue_publication_sources WHERE session_id=$1 AND operation_id=$2)")
            .bind(&session.0).bind(&operation.0).fetch_one(&mut **tx).await.map_err(db)?;
        return if orphan {
            Err(corrupt().into())
        } else {
            Ok(None)
        };
    };
    let last = selected(tx, &audit, proof)
        .await?
        .ok_or(StoreError::HostUnsupported)?;
    let mut result = Vec::new();
    let mut previous: Option<Source> = None;
    for sequence in 1..=last.sequence {
        let source = indexed(tx, &last.scope, sequence, proof).await?;
        let (_, audit) = super::reads::audited_leaf(tx, session, &source.operation, proof)
            .await?
            .ok_or_else(corrupt)?;
        source.verify(&audit)?;
        if let Some(previous) = &previous {
            source.follows(previous)?;
        }
        result.push(audit);
        previous = Some(source);
    }
    if previous.as_ref() != Some(&last) {
        return Err(corrupt().into());
    }
    Ok(Some(result))
}
pub(super) async fn selected(
    tx: &mut Transaction<'_, Postgres>,
    audit: &Audit,
    proof: &mut Proof,
) -> Result<Option<Source>> {
    let scope = Scope {
        session: audit.receipt.session_id.clone(),
        queue: audit.receipt.summary.queue_id.clone(),
    };
    if !protocol(tx, &scope, proof).await? {
        return Ok(None);
    }
    let value = source(tx, &scope.session, &audit.receipt.operation_id, proof).await?;
    value.verify(audit)?;
    let origin = indexed(tx, &scope, 1, proof).await?;
    origin.validate()?;
    charge(tx, "SELECT CAST(octet_length(origin_operation)+octet_length(origin_digest) AS BIGINT) FROM queue_publication_cohorts WHERE session_id=$1 AND queue_id=$2", &scope.session.0, &scope.queue.0, proof).await?;
    let anchored: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM queue_publication_cohorts WHERE session_id=$1 AND queue_id=$2 AND origin_operation=$3 AND origin_digest=$4 AND protocol_version=1)").bind(&scope.session.0).bind(&scope.queue.0).bind(&origin.operation.0).bind(origin.digest.as_str()).fetch_one(&mut **tx).await.map_err(db)?;
    if !anchored {
        return Err(corrupt().into());
    }
    if value.sequence > 1 {
        value.follows(&indexed(tx, &scope, value.sequence - 1, proof).await?)?;
    }
    Ok(Some(value))
}
#[cfg(test)]
pub(super) async fn history(
    tx: &mut Transaction<'_, Postgres>,
    scope: &Scope,
    proof: &mut Proof,
) -> Result<Option<Source>> {
    Ok(full_history(tx, scope, proof, None)
        .await?
        .and_then(|originals| originals.head))
}
async fn full_history(
    tx: &mut Transaction<'_, Postgres>,
    scope: &Scope,
    proof: &mut Proof,
    record: Option<&crate::queue::Record>,
) -> Result<Option<Originals>> {
    if !protocol(tx, scope, proof).await? {
        return Ok(None);
    }
    let (origin, head) = cohort(tx, scope, proof).await?;
    let mut previous: Option<Source> = None;
    let (mut enqueue, mut prepare, mut claim) = (None, None, None);
    let mut prepare_source = None;
    for sequence in 1..=head.sequence {
        let value = indexed(tx, scope, sequence, proof).await?;
        let (_, audit) = super::reads::audited_leaf(tx, &scope.session, &value.operation, proof)
            .await?
            .ok_or_else(corrupt)?;
        value.verify(&audit)?;
        if let Some(previous) = &previous {
            value.follows(previous)?;
        } else if value != origin {
            return Err(corrupt().into());
        }
        if let Some(record) = record {
            if sequence == head.sequence && audit.receipt.summary != record.summary {
                return Err(corrupt().into());
            }
            if audit.receipt.operation_id == record.enqueue_operation {
                enqueue = Some(audit);
            } else if record.prepare_operation.as_ref() == Some(&audit.receipt.operation_id) {
                proof.measure(&value).map_err(budget)?;
                prepare_source = Some(value.clone());
                prepare = Some(audit);
            } else if record
                .claim
                .as_ref()
                .is_some_and(|c| c.operation == audit.receipt.operation_id)
            {
                claim = Some(audit);
            }
        }
        previous = Some(value);
    }
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM queue_publication_sources WHERE session_id=$1 AND queue_id=$2",
    )
    .bind(&scope.session.0)
    .bind(&scope.queue.0)
    .fetch_one(&mut **tx)
    .await
    .map_err(db)?;
    let audit_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM future_queue_audit WHERE session_id=$1 AND queue_id=$2",
    )
    .bind(&scope.session.0)
    .bind(&scope.queue.0)
    .fetch_one(&mut **tx)
    .await
    .map_err(db)?;
    if previous.as_ref() != Some(&head)
        || u64::try_from(count).ok() != Some(head.sequence)
        || audit_count != count
    {
        return Err(corrupt().into());
    }
    let originals = Originals {
        head: Some(head),
        enqueue,
        prepare,
        claim,
        prepare_source,
    };
    if let Some(record) = record {
        originals.prepared(record)?;
    }
    Ok(Some(originals))
}
pub(super) async fn publish(
    tx: &mut Transaction<'_, Postgres>,
    audit: &Audit,
    previous: Option<Source>,
    fresh: bool,
    proof: &mut Proof,
) -> Result<Option<Source>> {
    proof.measure(audit).map_err(budget)?;
    let value = if let Some(previous) = previous {
        Source::new(
            audit,
            previous.sequence.checked_add(1).ok_or_else(corrupt)?,
            Some(previous.digest),
        )?
    } else if fresh {
        Source::new(audit, 1, None)?
    } else {
        return Ok(None);
    };
    proof.measure(&value).map_err(budget)?;
    sqlx::query("INSERT INTO queue_publication_sources(session_id,queue_id,protocol_version,queue_sequence,operation_id,sequence,kind,revision,previous_digest,source_digest,data_json) VALUES($1,$2,1,$3,$4,$5,$6,$7,$8,$9,$10)").bind(&value.scope.session.0).bind(&value.scope.queue.0).bind(i64::try_from(value.queue_sequence).map_err(|_| corrupt())?).bind(&value.operation.0).bind(i64::try_from(value.sequence).map_err(|_| corrupt())?).bind(value.kind.name()).bind(i64::try_from(value.revision.get()).map_err(|_| corrupt())?).bind(value.previous.as_ref().map(crabber_core::InputFingerprint::as_str)).bind(value.digest.as_str()).bind(serde_json::to_string(&value).map_err(|_| corrupt())?).execute(&mut **tx).await.map_err(db)?;
    if value.sequence == 1 {
        sqlx::query("INSERT INTO queue_publication_cohorts(session_id,queue_id,origin_operation,origin_digest,head_sequence,head_digest) VALUES($1,$2,$3,$4,1,$4)").bind(&value.scope.session.0).bind(&value.scope.queue.0).bind(&value.operation.0).bind(value.digest.as_str()).execute(&mut **tx).await.map_err(db)?;
    } else {
        let updated = sqlx::query("UPDATE queue_publication_cohorts SET head_sequence=$3,head_digest=$4 WHERE session_id=$1 AND queue_id=$2 AND head_sequence=$5 AND head_digest=$6").bind(&value.scope.session.0).bind(&value.scope.queue.0).bind(i64::try_from(value.sequence).map_err(|_| corrupt())?).bind(value.digest.as_str()).bind(i64::try_from(value.sequence-1).map_err(|_| corrupt())?).bind(value.previous.as_ref().map(crabber_core::InputFingerprint::as_str)).execute(&mut **tx).await.map_err(db)?;
        if updated.rows_affected() != 1 {
            return Err(corrupt().into());
        }
    }
    Ok(Some(value))
}
pub(super) async fn load_record(
    tx: &mut Transaction<'_, Postgres>,
    record: &crate::queue::Record,
    proof: &mut Proof,
) -> Result<Originals> {
    let scope = Scope {
        session: record.summary.session_id.clone(),
        queue: record.summary.queue_id.clone(),
    };
    if let Some(originals) = full_history(tx, &scope, proof, Some(record)).await? {
        return Ok(originals);
    }
    let (_, enqueue) =
        super::reads::audited_leaf(tx, &scope.session, &record.enqueue_operation, proof)
            .await?
            .ok_or_else(corrupt)?;
    let prepare = if let Some(id) = &record.prepare_operation {
        Some(
            super::reads::audited_leaf(tx, &scope.session, id, proof)
                .await?
                .ok_or_else(corrupt)?
                .1,
        )
    } else {
        None
    };
    let claim = if let Some(claim) = &record.claim {
        Some(
            super::reads::audited_leaf(tx, &scope.session, &claim.operation, proof)
                .await?
                .ok_or_else(corrupt)?
                .1,
        )
    } else {
        None
    };
    let originals = Originals {
        head: None,
        enqueue: Some(enqueue),
        prepare,
        claim,
        prepare_source: None,
    };
    originals.prepared(record)?;
    Ok(originals)
}

/// Verify every retained identity used by FIFO routing, including terminal rows.
pub(super) async fn routing(
    tx: &mut Transaction<'_, Postgres>,
    session: &SessionId,
    proof: &mut Proof,
) -> Result<()> {
    let mut safe = crate::public_session::budget::Budget::new(
        32 << 20,
        crate::PublicSessionLimit::HeaderBytes,
    );
    super::public::pending(tx, session, u16::MAX, &mut safe, proof)
        .await
        .map_err(|error| match error {
            crate::public_session::v2::proof::Failure::Limited(_) => ReadError::Budget(error),
            crate::public_session::v2::proof::Failure::Error(_) => ReadError::Domain(corrupt()),
        })?;
    Ok(())
}
/// Whole-scope carrier membership only; it grants no ownership-policy coverage.
pub(super) async fn whole_scope(
    tx: &mut Transaction<'_, Postgres>,
    session: &SessionId,
    proof: &mut Proof,
) -> Result<()> {
    proof.charge(1).map_err(budget)?;
    let orphan: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM future_queue q WHERE q.session_id=$1 AND q.source_protocol=1 AND NOT EXISTS(SELECT 1 FROM queue_publication_cohorts c WHERE c.session_id=q.session_id AND c.queue_id=q.queue_id) UNION ALL SELECT 1 FROM queue_publication_cohorts c WHERE c.session_id=$1 AND NOT EXISTS(SELECT 1 FROM future_queue q WHERE q.session_id=c.session_id AND q.queue_id=c.queue_id AND q.source_protocol=c.protocol_version) UNION ALL SELECT 1 FROM queue_publication_sources s WHERE s.session_id=$1 AND NOT EXISTS(SELECT 1 FROM queue_publication_cohorts c WHERE c.session_id=s.session_id AND c.queue_id=s.queue_id))").bind(&session.0).fetch_one(&mut **tx).await.map_err(db)?;
    if orphan {
        return Err(corrupt().into());
    }
    Ok(())
}
/// Receipt-only source closure. Full private input authenticity is checked only
/// by full readers, control writers and fresh owned inspection.
pub(super) async fn projected(
    tx: &mut Transaction<'_, Postgres>,
    scope: &Scope,
    proof: &mut Proof,
) -> Result<Option<Source>> {
    if !protocol(tx, scope, proof).await? {
        return Ok(None);
    }
    let (origin, head) = cohort(tx, scope, proof).await?;
    let mut previous: Option<Source> = None;
    for sequence in 1..=head.sequence {
        let value = indexed(tx, scope, sequence, proof).await?;
        let extraction = super::public::RECEIPT;
        let cost = format!(
            "SELECT CAST(octet_length(r.data_json)+COALESCE(octet_length({extraction}),0) AS BIGINT) FROM operation_receipts r LEFT JOIN future_queue_audit a ON a.session_id=r.session_id AND a.operation_id=r.operation_id WHERE r.session_id=$1 AND r.operation_id=$2 AND r.kind='future_queue' AND r.event_seq IS NULL"
        );
        charge(tx, &cost, &scope.session.0, &value.operation.0, proof).await?;
        let query = format!(
            "SELECT CASE WHEN octet_length(r.data_json)<=16384 THEN r.data_json END AS receipt,CASE WHEN octet_length({extraction})<=16384 THEN {extraction} END AS original FROM operation_receipts r LEFT JOIN future_queue_audit a ON a.session_id=r.session_id AND a.operation_id=r.operation_id WHERE r.session_id=$1 AND r.operation_id=$2 AND r.kind='future_queue' AND r.event_seq IS NULL"
        );
        let row = sqlx::query(&query)
            .bind(&scope.session.0)
            .bind(&value.operation.0)
            .fetch_optional(&mut **tx)
            .await
            .map_err(db)?
            .ok_or_else(corrupt)?;
        let receipt: crate::QueueReceipt = decode(
            &row.try_get::<Option<String>, _>("receipt")
                .map_err(db)?
                .ok_or_else(corrupt)?,
        )?;
        let original = super::public::projected_receipt(
            &row.try_get::<Option<String>, _>("original")
                .map_err(db)?
                .ok_or_else(corrupt)?,
        )
        .map_err(|_| corrupt())?;
        value.verify_receipt(&receipt)?;
        if receipt != original {
            return Err(corrupt().into());
        }
        if let Some(previous) = &previous {
            value.follows(previous)?;
        } else if value != origin {
            return Err(corrupt().into());
        }
        previous = Some(value);
    }
    let mismatch: bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM future_queue_audit a WHERE a.session_id=$1 AND a.queue_id=$2 AND NOT EXISTS(SELECT 1 FROM queue_publication_sources s WHERE s.session_id=a.session_id AND s.queue_id=a.queue_id AND s.operation_id=a.operation_id AND s.revision=a.revision) UNION ALL SELECT 1 FROM queue_publication_sources s WHERE s.session_id=$1 AND s.queue_id=$2 AND (s.sequence>$3 OR NOT EXISTS(SELECT 1 FROM future_queue_audit a WHERE a.session_id=s.session_id AND a.queue_id=s.queue_id AND a.operation_id=s.operation_id AND a.revision=s.revision)))").bind(&scope.session.0).bind(&scope.queue.0).bind(i64::try_from(head.sequence).map_err(|_|corrupt())?).fetch_one(&mut **tx).await.map_err(db)?;
    if mismatch || previous.as_ref() != Some(&head) {
        return Err(corrupt().into());
    }
    Ok(Some(head))
}
