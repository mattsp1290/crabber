//! Bounded current proof reads; immutable indexed sources select their own latest.
use super::{
    Audit, Error, Postgres, Record, SessionId, SessionOwnerId, SessionOwnerOperation,
    SessionOwnerOperationId, Transaction, operation, state, unknown, verify_current,
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sqlx::Row;
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Context {
    workspace: String,
    directory: String,
    origin: SessionOwnerOperationId,
}
fn decode<T: DeserializeOwned>(row: &sqlx::postgres::PgRow, field: &str) -> Result<T, Error> {
    let text = row
        .try_get::<Option<String>, _>(field)
        .map_err(unknown)?
        .ok_or(Error::UnknownStoreOutcome)?;
    serde_json::from_str(&text).map_err(unknown)
}
pub(super) async fn audit(
    tx: &mut Transaction<'_, Postgres>,
    session: &SessionId,
    id: &SessionOwnerOperationId,
) -> Result<Option<Audit>, Error> {
    let row=sqlx::query("SELECT CASE WHEN octet_length(data_json)<=32768 THEN data_json END AS data,CASE WHEN octet_length(owner_id)<=128 THEN owner_id END AS owner_id,sequence,CASE WHEN octet_length(operation)<=32 THEN operation END AS operation,CASE WHEN octet_length(state)<=32 THEN state END AS state,CASE WHEN octet_length(operation_id)<=128 THEN operation_id END AS operation_id FROM session_owner_audits WHERE session_id=$1 AND operation_id=$2").bind(&session.0).bind(&id.0).fetch_optional(&mut **tx).await.map_err(unknown)?;
    row.map(|row| checked_audit(&row, session, Some(id)))
        .transpose()
}
/// Charge the actual selected audit and routing before decoding it again.
pub(super) async fn audit_with_proof(
    tx: &mut Transaction<'_, Postgres>,
    session: &SessionId,
    id: &SessionOwnerOperationId,
    proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<Option<Audit>, Error> {
    let bytes: Option<i64> = sqlx::query_scalar("SELECT CAST(octet_length(data_json)+octet_length(owner_id)+octet_length(operation_id)+octet_length(operation)+octet_length(state)+8 AS BIGINT) FROM session_owner_audits WHERE session_id=$1 AND operation_id=$2")
        .bind(&session.0).bind(&id.0).fetch_optional(&mut **tx).await.map_err(unknown)?;
    proof.charge(8).map_err(|_| Error::Capacity)?;
    if let Some(bytes) = bytes {
        proof
            .charge(usize::try_from(bytes).map_err(unknown)?)
            .map_err(|_| Error::Capacity)?;
    }
    audit(tx, session, id).await
}
fn checked_audit(
    row: &sqlx::postgres::PgRow,
    session: &SessionId,
    id: Option<&SessionOwnerOperationId>,
) -> Result<Audit, Error> {
    let audit: Audit = decode(row, "data")?;
    audit.validate()?;
    if audit.receipt.session_id != *session
        || row
            .try_get::<Option<String>, _>("operation_id")
            .map_err(unknown)?
            .as_deref()
            != Some(&audit.receipt.operation_id.0)
        || id.is_some_and(|id| *id != audit.receipt.operation_id)
        || row
            .try_get::<Option<String>, _>("owner_id")
            .map_err(unknown)?
            .as_deref()
            != Some(&audit.record.snapshot.target.id.0)
        || row.try_get::<i64, _>("sequence").map_err(unknown)?
            != i64::try_from(audit.record.sequence).map_err(unknown)?
        || row
            .try_get::<Option<String>, _>("operation")
            .map_err(unknown)?
            .as_deref()
            != Some(operation(audit.receipt.operation))
        || row
            .try_get::<Option<String>, _>("state")
            .map_err(unknown)?
            .as_deref()
            != Some(state(audit.record.snapshot.state))
    {
        return Err(Error::UnknownStoreOutcome);
    }
    Ok(audit)
}
pub(super) async fn context(
    tx: &mut Transaction<'_, Postgres>,
    session: &SessionId,
    workspace: &str,
    directory: &str,
) -> Result<(), Error> {
    let row=sqlx::query("SELECT CASE WHEN octet_length(data_json)<=32768 THEN data_json END AS data FROM session_owner_contexts WHERE session_id=$1").bind(&session.0).fetch_optional(&mut **tx).await.map_err(unknown)?;
    let Some(row) = row else {
        let exists:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM session_owner_audits WHERE session_id=$1 UNION ALL SELECT 1 FROM session_owner_current WHERE session_id=$1)").bind(&session.0).fetch_one(&mut **tx).await.map_err(unknown)?;
        return if exists {
            Err(Error::UnknownStoreOutcome)
        } else {
            Ok(())
        };
    };
    let context: Context = decode(&row, "data")?;
    let origin=sqlx::query("SELECT CASE WHEN octet_length(data_json)<=32768 THEN data_json END AS data,CASE WHEN octet_length(owner_id)<=128 THEN owner_id END AS owner_id,sequence,CASE WHEN octet_length(operation)<=32 THEN operation END AS operation,CASE WHEN octet_length(state)<=32 THEN state END AS state,CASE WHEN octet_length(operation_id)<=128 THEN operation_id END AS operation_id FROM session_owner_audits WHERE session_id=$1 AND operation='Reserve' ORDER BY ordinal LIMIT 1").bind(&session.0).fetch_optional(&mut **tx).await.map_err(unknown)?.ok_or(Error::UnknownStoreOutcome)?;
    let origin = checked_audit(&origin, session, Some(&context.origin))?;
    if origin.receipt.operation != SessionOwnerOperation::Reserve
        || origin.record.snapshot.binding.workspace_id != context.workspace
        || origin.record.snapshot.binding.directory != context.directory
    {
        return Err(Error::UnknownStoreOutcome);
    }
    if workspace != context.workspace || directory != context.directory {
        return Err(Error::IdentityMismatch);
    }
    Ok(())
}
pub(super) async fn scope(
    tx: &mut Transaction<'_, Postgres>,
    session: &SessionId,
) -> Result<(), Error> {
    let corrupt:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM session_owner_audits a WHERE a.session_id=$1 AND a.operation='Reserve' AND NOT EXISTS(SELECT 1 FROM session_owner_current c WHERE c.session_id=a.session_id AND c.owner_id=a.owner_id) UNION ALL SELECT 1 FROM session_owner_current c WHERE c.session_id=$1 AND NOT EXISTS(SELECT 1 FROM session_owner_audits a WHERE a.session_id=c.session_id AND a.owner_id=c.owner_id AND a.operation='Reserve'))").bind(&session.0).fetch_one(&mut **tx).await.map_err(unknown)?;
    if corrupt {
        return Err(Error::UnknownStoreOutcome);
    }
    let orphan: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM session_owner_current c WHERE c.session_id=$1 AND c.source_protocol=1 AND NOT EXISTS(SELECT 1 FROM session_owner_publication_cohorts h WHERE h.session_id=c.session_id AND h.owner_id=c.owner_id) UNION ALL SELECT 1 FROM session_owner_publication_cohorts h WHERE h.session_id=$1 AND NOT EXISTS(SELECT 1 FROM session_owner_current c WHERE c.session_id=h.session_id AND c.owner_id=h.owner_id AND c.source_protocol=h.protocol_version) UNION ALL SELECT 1 FROM session_owner_publications p WHERE p.session_id=$1 AND NOT EXISTS(SELECT 1 FROM session_owner_publication_cohorts h WHERE h.session_id=p.session_id AND h.owner_id=p.owner_id))")
        .bind(&session.0).fetch_one(&mut **tx).await.map_err(unknown)?;
    if orphan {
        return Err(Error::UnknownStoreOutcome);
    }
    Ok(())
}
/// A selected current record and its actual source materializations share one allowance.
pub(super) async fn current_with_proof(
    tx: &mut Transaction<'_, Postgres>,
    session: &SessionId,
    id: &SessionOwnerId,
    proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<Record, Error> {
    let exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM session_owner_current WHERE session_id=$1 AND owner_id=$2)",
    )
    .bind(&session.0)
    .bind(&id.0)
    .fetch_one(&mut **tx)
    .await
    .map_err(unknown)?;
    proof.charge(1).map_err(|_| Error::Capacity)?;
    if exists {
        super::public::preflight(tx, session, &id.0, proof)
            .await
            .map_err(|error| match error {
                crate::public_session::v2::proof::Failure::Limited(_) => Error::Capacity,
                crate::public_session::v2::proof::Failure::Error(_) => Error::UnknownStoreOutcome,
            })?;
    }
    // Keep existing missing-current semantics: an original without current is
    // corruption, while an identity never reserved remains NotFound.
    current(tx, session, id).await
}
pub(super) async fn current(
    tx: &mut Transaction<'_, Postgres>,
    session: &SessionId,
    id: &SessionOwnerId,
) -> Result<Record, Error> {
    let row=sqlx::query("SELECT CASE WHEN octet_length(data_json)<=32768 THEN data_json END AS data FROM session_owner_current WHERE session_id=$1 AND owner_id=$2").bind(&session.0).bind(&id.0).fetch_optional(&mut **tx).await.map_err(unknown)?;
    let Some(row) = row else {
        let existed: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM session_owner_audits WHERE session_id=$1 AND owner_id=$2)",
        )
        .bind(&session.0)
        .bind(&id.0)
        .fetch_one(&mut **tx)
        .await
        .map_err(unknown)?;
        return Err(if existed {
            Error::UnknownStoreOutcome
        } else {
            Error::NotFound
        });
    };
    let record: Record = decode(&row, "data")?;
    record.validate()?;
    if record.snapshot.binding.session_id != *session || record.snapshot.target.id != *id {
        return Err(Error::UnknownStoreOutcome);
    }
    let latest=sqlx::query("SELECT CASE WHEN octet_length(data_json)<=32768 THEN data_json END AS data,CASE WHEN octet_length(owner_id)<=128 THEN owner_id END AS owner_id,sequence,CASE WHEN octet_length(operation)<=32 THEN operation END AS operation,CASE WHEN octet_length(state)<=32 THEN state END AS state,CASE WHEN octet_length(operation_id)<=128 THEN operation_id END AS operation_id FROM session_owner_audits WHERE session_id=$1 AND owner_id=$2 ORDER BY sequence DESC LIMIT 1").bind(&session.0).bind(&id.0).fetch_optional(&mut **tx).await.map_err(unknown)?.ok_or(Error::UnknownStoreOutcome)?;
    let latest = checked_audit(&latest, session, None)?;
    let origin = audit(tx, session, &record.origin)
        .await?
        .ok_or(Error::UnknownStoreOutcome)?;
    verify_current(&record, &origin, &latest)?;
    if let Some(link) = crate::session_owner::preparation::resource_source::link(&record) {
        let root_reserve = audit(tx, session, &link.root_reserve)
            .await?
            .ok_or(Error::UnknownStoreOutcome)?;
        let root_begin = audit(tx, session, &link.root_begin)
            .await?
            .ok_or(Error::UnknownStoreOutcome)?;
        crate::session_owner::preparation::resource_source::verify(
            &record,
            &root_reserve,
            &root_begin,
        )?;
    }

    if let Some(scope) = crate::session_owner::owned_scope::source::context(&record) {
        let reserve = audit(tx, session, &scope.source.reserve)
            .await?
            .ok_or(Error::UnknownStoreOutcome)?;
        let begin = audit(tx, session, &scope.source.begin)
            .await?
            .ok_or(Error::UnknownStoreOutcome)?;
        let activated = audit(tx, session, &scope.id).await?;
        crate::session_owner::owned_scope::source::current(
            &record,
            &reserve,
            &begin,
            activated.as_ref(),
        )?;
    }
    verify_cancellation(tx, &record).await?;
    context(
        tx,
        session,
        &record.snapshot.binding.workspace_id,
        &record.snapshot.binding.directory,
    )
    .await?;
    Ok(record)
}
pub(super) async fn capacity(
    tx: &mut Transaction<'_, Postgres>,
    session: &SessionId,
) -> Result<(), Error> {
    let count:i64=sqlx::query_scalar("SELECT COUNT(*) FROM session_owner_audits a WHERE a.session_id=$1 AND a.state<>'Joined' AND NOT EXISTS(SELECT 1 FROM session_owner_audits b WHERE b.session_id=a.session_id AND b.owner_id=a.owner_id AND b.sequence>a.sequence)").bind(&session.0).fetch_one(&mut **tx).await.map_err(unknown)?;
    if count >= 1024 {
        Err(Error::Capacity)
    } else {
        Ok(())
    }
}
pub(super) async fn publish(
    tx: &mut Transaction<'_, Postgres>,
    audit: &Audit,
    fresh: bool,
) -> Result<(), Error> {
    publish_with_proof(
        tx,
        audit,
        fresh,
        &mut crate::public_session::v2::proof::Proof::bounded(32 * 1024 * 1024),
    )
    .await
}
pub(super) async fn publish_with_proof(
    tx: &mut Transaction<'_, Postgres>,
    audit: &Audit,
    fresh: bool,
    proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<(), Error> {
    audit.validate()?;
    let protocol = Box::pin(super::protocol::prepare(tx, audit, fresh, proof)).await?;
    let mut prepared = audit.clone();
    if protocol {
        prepared.version = 3;
    }
    let audit = &prepared;
    audit.validate()?;
    let text = serde_json::to_string(audit).map_err(unknown)?;
    let record = &audit.record;
    let binding = &record.snapshot.binding;
    let birth = if fresh {
        Some(super::scope_origin::context_protocol(tx, &binding.session_id, proof).await?)
    } else {
        None
    };
    if let Some(birth) = &birth {
        let context = Context {
            workspace: binding.workspace_id.clone(),
            directory: binding.directory.clone(),
            origin: record.origin.clone(),
        };
        if crate::snapshot::encoded_bytes(&context, 32768).is_none() {
            return Err(Error::InvalidRequest);
        }
        let native_protocol = super::super::native_parent::source_protocol::context_birth(
            tx,
            &binding.session_id,
            birth.absent,
            proof,
        )
        .await
        .map_err(unknown)?;
        sqlx::query("INSERT INTO session_owner_contexts(session_id,data_json,scope_protocol,native_run_protocol) VALUES($1,$2,$3,$4) ON CONFLICT(session_id) DO NOTHING").bind(&binding.session_id.0).bind(serde_json::to_string(&context).map_err(unknown)?).bind(birth.protocol).bind(native_protocol).execute(&mut **tx).await.map_err(unknown)?;
    }
    sqlx::query("INSERT INTO session_owner_audits(session_id,operation_id,owner_id,sequence,operation,state,data_json) VALUES($1,$2,$3,$4,$5,$6,$7)").bind(&binding.session_id.0).bind(&audit.receipt.operation_id.0).bind(&binding.id.0).bind(i64::try_from(record.sequence).map_err(unknown)?).bind(operation(audit.receipt.operation)).bind(state(record.snapshot.state)).bind(&text).execute(&mut **tx).await.map_err(unknown)?;
    sqlx::query("INSERT INTO session_owner_current(session_id,owner_id,data_json,source_protocol) VALUES($1,$2,$3,CASE WHEN $4 THEN 1 ELSE COALESCE((SELECT source_protocol FROM session_owner_current WHERE session_id=$1 AND owner_id=$2),0) END) ON CONFLICT(session_id,owner_id) DO UPDATE SET data_json=excluded.data_json").bind(&binding.session_id.0).bind(&binding.id.0).bind(serde_json::to_string(record).map_err(unknown)?).bind(fresh).execute(&mut **tx).await.map_err(unknown)?;
    if protocol {
        Box::pin(super::protocol::publish(tx, audit, &text, fresh)).await?;
    }
    if let Some(birth) = &birth {
        super::scope_origin::record_absent(tx, audit, birth).await?;
    }
    Ok(())
}

/// Only the exact owner's public publications are selected, bounded before transfer.
pub(super) async fn preparation_audits(
    tx: &mut Transaction<'_, Postgres>,
    session: &SessionId,
    owner: &SessionOwnerId,
    child: Option<&SessionOwnerId>,
    proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<Vec<Audit>, Error> {
    let service_ids = services::service_ids(tx, session, owner).await?;
    let costs: Vec<i64> = sqlx::query_scalar("SELECT CAST(bytes AS BIGINT) FROM (SELECT octet_length(data_json)+octet_length(owner_id)+octet_length(operation_id)+octet_length(operation)+octet_length(state)+8 AS bytes FROM session_owner_audits WHERE session_id=$1 AND (owner_id=$2 OR owner_id=$3 OR owner_id=$4 OR owner_id=$5 OR owner_id=$6) AND operation<>'Renew' ORDER BY sequence LIMIT 65) projected").bind(&session.0).bind(&owner.0).bind(child.map_or(owner.0.as_str(), |id| id.0.as_str())).bind(service_ids.first().map_or(owner.0.as_str(), |id| id.0.as_str())).bind(service_ids.get(1).map_or(owner.0.as_str(), |id| id.0.as_str())).bind(service_ids.get(2).map_or(owner.0.as_str(), |id| id.0.as_str())).fetch_all(&mut **tx).await.map_err(unknown)?;
    for bytes in costs {
        proof
            .charge(8)
            .and_then(|()| {
                proof.charge(
                    usize::try_from(bytes).map_err(crate::public_session::v2::proof::unknown)?,
                )
            })
            .map_err(|error| match error {
                crate::public_session::v2::proof::Failure::Limited(_) => Error::Capacity,
                crate::public_session::v2::proof::Failure::Error(_) => Error::UnknownStoreOutcome,
            })?;
    }
    let rows=sqlx::query("SELECT CASE WHEN octet_length(data_json)<=32768 THEN data_json END AS data,CASE WHEN octet_length(owner_id)<=128 THEN owner_id END AS owner_id,sequence,CASE WHEN octet_length(operation)<=32 THEN operation END AS operation,CASE WHEN octet_length(state)<=32 THEN state END AS state,CASE WHEN octet_length(operation_id)<=128 THEN operation_id END AS operation_id FROM session_owner_audits WHERE session_id=$1 AND (owner_id=$2 OR owner_id=$3 OR owner_id=$4 OR owner_id=$5 OR owner_id=$6) AND operation<>'Renew' ORDER BY sequence LIMIT 65").bind(&session.0).bind(&owner.0).bind(child.map_or(owner.0.as_str(), |id| id.0.as_str())).bind(service_ids.first().map_or(owner.0.as_str(), |id| id.0.as_str())).bind(service_ids.get(1).map_or(owner.0.as_str(), |id| id.0.as_str())).bind(service_ids.get(2).map_or(owner.0.as_str(), |id| id.0.as_str())).fetch_all(&mut **tx).await.map_err(unknown)?;
    if rows.len() > crate::session_owner::preparation::proof::MAX_PUBLICATIONS {
        return Err(Error::Capacity);
    }
    rows.into_iter()
        .map(|row| {
            let audit = checked_audit(&row, session, None)?;
            if audit.record.snapshot.target.id != *owner
                && child != Some(&audit.record.snapshot.target.id)
                && !service_ids.contains(&audit.record.snapshot.target.id)
            {
                return Err(Error::UnknownStoreOutcome);
            }
            Ok(audit)
        })
        .collect()
}

mod preparation;
pub(super) use preparation::has_preparation;

pub(super) use publications::{preparation_order, publication_count};
mod publications;

async fn verify_cancellation(
    tx: &mut Transaction<'_, Postgres>,
    record: &Record,
) -> Result<(), Error> {
    let Some(cancel) = &record.cancellation else {
        return Ok(());
    };
    let request = audit(tx, &record.snapshot.binding.session_id, &cancel.request.id)
        .await?
        .map(Box::new);
    let observed = if let Some(observed) = &cancel.observed {
        audit(
            tx,
            &record.snapshot.binding.session_id,
            &observed.request.id,
        )
        .await?
        .map(Box::new)
    } else {
        None
    };
    crate::session_owner::cancellation::proof::sources(
        record,
        request.as_deref(),
        observed.as_deref(),
    )
}

mod services;
pub(super) use services::services_with_proof;

mod service_cohort;
pub(super) use service_cohort::ensure_generation_slot;
