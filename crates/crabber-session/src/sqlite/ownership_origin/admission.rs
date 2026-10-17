//! Sealed preparation evidence committed with the actual Run and messages.
use crate::session_owner::preparation::admission::Witness;
use crate::{StoreError, queue::AdmissionGuard};
use crabber_core::{Run, Session, SessionRevision};

pub(in super::super) async fn record_admission(
    tx: &mut crate::transaction::Transaction<'_, sqlx::Sqlite>,
    session: &Session,
    run: &Run,
    new_session: bool,
    guard: AdmissionGuard<'_>,
    read_proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<Option<crate::native_enrollment::caller::initial::FirstPublication>, StoreError> {
    if !guard.has_owned_resources() {
        return Ok(None);
    }
    read_proof.measure(&(session, run)).map_err(invalid)?;
    let preparation = guard.preparation().ok_or(StoreError::Conflict)?;
    let raw: i64 = sqlx::query_scalar("SELECT public_revision FROM sessions WHERE id=$1")
        .bind(&session.id.0)
        .fetch_one(&mut **tx)
        .await
        .map_err(super::super::db)?;
    let revision = SessionRevision::new(u64::try_from(raw).map_err(invalid)?).map_err(invalid)?;
    let witness = Witness::from_preparation(preparation, session, run, revision)
        .map_err(invalid)?
        .ok_or(StoreError::Conflict)?;
    read_proof.measure(&witness).map_err(invalid)?;
    let issuance = witness.initial_issuance(run).map_err(invalid)?;
    read_proof.measure(&issuance).map_err(invalid)?;
    let exists: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM native_run_issuances WHERE run_id=$1)")
            .bind(&run.id.0)
            .fetch_one(&mut **tx)
            .await
            .map_err(super::super::db)?;
    if exists {
        return Err(StoreError::Conflict);
    }
    let encoded = witness.encoded().map_err(invalid)?;
    let join = witness.join_id().map_err(invalid)?;
    let initial = serde_json::to_string(&issuance).map_err(invalid)?;
    let first = super::super::native_parent::source_protocol::append_captured(
        tx, &issuance, true, read_proof,
    )
    .await
    .map_err(invalid)?
    .first
    .ok_or(StoreError::Conflict)?;
    sqlx::query(
        "INSERT INTO native_run_issuances(session_id,run_id,sequence,data_json) VALUES($1,$2,1,$3)",
    )
    .bind(&session.id.0)
    .bind(&run.id.0)
    .bind(initial)
    .execute(&mut **tx)
    .await
    .map_err(super::super::db)?;
    sqlx::query("INSERT INTO preparation_admission_witnesses(run_id,session_id,root_join,data_json) VALUES($1,$2,$3,$4)")
        .bind(&run.id.0).bind(&session.id.0).bind(&join.0).bind(encoded).execute(&mut **tx).await.map_err(super::super::db)?;
    if new_session {
        let origin =
            crate::deletion::origin::OwnershipOrigin::prepared(&witness).map_err(invalid)?;
        super::record(tx, &origin).await?;
    }
    Ok(Some(first))
}
fn invalid(_: impl std::fmt::Debug) -> StoreError {
    StoreError::Validation("preparation admission evidence unavailable".into())
}

/// SQL bounds each original carrier before transferring or decoding it. The
/// resulting byte count joins the caller's single whole-operation allowance.
pub(super) async fn coverage(
    tx: &mut crate::transaction::Transaction<'_, sqlx::Sqlite>,
    session: &crabber_core::SessionId,
    evidence: crate::deletion::coverage::sql::OriginEvidence,
) -> Result<crate::deletion::coverage::CoverageRead, crate::DeletionError> {
    use crate::DeletionError as Error;
    use crate::deletion::coverage::CoverageRead;
    use sqlx::Row;
    let row = sqlx::query("SELECT length(CAST(w.data_json AS BLOB)) AS witness_bytes,length(CAST(i.data_json AS BLOB)) AS initial_bytes,CASE WHEN length(CAST(w.data_json AS BLOB))<=8192 THEN w.data_json END AS witness,CASE WHEN length(CAST(i.data_json AS BLOB))<=2048 THEN i.data_json END AS initial,CASE WHEN length(CAST(w.root_join AS BLOB))<=128 THEN w.root_join END AS root_join FROM preparation_admission_witnesses w LEFT JOIN native_run_issuances i ON i.session_id=w.session_id AND i.run_id=w.run_id AND i.sequence=1 WHERE w.session_id=$1 AND w.run_id=$2")
        .bind(&session.0).bind(evidence.operation()).fetch_optional(&mut **tx).await.map_err(unknown)?.ok_or(Error::UnknownStoreOutcome)?;
    let witness_bytes: i64 = row.try_get("witness_bytes").map_err(unknown)?;
    let initial_bytes: i64 = row.try_get("initial_bytes").map_err(unknown)?;
    if witness_bytes > 8192 || initial_bytes > 2048 {
        return Ok(CoverageRead::Limited);
    }
    let witness: Witness =
        serde_json::from_str(row.try_get("witness").map_err(unknown)?).map_err(unknown)?;
    let initial =
        serde_json::from_str(row.try_get("initial").map_err(unknown)?).map_err(unknown)?;
    witness.validate().map_err(unknown)?;
    if witness.session_id() != session
        || witness.run_id().0 != evidence.operation()
        || row
            .try_get::<Option<&str>, _>("root_join")
            .map_err(unknown)?
            != Some(witness.join_id().map_err(unknown)?.0.as_str())
    {
        return Err(Error::UnknownStoreOutcome);
    }
    let mut bytes = usize::try_from(witness_bytes + initial_bytes).map_err(unknown)?
        + witness.join_id().map_err(unknown)?.0.len();
    let mut audits = Vec::with_capacity(5);
    for id in witness.audit_ids().map_err(unknown)? {
        let length: i64 = sqlx::query_scalar("SELECT length(CAST(data_json AS BLOB)) FROM session_owner_audits WHERE session_id=$1 AND operation_id=$2")
            .bind(&session.0).bind(&id.0).fetch_optional(&mut **tx).await.map_err(unknown)?.ok_or(Error::UnknownStoreOutcome)?;
        if length > 32768 {
            return Ok(CoverageRead::Limited);
        }
        bytes += usize::try_from(length).map_err(unknown)?;
        let audit = super::super::session_owner::source_audit(tx, session, &id)
            .await
            .map_err(unknown)?
            .ok_or(Error::UnknownStoreOutcome)?;
        audits.push(audit);
    }
    evidence.prepared(
        &witness,
        &initial,
        &audits.try_into().map_err(|_| Error::UnknownStoreOutcome)?,
        bytes,
    )
}
fn unknown(_: impl std::fmt::Debug) -> crate::DeletionError {
    crate::DeletionError::UnknownStoreOutcome
}
