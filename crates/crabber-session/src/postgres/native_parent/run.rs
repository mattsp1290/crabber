//! The journal is independent of mutable Run rows. No lease renewal entry.
use crate::native_parent::{Error, RunIssuance, RunState};
use crabber_core::{Run, RunStatus};
use sqlx::{Postgres, Row, Transaction};
pub(super) async fn latest(
    tx: &mut Transaction<'_, Postgres>,
    run: &Run,
) -> Result<Option<RunIssuance>, Error> {
    let mut proof = crate::public_session::v2::proof::Proof::bounded(32 << 20);
    let latest = super::source_protocol::current(tx, &run.session_id, &run.id, &mut proof)
        .await
        .map_err(|_| Error)?;
    if let Some(latest) = latest.as_ref() {
        latest.verify(run)?;
    }
    Ok(latest)
}
async fn latest_with_proof(
    tx: &mut Transaction<'_, Postgres>,
    run: &Run,
    proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<Option<RunIssuance>, Error> {
    proof.charge(8).map_err(|_| Error)?;
    let sequence: Option<i64> = sqlx::query_scalar(
        "SELECT MAX(sequence) FROM native_run_issuances WHERE session_id=$1 AND run_id=$2",
    )
    .bind(&run.session_id.0)
    .bind(&run.id.0)
    .fetch_one(&mut **tx)
    .await
    .map_err(|_| Error)?;
    match sequence {
        None => Ok(None),
        Some(sequence) => super::source_protocol::original(
            tx,
            &run.session_id,
            &run.id,
            u64::try_from(sequence).map_err(|_| Error)?,
            proof,
        )
        .await
        .map_err(|_| Error),
    }
}
/// Called before the actual Run UPDATE, or immediately after an actual INSERT.
pub(in super::super) async fn change(
    tx: &mut Transaction<'_, Postgres>,
    run: &Run,
    creation: bool,
) -> Result<(), crate::StoreError> {
    if crate::POSTGRES_SCHEMA_VERSION < 13 {
        return Ok(());
    }
    change_inner(tx, run, creation, None)
        .await
        .map_err(|_| crate::StoreError::Validation("native run fence evidence unavailable".into()))
}
async fn change_inner(
    tx: &mut Transaction<'_, Postgres>,
    run: &Run,
    creation: bool,
    completion: Option<&crate::native_parent::CompletionSource>,
) -> Result<(), Error> {
    let mut proof = crate::public_session::v2::proof::Proof::bounded(32 << 20);
    let previous = if creation {
        None
    } else {
        super::super::read_proof::charge(tx,sqlx::query_scalar("SELECT CAST(octet_length(claim_token)+octet_length(status) AS BIGINT) FROM runs WHERE session_id=$1 AND id=$2").bind(&run.session_id.0).bind(&run.id.0),&mut proof).await.map_err(|_|Error)?;
        let row=sqlx::query("SELECT CASE WHEN octet_length(claim_token)<=512 THEN claim_token END AS token,CASE WHEN octet_length(status)<=32 THEN status END AS status FROM runs WHERE session_id=$1 AND id=$2")
            .bind(&run.session_id.0).bind(&run.id.0).fetch_one(&mut **tx).await.map_err(|_|Error)?;
        let token = row
            .try_get::<Option<String>, _>("token")
            .map_err(|_| Error)?
            .ok_or(Error)?;
        let status = row
            .try_get::<Option<String>, _>("status")
            .map_err(|_| Error)?
            .ok_or(Error)?;
        let status: RunStatus =
            serde_json::from_value(serde_json::Value::String(status)).map_err(|_| Error)?;
        Some(RunState::new(&run.session_id, &run.id, status, &token)?)
    };
    let latest = latest_with_proof(tx, run, &mut proof).await?;
    if let Some(previous) = previous.as_ref() {
        super::source_protocol::verify_previous(tx, previous, latest.as_ref(), &mut proof)
            .await
            .map_err(|_| Error)?;
    }
    if let Some(mut value) =
        crate::native_parent::advance(previous.as_ref(), RunState::from_run(run)?, latest.as_ref())?
    {
        if let Some(source) = completion {
            if latest.is_none() {
                return Err(Error);
            }
            value = value.completed(previous.as_ref().ok_or(Error)?, source.clone())?;
        }
        value.validate()?;
        super::source_protocol::append(tx, &value, creation, &mut proof)
            .await
            .map_err(|_| Error)?;
        sqlx::query("INSERT INTO native_run_issuances(session_id,run_id,sequence,data_json) VALUES($1,$2,$3,$4)")
            .bind(&run.session_id.0).bind(&run.id.0).bind(i64::try_from(value.sequence).map_err(|_|Error)?)
            .bind(serde_json::to_string(&value).map_err(|_|Error)?)
            .execute(&mut **tx).await.map_err(|_|Error)?;
    } else if completion.is_some() {
        return Err(Error);
    }
    Ok(())
}

pub(in super::super) async fn change_completed(
    tx: &mut Transaction<'_, Postgres>,
    run: &Run,
    source: &crate::native_parent::CompletionSource,
) -> Result<(), crate::StoreError> {
    change_inner(tx, run, false, Some(source))
        .await
        .map_err(|_| crate::StoreError::Validation("native completion evidence unavailable".into()))
}
