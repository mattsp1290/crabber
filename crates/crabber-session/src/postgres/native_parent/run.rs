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
async fn change_inner(
    tx: &mut Transaction<'_, Postgres>,
    run: &Run,
    creation: bool,
    completion: Option<&crate::native_parent::CompletionSource>,
    proof: &mut crate::public_session::v2::proof::Proof,
    capture: crate::native_enrollment::caller::initial::Capture,
) -> Result<crate::native_enrollment::caller::initial::RunPublication, Error> {
    proof.measure(&(run, completion)).map_err(|_| Error)?;
    let previous = if creation {
        None
    } else {
        super::super::read_proof::charge(tx,sqlx::query_scalar("SELECT CAST(octet_length(claim_token)+octet_length(status) AS BIGINT) FROM runs WHERE session_id=$1 AND id=$2").bind(&run.session_id.0).bind(&run.id.0),proof).await.map_err(|_|Error)?;
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
    let latest = latest_with_proof(tx, run, proof).await?;
    if let Some(previous) = previous.as_ref() {
        super::source_protocol::verify_previous(tx, previous, latest.as_ref(), proof)
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
        let captured = match capture {
            crate::native_enrollment::caller::initial::Capture::Original => {
                super::source_protocol::append_captured(tx, &value, creation, proof)
                    .await
                    .map_err(|_| Error)?
            }
            crate::native_enrollment::caller::initial::Capture::None => {
                super::source_protocol::append(tx, &value, creation, proof)
                    .await
                    .map_err(|_| Error)?;
                crate::native_enrollment::caller::initial::SourcePublication::default()
            }
        };
        sqlx::query("INSERT INTO native_run_issuances(session_id,run_id,sequence,data_json) VALUES($1,$2,$3,$4)")
            .bind(&run.session_id.0).bind(&run.id.0).bind(i64::try_from(value.sequence).map_err(|_|Error)?)
            .bind(serde_json::to_string(&value).map_err(|_|Error)?)
            .execute(&mut **tx).await.map_err(|_|Error)?;
        return Ok(crate::native_enrollment::caller::initial::RunPublication {
            sequence: Some(value.sequence),
            first: captured.first,
            updated: captured.updated,
        });
    } else if completion.is_some() {
        return Err(Error);
    }
    Ok(crate::native_enrollment::caller::initial::RunPublication {
        sequence: None,
        first: None,
        updated: None,
    })
}

pub(in super::super) async fn initial_with_proof(
    tx: &mut Transaction<'_, Postgres>,
    run: &Run,
    proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<crate::native_enrollment::caller::initial::FirstPublication, crate::StoreError> {
    change_inner(
        tx,
        run,
        true,
        None,
        proof,
        crate::native_enrollment::caller::initial::Capture::Original,
    )
    .await
    .map_err(|_| crate::StoreError::Conflict)?
    .first
    .ok_or(crate::StoreError::Conflict)
}

pub(in super::super) async fn change_with_proof(
    tx: &mut Transaction<'_, Postgres>,
    run: &Run,
    creation: bool,
    source: Option<&crate::native_parent::CompletionSource>,
    proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<(), crate::StoreError> {
    super::record_run_captured_with_proof(tx, run, creation, source, proof)
        .await
        .map(|_| ())
        .map_err(|_| crate::StoreError::Validation("native run source evidence unavailable".into()))
}

pub(in super::super) async fn change_captured_with_proof(
    tx: &mut Transaction<'_, Postgres>,
    run: &Run,
    creation: bool,
    source: Option<&crate::native_parent::CompletionSource>,
    proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<Option<u64>, crate::StoreError> {
    change_inner(
        tx,
        run,
        creation,
        source,
        proof,
        crate::native_enrollment::caller::initial::Capture::None,
    )
    .await
    .map(crate::native_enrollment::caller::initial::RunPublication::into_sequence)
    .map_err(|_| crate::StoreError::Validation("native run source evidence unavailable".into()))
}

pub(in super::super) async fn publication_with_proof(
    tx: &mut Transaction<'_, Postgres>,
    run: &Run,
    creation: bool,
    source: Option<&crate::native_parent::CompletionSource>,
    proof: &mut crate::public_session::v2::proof::Proof,
    capture: crate::native_enrollment::caller::initial::Capture,
) -> Result<crate::native_enrollment::caller::initial::RunPublication, crate::StoreError> {
    change_inner(tx, run, creation, source, proof, capture)
        .await
        .map_err(|_| crate::StoreError::Validation("native run source evidence unavailable".into()))
}
