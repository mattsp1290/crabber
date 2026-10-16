//! The journal is independent of mutable Run rows. No lease renewal entry.
use crate::native_parent::{Error, RunIssuance, RunState};
use crabber_core::{Run, RunStatus};
use sqlx::{Row, Sqlite, Transaction};
pub(super) async fn latest(
    tx: &mut Transaction<'_, Sqlite>,
    run: &Run,
) -> Result<Option<RunIssuance>, Error> {
    if crate::SQLITE_SCHEMA_VERSION < 9 {
        return Ok(None);
    }
    let row=sqlx::query("SELECT sequence,CASE WHEN length(CAST(data_json AS BLOB))<=2048 THEN data_json END AS data FROM native_run_issuances WHERE session_id=?1 AND run_id=?2 ORDER BY sequence DESC LIMIT 1")
        .bind(&run.session_id.0).bind(&run.id.0).fetch_optional(&mut **tx).await.map_err(|_|Error)?;
    row.map(|row| {
        let text = row
            .try_get::<Option<String>, _>("data")
            .map_err(|_| Error)?
            .ok_or(Error)?;
        let value: RunIssuance = serde_json::from_str(&text).map_err(|_| Error)?;
        value.validate()?;
        if i64::try_from(value.sequence).map_err(|_| Error)?
            != row.try_get::<i64, _>("sequence").map_err(|_| Error)?
            || value.state.session_id != run.session_id
            || value.state.run_id != run.id
        {
            return Err(Error);
        }
        Ok(value)
    })
    .transpose()
}
/// Called before the actual Run UPDATE, or immediately after an actual INSERT.
pub(in super::super) async fn change(
    tx: &mut Transaction<'_, Sqlite>,
    run: &Run,
    creation: bool,
) -> Result<(), crate::StoreError> {
    if crate::SQLITE_SCHEMA_VERSION < 9 {
        return Ok(());
    }
    change_inner(tx, run, creation)
        .await
        .map_err(|_| crate::StoreError::Validation("native run fence evidence unavailable".into()))
}
async fn change_inner(
    tx: &mut Transaction<'_, Sqlite>,
    run: &Run,
    creation: bool,
) -> Result<(), Error> {
    let previous = if creation {
        None
    } else {
        let row=sqlx::query("SELECT CASE WHEN length(CAST(claim_token AS BLOB))<=512 THEN claim_token END AS token,CASE WHEN length(CAST(status AS BLOB))<=32 THEN status END AS status FROM runs WHERE session_id=?1 AND id=?2")
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
    let latest = latest(tx, run).await?;
    if let Some(value) =
        crate::native_parent::advance(previous.as_ref(), RunState::from_run(run)?, latest.as_ref())?
    {
        value.validate()?;
        sqlx::query("INSERT INTO native_run_issuances(session_id,run_id,sequence,data_json) VALUES(?1,?2,?3,?4)")
            .bind(&run.session_id.0).bind(&run.id.0).bind(i64::try_from(value.sequence).map_err(|_|Error)?)
            .bind(serde_json::to_string(&value).map_err(|_|Error)?)
            .execute(&mut **tx).await.map_err(|_|Error)?;
    }
    Ok(())
}
