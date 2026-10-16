//! SQL transfers only a bounded original batch, independent of current private state.
use super::super::SqliteStore;
use super::Error;
use crabber_core::{InputFingerprint, RunId, SessionId};
use sqlx::Row;
fn unknown(_: impl std::fmt::Debug) -> Error {
    Error::UnknownStoreOutcome
}
impl SqliteStore {
    pub(in super::super) async fn read_original_tool_plan(
        &self,
        session: &SessionId,
        run: &RunId,
        target: &crate::ContinuationTarget,
    ) -> Result<InputFingerprint, Error> {
        crate::continuation_origin::validate_scope(session, run, target)
            .map_err(|()| Error::InvalidRequest)?;
        let mut tx = self.readers.begin().await.map_err(unknown)?;

        let result = async {
            let row = sqlx::query(r"SELECT format_version AS version,CASE WHEN length(CAST(json_extract(data_json,'$.batch') AS BLOB))<=4194304 THEN json_extract(data_json,'$.batch') END AS batch FROM tool_continuations WHERE session_id=?1 AND run_id=?2 AND id=?3 AND generation=?4").bind(&session.0).bind(&run.0).bind(&target.id.0).bind(i64::try_from(target.generation.get()).map_err(unknown)?).fetch_optional(&mut *tx).await.map_err(unknown)?.ok_or(Error::UnknownStoreOutcome)?;
            if row.try_get::<i64, _>("version").map_err(unknown)? != 1 { return Err(Error::UnknownStoreOutcome); }
            let raw = row.try_get::<Option<String>, _>("batch").map_err(unknown)?.ok_or(Error::UnknownStoreOutcome)?;
            let batch: crate::ToolContinuationSuspension = serde_json::from_str(&raw).map_err(unknown)?;
            let receipt = super::read::receipt(&mut tx, session, &batch.id).await?.map(|(receipt, _)| receipt).ok_or(Error::UnknownStoreOutcome)?;
            crate::continuation_origin::tool(&batch, session, run, target, &receipt)
        }.await;
        tx.rollback().await.map_err(unknown)?;
        result
    }
}
