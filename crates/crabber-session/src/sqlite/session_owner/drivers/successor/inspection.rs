//! Exact original closure, with bounded accepted-input projection only.
use super::{Error, Proof, source, unknown};
use crate::session_owner::preparation::{
    InspectPreparedApprovalDriver, PreparedApprovalDriverMetadata,
};
use crate::transaction::Transaction;
use sqlx::Sqlite;
impl crate::SqliteStore {
    pub(in crate::sqlite) async fn inspect_approval_driver(
        &self,
        request: &InspectPreparedApprovalDriver,
    ) -> Result<PreparedApprovalDriverMetadata, Error> {
        crate::session_owner::preparation::approval_driver::validate(request)?;
        let mut tx = crate::transaction::begin(&self.readers)
            .await
            .map_err(unknown)?;
        let mut proof = Proof::bounded(32 << 20);
        let result = Box::pin(inspect(&mut tx, request, &mut proof)).await?;
        tx.commit().await.map_err(unknown)?;
        Ok(result)
    }
}
async fn inspect(
    tx: &mut Transaction<'_, Sqlite>,
    request: &InspectPreparedApprovalDriver,
    proof: &mut Proof,
) -> Result<PreparedApprovalDriverMetadata, Error> {
    let selected = Box::pin(source::original(tx, &request.request, proof, None)).await?;
    let receipt = selected.approval_receipt.ok_or(Error::Unsupported)?;
    let scope = crate::session_owner::preparation::approval_driver::scope(&receipt)?;
    if !crate::sqlite::continuation_input::protocol(tx, &scope, proof)
        .await
        .map_err(unknown)?
    {
        return Err(Error::Unsupported);
    }
    let answers = answers(tx, &scope, proof).await?;
    let inputs: Vec<_> = answers
        .iter()
        .map(crate::continuation_input::Input::ApprovalAnswer)
        .collect();
    crate::sqlite::continuation_input::answers(tx, &scope, &inputs, proof)
        .await
        .map_err(unknown)?;
    crate::session_owner::preparation::approval_driver::metadata(request, receipt)
}

async fn answers(
    tx: &mut Transaction<'_, Sqlite>,
    scope: &crate::continuation_input::Scope,
    proof: &mut Proof,
) -> Result<Vec<crate::ApprovalAnswer>, Error> {
    crate::sqlite::read_proof::charge(tx, sqlx::query_scalar("SELECT length(CAST(data_json AS BLOB)) FROM continuations WHERE session_id=$1 AND run_id=$2 AND id=$3").bind(&scope.session.0).bind(&scope.run.0).bind(&scope.continuation.id.0), proof).await.map_err(unknown)?;
    let raw: Option<String> = sqlx::query_scalar("SELECT CASE WHEN length(CAST(a AS BLOB))<=4194304 THEN a END FROM (SELECT json_group_array(json(json_extract(q.value,'$.answer'))) AS a FROM continuations c,json_each(CASE WHEN length(CAST(c.data_json AS BLOB))<=4194304 THEN c.data_json END,'$.questions') q WHERE c.session_id=$1 AND c.run_id=$2 AND c.id=$3 AND json_type(q.value,'$.answer')<>'null') bounded").bind(&scope.session.0).bind(&scope.run.0).bind(&scope.continuation.id.0).fetch_one(&mut **tx).await.map_err(unknown)?;
    let answers: Vec<crate::ApprovalAnswer> =
        serde_json::from_str(raw.as_deref().ok_or(Error::UnknownStoreOutcome)?).map_err(unknown)?;
    if answers.len() > 64 {
        return Err(Error::UnknownStoreOutcome);
    }
    Ok(answers)
}

#[cfg(test)]
#[path = "inspection/tests.rs"]
mod tests;
