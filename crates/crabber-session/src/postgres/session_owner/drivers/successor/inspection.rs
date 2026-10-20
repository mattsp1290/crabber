//! Exact original closure, with bounded accepted-input projection only.
use super::{Error, Proof, source, unknown};
use crate::postgres::transaction::Transaction;
use crate::session_owner::preparation::{
    InspectPreparedApprovalDriver, PreparedApprovalDriverMetadata,
};
use sqlx::Postgres;
impl crate::PostgresStore {
    pub(in crate::postgres) async fn inspect_approval_driver(
        &self,
        request: &InspectPreparedApprovalDriver,
    ) -> Result<PreparedApprovalDriverMetadata, Error> {
        crate::session_owner::preparation::approval_driver::validate(request)?;
        let mut tx = crate::postgres::transaction::begin(&self.pool)
            .await
            .map_err(unknown)?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .execute(&mut *tx)
            .await
            .map_err(unknown)?;
        let mut proof = Proof::bounded(32 << 20);
        let result = Box::pin(inspect(&mut tx, request, &mut proof)).await?;
        tx.commit().await.map_err(unknown)?;
        Ok(result)
    }
}
async fn inspect(
    tx: &mut Transaction<'_, Postgres>,
    request: &InspectPreparedApprovalDriver,
    proof: &mut Proof,
) -> Result<PreparedApprovalDriverMetadata, Error> {
    let selected = Box::pin(source::original(tx, &request.request, proof, None)).await?;
    let receipt = selected.approval_receipt.ok_or(Error::Unsupported)?;
    let scope = crate::session_owner::preparation::approval_driver::scope(&receipt)?;
    if !crate::postgres::continuation_input::protocol(tx, &scope, proof)
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
    crate::postgres::continuation_input::answers(tx, &scope, &inputs, proof)
        .await
        .map_err(unknown)?;
    crate::session_owner::preparation::approval_driver::metadata(request, receipt)
}

async fn answers(
    tx: &mut Transaction<'_, Postgres>,
    scope: &crate::continuation_input::Scope,
    proof: &mut Proof,
) -> Result<Vec<crate::ApprovalAnswer>, Error> {
    crate::postgres::read_proof::charge(tx, sqlx::query_scalar("SELECT CAST(octet_length(data_json) AS BIGINT) FROM continuations WHERE session_id=$1 AND run_id=$2 AND id=$3").bind(&scope.session.0).bind(&scope.run.0).bind(&scope.continuation.id.0), proof).await.map_err(unknown)?;
    let raw: Option<String> = sqlx::query_scalar("SELECT CASE WHEN octet_length(a::text)<=4194304 THEN a::text END FROM (SELECT COALESCE(json_agg(q->'answer'),'[]'::json) AS a FROM continuations c CROSS JOIN LATERAL json_array_elements((CASE WHEN octet_length(c.data_json)<=4194304 THEN c.data_json END)::json->'questions') q WHERE c.session_id=$1 AND c.run_id=$2 AND c.id=$3 AND json_typeof(q->'answer')<>'null') bounded").bind(&scope.session.0).bind(&scope.run.0).bind(&scope.continuation.id.0).fetch_one(&mut **tx).await.map_err(unknown)?;
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
