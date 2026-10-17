//! Pure selected original Tool/Frontend metadata; no fresh authority.
use super::{Error, Proof, source, unknown};
use crate::session_owner::preparation::{InspectPreparedToolDriver, PreparedToolDriverMetadata};
use sqlx::{Postgres, Transaction};
impl crate::PostgresStore {
    pub(in crate::postgres) async fn inspect_tool_driver(
        &self,
        request: &InspectPreparedToolDriver,
    ) -> Result<PreparedToolDriverMetadata, Error> {
        crate::session_owner::preparation::tool_driver::validate(request)?;
        let mut tx = self.pool.begin().await.map_err(unknown)?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .execute(&mut *tx)
            .await
            .map_err(unknown)?;
        let result = Box::pin(inspect(&mut tx, request, &mut Proof::bounded(32 << 20))).await?;
        tx.commit().await.map_err(unknown)?;
        Ok(result)
    }
}
async fn inspect(
    tx: &mut Transaction<'_, Postgres>,
    request: &InspectPreparedToolDriver,
    proof: &mut Proof,
) -> Result<PreparedToolDriverMetadata, Error> {
    let selected = Box::pin(source::original(tx, &request.request, proof, None)).await?;
    let receipt = selected.tool_receipt.ok_or(Error::Unsupported)?;
    crate::session_owner::preparation::tool_driver::metadata(request, receipt)
}
