//! Backend-local persistence; transaction and failure ordering are unchanged.
use super::{PostgresStore, db, deletion};
use crate::StoreError;
use crabber_core::SessionId;
use sqlx::Row;
use std::collections::BTreeMap;

impl PostgresStore {
    pub(in crate::postgres) async fn read_extension_state(
        &self,
        extension_id: &str,
        session: &SessionId,
    ) -> Result<BTreeMap<String, String>, StoreError> {
        let mut tx = crate::postgres::transaction::begin(&self.pool)
            .await
            .map_err(db)?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        deletion::ensure_live(&mut tx, session).await?;
        let rows = sqlx::query(
            "SELECT key,value FROM extension_state WHERE session_id=$1 AND extension_id=$2",
        )
        .bind(&session.0)
        .bind(extension_id)
        .fetch_all(&mut *tx)
        .await
        .map_err(db)?;
        let values = rows
            .into_iter()
            .map(|row| {
                Ok((
                    row.try_get("key").map_err(db)?,
                    row.try_get("value").map_err(db)?,
                ))
            })
            .collect::<Result<BTreeMap<_, _>, StoreError>>()?;
        tx.commit().await.map_err(db)?;
        Ok(values)
    }
}
