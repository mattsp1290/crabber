//! Session-scoped extension state reads retain the existing live-session transaction.
use super::transactions::finish;
use super::{SqliteStore, StoreError, db};
use crabber_core::SessionId;
use sqlx::Row;
use std::collections::BTreeMap;

impl SqliteStore {
    pub(in crate::sqlite) async fn read_extension_state(
        &self,
        extension_id: &str,
        session: &SessionId,
    ) -> Result<BTreeMap<String, String>, StoreError> {
        let mut tx = crate::transaction::begin(&self.readers).await.map_err(db)?;
        let result = async {
            super::deletion::ensure_live(&mut tx, session).await?;
            let rows = sqlx::query(
                "SELECT key,value FROM extension_state WHERE session_id=$1 AND extension_id=$2",
            )
            .bind(&session.0)
            .bind(extension_id)
            .fetch_all(&mut *tx)
            .await
            .map_err(db)?;
            rows.into_iter()
                .map(|row| {
                    Ok((
                        row.try_get("key").map_err(db)?,
                        row.try_get("value").map_err(db)?,
                    ))
                })
                .collect()
        }
        .await;
        finish(tx, result, std::convert::identity).await
    }
}
