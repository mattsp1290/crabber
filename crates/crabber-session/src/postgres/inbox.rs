//! Backend-local persistence; transaction and failure ordering are unchanged.
use super::{PostgresStore, bump_public_revision, db, inbox_kind, json};
use crate::{InboxKind, StoreError};
use crabber_core::{Message, SessionId};

impl PostgresStore {
    pub(in crate::postgres) async fn enqueue_inbox_message(
        &self,
        session: &SessionId,
        kind: InboxKind,
        message: Message,
    ) -> Result<(), StoreError> {
        let mut tx = self.session_transaction(session).await?;
        if message.session_id != *session {
            return Err(StoreError::Validation(
                "inbox message has wrong session".into(),
            ));
        }
        sqlx::query("SELECT 1 FROM sessions WHERE id=$1 FOR UPDATE")
            .bind(&session.0)
            .fetch_optional(&mut *tx)
            .await
            .map_err(db)?
            .ok_or(StoreError::NotFound)?;
        let record = serde_json::to_string(&message)
            .map_err(|_| StoreError::Validation("record encoding failed".into()))?;
        sqlx::query("INSERT INTO inbox(session_id,kind,data,snapshot_record) VALUES($1,$2,$3,$4)")
            .bind(&session.0)
            .bind(inbox_kind(kind))
            .bind(json(&message)?)
            .bind(record)
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        bump_public_revision(&mut tx, session).await?;
        tx.commit().await.map_err(db)
    }
}
