//! Inbox persistence owns its immediate transaction and revision publication.
use super::records::inbox_kind;
use super::transactions::finish;
use super::{SqliteStore, StoreError, db, text};
use crate::InboxKind;
use crabber_core::{Message, SessionId};

impl SqliteStore {
    pub(in crate::sqlite) async fn enqueue_inbox_message(
        &self,
        session: &SessionId,
        kind: InboxKind,
        message: Message,
    ) -> Result<(), StoreError> {
        let mut tx = self.begin_write().await?;
        let result = async {
            super::deletion::ensure_live(&mut tx, session).await?;
            if message.session_id != *session {
                return Err(StoreError::Validation(
                    "inbox message has wrong session".into(),
                ));
            }

            sqlx::query("SELECT 1 FROM sessions WHERE id=$1")
                .bind(&session.0)
                .fetch_optional(&mut *tx)
                .await
                .map_err(db)?
                .ok_or(StoreError::NotFound)?;
            sqlx::query("INSERT INTO inbox(session_id,kind,data) VALUES($1,$2,$3)")
                .bind(&session.0)
                .bind(inbox_kind(kind))
                .bind(text(&message)?)
                .execute(&mut *tx)
                .await
                .map_err(db)?;
            super::records::bump_public_revision(&mut tx, session).await?;
            Ok(())
        }
        .await;
        finish(tx, result, std::convert::identity).await
    }
}
