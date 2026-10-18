//! Bounded erasure metadata in one coherent statement, with no history reads.
use super::SqliteStore;
use crate::deletion::Tombstone;
use crate::{DeletionError as Error, DeletionReceipt, SessionDisposition};
use crabber_core::{ControlId, InputFingerprint, SessionId, SessionRevision};
use sqlx::Row;
mod coordinator;
mod purge;
mod readiness;
#[cfg(test)]
mod tests;

fn unknown(_: impl std::fmt::Debug) -> Error {
    Error::UnknownStoreOutcome
}
pub(super) async fn ensure_live(
    tx: &mut crate::transaction::Transaction<'_, sqlx::Sqlite>,
    session: &SessionId,
) -> Result<(), crate::StoreError> {
    let deleted: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=$1)")
            .bind(&session.0)
            .fetch_one(&mut **tx)
            .await
            .map_err(super::db)?;
    if deleted {
        return Err(crate::StoreError::SessionDeleted);
    }
    Ok(())
}
fn revision(value: i64) -> Result<SessionRevision, Error> {
    SessionRevision::new(u64::try_from(value).map_err(unknown)?).map_err(unknown)
}
const VIEW: &str = "SELECT t.session_id IS NOT NULL AS present,CASE WHEN length(CAST(t.operation_id AS BLOB))<=128 THEN t.operation_id END AS operation_id,t.expected_revision,t.final_revision,CASE WHEN length(CAST(t.semantic_digest AS BLOB))<=64 THEN t.semantic_digest END AS semantic_digest,t.deleted_at_seconds,t.deleted_at_nanos,t.format_version,(SELECT public_revision FROM sessions WHERE id=$1) AS live_revision FROM (SELECT 1) singleton LEFT JOIN session_tombstones t ON t.session_id=$1";
fn decode_view(
    row: &sqlx::sqlite::SqliteRow,
    session: &SessionId,
) -> Result<(Option<Tombstone>, Option<SessionRevision>), Error> {
    let live = row
        .try_get::<Option<i64>, _>("live_revision")
        .map_err(unknown)?
        .map(revision)
        .transpose()?;
    if !row.try_get::<bool, _>("present").map_err(unknown)? {
        return Ok((None, live));
    }
    let tombstone = Tombstone {
        format_version: u8::try_from(row.try_get::<i64, _>("format_version").map_err(unknown)?)
            .map_err(unknown)?,
        expected_revision: revision(row.try_get("expected_revision").map_err(unknown)?)?,
        receipt: DeletionReceipt {
            session_id: session.clone(),
            operation_id: ControlId(row.try_get("operation_id").map_err(unknown)?),
            semantic_digest: InputFingerprint::new(
                row.try_get::<String, _>("semantic_digest")
                    .map_err(unknown)?,
            )
            .map_err(unknown)?,
            final_revision: revision(row.try_get("final_revision").map_err(unknown)?)?,
            deleted_at: crate::deletion::timestamp(
                row.try_get("deleted_at_seconds").map_err(unknown)?,
                row.try_get("deleted_at_nanos").map_err(unknown)?,
            )?,
        },
    };
    tombstone.verify(session)?;
    if live.is_some() {
        return Err(Error::UnknownStoreOutcome);
    }
    Ok((Some(tombstone), None))
}
impl SqliteStore {
    async fn deletion_view(
        &self,
        session: &SessionId,
    ) -> Result<(Option<Tombstone>, Option<SessionRevision>), Error> {
        crate::deletion::validate_session(session)?;
        let row = sqlx::query(VIEW)
            .bind(&session.0)
            .fetch_one(&self.readers)
            .await
            .map_err(unknown)?;
        decode_view(&row, session)
    }
    pub(super) async fn deletion_receipt(
        &self,
        session: &SessionId,
        id: &ControlId,
    ) -> Result<Option<DeletionReceipt>, Error> {
        crate::management::validate_scope(session, id).map_err(|_| Error::InvalidRequest)?;
        let (tombstone, _) = self.deletion_view(session).await?;
        let Some(tombstone) = tombstone else {
            return Ok(None);
        };
        if tombstone.receipt.operation_id != *id {
            return Err(Error::Deleted);
        }
        Ok(Some(tombstone.receipt))
    }
    pub(super) async fn disposition(
        &self,
        session: &SessionId,
    ) -> Result<SessionDisposition, Error> {
        let (tombstone, live) = self.deletion_view(session).await?;
        Ok(match (tombstone, live) {
            (Some(tombstone), _) => SessionDisposition::Deleted {
                final_revision: tombstone.receipt.final_revision,
                deleted_at: tombstone.receipt.deleted_at,
            },
            (None, Some(revision)) => SessionDisposition::Present { revision },
            (None, None) => SessionDisposition::Missing,
        })
    }
}

#[cfg(test)]
pub(in crate::sqlite) use readiness::classifier_tests::verify as verify_initial_classifier_fixture;
