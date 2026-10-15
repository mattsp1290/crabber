use super::{SqliteStore, db};
use crate::{
    ListSessions, ManageSession, ManagementError as Error, ManagementReceipt, SessionListPage,
    SessionSummary, management,
};
use crabber_core::{ControlId, Session, SessionId, SessionRevision};
use sqlx::{Row, Sqlite, Transaction};

const CURRENT: &str = "SELECT s.id,s.public_revision,CASE WHEN length(CAST(s.data AS BLOB))<=16384 THEN s.data END AS session_data,m.workspace_id,m.archived,m.format_version,CASE WHEN length(CAST(m.data_json AS BLOB))<=2048 THEN m.data_json END AS metadata FROM sessions s LEFT JOIN session_management m ON m.session_id=s.id WHERE s.id=$1";
const RECEIPT: &str = "SELECT r.kind,r.semantic_digest,r.event_seq,CASE WHEN length(CAST(r.data_json AS BLOB))<=16384 THEN r.data_json END AS receipt,CASE WHEN length(CAST(a.data_json AS BLOB))<=32768 THEN a.data_json END AS audit FROM operation_receipts r LEFT JOIN session_management_audit a ON a.session_id=r.session_id AND a.operation_id=r.operation_id WHERE r.session_id=$1 AND r.operation_id=$2";
const LIST: &str = "SELECT s.id,s.public_revision,CASE WHEN length(CAST(s.data AS BLOB))<=16384 THEN s.data END AS session_data,m.workspace_id,m.archived,m.format_version,CASE WHEN length(CAST(m.data_json AS BLOB))<=2048 THEN m.data_json END AS metadata FROM sessions s LEFT JOIN session_management m ON m.session_id=s.id WHERE json_extract(s.data,'$.workspace_id')=$1 AND ($2 OR COALESCE(m.archived,0)=0) AND s.id>$3 ORDER BY s.id COLLATE BINARY LIMIT $4";
fn decode_current(
    row: &sqlx::sqlite::SqliteRow,
) -> Result<(Session, management::Metadata, SessionRevision), Error> {
    let session: Session =
        super::records::decode_row(row, "session_data").map_err(management::unknown)?;
    if session.id.0 != row.get::<String, _>("id") {
        return Err(Error::UnknownStoreOutcome);
    }
    let metadata = match row.get::<Option<i32>, _>("format_version") {
        None => management::Metadata::default(),
        Some(1) => {
            let text: String = row
                .get::<Option<String>, _>("metadata")
                .ok_or(Error::UnknownStoreOutcome)?;
            let metadata: management::Metadata =
                serde_json::from_str(&text).map_err(management::unknown)?;
            if row.get::<Option<String>, _>("workspace_id").as_deref()
                != Some(&session.workspace_id)
                || row.get::<Option<bool>, _>("archived") != Some(metadata.archived)
            {
                return Err(Error::UnknownStoreOutcome);
            }
            metadata
        }
        Some(_) => return Err(Error::UnknownStoreOutcome),
    };
    management::validate_stored(&session, &metadata).map_err(management::unknown)?;
    let raw: i64 = row.get("public_revision");
    let revision = SessionRevision::new(u64::try_from(raw).map_err(management::unknown)?)
        .map_err(management::unknown)?;
    Ok((session, metadata, revision))
}
async fn current(
    tx: &mut Transaction<'_, Sqlite>,
    id: &SessionId,
) -> Result<Option<(Session, management::Metadata, SessionRevision)>, Error> {
    sqlx::query(CURRENT)
        .bind(&id.0)
        .fetch_optional(&mut **tx)
        .await
        .map_err(db)
        .map_err(management::unknown)?
        .map(|row| decode_current(&row))
        .transpose()
}
async fn receipt(
    tx: &mut Transaction<'_, Sqlite>,
    session: &SessionId,
    id: &ControlId,
) -> Result<Option<ManagementReceipt>, Error> {
    let Some(row) = sqlx::query(RECEIPT)
        .bind(&session.0)
        .bind(&id.0)
        .fetch_optional(&mut **tx)
        .await
        .map_err(db)
        .map_err(management::unknown)?
    else {
        return Ok(None);
    };
    if row.get::<String, _>("kind") != "session_management" {
        return Err(Error::SemanticConflict);
    }
    if row.get::<Option<i64>, _>("event_seq").is_some() {
        return Err(Error::UnknownStoreOutcome);
    }
    let saved: ManagementReceipt = serde_json::from_str(
        &row.get::<Option<String>, _>("receipt")
            .ok_or(Error::UnknownStoreOutcome)?,
    )
    .map_err(management::unknown)?;
    let audit: management::Audit = serde_json::from_str(
        &row.get::<Option<String>, _>("audit")
            .ok_or(Error::UnknownStoreOutcome)?,
    )
    .map_err(management::unknown)?;
    if saved.session_id != *session
        || saved.operation_id != *id
        || saved.semantic_digest.as_str() != row.get::<String, _>("semantic_digest")
    {
        return Err(Error::UnknownStoreOutcome);
    }
    management::verify_audit(&audit, &saved)?;
    Ok(Some(saved))
}
async fn finish<T>(tx: Transaction<'_, Sqlite>, result: Result<T, Error>) -> Result<T, Error> {
    super::transactions::finish(tx, result, management::unknown).await
}
impl SqliteStore {
    pub(super) async fn management_receipt(
        &self,
        session: &SessionId,
        id: &ControlId,
    ) -> Result<Option<ManagementReceipt>, Error> {
        management::validate_scope(session, id)?;
        let mut tx = self
            .readers
            .begin()
            .await
            .map_err(db)
            .map_err(management::unknown)?;
        let result = receipt(&mut tx, session, id).await;
        finish(tx, result).await
    }
    pub(super) async fn read_summary(&self, session: &SessionId) -> Result<SessionSummary, Error> {
        let mut tx = self
            .readers
            .begin()
            .await
            .map_err(db)
            .map_err(management::unknown)?;
        let result = current(&mut tx, session).await.and_then(|value| {
            let (session, metadata, revision) = value.ok_or(Error::NotFound)?;
            Ok(management::summary(&session, metadata, revision))
        });
        finish(tx, result).await
    }
    pub(super) async fn summaries(&self, request: ListSessions) -> Result<SessionListPage, Error> {
        management::validate_list(&request)?;
        let rows = sqlx::query(LIST)
            .bind(&request.workspace_id)
            .bind(request.include_archived)
            .bind(
                request
                    .cursor
                    .as_ref()
                    .map_or("", |cursor| cursor.after.0.as_str()),
            )
            .bind(i64::from(request.limit) + 1)
            .fetch_all(&self.readers)
            .await
            .map_err(db)
            .map_err(management::unknown)?;
        let summaries = rows
            .into_iter()
            .map(|row| {
                let (session, metadata, revision) = decode_current(&row)?;
                Ok(management::summary(&session, metadata, revision))
            })
            .collect::<Result<Vec<_>, Error>>()?;
        Ok(management::list_page(&request, summaries))
    }
    pub(super) async fn manage(&self, request: ManageSession) -> Result<ManagementReceipt, Error> {
        let digest = request.fingerprint()?;
        let mut tx = self.begin_write().await.map_err(management::unknown)?;
        let result = async {

            if let Some(saved) = receipt(&mut tx, &request.session_id, &request.id).await? {
                if saved.semantic_digest != digest { return Err(Error::SemanticConflict); }
                return Ok((saved, false));
            }
            let existing = current(&mut tx, &request.session_id).await?;
            let now = self.clock.now();
            let (session, metadata, revision) = management::apply(&request, existing, now)?;
            let saved = ManagementReceipt { session_id: session.id.clone(), operation_id: request.id.clone(), semantic_digest: digest, revision, summary: management::summary(&session, metadata.clone(), revision) };
            sqlx::query("INSERT INTO sessions(id,data,public_revision) VALUES($1,$2,$3) ON CONFLICT(id) DO UPDATE SET data=excluded.data,public_revision=excluded.public_revision").bind(&session.id.0).bind(super::text(&session).map_err(management::unknown)?).bind(i64::try_from(revision.get()).map_err(management::unknown)?).execute(&mut *tx).await.map_err(db).map_err(management::unknown)?;
            sqlx::query("INSERT INTO session_management(session_id,workspace_id,archived,format_version,data_json) VALUES($1,$2,$3,1,$4) ON CONFLICT(session_id) DO UPDATE SET archived=excluded.archived,data_json=excluded.data_json").bind(&session.id.0).bind(&session.workspace_id).bind(metadata.archived).bind(serde_json::to_string(&metadata).map_err(management::unknown)?).execute(&mut *tx).await.map_err(db).map_err(management::unknown)?;
            sqlx::query("INSERT INTO session_management_audit(session_id,operation_id,data_json) VALUES($1,$2,$3)").bind(&session.id.0).bind(&request.id.0).bind(serde_json::to_string(&management::Audit { request: request.clone(), receipt: saved.clone(), created_at: now }).map_err(management::unknown)?).execute(&mut *tx).await.map_err(db).map_err(management::unknown)?;
            #[cfg(test)]
            if self.management_fault.load(std::sync::atomic::Ordering::SeqCst) == 1 { return Err(Error::UnknownStoreOutcome); }
            sqlx::query("INSERT INTO operation_receipts(session_id,operation_id,kind,semantic_digest,event_seq,data_json) VALUES($1,$2,'session_management',$3,NULL,$4)").bind(&session.id.0).bind(&request.id.0).bind(saved.semantic_digest.as_str()).bind(serde_json::to_string(&saved).map_err(management::unknown)?).execute(&mut *tx).await.map_err(db).map_err(management::unknown)?;
            Ok((saved, true))
        }.await;
        let result = finish(tx, result).await;
        let (saved, changed) = result?;
        let _ = changed; // Production builds do not inject acknowledgement loss.
        #[cfg(test)]
        if self
            .management_fault
            .load(std::sync::atomic::Ordering::SeqCst)
            == 2
            && changed
        {
            return Err(Error::UnknownStoreOutcome);
        }
        Ok(saved)
    }
}

#[cfg(test)]
#[path = "management/tests.rs"]
mod tests;

/// Admission uses the same bounded, coherent decoder as metadata reads. Missing
/// metadata is the explicit legacy baseline; contradictory persisted metadata
/// cannot bypass archive visibility by changing only its relational index.
pub(super) async fn admission_archived(
    tx: &mut Transaction<'_, Sqlite>,
    id: &SessionId,
) -> Result<bool, crate::StoreError> {
    let query = format!("{CURRENT} AND m.session_id IS NOT NULL");
    let row = sqlx::query(&query)
        .bind(&id.0)
        .fetch_optional(&mut **tx)
        .await
        .map_err(db)?;
    let Some(row) = row else {
        return Ok(false);
    };
    let (_, metadata, _) = decode_current(&row)
        .map_err(|_| crate::StoreError::Validation("invalid session management metadata".into()))?;
    Ok(metadata.archived)
}
