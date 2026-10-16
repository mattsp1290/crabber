//! Current and immutable original membership are independently selected.
use super::super::{Error, Record, SessionId, SessionOwnerId, Sqlite, Transaction, unknown};
use sqlx::Row;
pub(in super::super) async fn services(
    tx: &mut Transaction<'_, Sqlite>,
    session: &SessionId,
    root: &SessionOwnerId,
) -> Result<Vec<Record>, Error> {
    let ids = service_ids(tx, session, root).await?;
    let mut records = Vec::with_capacity(ids.len());
    for id in ids {
        let record = super::current(tx, session, &id).await?;
        if record
            .service
            .as_ref()
            .is_none_or(|link| link.root.root.id != *root || link.root.session_id != *session)
        {
            return Err(Error::UnknownStoreOutcome);
        }
        records.push(record);
    }
    Ok(records)
}
pub(in super::super) async fn service_ids(
    tx: &mut Transaction<'_, Sqlite>,
    session: &SessionId,
    root: &SessionOwnerId,
) -> Result<Vec<SessionOwnerId>, Error> {
    let rows = sqlx::query(QUERY)
        .bind(&session.0)
        .bind(&root.0)
        .fetch_all(&mut **tx)
        .await
        .map_err(unknown)?;
    if rows.len() > crate::session_owner::service::MAX_SESSION_SERVICES_PER_PREPARATION {
        return Err(Error::Capacity);
    }
    rows.into_iter()
        .map(|row| {
            let id: Option<String> = row.try_get("id").map_err(unknown)?;
            let id = id.ok_or(Error::UnknownStoreOutcome)?;
            crate::session_owner::validation::identifier(&id, 128, false).map_err(unknown)?;
            Ok(SessionOwnerId(id))
        })
        .collect()
}
const QUERY: &str = r"SELECT CASE WHEN length(CAST(owner_id AS BLOB))<=128 THEN owner_id END AS id FROM (
SELECT owner_id FROM session_owner_current WHERE session_id=$1 AND CASE WHEN length(CAST(data_json AS BLOB))>32768 THEN true ELSE json_extract(data_json,'$.service.root.root.id')=$2 END
UNION SELECT owner_id FROM session_owner_audits WHERE session_id=$1 AND operation='Reserve' AND CASE WHEN length(CAST(data_json AS BLOB))>32768 THEN true ELSE json_extract(data_json,'$.record.service.root.root.id')=$2 END
) candidates ORDER BY owner_id LIMIT 4";
