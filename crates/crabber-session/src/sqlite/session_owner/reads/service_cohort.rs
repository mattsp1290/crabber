//! Authenticate retired generations before allowing a new scoped dependency.
use super::super::{Error, SessionId, Sqlite, Transaction, unknown};
use crate::public_session::v2::proof::{Failure, Proof};
use crate::session_owner::{
    SessionOwnerState, preparation::PreparationGrant, service::SessionServiceLink,
};
use sqlx::Row;
fn failure(error: &Failure) -> Error {
    match error {
        Failure::Limited(_) => Error::Capacity,
        Failure::Error(_) => Error::UnknownStoreOutcome,
    }
}
pub(in super::super) async fn ensure_generation_slot(
    tx: &mut Transaction<'_, Sqlite>,
    grant: &PreparationGrant,
    source: &SessionServiceLink,
) -> Result<(), Error> {
    let binding = &grant.original_reservation().owner.binding;
    let session: &SessionId = &binding.session_id;
    let mut proof = Proof::bounded(32 * 1024 * 1024);
    let mut after = String::new();
    let mut live = 0;
    loop {
        let rows = sqlx::query(IDS)
            .bind(&session.0)
            .bind(&after)
            .fetch_all(&mut **tx)
            .await
            .map_err(unknown)?;
        if rows.is_empty() {
            break;
        }
        for row in &rows {
            let bytes: i64 = row.try_get("bytes").map_err(unknown)?;
            proof
                .charge(usize::try_from(bytes).map_err(unknown)?)
                .map_err(|error| failure(&error))?;
        }
        for row in rows {
            let id: Option<String> = row.try_get("id").map_err(unknown)?;
            let id = id.ok_or(Error::UnknownStoreOutcome)?;
            if id <= after {
                return Err(Error::UnknownStoreOutcome);
            }
            super::super::public::preflight(tx, session, &id, &mut proof)
                .await
                .map_err(|error| failure(&error))?;
            let record =
                super::current(tx, session, &crabber_core::SessionOwnerId(id.clone())).await?;
            let Some(link) = record.service.as_ref() else {
                return Err(Error::Busy);
            };
            if record.snapshot.binding.workspace_id != binding.workspace_id
                || record.snapshot.binding.directory != binding.directory
            {
                return Err(Error::IdentityMismatch);
            }
            if record.snapshot.state != SessionOwnerState::Joined || record.owned_join.is_none() {
                live += 1;
                if live > crate::session_owner::service::MAX_SESSION_SERVICES_PER_PREPARATION {
                    return Err(Error::Capacity);
                }
                if link.generation.scope == source.generation.scope {
                    return Err(Error::Busy);
                }
            }
            after = id;
        }
    }
    Ok(())
}
const IDS: &str = r"SELECT CAST(length(CAST(owner_id AS BLOB)) AS BIGINT) AS bytes,CASE WHEN length(CAST(owner_id AS BLOB))<=128 THEN owner_id END AS id FROM (
SELECT owner_id FROM session_owner_current WHERE session_id=$1 AND CASE WHEN length(CAST(data_json AS BLOB))>32768 THEN true ELSE json_extract(data_json,'$.service') IS NOT NULL OR json_extract(data_json,'$.snapshot.binding.kind')='SessionService' END
UNION SELECT owner_id FROM session_owner_audits WHERE session_id=$1 AND operation='Reserve' AND CASE WHEN length(CAST(data_json AS BLOB))>32768 THEN true ELSE json_extract(data_json,'$.record.service') IS NOT NULL OR json_extract(data_json,'$.record.snapshot.binding.kind')='SessionService' END
) candidates WHERE owner_id COLLATE BINARY>$2 COLLATE BINARY ORDER BY owner_id COLLATE BINARY LIMIT 32";
