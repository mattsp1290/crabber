//! Authenticate retired generations before allowing a new scoped dependency.
use super::super::{Error, Postgres, SessionId, Transaction, unknown};
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
    tx: &mut Transaction<'_, Postgres>,
    grant: &PreparationGrant,
    source: &SessionServiceLink,
    proof: &mut Proof,
) -> Result<(), Error> {
    let binding = &grant.original_reservation().owner.binding;
    let session: &SessionId = &binding.session_id;
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
            super::super::public::preflight(tx, session, &id, proof)
                .await
                .map_err(|error| failure(&error))?;
            super::super::protocol::require(
                tx,
                session,
                &crabber_core::SessionOwnerId(id.clone()),
                proof,
            )
            .await
            .map_err(|error| error.into_domain_or(Error::Capacity))?;
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
const IDS: &str = r#"SELECT CAST(octet_length(owner_id) AS BIGINT) AS bytes,CASE WHEN octet_length(owner_id)<=128 THEN owner_id END AS id FROM (
SELECT owner_id FROM session_owner_current WHERE session_id=$1 AND (source_protocol=0 OR CASE WHEN octet_length(data_json)>32768 THEN true ELSE data_json::json #> '{service}' IS NOT NULL OR data_json::json #>> '{snapshot,binding,kind}'='SessionService' END)
UNION SELECT owner_id FROM session_owner_audits WHERE session_id=$1 AND operation='Reserve' AND CASE WHEN octet_length(data_json)>32768 THEN true ELSE data_json::json #> '{record,service}' IS NOT NULL OR data_json::json #>> '{record,snapshot,binding,kind}'='SessionService' END
UNION SELECT owner_id FROM session_owner_publications WHERE session_id=$1 AND sequence=1 AND (data_json::json #> '{record,service}' IS NOT NULL OR data_json::json #>> '{record,snapshot,binding,kind}'='SessionService')
) candidates WHERE owner_id COLLATE "C">$2 COLLATE "C" ORDER BY owner_id COLLATE "C" LIMIT 32"#;
