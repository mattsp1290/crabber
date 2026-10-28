//! Original immutable metadata replay does not consult current eligibility.
use super::super::{Error, SessionId, SessionOwnerId, reads, unknown};
use crate::session_owner::service::{
    RegisterSessionServiceDependency, SessionServiceDependencyReceipt,
};
use crate::transaction::Transaction;
use sqlx::Sqlite;
pub(super) async fn exists(
    tx: &mut Transaction<'_, Sqlite>,
    session: &SessionId,
    id: &SessionOwnerId,
) -> Result<bool, Error> {
    sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM session_owner_current WHERE session_id=$1 AND owner_id=$2)",
    )
    .bind(&session.0)
    .bind(&id.0)
    .fetch_one(&mut **tx)
    .await
    .map_err(unknown)
}
pub(super) async fn receipt_with_proof(
    tx: &mut Transaction<'_, Sqlite>,
    request: &RegisterSessionServiceDependency,
    proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<Option<SessionServiceDependencyReceipt>, Error> {
    let source = request.source()?;
    let ids = source.identities()?;
    let session = &source.root.session_id;
    let Some(reserved) = reads::audit_with_proof(tx, session, &ids.reserve, proof)
        .await
        .map_err(crate::public_session::v2::proof::ReadError::owner)?
    else {
        return Ok(None);
    };
    if reserved.receipt.semantic_digest
        != source.fingerprint(&source.reservation(&request.grant)?)?
        || reserved.record.service.as_deref() != Some(&source)
    {
        return Err(Error::SemanticConflict);
    }
    let began = reads::audit_with_proof(tx, session, &ids.begin, proof)
        .await
        .map_err(crate::public_session::v2::proof::ReadError::owner)?
        .ok_or(Error::UnknownStoreOutcome)?;
    let root_reserve = reads::audit_with_proof(tx, session, &source.root.root_reserve, proof)
        .await
        .map_err(crate::public_session::v2::proof::ReadError::owner)?
        .ok_or(Error::UnknownStoreOutcome)?;
    let root_begin = reads::audit_with_proof(tx, session, &source.root.root_begin, proof)
        .await
        .map_err(crate::public_session::v2::proof::ReadError::owner)?
        .ok_or(Error::UnknownStoreOutcome)?;
    crate::session_owner::audit::verify_current(&began.record, &reserved, &began)?;
    crate::session_owner::preparation::resource_source::verify(
        &reserved.record,
        &root_reserve,
        &root_begin,
    )?;
    let receipt = SessionServiceDependencyReceipt {
        source,
        reserved: reserved.receipt,
        began: began.receipt,
    };
    receipt.verify()?;
    Ok(Some(receipt))
}

pub(super) fn ensure_available(
    records: &[super::super::Record],
    scope: &crate::session_owner::service::SessionServiceGenerationScope,
) -> Result<(), Error> {
    if records.len() >= crate::session_owner::service::MAX_SESSION_SERVICES_PER_PREPARATION {
        return Err(Error::Capacity);
    }
    if records.iter().any(|record| {
        record.service.as_ref().is_some_and(|link| {
            std::mem::discriminant(&link.generation.scope) == std::mem::discriminant(scope)
        })
    }) {
        return Err(Error::InvalidRequest);
    }
    Ok(())
}
