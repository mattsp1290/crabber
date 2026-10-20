//! Compound Reserve+Begin publication for the exact authenticated service source.
use super::super::{
    Audit, Error, MutateSessionOwner, Record, SessionOwnerCommand, SessionOwnerOperation,
    publish_revision, reads,
};
use crate::session_owner::{
    SessionOwnerAuthority,
    service::{
        RegisterSessionServiceDependency, SessionServiceDependencyReceipt, SessionServiceLink,
    },
};
pub(super) async fn register(
    tx: &mut crate::postgres::transaction::Transaction<'_, sqlx::Postgres>,
    request: &RegisterSessionServiceDependency,
    source: &SessionServiceLink,
    now: time::OffsetDateTime,
    proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<(SessionServiceDependencyReceipt, SessionOwnerAuthority), Error> {
    let session = &source.root.session_id;
    let reservation = source.reservation(&request.grant)?;
    if super::metadata::exists(tx, session, &reservation.binding.id).await? {
        return Err(Error::SemanticConflict);
    }
    reads::capacity(tx, session).await?;

    let mut record = Record::reserve(&reservation, None, now)?;
    record.service = Some(Box::new(source.clone()));
    let authority = record.grant()?;
    let reserved = record.receipt(
        reservation.id.clone(),
        source.fingerprint(&reservation)?,
        SessionOwnerOperation::Reserve,
    );
    let publication = publish_revision(tx, session, true).await?;
    reads::publish_with_proof(
        tx,
        &Audit {
            version: 2,
            receipt: reserved,
            record: record.clone(),
            publication: Some(publication),
        },
        true,
        proof,
    )
    .await?;
    let command = MutateSessionOwner {
        id: source.identities()?.begin,
        session_id: session.clone(),
        target: authority.target().clone(),
        command: SessionOwnerCommand::Begin,
    };
    record.mutate(&command, Some(&authority), now)?;
    let began = record.receipt(
        command.id.clone(),
        command.fingerprint()?,
        SessionOwnerOperation::Begin,
    );
    let publication = publish_revision(tx, session, true).await?;
    reads::publish_with_proof(
        tx,
        &Audit {
            version: 2,
            receipt: began,
            record,
            publication: Some(publication),
        },
        false,
        proof,
    )
    .await?;
    let receipt = super::metadata::receipt_with_proof(tx, request, proof)
        .await?
        .ok_or(Error::UnknownStoreOutcome)?;

    Ok((receipt, authority))
}
