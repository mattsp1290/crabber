//! Same-transaction independent original chain, never a reconstructed parent grant.
use crate::public_session::v2::proof::Proof;
use crate::session_owner::{SessionOwnerError as Error, record::Record};
use crate::transaction::Transaction;
use sqlx::Sqlite;
pub(in crate::sqlite) fn verify<'a, 'connection: 'a>(
    tx: &'a mut Transaction<'connection, Sqlite>,
    record: &'a Record,
    proof: &'a mut Proof,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), Error>> + Send + 'a>> {
    Box::pin(verify_inner(tx, record, proof))
}
async fn verify_inner(
    tx: &mut Transaction<'_, Sqlite>,
    record: &Record,
    proof: &mut Proof,
) -> Result<(), Error> {
    if record.snapshot.binding.kind != crate::session_owner::SessionOwnerKind::DetachedInvocation {
        return Err(Error::Unsupported);
    }
    let mut selected = None;
    loop {
        let current = selected.as_ref().unwrap_or(record);
        let Some(target) = current
            .snapshot
            .binding
            .parent
            .as_ref()
            .and_then(|parent| parent.invocation.as_ref())
        else {
            break;
        };
        let parent = super::current_owner_with_proof(
            tx,
            &current.snapshot.binding.session_id,
            &target.id,
            proof,
        )
        .await?;
        crate::session_owner::invocation_parent::original(current, &parent)
            .map_err(|_| Error::UnknownStoreOutcome)?;
        selected = Some(parent);
    }
    let current = selected.as_ref().unwrap_or(record);
    let witness = current.parent.as_ref().ok_or(Error::Unsupported)?;
    if !crate::sqlite::native_parent::source_protocol::history(
        tx,
        &witness.source.session_id,
        &witness.source.run_id,
        proof,
    )
    .await
    .map_err(|_| Error::UnknownStoreOutcome)?
    {
        return Err(Error::Unsupported);
    }
    super::public::deletion::verify_parent(tx, witness, proof)
        .await
        .map_err(|_| Error::UnknownStoreOutcome)
}
