//! Current closed membership is authenticated before replay or any publisher.
use super::{storage, write};
use crate::native_enrollment::{initial_consumer as domain, pending_initial as birth};
use crate::session_owner::{
    SessionOwnerError as Error,
    owned_scope::{NativeConsumerParts, PendingInitialConsumerNativeReply},
    preparation::PreparationCommitError,
};
use crate::sqlite::SqliteStore;
use crate::transaction::Transaction;
use sqlx::Sqlite;
mod publish;
pub(super) async fn apply(
    store: &SqliteStore,
    tx: &mut Transaction<'_, Sqlite>,
    parts: &mut NativeConsumerParts<'_>,
) -> Result<PendingInitialConsumerNativeReply, PreparationCommitError> {
    super::super::super::deletion::ensure_live(tx, parts.context.session_id())
        .await
        .map_err(crate::deletion::owner_error)?;
    verify_prefix(tx, parts).await?;
    let original = super::super::pending_initial::storage::load(
        tx,
        &parts.expected.key,
        &parts.expected.identity.epoch,
        parts.proof,
    )
    .await
    .map_err(|error| domain::failure(&error))?;
    let selected = storage::load(tx, parts.expected, parts.proof)
        .await
        .map_err(|error| domain::failure(&error))?;
    let view = selected
        .selection(parts.proof)
        .map_err(|error| domain::failure(&error))?;
    view.verify(parts.expected, parts.proof)
        .map_err(|error| domain::failure(&error))?;
    view.verify_birth(original.view(), parts.proof)
        .map_err(|error| domain::failure(&error))?;
    super::members::verify(tx, &view, parts.proof)
        .await
        .map_err(|error| domain::failure(&error))?;
    let assignment = original
        .view()
        .assignment
        .ok_or(Error::UnknownStoreOutcome)?;
    parts
        .proof
        .measure(&parts.birth.assignment)
        .map_err(|_| Error::Capacity)?;
    if assignment != &parts.birth.assignment {
        return Err(Error::IdentityMismatch.into());
    }
    let run = super::super::super::attempt::accounting::load(tx, &assignment.run_id, parts.proof)
        .await
        .map_err(|_| Error::UnknownStoreOutcome)?;
    if run.session_id != parts.expected.key.session_id {
        return Err(Error::IdentityMismatch.into());
    }
    // This metadata/current guard authenticates Pending originals but supplies no
    // raw permission. The private transport+exact consumer relation remain required.
    let current =
        super::super::super::native_parent::current_issuance_with_proof(tx, &run, parts.proof)
            .await
            .map_err(|_| Error::UnknownStoreOutcome)?;
    parts.proof.measure(&current).map_err(|_| Error::Capacity)?;
    let result = match domain::prepare_call(
        &view,
        original.view(),
        parts.expected,
        parts.call_id,
        parts.input,
        parts.proof,
    )? {
        domain::CallDecision::Replayed(record) => {
            parts.proof.measure(record).map_err(|_| Error::Capacity)?;
            domain::ConsumerResult::Replayed(record.clone())
        }
        domain::CallDecision::Fresh(verified) => {
            let (output, captured) =
                Box::pin(publish::apply(store, tx, parts, &run, &verified)).await?;
            domain::verify_native_output(&output, &captured, parts.proof)?;
            let mut delta = domain::capture_actual(*verified, captured, parts.proof)?;
            write::persist(tx, &delta, parts.proof)
                .await
                .map_err(|error| domain::failure(&error))?;
            let record = delta.result.take().ok_or(Error::UnknownStoreOutcome)?;
            domain::ConsumerResult::Applied { output, record }
        }
    };
    result.verify_returned(
        parts.expected,
        parts.call_id,
        parts.input,
        &run.id,
        parts.proof,
    )?;
    Ok(PendingInitialConsumerNativeReply::from_result(result))
}

async fn verify_prefix(
    tx: &mut Transaction<'_, Sqlite>,
    parts: &mut NativeConsumerParts<'_>,
) -> Result<(), PreparationCommitError> {
    parts
        .association
        .verify(parts.original, parts.context, parts.expected, parts.proof)?;
    let audits = Box::pin(
        super::super::super::session_owner::pending_prefix::selected(
            tx,
            parts.original,
            Some(parts.selected),
            parts.proof,
        ),
    )
    .await?;
    let prefix = birth::prefix::verify_selected(
        parts.original,
        birth::prefix::InitialPrefixAudits {
            root_reserve: &audits[0],
            root_begin: &audits[1],
            child_reserve: &audits[2],
            child_begin: &audits[3],
            activated: &audits[4],
        },
        Some(parts.selected),
        parts.proof,
    )?;
    if prefix.correlation(&parts.expected.identity, parts.proof)? != *parts.expected {
        return Err(Error::SemanticConflict.into());
    }
    Ok(())
}
