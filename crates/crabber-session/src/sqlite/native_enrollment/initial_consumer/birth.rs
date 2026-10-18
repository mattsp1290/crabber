//! Atomic hooks around the unchanged closed birth protocol.
use super::{storage, write};
use crate::native_enrollment::{initial_consumer as consumer, pending_initial as birth};
use crate::session_owner::{
    SessionOwnerError as Error, owned_scope::NativeInitialParts,
    preparation::PreparationCommitError,
};
use crate::transaction::Transaction;
use sqlx::Sqlite;
pub(in crate::sqlite) async fn check_birth(
    tx: &mut Transaction<'_, Sqlite>,
    parts: &mut NativeInitialParts<'_>,
    birth_view: birth::Bundle<'_>,
) -> Result<storage::Selected, PreparationCommitError> {
    let selected = storage::load(tx, parts.expected, parts.proof)
        .await
        .map_err(|error| consumer::failure(&error))?;
    let view = selected
        .selection(parts.proof)
        .map_err(|error| consumer::failure(&error))?;
    view.verify(parts.expected, parts.proof)
        .map_err(|error| consumer::failure(&error))?;
    super::members::verify(tx, &view, parts.proof)
        .await
        .map_err(|error| consumer::failure(&error))?;
    match parts.consumer {
        Some(actual) => {
            actual.verify(parts.original, parts.context, parts.expected, parts.proof)?;
            if view.empty() {
                if !birth_view.empty() {
                    return Err(Error::Unsupported.into());
                }
            } else {
                view.verify_birth(birth_view, parts.proof)
                    .map_err(|error| consumer::failure(&error))?;
            }
        }
        None => {
            if !view.empty() {
                return Err(Error::Unsupported.into());
            }
        }
    }
    Ok(selected)
}
pub(in crate::sqlite) async fn open(
    tx: &mut Transaction<'_, Sqlite>,
    parts: &mut NativeInitialParts<'_>,
    prefix: &birth::prefix::HistoricalInitialPrefix<'_>,
    opened: &birth::OpenRecord,
    selected: &storage::Selected,
) -> Result<(), PreparationCommitError> {
    if parts.consumer.is_some() {
        let view = selected
            .selection(parts.proof)
            .map_err(|error| consumer::failure(&error))?;
        let delta = consumer::prepare_anchor(prefix, opened, &view, parts.proof)?;
        write::persist(tx, &delta, parts.proof)
            .await
            .map_err(|error| consumer::failure(&error))?;
    }
    Ok(())
}
pub(in crate::sqlite) async fn complete(
    tx: &mut Transaction<'_, Sqlite>,
    parts: &mut NativeInitialParts<'_>,
    before: birth::Bundle<'_>,
    actual: &birth::EnrolledInitialData,
    selected: &storage::Selected,
) -> Result<(), PreparationCommitError> {
    if parts.consumer.is_some() {
        let view = selected
            .selection(parts.proof)
            .map_err(|error| consumer::failure(&error))?;
        let delta = consumer::link_initial(&view, before, actual, parts.proof)?;
        write::persist(tx, &delta, parts.proof)
            .await
            .map_err(|error| consumer::failure(&error))?;
    }
    Ok(())
}
pub(in crate::sqlite) async fn close(
    tx: &mut Transaction<'_, Sqlite>,
    parts: &mut NativeInitialParts<'_>,
    prefix: &birth::prefix::HistoricalInitialPrefix<'_>,
    before: birth::Bundle<'_>,
    selected: &storage::Selected,
) -> Result<Option<birth::CloseRecord>, PreparationCommitError> {
    let Some(actual) = parts.consumer else {
        return Ok(None);
    };
    let local = actual.verify_close(parts.original, parts.context, parts.expected, parts.proof)?;
    let view = selected
        .selection(parts.proof)
        .map_err(|error| consumer::failure(&error))?;
    let record = match consumer::prepare_paired_close(
        &view,
        before,
        prefix,
        parts.expected,
        local,
        parts.proof,
    )? {
        consumer::PairedCloseDecision::Publish(delta) => {
            super::super::pending_initial::write::persist(tx, &delta.birth, parts.proof)
                .await
                .map_err(|error| consumer::failure(&error))?;
            write::persist(tx, &delta.consumer, parts.proof)
                .await
                .map_err(|error| consumer::failure(&error))?;
            delta.record
        }
        consumer::PairedCloseDecision::Replayed(record) => *record,
    };
    Ok(Some(record))
}
