//! Original source verification, closed operation selection and atomic assignment.
use super::{budget, publish_origin, storage, write};
use crate::native_enrollment::pending_initial::{
    Assignment, CloseDecision, EnrolledInitialData, OpenDecision, prefix, prepare_assignment,
    prepare_close, prepare_open, verify_complete,
};
use crate::postgres::PostgresStore;
use crate::postgres::transaction::Transaction;
use crate::session_owner::{
    SessionOwnerError as Error,
    owned_scope::{NativeInitialOperation, NativeInitialParts, PendingInitialNativeReply},
    preparation::PreparationCommitError,
};
use sqlx::Postgres;
pub(super) async fn apply(
    store: &PostgresStore,
    tx: &mut Transaction<'_, Postgres>,
    parts: &mut NativeInitialParts<'_>,
) -> Result<PendingInitialNativeReply, PreparationCommitError> {
    let session = parts.context.session_id();

    crate::postgres::deletion::ensure_live(tx, session)
        .await
        .map_err(crate::deletion::owner_error)?;
    let audits = Box::pin(crate::postgres::session_owner::pending_prefix::selected(
        tx,
        parts.original,
        Some(parts.selected),
        parts.proof,
    ))
    .await?;
    let prefix = prefix::verify_selected(
        parts.original,
        prefix::InitialPrefixAudits {
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
    let state = storage::load(
        tx,
        &parts.expected.key,
        &parts.expected.identity.epoch,
        parts.proof,
    )
    .await
    .map_err(budget)?;
    state
        .view()
        .verify(&parts.expected.key, parts.proof)
        .map_err(budget)?;
    let consumer = super::super::initial_consumer::check_birth(tx, parts, state.view()).await?;
    if let Some(raw) = Box::pin(crate::postgres::session_owner::pending_raw::read(
        tx,
        parts.original,
        parts.expected,
        parts.selected,
        parts.proof,
    ))
    .await?
    {
        if state.slot.is_some() {
            return Err(Error::UnknownStoreOutcome.into());
        }
        return PendingInitialNativeReply::raw_completed(raw, parts.proof);
    }

    let reply = change(store, tx, parts, &prefix, &state, &consumer).await?;
    #[cfg(test)]
    if store
        .session_owner_fault
        .load(std::sync::atomic::Ordering::SeqCst)
        == 1
    {
        return Err(Error::UnknownStoreOutcome.into());
    }
    Ok(reply)
}
async fn change(
    store: &PostgresStore,
    tx: &mut Transaction<'_, Postgres>,
    parts: &mut NativeInitialParts<'_>,
    prefix: &prefix::HistoricalInitialPrefix<'_>,
    state: &storage::Selected,
    consumer: &super::super::initial_consumer::storage::Selected,
) -> Result<PendingInitialNativeReply, PreparationCommitError> {
    let reply = match parts.operation {
        NativeInitialOperation::Open => {
            match prepare_open(prefix, parts.expected, state.view(), parts.proof)? {
                OpenDecision::Publish { record, delta } => {
                    Box::pin(store.pending_initial_fresh(tx, parts.original, parts.proof)).await?;
                    write::persist(tx, &delta, parts.proof)
                        .await
                        .map_err(budget)?;
                    super::super::initial_consumer::open(tx, parts, prefix, &record, consumer)
                        .await?;
                    PendingInitialNativeReply::opened(record, parts.proof)?
                }
                OpenDecision::Replayed(_) => {
                    PendingInitialNativeReply::already_opened(parts.expected.clone(), parts.proof)?
                }
                OpenDecision::Closed(record) => {
                    PendingInitialNativeReply::closed(record, parts.proof)?
                }
                OpenDecision::Conflict => return Err(Error::SemanticConflict.into()),
            }
        }
        NativeInitialOperation::Seal => {
            if let Some(record) =
                super::super::initial_consumer::close(tx, parts, prefix, state.view(), consumer)
                    .await?
            {
                return PendingInitialNativeReply::closed(record, parts.proof);
            }
            match prepare_close(prefix, parts.expected, state.view(), parts.proof)? {
                CloseDecision::Publish { record, delta } => {
                    write::persist(tx, &delta, parts.proof)
                        .await
                        .map_err(budget)?;
                    PendingInitialNativeReply::closed(record, parts.proof)?
                }
                CloseDecision::Replayed(record) => {
                    PendingInitialNativeReply::closed(record, parts.proof)?
                }
                CloseDecision::Conflict => return Err(Error::SemanticConflict.into()),
            }
        }
        NativeInitialOperation::Complete => {
            complete(store, tx, parts, prefix, state, consumer).await?
        }
    };
    Ok(reply)
}
async fn complete(
    store: &PostgresStore,
    tx: &mut Transaction<'_, Postgres>,
    parts: &mut NativeInitialParts<'_>,
    prefix: &prefix::HistoricalInitialPrefix<'_>,
    state: &storage::Selected,
    consumer: &super::super::initial_consumer::storage::Selected,
) -> Result<PendingInitialNativeReply, PreparationCommitError> {
    state
        .view()
        .verify(&parts.expected.key, parts.proof)
        .map_err(budget)?;
    if state
        .epoch
        .as_ref()
        .is_none_or(|epoch| epoch.correlation != *parts.expected)
    {
        return Err(Error::SemanticConflict.into());
    }
    Ok(if let Some(close) = &state.close {
        parts.proof.measure(close).map_err(|_| Error::Capacity)?;
        PendingInitialNativeReply::closed(close.clone(), parts.proof)?
    } else if let Some(assignment) = &state.assignment {
        parts
            .proof
            .measure(assignment)
            .map_err(|_| Error::Capacity)?;
        PendingInitialNativeReply::already_published(assignment.clone(), parts.proof)?
    } else {
        let opened = parts.opened.ok_or(Error::InvalidTransition)?;
        if state.epoch.as_ref() != Some(&opened.epoch)
            || state.slot.as_ref() != Some(&opened.slot)
            || state.registered.as_ref() != Some(&opened.registered)
        {
            return Err(Error::SemanticConflict.into());
        }
        let call = verify_complete(
            prefix,
            parts.expected,
            parts.selected,
            state.view(),
            parts.proof,
        )?;
        let (completion, first) =
            Box::pin(store.complete_pending_initial(tx, parts.original, &call, parts.proof))
                .await?;
        let assignment = Assignment::from_actual(&call, &completion, &first, parts.proof)?;
        let delta = prepare_assignment(&call, &assignment, state.view(), parts.proof)?;
        write::persist(tx, &delta, parts.proof)
            .await
            .map_err(budget)?;
        publish_origin(tx, &assignment, &first, parts.proof).await?;
        let actual = EnrolledInitialData {
            completion,
            first,
            assignment,
        };
        super::super::initial_consumer::complete(tx, parts, state.view(), &actual, consumer)
            .await?;
        PendingInitialNativeReply::completed(actual)
    })
}
