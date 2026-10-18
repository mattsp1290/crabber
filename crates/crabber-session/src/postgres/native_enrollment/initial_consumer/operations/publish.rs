//! Exact native publishers; no public Raw wrappers or reconstructed authority.
use crate::native_enrollment::initial_consumer as domain;
use crate::postgres::transaction::Transaction;
use crate::public_session::v2::proof::Proof;
use crate::session_owner::{
    SessionOwnerError as Error, owned_scope::NativeConsumerParts,
    preparation::PreparationCommitError,
};
use crabber_core::{Run, RunFence};
use sqlx::Postgres;
fn copied<T: serde::Serialize + Clone>(
    value: &T,
    proof: &mut Proof,
) -> Result<T, PreparationCommitError> {
    proof
        .measure(value)
        .map_err(|error| domain::failure(&error))?;
    Ok(value.clone())
}
fn owned(value: &str, proof: &mut Proof) -> Result<String, PreparationCommitError> {
    proof
        .measure(&value)
        .map_err(|error| domain::failure(&error))?;
    Ok(value.to_owned())
}
pub(super) async fn apply(
    store: &crate::postgres::PostgresStore,
    tx: &mut Transaction<'_, Postgres>,
    parts: &mut NativeConsumerParts<'_>,
    run: &Run,
    call: &domain::VerifiedConsumerCall<'_, '_>,
) -> Result<(domain::ConsumerOutput, domain::Original), PreparationCommitError> {
    let admitted = match &parts.birth.completion {
        crate::session_owner::preparation::PreparationCompletion::Admitted(value) => value,
        crate::session_owner::preparation::PreparationCompletion::Keyed(
            crate::KeyedAdmitOutcome::Started { admitted, .. },
        ) => admitted,
        _ => return Err(Error::Unsupported.into()),
    };
    let fence = &admitted.fence;
    parts
        .proof
        .measure(&(run, fence))
        .map_err(|error| domain::failure(&error))?;
    crate::attempt::verify_run(run, fence, store.clock.now()).map_err(|_| Error::StaleOwner)?;
    match parts.input {
        domain::ConsumerInput::Register(_) => {
            Box::pin(register(store, tx, parts, fence, call)).await
        }
        domain::ConsumerInput::Activate { .. } => {
            Box::pin(activate(store, tx, parts, fence, call)).await
        }
        domain::ConsumerInput::KeyedBegin { .. } => {
            Box::pin(keyed_begin(store, tx, parts, run, fence)).await
        }
        domain::ConsumerInput::AssistantMessage(_) => Box::pin(message(tx, parts, run)).await,
        domain::ConsumerInput::Lifecycle { .. } => Box::pin(lifecycle(tx, parts, run)).await,
        domain::ConsumerInput::ReserveInitial(_)
        | domain::ConsumerInput::BeginInitial { .. }
        | domain::ConsumerInput::Conclude { .. } => {
            Box::pin(attempt(store, tx, parts, run, fence, call)).await
        }
    }
}
async fn attempt(
    store: &crate::postgres::PostgresStore,
    tx: &mut Transaction<'_, Postgres>,
    parts: &mut NativeConsumerParts<'_>,
    run: &Run,
    fence: &RunFence,
    call: &domain::VerifiedConsumerCall<'_, '_>,
) -> Result<(domain::ConsumerOutput, domain::Original), PreparationCommitError> {
    let (request, authority, conclusion) = match parts.input {
        domain::ConsumerInput::ReserveInitial(request) => (*request, None, None),
        domain::ConsumerInput::BeginInitial { request, authority } => {
            (*request, Some(*authority), None)
        }
        domain::ConsumerInput::Conclude {
            request,
            authority,
            cleanup,
        } => {
            cleanup.verify(parts.association, parts.proof)?;
            (&request.finish, Some(*authority), Some(*request))
        }
        _ => return Err(Error::Unsupported.into()),
    };
    let before_run = conclusion.map(|_| copied(run, parts.proof)).transpose()?;
    let (outcome, actual) = Box::pin(crate::postgres::attempt::apply_initial(
        store,
        tx,
        run,
        fence,
        crate::postgres::attempt::InitialInput {
            request,
            authority,
            conclusion,
        },
        call,
        parts.proof,
    ))
    .await
    .map_err(|_| Error::UnknownStoreOutcome)?;
    let original = if let Some(request) = conclusion {
        let value = domain::ConclusionOriginal {
            request: copied(request, parts.proof)?,
            attempt: actual.attempt,
            before_run: before_run.ok_or(Error::UnknownStoreOutcome)?,
            after_run: actual.after_run.ok_or(Error::UnknownStoreOutcome)?,
            settlement: actual.settlement.ok_or(Error::UnknownStoreOutcome)?,
        };
        parts
            .proof
            .charge(std::mem::size_of::<domain::ConclusionOriginal>())
            .map_err(|error| domain::failure(&error))?;
        domain::Original::Conclude(Box::new(value))
    } else {
        parts
            .proof
            .charge(std::mem::size_of::<domain::AttemptOriginal>())
            .map_err(|error| domain::failure(&error))?;
        if matches!(parts.input, domain::ConsumerInput::ReserveInitial(_)) {
            domain::Original::ReserveInitial(Box::new(actual.attempt))
        } else {
            domain::Original::BeginInitial(Box::new(actual.attempt))
        }
    };
    parts
        .proof
        .charge(std::mem::size_of::<crate::AttemptMutationOutcome>())
        .map_err(|error| domain::failure(&error))?;
    Ok((domain::ConsumerOutput::Attempt(Box::new(outcome)), original))
}

async fn register(
    store: &crate::postgres::PostgresStore,
    tx: &mut Transaction<'_, Postgres>,
    parts: &mut NativeConsumerParts<'_>,
    fence: &RunFence,
    call: &domain::VerifiedConsumerCall<'_, '_>,
) -> Result<(domain::ConsumerOutput, domain::Original), PreparationCommitError> {
    let domain::ConsumerInput::Register(request) = parts.input else {
        return Err(Error::Unsupported.into());
    };

    if &request.fence != fence {
        return Err(Error::StaleOwner.into());
    }
    let link = Box::pin(crate::postgres::session_owner::drivers::fresh::enrolled(
        tx,
        request,
        None,
        store.clock.now(),
        call,
        parts.proof,
    ))
    .await?;
    let source = copied(&link, parts.proof)?;
    let actual = store
        .publish_initial_driver(tx, request, link, parts.proof)
        .await?;
    let crate::session_owner::driver::ExecutionDriverOutcome::Applied { receipt, .. } =
        &actual.outcome
    else {
        return Err(Error::UnknownStoreOutcome.into());
    };
    let original = domain::RegisterOriginal {
        request: domain::DriverRequest {
            session: copied(request.context.session_id(), parts.proof)?,
            workspace: owned(request.context.workspace_id(), parts.proof)?,
            directory: owned(request.context.directory(), parts.proof)?,
            fence: copied(&request.fence, parts.proof)?,
        },
        receipt: copied(receipt, parts.proof)?,
        source,
        reserve: actual.reserve,
        begin: actual.begin,
    };
    parts
        .proof
        .charge(std::mem::size_of::<domain::RegisterOriginal>())
        .map_err(|error| domain::failure(&error))?;
    parts
        .proof
        .charge(std::mem::size_of::<
            crate::session_owner::driver::ExecutionDriverOutcome,
        >())
        .map_err(|error| domain::failure(&error))?;
    Ok((
        domain::ConsumerOutput::Registered(Box::new(actual.outcome)),
        domain::Original::Register(Box::new(original)),
    ))
}

async fn activate(
    store: &crate::postgres::PostgresStore,
    tx: &mut Transaction<'_, Postgres>,
    parts: &mut NativeConsumerParts<'_>,
    fence: &RunFence,
    call: &domain::VerifiedConsumerCall<'_, '_>,
) -> Result<(domain::ConsumerOutput, domain::Original), PreparationCommitError> {
    let domain::ConsumerInput::Activate { request, grant } = parts.input else {
        return Err(Error::Unsupported.into());
    };

    if &grant.original_request().fence != fence {
        return Err(Error::StaleOwner.into());
    }
    let record = Box::pin(
        crate::postgres::session_owner::drivers::activation::enrolled(
            tx,
            request,
            grant,
            store.clock.now(),
            call,
            parts.proof,
        ),
    )
    .await?;
    let actual = store
        .publish_driver_activation(tx, request, record, parts.proof)
        .await?;
    let receipt = copied(&actual.audit.receipt, parts.proof)?;
    let original = domain::ActivateOriginal {
        request: copied(*request, parts.proof)?,
        receipt: copied(&receipt, parts.proof)?,
        audit: actual.audit,
    };
    parts
        .proof
        .charge(std::mem::size_of::<domain::ActivateOriginal>())
        .map_err(|error| domain::failure(&error))?;
    parts
        .proof
        .charge(std::mem::size_of::<crate::session_owner::SessionOwnerOutcome>())
        .map_err(|error| domain::failure(&error))?;
    Ok((
        domain::ConsumerOutput::Activated(Box::new(
            crate::session_owner::SessionOwnerOutcome::Applied {
                receipt,
                authority: None,
            },
        )),
        domain::Original::Activate(Box::new(original)),
    ))
}

async fn keyed_begin(
    store: &crate::postgres::PostgresStore,
    tx: &mut Transaction<'_, Postgres>,
    parts: &mut NativeConsumerParts<'_>,
    run: &Run,
    fence: &RunFence,
) -> Result<(domain::ConsumerOutput, domain::Original), PreparationCommitError> {
    let domain::ConsumerInput::KeyedBegin { fence: input } = parts.input else {
        return Err(Error::Unsupported.into());
    };

    if *input != fence {
        return Err(Error::StaleOwner.into());
    }
    let actual = crate::postgres::admission_execution::begin_initial_in(
        tx,
        run,
        fence,
        store.clock.now(),
        parts.proof,
    )
    .await
    .map_err(|_| Error::UnknownStoreOutcome)?;
    let (init, begin) = actual.sources.ok_or(Error::UnknownStoreOutcome)?;
    let revision = crate::postgres::attempt::records::revision(tx, &run.session_id)
        .await
        .map_err(|_| Error::UnknownStoreOutcome)?;
    let original = domain::KeyedBeginOriginal {
        fence: copied(fence, parts.proof)?,
        init,
        begin,
        previous_state: crate::AdmissionExecutionState::Unstarted,
        record: actual.record,
        revision,
    };
    parts
        .proof
        .charge(std::mem::size_of::<domain::KeyedBeginOriginal>())
        .map_err(|error| domain::failure(&error))?;
    Ok((
        domain::ConsumerOutput::KeyedBegan { revision },
        domain::Original::KeyedBegin(Box::new(original)),
    ))
}

async fn message(
    tx: &mut Transaction<'_, Postgres>,
    parts: &mut NativeConsumerParts<'_>,
    run: &Run,
) -> Result<(domain::ConsumerOutput, domain::Original), PreparationCommitError> {
    let domain::ConsumerInput::AssistantMessage(message) = parts.input else {
        return Err(Error::Unsupported.into());
    };

    parts
        .proof
        .measure(*message)
        .map_err(|error| domain::failure(&error))?;
    crate::postgres::insert_message(tx, run, message)
        .await
        .map_err(|_| Error::UnknownStoreOutcome)?;
    let revision = crate::postgres::attempt::records::revision(tx, &run.session_id)
        .await
        .map_err(|_| Error::UnknownStoreOutcome)?;
    let original = domain::MessageOriginal {
        message: copied(*message, parts.proof)?,
        revision,
    };
    parts
        .proof
        .charge(std::mem::size_of::<domain::MessageOriginal>())
        .map_err(|error| domain::failure(&error))?;
    Ok((
        domain::ConsumerOutput::Message { revision },
        domain::Original::AssistantMessage(Box::new(original)),
    ))
}

async fn lifecycle(
    tx: &mut Transaction<'_, Postgres>,
    parts: &mut NativeConsumerParts<'_>,
    run: &Run,
) -> Result<(domain::ConsumerOutput, domain::Original), PreparationCommitError> {
    let domain::ConsumerInput::Lifecycle { kind, event } = parts.input else {
        return Err(Error::Unsupported.into());
    };

    parts
        .proof
        .measure(*event)
        .map_err(|error| domain::failure(&error))?;
    let cursor = crate::postgres::insert_event_captured(tx, run, event)
        .await
        .map_err(|_| Error::UnknownStoreOutcome)?;
    let mut actual = copied(*event, parts.proof)?;
    actual.cursor = Some(cursor);
    let revision = crate::postgres::attempt::records::revision(tx, &run.session_id)
        .await
        .map_err(|_| Error::UnknownStoreOutcome)?;
    let original = domain::LifecycleOriginal {
        kind: *kind,
        input: copied(*event, parts.proof)?,
        event: actual,
        cursor,
        revision,
    };
    parts
        .proof
        .charge(std::mem::size_of::<domain::LifecycleOriginal>())
        .map_err(|error| domain::failure(&error))?;
    Ok((
        domain::ConsumerOutput::Lifecycle { cursor, revision },
        domain::Original::Lifecycle(Box::new(original)),
    ))
}
