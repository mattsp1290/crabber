//! Actual native publication and Raw hook share the original transaction.
use super::super::{OperationReceipt, State, bump_public_revision};
use crate::native_enrollment::caller::{self, Caller, Fresh};
use crate::public_session::v2::proof::Proof;
use crate::{AttemptError as Error, AttemptMutation, AttemptMutationOutcome, AttemptReceipt};
use crabber_core::{EventRecord, InputFingerprint, Run};
pub(super) struct Prepared<'a> {
    pub run: &'a Run,
    pub request: AttemptMutation,
    pub record: crate::attempt::AttemptRecord,
    pub grant: bool,
    pub conclusion: Option<&'a crate::AttemptConclusion>,
    pub settlement: Option<EventRecord>,
    pub digest: InputFingerprint,
    pub now: time::OffsetDateTime,
}
pub(super) fn publish(
    state: &mut State,
    prepared: Prepared<'_>,
    caller: Caller,
    read_proof: &mut Proof,
    #[cfg(test)] store: &super::super::MemoryStore,
) -> Result<AttemptMutationOutcome, Error> {
    let Prepared {
        run,
        request,
        record,
        grant,
        conclusion,
        settlement,
        digest,
        now,
    } = prepared;
    let revision = super::revision(state, &run.session_id)
        .next()
        .map_err(|_| Error::RevisionExhausted)?;
    caller::measure_record(&record, read_proof).map_err(|_| Error::UnknownStoreOutcome)?;
    let authority = grant.then(|| record.authority()).transpose()?;
    read_proof
        .measure(&(&request.command, &record.snapshot))
        .map_err(|_| Error::UnknownStoreOutcome)?;
    let mut event = crate::attempt_conclusion::publication(
        run,
        &request,
        conclusion,
        &record.snapshot,
        digest.clone(),
        settlement,
        now,
    )?;
    let cursor = state
        .events
        .next_cursor()
        .map_err(|_| Error::UnknownStoreOutcome)?;
    event.cursor = Some(cursor);
    read_proof
        .measure(&record.snapshot)
        .map_err(|_| Error::UnknownStoreOutcome)?;
    let receipt = AttemptReceipt {
        operation_id: request.id.clone(),
        semantic_digest: digest,
        revision,
        event_cursor: cursor,
        attempt: record.snapshot.clone(),
    };
    if caller::relevant(&request.command) {
        super::super::native_enrollment::record_raw_native(
            state,
            caller,
            &Fresh::Attempt(caller::Attempt {
                run,
                request: &request,
                record: &record,
                receipt: &receipt,
                event: &event,
                issued: authority.as_ref(),
            }),
            read_proof,
            #[cfg(test)]
            &store.native_raw_probe,
        )
        .map_err(|_| Error::UnknownStoreOutcome)?;
    }
    super::super::native_parent::record_attempt(
        state,
        run,
        &request.command,
        &record,
        &receipt,
        read_proof,
    )?;
    state
        .attempts
        .insert(request.command.target().id.clone(), record);
    bump_public_revision(state, &run.session_id).map_err(|_| Error::UnknownStoreOutcome)?;
    state
        .events
        .append(event)
        .map_err(|_| Error::UnknownStoreOutcome)?;
    state.operation_receipts.insert(
        (run.session_id.clone(), request.id),
        OperationReceipt::Attempt(receipt.clone()),
    );
    super::super::run_control::suspension::save(state, run, conclusion, &receipt);
    #[cfg(test)]
    if store
        .attempt_fault
        .load(std::sync::atomic::Ordering::SeqCst)
        == 1
    {
        return Err(Error::UnknownStoreOutcome);
    }
    Ok(AttemptMutationOutcome::Applied { receipt, authority })
}
