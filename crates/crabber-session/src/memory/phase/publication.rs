//! Verify the actual retained result within the publishing transaction.
use super::{Error, Proof, State, phase};
use crate::native_enrollment::caller::{self, Caller, Fresh};
#[derive(Clone, Copy)]
pub(super) struct Published<'a> {
    pub request: &'a crate::PhaseMutation,
    pub effect: phase::PhaseEffect<'a>,
    pub receipt: &'a crate::PhaseReceipt,
    pub issued: Option<&'a crate::PhaseAuthority>,
    pub running_event: Option<crabber_core::EventCursor>,
    pub caller: Caller,
}
pub(super) fn record(
    state: &State,
    run: &crabber_core::Run,
    published: Published<'_>,
    proof: &mut Proof,
    #[cfg(test)] probe: &caller::Probe,
) -> Result<(), Error> {
    let request = published.request;
    if !matches!(
        request.command,
        crate::PhaseCommand::Claim { .. } | crate::PhaseCommand::Begin { .. }
    ) {
        return Ok(());
    }
    super::super::native_enrollment::record_raw_native(
        state,
        published.caller,
        &Fresh::Phase(caller::Phase {
            run,
            parent: state
                .attempts
                .get(&request.command.target().attempt.id)
                .ok_or(Error::UnknownStoreOutcome)?,
            call: state
                .calls
                .get(&request.command.target().call_id)
                .ok_or(Error::UnknownStoreOutcome)?,
            request,
            effect: published.effect,
            record: state
                .phases
                .get(&request.command.target().id)
                .ok_or(Error::UnknownStoreOutcome)?,
            receipt: published.receipt,
            event: state
                .events
                .get(published.receipt.event_cursor)
                .ok_or(Error::UnknownStoreOutcome)?,
            running_event: published
                .running_event
                .map(|cursor| state.events.get(cursor).ok_or(Error::UnknownStoreOutcome))
                .transpose()?,
            issued: published.issued,
        }),
        proof,
        #[cfg(test)]
        probe,
    )
    .map_err(|_| Error::UnknownStoreOutcome)
}

pub(super) fn start(
    state: &mut State,
    run: &crabber_core::Run,
    record: &phase::PhaseRecord,
    start: Option<&crate::PhaseStart>,
    proof: &mut Proof,
) -> Result<Option<crabber_core::EventCursor>, Error> {
    let Some(start) = start else {
        return Ok(None);
    };
    let call = state
        .calls
        .get(&record.snapshot.target.call_id)
        .ok_or(Error::NotFound)?;
    let event = start.materialize(run, call)?;
    proof
        .measure(&event)
        .map_err(|_| Error::UnknownStoreOutcome)?;
    super::super::insert_event_captured(state, run, event)
        .map(Some)
        .map_err(|_| Error::UnknownStoreOutcome)
}
