//! Borrow the actual published phase result within the original transaction.
use super::{Error, Proof, phase};
use crate::native_enrollment::caller::{self, Caller, Fresh};
use sqlx::{Sqlite, Transaction};
#[derive(Clone, Copy)]
pub(super) struct Published<'a> {
    pub record: &'a phase::PhaseRecord,
    pub receipt: &'a crate::PhaseReceipt,
    pub event: &'a crabber_core::EventRecord,
    pub running_event: Option<&'a crabber_core::EventRecord>,
    pub issued: Option<&'a crate::PhaseAuthority>,
}
pub(super) async fn record(
    tx: &mut Transaction<'_, Sqlite>,
    run: &crabber_core::Run,
    request: &crate::PhaseMutation,
    context: (phase::PhaseEffect<'_>, Caller),
    published: Published<'_>,
    proof: &mut Proof,
    #[cfg(test)] probe: &caller::Probe,
) -> Result<(), Error> {
    if !matches!(
        request.command,
        crate::PhaseCommand::Claim { .. } | crate::PhaseCommand::Begin { .. }
    ) {
        return Ok(());
    }
    let parent = super::super::attempt::load_host_attempt_with_proof(
        tx,
        &run.id,
        Some(&request.command.target().attempt.id),
        proof,
    )
    .await
    .map_err(|_| Error::UnknownStoreOutcome)?
    .ok_or(Error::NotFound)?
    .0;
    let call =
        super::calls::load_with_proof(tx, run, &request.command.target().call_id, proof).await?;
    super::super::native_enrollment::record_raw_native(
        tx,
        context.1,
        &Fresh::Phase(caller::Phase {
            run,
            parent: &parent,
            call: &call,
            request,
            effect: context.0,
            record: published.record,
            receipt: published.receipt,
            event: published.event,
            running_event: published.running_event,
            issued: published.issued,
        }),
        proof,
        #[cfg(test)]
        probe,
    )
    .await
    .map_err(|_| Error::UnknownStoreOutcome)
}
