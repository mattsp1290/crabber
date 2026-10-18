//! Closed Initial calls reuse the same canonical Attempt publisher and transaction.
use crate::native_enrollment::initial_consumer as domain;
use crate::transaction::Transaction;
use crate::{
    AttemptAuthority, AttemptConclusion, AttemptError as Error, AttemptMutation,
    AttemptMutationOutcome,
};
use sqlx::Sqlite;
pub(super) struct Capture<'a, 'call, 'view, 'records> {
    pub call: &'call domain::VerifiedConsumerCall<'view, 'records>,
    pub output: &'a mut Option<Published>,
}
pub(in crate::sqlite) struct Published {
    pub attempt: domain::AttemptOriginal,
    pub after_run: Option<crabber_core::Run>,
    pub settlement: Option<crabber_core::EventRecord>,
}
#[derive(Default)]
pub(super) struct Transition {
    pub event: Option<crabber_core::EventRecord>,
    pub publication: Option<domain::NativePublication>,
    pub after_run: Option<crabber_core::Run>,
}
pub(super) fn stored(record: crate::attempt::AttemptRecord) -> domain::StoredAttempt {
    domain::StoredAttempt {
        snapshot: record.snapshot,
        owner: record.owner,
        token: record.token,
        run_token: record.run_token,
        lease_until: record.lease_until,
    }
}
pub(in crate::sqlite) async fn apply(
    store: &crate::sqlite::SqliteStore,
    tx: &mut Transaction<'_, Sqlite>,
    run: &crabber_core::Run,
    fence: &crabber_core::RunFence,
    input: Input<'_>,
    call: &domain::VerifiedConsumerCall<'_, '_>,
    proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<(AttemptMutationOutcome, Published), Error> {
    let Input {
        request,
        authority,
        conclusion,
    } = input;
    crate::attempt::validate_request(request)?;
    proof
        .measure(&(fence, request, conclusion))
        .map_err(|_| Error::UnknownStoreOutcome)?;
    crate::native_enrollment::caller::measure_authority(authority, proof)
        .map_err(|_| Error::UnknownStoreOutcome)?;
    let executor = crate::sqlite::execution::SqliteExecution {
        store: store.clone(),
        fence: fence.clone(),
    };
    let mut output = None;
    let result = Box::pin(executor.apply_attempt_selected(
        tx,
        run,
        request,
        (authority, conclusion, false),
        (
            crate::native_enrollment::caller::Caller::Raw,
            Some(Capture {
                call,
                output: &mut output,
            }),
        ),
        proof,
    ))
    .await?;
    Ok((result, output.ok_or(Error::UnknownStoreOutcome)?))
}

pub(in crate::sqlite) struct Input<'a> {
    pub request: &'a AttemptMutation,
    pub authority: Option<&'a AttemptAuthority>,
    pub conclusion: Option<&'a AttemptConclusion>,
}

pub(super) struct Captured<'a> {
    pub run: &'a crabber_core::Run,
    pub request: &'a AttemptMutation,
    pub record: &'a crate::attempt::AttemptRecord,
    pub receipt: &'a crate::AttemptReceipt,
    pub source_event: &'a crabber_core::EventRecord,
    pub issued: Option<&'a AttemptAuthority>,
    pub before: Option<domain::StoredAttempt>,
    pub settlement: Transition,
}

pub(super) async fn capture(
    tx: &mut Transaction<'_, Sqlite>,
    input: Captured<'_>,
    read_proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<Published, Error> {
    let Captured {
        run,
        request,
        record,
        receipt,
        source_event,
        issued,
        before,
        settlement,
    } = input;
    // This is the same actual canonical publisher's fresh output.
    if crate::native_enrollment::caller::relevant(&request.command) {
        crate::native_enrollment::caller::Fresh::Attempt(
            crate::native_enrollment::caller::Attempt {
                run,
                request,
                record,
                receipt,
                event: source_event,
                issued,
            },
        )
        .measure_and_verify(read_proof)
        .map_err(|_| Error::UnknownStoreOutcome)?;
    }
    read_proof
        .measure(&(request, &receipt, &source_event))
        .map_err(|_| Error::UnknownStoreOutcome)?;
    let mut event = source_event.clone();
    event.cursor = Some(receipt.event_cursor);
    let original = crate::native_enrollment::initial_consumer::AttemptOriginal {
        request: request.clone(),
        before,
        after: {
            let (actual, _) = super::records::load_with_proof(
                tx,
                &run.id,
                Some(&request.command.target().id),
                read_proof,
            )
            .await
            .map_err(|_| Error::UnknownStoreOutcome)?
            .ok_or(Error::UnknownStoreOutcome)?;
            stored(actual)
        },
        receipt: receipt.clone(),
        event,
        publication: settlement.publication,
    };
    Ok(Published {
        attempt: original,
        after_run: settlement.after_run,
        settlement: settlement.event,
    })
}

pub(super) struct EventInput<'a> {
    pub run: &'a crabber_core::Run,
    pub request: &'a AttemptMutation,
    pub conclusion: Option<&'a AttemptConclusion>,
    pub snapshot: &'a crate::AttemptSnapshot,
    pub digest: &'a crabber_core::InputFingerprint,
    pub now: time::OffsetDateTime,
    pub retain: bool,
}
pub(super) fn publication(
    input: &EventInput<'_>,
    settlement: &mut Transition,
    read_proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<crabber_core::EventRecord, Error> {
    let EventInput {
        run,
        request,
        conclusion,
        snapshot,
        digest,
        now,
        retain,
    } = *input;
    if retain {
        read_proof
            .measure(&settlement.event)
            .map_err(|_| Error::UnknownStoreOutcome)?;
    }
    let published_settlement = if retain {
        settlement.event.clone()
    } else {
        settlement.event.take()
    };
    let event = crate::attempt_conclusion::publication(
        run,
        request,
        conclusion,
        snapshot,
        digest.clone(),
        published_settlement,
        now,
    )?;
    read_proof
        .measure(&event)
        .map_err(|_| Error::UnknownStoreOutcome)?;
    Ok(event)
}

pub(super) struct Finalized<'a> {
    pub actual: crate::native_enrollment::caller::Attempt<'a>,
    pub before: Option<domain::StoredAttempt>,
    pub settlement: Transition,
    pub conclusion: Option<&'a AttemptConclusion>,
}
