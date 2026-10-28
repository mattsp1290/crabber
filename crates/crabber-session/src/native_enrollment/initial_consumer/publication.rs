//! Native-only creation from an authenticated original prefix and actual outputs.
use super::{
    Anchor, AssignmentLink, Bundle, CallResult, ConsumerClose, ConsumerInput, Correlation,
    DriverRetirement, Failure, Head, InputFingerprint, NoOpenedMember, Origin, Original, Proof,
    Publication, PublicationKind, RecordDelta, RegisteredCall, ReverseMembership, RosterEntry,
    Selection, ResultIndex, VerifiedConsumerCall, copied, digest, roster_digest, unknown, verify_order,
};
use crate::native_enrollment::pending_initial::{self as birth, prefix::HistoricalInitialPrefix};
use crate::session_owner::SessionOwnerError as Error;
use crate::session_owner::preparation::PreparationCommitError;

pub(crate) fn failure(error: &Failure) -> PreparationCommitError {
    match error {
        Failure::Limited(_) => Error::Capacity.into(),
        Failure::Error(_) => Error::UnknownStoreOutcome.into(),
    }
}
pub(crate) fn admission_failure(proof: &Proof, error: &Failure) -> PreparationCommitError {
    let fallback = failure(error);
    proof.admission_failure(error, fallback)
}

fn checked_prefix(
    prefix: &HistoricalInitialPrefix<'_>,
    expected: &Correlation,
    proof: &mut Proof,
) -> Result<(), PreparationCommitError> {
    if prefix.correlation(&expected.identity, proof)? != *expected {
        return Err(Error::SemanticConflict.into());
    }
    Ok(())
}
fn append(
    view: &Selection<'_>,
    origin: &Origin,
    kind: PublicationKind,
    original_digest: InputFingerprint,
    proof: &mut Proof,
) -> Result<(Publication, Head), Failure> {
    let sequence = u64::try_from(view.publications.len())
        .map_err(unknown)?
        .checked_add(1)
        .ok_or_else(|| unknown("consumer sequence exhausted"))?;
    let mut publication = Publication {
        version: 1,
        correlation: copied(origin.correlation(), proof)?,
        sequence,
        kind,
        original_digest,
        previous_digest: view.head.map(|h| copied(&h.digest, proof)).transpose()?,
        digest: InputFingerprint::new("0".repeat(64)).map_err(unknown)?,
    };
    proof.measure(&publication)?;
    publication.digest = publication.canonical_digest()?;
    let head = Head {
        version: 1,
        correlation: copied(origin.correlation(), proof)?,
        origin_digest: origin.digest()?,
        sequence,
        digest: copied(&publication.digest, proof)?,
    };
    proof.measure(&head)?;
    Ok((publication, head))
}
fn delta(
    origin: Option<Origin>,
    assignment: Option<AssignmentLink>,
    publication: Publication,
    head: Head,
) -> RecordDelta {
    RecordDelta {
        origin,
        assignment,
        call: None,
        result: None,
        roster: None,
        reverse: Vec::new(),
        close: None,
        publication,
        head,
    }
}
/// Called only after verifying the actual constructor-held resource association
/// under the native gate. Existing birth epochs never enter this constructor.
pub(crate) fn prepare_anchor(
    prefix: &HistoricalInitialPrefix<'_>,
    opened: &birth::OpenRecord,
    view: &Selection<'_>,
    proof: &mut Proof,
) -> Result<RecordDelta, PreparationCommitError> {
    prepare_anchor_policy(prefix, opened, view, None, proof)
}
pub(crate) fn prepare_fixed_anchor(
    prefix: &HistoricalInitialPrefix<'_>,
    opened: &birth::OpenRecord,
    view: &Selection<'_>,
    policy: &super::OriginalPolicy,
    proof: &mut Proof,
) -> Result<RecordDelta, PreparationCommitError> {
    policy
        .validate(opened.correlation(), proof)
        .map_err(|error| admission_failure(proof, &error))?;
    prepare_anchor_policy(prefix, opened, view, Some(policy), proof)
}
fn prepare_anchor_policy(
    prefix: &HistoricalInitialPrefix<'_>,
    opened: &birth::OpenRecord,
    view: &Selection<'_>,
    policy: Option<&super::OriginalPolicy>,
    proof: &mut Proof,
) -> Result<RecordDelta, PreparationCommitError> {
    let expected = opened.correlation();
    checked_prefix(prefix, expected, proof)?;
    if !matches!(
        view.verify(expected, proof)
            .map_err(|error| admission_failure(proof, &error))?,
        Bundle::Absent
    ) {
        return Err(Error::Unsupported.into());
    }
    if opened.epoch.kind != birth::EpochKind::Opened {
        return Err(Error::Unsupported.into());
    }
    proof.measure(opened).map_err(|error| admission_failure(proof, &error))?;
    let origin = Origin::Opened(Anchor {
        version: if policy.is_some() { 2 } else { 1 },
        original_policy: policy
            .map(|p| {
                proof.charge(std::mem::size_of::<super::OriginalPolicy>())?;
                copied(p, proof).map(Box::new)
            })
            .transpose()
            .map_err(|error| admission_failure(proof, &error))?,
        correlation: copied(expected, proof).map_err(|error| admission_failure(proof, &error))?,
        birth_open_digest: digest(&(
            "crabber.pending-initial.open.v1",
            &opened.epoch,
            &opened.slot,
            &opened.registered,
        ))
        .map_err(|error| admission_failure(proof, &error))?,
    });
    proof.measure(&origin).map_err(|error| admission_failure(proof, &error))?;
    let (publication, head) = append(
        view,
        &origin,
        PublicationKind::Opened,
        origin.digest().map_err(|error| admission_failure(proof, &error))?,
        proof,
    )
    .map_err(|error| admission_failure(proof, &error))?;
    Ok(delta(Some(origin), None, publication, head))
}
pub(crate) fn link_initial(
    view: &Selection<'_>,
    birth_view: birth::Bundle<'_>,
    actual: &birth::EnrolledInitialData,
    proof: &mut Proof,
) -> Result<RecordDelta, PreparationCommitError> {
    view.verify_birth(birth_view, proof)
        .map_err(|error| admission_failure(proof, &error))?;
    let origin = view.origin.ok_or(Error::Unsupported)?;
    if !matches!(origin, Origin::Opened(_))
        || origin.correlation() != actual.correlation()
        || view.assignment.is_some()
        || view.close.is_some()
    {
        return Err(Error::InvalidTransition.into());
    }
    proof
        .measure(&(
            &actual.first.issuer,
            &actual.first.source,
            &actual.assignment,
        ))
        .map_err(|error| admission_failure(proof, &error))?;
    actual
        .first
        .source
        .verify(&actual.first.issuer)
        .map_err(|_| Error::UnknownStoreOutcome)?;
    if actual.first.issuer.sequence != 1
        || actual.first.source.issuance_digest != actual.assignment.first_issuer_digest
        || actual.first.source.chain_digest != actual.assignment.first_source_digest
    {
        return Err(Error::UnknownStoreOutcome.into());
    }
    let assignment = AssignmentLink {
        version: 1,
        correlation: copied(actual.correlation(), proof).map_err(|error| admission_failure(proof, &error))?,
        anchor_digest: origin.digest().map_err(|error| admission_failure(proof, &error))?,
        assignment: copied(&actual.assignment, proof).map_err(|error| admission_failure(proof, &error))?,
    };
    proof
        .measure(&assignment)
        .map_err(|error| admission_failure(proof, &error))?;
    let (publication, head) = append(
        view,
        origin,
        PublicationKind::Assigned,
        assignment.digest().map_err(|error| admission_failure(proof, &error))?,
        proof,
    )
    .map_err(|error| admission_failure(proof, &error))?;
    Ok(delta(None, Some(assignment), publication, head))
}
pub(crate) enum CallDecision<'a, 'records> {
    Fresh(Box<VerifiedConsumerCall<'a, 'records>>),
    Replayed(&'a CallResult),
}
pub(crate) fn prepare_call<'a, 'records>(
    view: &'a Selection<'records>,
    birth_view: birth::Bundle<'_>,
    expected: &Correlation,
    call_id: &crabber_core::ControlId,
    input: &ConsumerInput<'_>,
    proof: &mut Proof,
) -> Result<CallDecision<'a, 'records>, PreparationCommitError> {
    view.verify_birth(birth_view, proof)
        .map_err(|error| admission_failure(proof, &error))?;
    let origin = view.origin.ok_or(Error::Unsupported)?;
    if origin.correlation() != expected
        || !matches!(origin, Origin::Opened(_))
        || view.close.is_some()
    {
        return Err(Error::Unsupported.into());
    }
    let assignment = view.assignment.ok_or(Error::Unsupported)?;
    let kind = input.kind();
    let input_digest = input.fingerprint(expected, assignment.run_id(), proof)?;
    crate::session_owner::validation::identifier(&call_id.0, 128, false)?;
    if let Some(result) = view.results.iter().copied().find(|r| &r.call == call_id) {
        let registered = view
            .calls
            .iter()
            .copied()
            .find(|r| &r.call == call_id)
            .ok_or(Error::UnknownStoreOutcome)?;
        if registered.kind != kind || registered.input_digest != input_digest {
            return Err(Error::SemanticConflict.into());
        }
        return Ok(CallDecision::Replayed(result));
    }
    let results = ResultIndex::new(&view.results, proof).map_err(|error| admission_failure(proof, &error))?;
    let mut seen = Vec::new();
    for p in view
        .publications
        .iter()
        .filter(|p| p.kind == PublicationKind::CommittedCall)
    {
        let (_, r) = results.get(&p.original_digest).ok_or(Error::UnknownStoreOutcome)?;
        proof
            .measure(&p.original_digest)
            .map_err(|error| admission_failure(proof, &error))?;
        seen.push((r.original.kind(), p.original_digest.clone()));
    }
    verify_order(
        kind,
        &seen,
        assignment.assignment.keyed_receipt_digest.is_some(),
    )
    .map_err(|error| admission_failure(proof, &error))?;
    let registration = RegisteredCall {
        version: 1,
        correlation: copied(expected, proof).map_err(|error| admission_failure(proof, &error))?,
        call: copied(call_id, proof).map_err(|error| admission_failure(proof, &error))?,
        anchor_digest: origin.digest().map_err(|error| admission_failure(proof, &error))?,
        kind,
        input_digest,
        prerequisites: seen.into_iter().map(|(_, digest)| digest).collect(),
    };
    proof
        .measure(&registration)
        .map_err(|error| admission_failure(proof, &error))?;
    proof
        .charge(std::mem::size_of::<VerifiedConsumerCall<'_, '_>>())
        .map_err(|error| admission_failure(proof, &error))?;
    Ok(CallDecision::Fresh(Box::new(VerifiedConsumerCall {
        selected: view,
        registration,
    })))
}
pub(crate) fn capture_actual(
    verified: VerifiedConsumerCall<'_, '_>,
    original: Original,
    proof: &mut Proof,
) -> Result<RecordDelta, PreparationCommitError> {
    let view = verified.selected;
    let origin = view.origin.ok_or(Error::UnknownStoreOutcome)?;
    let assignment = view.assignment.ok_or(Error::UnknownStoreOutcome)?;
    original
        .verify(
            &origin.correlation().key.session_id,
            assignment.run_id(),
            proof,
        )
        .map_err(|error| admission_failure(proof, &error))?;
    if original.kind() != verified.kind()
        || original.input_digest().map_err(|error| admission_failure(proof, &error))?
            != verified.registration.input_digest
    {
        return Err(Error::UnknownStoreOutcome.into());
    }
    let result = CallResult {
        version: 1,
        correlation: copied(verified.correlation(), proof).map_err(|error| admission_failure(proof, &error))?,
        call: copied(&verified.registration.call, proof).map_err(|error| admission_failure(proof, &error))?,
        registration_digest: verified
            .registration
            .digest()
            .map_err(|error| admission_failure(proof, &error))?,
        original,
    };
    proof.measure(&result).map_err(|error| admission_failure(proof, &error))?;
    let result_digest = result.digest().map_err(|error| admission_failure(proof, &error))?;
    let mut reverse = Vec::new();
    let mut reverse_digests = Vec::new();
    for (kind, member_key) in result
        .original
        .reverse_keys()
        .map_err(|error| admission_failure(proof, &error))?
    {
        let row = ReverseMembership {
            version: 1,
            correlation: copied(result.correlation(), proof).map_err(|error| admission_failure(proof, &error))?,
            call: copied(&result.call, proof).map_err(|error| admission_failure(proof, &error))?,
            kind,
            member_key,
            result_digest: copied(&result_digest, proof).map_err(|error| admission_failure(proof, &error))?,
        };
        proof.measure(&row).map_err(|error| admission_failure(proof, &error))?;
        reverse_digests.push(row.digest().map_err(|error| admission_failure(proof, &error))?);
        reverse.push(row);
    }
    let roster = RosterEntry {
        version: 1,
        correlation: copied(result.correlation(), proof).map_err(|error| admission_failure(proof, &error))?,
        call: copied(&result.call, proof).map_err(|error| admission_failure(proof, &error))?,
        registration_digest: copied(&result.registration_digest, proof)
            .map_err(|error| admission_failure(proof, &error))?,
        result_digest: copied(&result_digest, proof).map_err(|error| admission_failure(proof, &error))?,
        reverse_digests,
    };
    proof.measure(&roster).map_err(|error| admission_failure(proof, &error))?;
    let (publication, head) = append(
        view,
        origin,
        PublicationKind::CommittedCall,
        result_digest,
        proof,
    )
    .map_err(|error| admission_failure(proof, &error))?;
    Ok(RecordDelta {
        origin: None,
        assignment: None,
        call: Some(verified.registration),
        result: Some(result),
        roster: Some(roster),
        reverse,
        close: None,
        publication,
        head,
    })
}
pub(crate) struct PairedCloseDelta {
    pub consumer: RecordDelta,
    pub birth: Box<birth::RecordDelta>,
    pub record: birth::CloseRecord,
}
pub(crate) enum PairedCloseDecision {
    Publish(Box<PairedCloseDelta>),
    Replayed(Box<birth::CloseRecord>),
}
/// The caller supplies the actual local joined roster only after validating its
/// private joined/resource association. Historical prefix comes from native
/// selected original audits, never from the local witness or current head.
#[cfg(any(feature = "postgres", feature = "sqlite"))]
pub(crate) fn prepare_paired_close(
    view: &Selection<'_>,
    birth_view: birth::Bundle<'_>,
    prefix: &HistoricalInitialPrefix<'_>,
    expected: &Correlation,
    local_calls: &[crabber_core::ControlId],
    retirement: (
        &DriverRetirement,
        crate::session_owner::owned_scope::OwnedMainDisposition,
    ),
    proof: &mut Proof,
) -> Result<PairedCloseDecision, PreparationCommitError> {
    prepare_paired_policy_close(
        view,
        birth_view,
        prefix,
        expected,
        local_calls,
        (retirement.0, retirement.1, None),
        proof,
    )
}
pub(crate) fn prepare_paired_policy_close(
    view: &Selection<'_>,
    birth_view: birth::Bundle<'_>,
    prefix: &HistoricalInitialPrefix<'_>,
    expected: &Correlation,
    local_calls: &[crabber_core::ControlId],
    retirement: (
        &DriverRetirement,
        crate::session_owner::owned_scope::OwnedMainDisposition,
        Option<&super::PolicyTerminal>,
    ),
    proof: &mut Proof,
) -> Result<PairedCloseDecision, PreparationCommitError> {
    checked_prefix(prefix, expected, proof)?;
    let verified = verify_close_members(
        view,
        birth_view,
        expected,
        local_calls,
        (retirement.0, retirement.1),
        proof,
    )?;
    let (retirement, disposition, policy_terminal) = retirement;
    verify_policy_tail(view, policy_terminal, proof)?;
    match birth::prepare_close(prefix, expected, birth_view, proof)? {
        birth::CloseDecision::Publish {
            record,
            delta: birth_delta,
        } => {
            let birth_close_digest = digest(&("crabber.pending-initial.close.v1", &record))
                .map_err(|error| admission_failure(proof, &error))?;
            let (consumer, kind, original_digest) = if let Some(origin) = view.origin {
                let close = ConsumerClose {
                    version: if policy_terminal.is_some() { 3 } else { 2 },
                    policy_terminal: policy_terminal
                        .map(|p| copied(p, proof))
                        .transpose()
                        .map_err(|error| admission_failure(proof, &error))?,
                    retirement: copied(retirement, proof).map_err(|error| admission_failure(proof, &error))?,
                    disposition,
                    correlation: copied(expected, proof).map_err(|error| admission_failure(proof, &error))?,
                    anchor_digest: origin.digest().map_err(|error| admission_failure(proof, &error))?,
                    birth_close_digest,
                    prior_head_digest: copied(
                        &view.head.ok_or(Error::UnknownStoreOutcome)?.digest,
                        proof,
                    )
                    .map_err(|error| admission_failure(proof, &error))?,
                    roster_digest: roster_digest(&view.roster).map_err(|error| admission_failure(proof, &error))?,
                };
                proof.measure(&close).map_err(|error| admission_failure(proof, &error))?;
                let d = close.digest().map_err(|error| admission_failure(proof, &error))?;
                (Some(close), PublicationKind::Closed, d)
            } else {
                (
                    None,
                    PublicationKind::NoOpenedMember,
                    InputFingerprint::new("0".repeat(64))
                        .map_err(|_| Error::UnknownStoreOutcome)?,
                )
            };
            let new_origin = if view.origin.is_none() {
                Some(Origin::NoOpenedMember(NoOpenedMember {
                    version: 2,
                    retirement: copied(retirement, proof).map_err(|error| admission_failure(proof, &error))?,
                    disposition,
                    correlation: copied(expected, proof).map_err(|error| admission_failure(proof, &error))?,
                    birth_close_digest: digest(&("crabber.pending-initial.close.v1", &record))
                        .map_err(|error| admission_failure(proof, &error))?,
                }))
            } else {
                None
            };
            let origin = new_origin
                .as_ref()
                .or(view.origin)
                .ok_or(Error::UnknownStoreOutcome)?;
            proof.measure(origin).map_err(|error| admission_failure(proof, &error))?;
            let original_digest = if new_origin.is_some() {
                origin.digest().map_err(|error| admission_failure(proof, &error))?
            } else {
                original_digest
            };
            let (publication, head) = append(view, origin, kind, original_digest, proof)
                .map_err(|error| admission_failure(proof, &error))?;
            let mut consumer_delta = delta(new_origin, None, publication, head);
            consumer_delta.close = consumer;
            Ok(PairedCloseDecision::Publish(Box::new(PairedCloseDelta {
                consumer: consumer_delta,
                birth: birth_delta,
                record,
            })))
        }
        birth::CloseDecision::Replayed(record) => {
            view.verify_birth(birth_view, proof)
                .map_err(|error| admission_failure(proof, &error))?;
            if matches!(verified, Bundle::Absent) {
                return Err(Error::UnknownStoreOutcome.into());
            }
            proof
                .charge(std::mem::size_of::<birth::CloseRecord>())
                .map_err(|error| admission_failure(proof, &error))?;
            Ok(PairedCloseDecision::Replayed(Box::new(record)))
        }
        birth::CloseDecision::Conflict => Err(Error::SemanticConflict.into()),
    }
}

/// Validate the complete local/native member relation before either close is
/// constructed. The returned bundle class carries no mutation authority.
fn verify_close_members(
    view: &Selection<'_>,
    birth_view: birth::Bundle<'_>,
    expected: &Correlation,
    local_calls: &[crabber_core::ControlId],
    retirement: (
        &DriverRetirement,
        crate::session_owner::owned_scope::OwnedMainDisposition,
    ),
    proof: &mut Proof,
) -> Result<Bundle, PreparationCommitError> {
    let verified = view
        .verify(expected, proof)
        .map_err(|error| admission_failure(proof, &error))?;
    proof
        .measure(&local_calls)
        .map_err(|error| admission_failure(proof, &error))?;
    if view.calls.iter().any(|r| !local_calls.contains(&r.call)) {
        return Err(Error::UnknownStoreOutcome.into());
    }
    if !matches!(verified, Bundle::Absent) {
        view.verify_birth(birth_view, proof)
            .map_err(|error| admission_failure(proof, &error))?;
    }
    if matches!(verified, Bundle::Absent) && !birth_view.empty() {
        return Err(Error::Unsupported.into());
    }
    let (retirement, disposition) = retirement;
    retirement
        .verify(view, expected, disposition, proof)
        .map_err(|error| admission_failure(proof, &error))?;
    if let Some((saved, actual_disposition)) = view.retirement()
        && (actual_disposition != disposition
            || digest(saved).map_err(|error| admission_failure(proof, &error))?
                != digest(retirement).map_err(|error| admission_failure(proof, &error))?)
    {
        return Err(Error::SemanticConflict.into());
    }
    Ok(verified)
}

fn verify_policy_tail(
    view: &Selection<'_>,
    terminal: Option<&super::PolicyTerminal>,
    proof: &mut Proof,
) -> Result<(), PreparationCommitError> {
    match (view.origin.and_then(Origin::original_policy), terminal) {
        (None, None) => Ok(()),
        (Some(original), Some(terminal)) => {
            terminal
                .verify(original, proof)
                .map_err(|error| admission_failure(proof, &error))?;
            if let Some(saved) = view.close.and_then(|close| close.policy_terminal.as_ref())
                && digest(saved).map_err(|error| admission_failure(proof, &error))?
                    != digest(terminal).map_err(|error| admission_failure(proof, &error))?
            {
                return Err(Error::SemanticConflict.into());
            }
            Ok(())
        }
        _ => Err(Error::Unsupported.into()),
    }
}
