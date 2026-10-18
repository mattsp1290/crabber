use super::{
    AssignmentLink, CallKind, CallResult, ConsumerClose, Correlation, Failure, Head, Origin, Proof,
    Publication, PublicationKind, RegisteredCall, ReverseMembership, RosterEntry, digest, unknown,
};
use std::collections::BTreeSet;

/// Complete selected forward and independent original/reverse rows. A missing
/// projection never silently turns this into an absent consumer.
pub(crate) struct Selection<'a> {
    pub origin: Option<&'a Origin>,
    pub cohort: Option<&'a Origin>,
    pub assignment: Option<&'a AssignmentLink>,
    pub calls: Vec<&'a RegisteredCall>,
    pub results: Vec<&'a CallResult>,
    pub roster: Vec<&'a RosterEntry>,
    pub reverse: Vec<&'a ReverseMembership>,
    pub publications: &'a [Publication],
    pub head: Option<&'a Head>,
    pub close: Option<&'a ConsumerClose>,
}
pub(crate) enum Bundle {
    Absent,
    Opened,
    NoOpenedMember,
}
impl Selection<'_> {
    pub(crate) fn empty(&self) -> bool {
        self.origin.is_none()
            && self.cohort.is_none()
            && self.assignment.is_none()
            && self.calls.is_empty()
            && self.results.is_empty()
            && self.roster.is_empty()
            && self.reverse.is_empty()
            && self.publications.is_empty()
            && self.head.is_none()
            && self.close.is_none()
    }
    pub(crate) fn verify(
        &self,
        expected: &Correlation,
        proof: &mut Proof,
    ) -> Result<Bundle, Failure> {
        proof.measure(&(
            self.origin,
            self.cohort,
            self.assignment,
            &self.calls,
            &self.results,
            &self.roster,
            &self.reverse,
            self.publications,
            self.head,
            self.close,
        ))?;
        if self.empty() {
            return Ok(Bundle::Absent);
        }
        let origin = self
            .origin
            .ok_or_else(|| unknown("missing consumer origin"))?;
        origin.validate(proof)?;
        if origin.correlation().key != expected.key {
            return Err(unknown("consumer source routing"));
        }
        let cohort = self
            .cohort
            .ok_or_else(|| unknown("missing consumer independent cohort"))?;
        if cohort.digest()? != origin.digest()? || cohort.correlation() != origin.correlation() {
            return Err(unknown("changed consumer original winner"));
        }
        match origin {
            Origin::Opened(_) => {
                self.verify_assignment(origin)?;
                self.verify_calls(origin, proof)?;
                self.verify_publications(origin)?;
                Ok(Bundle::Opened)
            }
            Origin::NoOpenedMember(_) => {
                if self.assignment.is_some()
                    || !self.calls.is_empty()
                    || !self.results.is_empty()
                    || !self.roster.is_empty()
                    || !self.reverse.is_empty()
                    || self.close.is_some()
                {
                    return Err(unknown("no-Open consumer has opened members"));
                }
                self.verify_publications(origin)?;
                Ok(Bundle::NoOpenedMember)
            }
        }
    }
    fn verify_assignment(&self, origin: &Origin) -> Result<(), Failure> {
        if let Some(a) = self.assignment {
            if a.version != 1
                || a.correlation != *origin.correlation()
                || a.anchor_digest != origin.digest()?
                || a.assignment.correlation != a.correlation
            {
                return Err(unknown("consumer initial assignment link"));
            }
        } else if !self.calls.is_empty()
            || !self.results.is_empty()
            || !self.roster.is_empty()
            || !self.reverse.is_empty()
        {
            return Err(unknown("consumer calls without actual assignment"));
        }
        Ok(())
    }
    fn verify_calls(&self, origin: &Origin, proof: &mut Proof) -> Result<(), Failure> {
        if self.calls.len() != self.results.len() || self.calls.len() != self.roster.len() {
            return Err(unknown("consumer erased call originals"));
        }
        let mut ids = BTreeSet::new();
        let mut reverses = BTreeSet::new();
        let mut seen = Vec::new();
        let ordered = self
            .publications
            .iter()
            .filter(|p| p.kind == PublicationKind::CommittedCall);
        for publication in ordered {
            let result = self
                .results
                .iter()
                .copied()
                .find(|r| r.digest().ok().as_ref() == Some(&publication.original_digest))
                .ok_or_else(|| unknown("consumer call publication membership"))?;
            proof.measure(&result.call)?;
            if !ids.insert(result.call.0.as_str()) {
                return Err(unknown("duplicate consumer call"));
            }
            let registration = self
                .calls
                .iter()
                .copied()
                .find(|r| r.call == result.call)
                .ok_or_else(|| unknown("missing registered consumer call"))?;
            let roster = self
                .roster
                .iter()
                .copied()
                .find(|r| r.call == result.call)
                .ok_or_else(|| unknown("missing original call roster"))?;
            self.verify_call(origin, registration, result, roster, &seen, proof)?;
            for reverse in self.reverse.iter().filter(|r| r.call == result.call) {
                if !reverses.insert((reverse.kind, reverse.member_key.as_str())) {
                    return Err(unknown("consumer duplicate reverse assignment"));
                }
            }
            seen.push((registration.kind, result.digest()?));
        }
        if ids.len() != self.calls.len() || reverses.len() != self.reverse.len() {
            return Err(unknown("consumer extra call/reverse original"));
        }
        Ok(())
    }
    fn verify_call(
        &self,
        origin: &Origin,
        call: &RegisteredCall,
        result: &CallResult,
        roster: &RosterEntry,
        seen: &[(CallKind, crabber_core::InputFingerprint)],
        proof: &mut Proof,
    ) -> Result<(), Failure> {
        let c = origin.correlation();
        let assignment = self
            .assignment
            .ok_or_else(|| unknown("missing consumer assignment"))?;
        if call.version != 1
            || result.version != 1
            || roster.version != 1
            || call.correlation != *c
            || result.correlation != *c
            || roster.correlation != *c
            || call.anchor_digest != origin.digest()?
            || result.registration_digest != call.digest()?
            || roster.registration_digest != call.digest()?
            || roster.result_digest != result.digest()?
            || call.kind != result.original.kind()
            || call.input_digest != result.original.input_digest()?
            || roster.call != call.call
            || result.call != call.call
        {
            return Err(unknown("consumer original call association"));
        }
        crate::session_owner::validation::identifier(&call.call.0, 128, false).map_err(unknown)?;
        result
            .original
            .verify(&c.key.session_id, assignment.run_id(), proof)?;
        verify_order(
            call.kind,
            seen,
            assignment.assignment.keyed_receipt_digest.is_some(),
        )?;
        let expected: Vec<_> = seen.iter().map(|(_, digest)| digest).collect();
        if call.prerequisites.iter().collect::<Vec<_>>() != expected {
            return Err(unknown("consumer call prerequisite prefix"));
        }
        let keys = result.original.reverse_keys()?;
        let actual: Vec<_> = self
            .reverse
            .iter()
            .copied()
            .filter(|r| r.call == call.call)
            .collect();
        if actual.len() != keys.len() || roster.reverse_digests.len() != keys.len() {
            return Err(unknown("consumer reverse roster size"));
        }
        for ((kind, key), reverse) in keys.iter().zip(actual) {
            if reverse.version != 1
                || reverse.correlation != *c
                || reverse.kind != *kind
                || reverse.member_key != *key
                || reverse.result_digest != result.digest()?
                || !roster.reverse_digests.contains(&reverse.digest()?)
            {
                return Err(unknown("consumer reverse original"));
            }
        }
        Ok(())
    }
    fn verify_publications(&self, origin: &Origin) -> Result<(), Failure> {
        let head = self.head.ok_or_else(|| unknown("missing consumer head"))?;
        let expected = 1
            + usize::from(self.assignment.is_some())
            + self.calls.len()
            + usize::from(self.close.is_some());
        if self.publications.len() != expected {
            return Err(unknown("consumer publication set"));
        }
        let mut previous = None;
        let mut calls = BTreeSet::new();
        for (i, p) in self.publications.iter().enumerate() {
            let (kind, original_digest) = if i == 0 {
                (
                    match origin {
                        Origin::Opened(_) => PublicationKind::Opened,
                        Origin::NoOpenedMember(_) => PublicationKind::NoOpenedMember,
                    },
                    origin.digest()?,
                )
            } else if i == 1 && self.assignment.is_some() {
                (
                    PublicationKind::Assigned,
                    self.assignment
                        .ok_or_else(|| unknown("missing assignment"))?
                        .digest()?,
                )
            } else if i + 1 == expected && self.close.is_some() {
                (
                    PublicationKind::Closed,
                    self.close
                        .ok_or_else(|| unknown("missing close"))?
                        .digest()?,
                )
            } else {
                if !self
                    .results
                    .iter()
                    .any(|r| r.digest().ok().as_ref() == Some(&p.original_digest))
                    || !calls.insert(p.original_digest.as_str())
                {
                    return Err(unknown("consumer publication call original"));
                }
                (PublicationKind::CommittedCall, p.original_digest.clone())
            };
            if p.version != 1
                || p.correlation != *origin.correlation()
                || p.sequence != u64::try_from(i + 1).map_err(unknown)?
                || p.kind != kind
                || p.original_digest != original_digest
                || p.previous_digest.as_ref() != previous
                || p.digest != p.canonical_digest()?
            {
                return Err(unknown("consumer immutable publication prefix"));
            }
            previous = Some(&p.digest);
        }
        if head.version != 1
            || head.correlation != *origin.correlation()
            || head.origin_digest != origin.digest()?
            || head.sequence != u64::try_from(expected).map_err(unknown)?
            || Some(&head.digest) != previous
        {
            return Err(unknown("consumer protected head"));
        }
        if let Some(close) = self.close
            && (close.version != 1
                || close.correlation != *origin.correlation()
                || close.anchor_digest != origin.digest()?
                || self
                    .publications
                    .iter()
                    .rev()
                    .nth(1)
                    .is_none_or(|p| p.digest != close.prior_head_digest)
                || close.roster_digest != roster_digest(&self.roster)?)
        {
            return Err(unknown("consumer absorbing close association"));
        }
        Ok(())
    }
    pub(crate) fn verify_birth(
        &self,
        birth: crate::native_enrollment::pending_initial::Bundle<'_>,
        proof: &mut Proof,
    ) -> Result<(), Failure> {
        let Some(origin) = self.origin else {
            if !self.empty() {
                return Err(unknown("consumer missing origin"));
            }
            return Ok(());
        };
        self.verify(origin.correlation(), proof)?;
        birth.verify(&origin.correlation().key, proof)?;
        let epoch = birth
            .epoch
            .ok_or_else(|| unknown("consumer missing birth epoch"))?;
        if epoch.correlation != *origin.correlation() {
            return Err(unknown("consumer/birth correlation"));
        }
        match origin {
            Origin::Opened(a) => {
                if epoch.kind != crate::native_enrollment::pending_initial::EpochKind::Opened
                    || birth.publications.first().is_none_or(|p| {
                        p.kind != crate::native_enrollment::pending_initial::PublicationKind::Opened
                            || p.record_digest != a.birth_open_digest
                    })
                {
                    return Err(unknown("consumer/birth Open"));
                }
                match (self.assignment, birth.assignment) {
                    (None, None) => (),
                    (Some(link), Some(actual))
                        if link.assignment.digest()? == actual.digest()? => {}
                    _ => return Err(unknown("consumer/birth assignment")),
                }
                match (self.close, birth.close) {
                    (None, None) => (),
                    (Some(close), Some(actual))
                        if close.birth_close_digest
                            == digest(&("crabber.pending-initial.close.v1", actual))? => {}
                    _ => return Err(unknown("consumer/birth unpaired close")),
                }
            }
            Origin::NoOpenedMember(n) => {
                if epoch.kind
                    != crate::native_enrollment::pending_initial::EpochKind::SealedBeforeOpen
                    || birth.registered.is_some()
                    || birth.assignment.is_some()
                    || birth.close.is_none_or(|c| {
                        digest(&("crabber.pending-initial.close.v1", c))
                            .ok()
                            .as_ref()
                            != Some(&n.birth_close_digest)
                    })
                {
                    return Err(unknown("no-Open consumer canonical birth close"));
                }
            }
        }
        Ok(())
    }
}
pub(crate) fn verify_order(
    kind: CallKind,
    seen: &[(CallKind, crabber_core::InputFingerprint)],
    keyed: bool,
) -> Result<(), Failure> {
    let has = |k| seen.iter().any(|(actual, _)| *actual == k);
    if has(kind) {
        return Err(unknown("consumer effect already committed"));
    }
    let required = match kind {
        CallKind::Register => None,
        CallKind::Activate => Some(CallKind::Register),
        CallKind::KeyedBegin if keyed => Some(CallKind::Activate),
        CallKind::KeyedBegin => return Err(unknown("unkeyed consumer invented Begin")),
        CallKind::ReserveInitial => Some(if keyed {
            CallKind::KeyedBegin
        } else {
            CallKind::Activate
        }),
        CallKind::BeginInitial => Some(CallKind::ReserveInitial),
        CallKind::RunAdmitted | CallKind::Conclude => Some(CallKind::BeginInitial),
        CallKind::RunStarted => Some(CallKind::RunAdmitted),
        CallKind::TurnStarted => Some(CallKind::RunStarted),
        CallKind::AssistantMessage => Some(CallKind::TurnStarted),
        CallKind::MessageCommitted => Some(CallKind::AssistantMessage),
        CallKind::TurnCompleted => Some(CallKind::MessageCommitted),
    };
    if required.is_some_and(|k| !has(k)) || has(CallKind::Conclude) {
        return Err(unknown("consumer effect order"));
    }
    Ok(())
}
pub(crate) struct RecordDelta {
    pub origin: Option<Origin>,
    pub assignment: Option<AssignmentLink>,
    pub call: Option<RegisteredCall>,
    pub result: Option<CallResult>,
    pub roster: Option<RosterEntry>,
    pub reverse: Vec<ReverseMembership>,
    pub close: Option<ConsumerClose>,
    pub publication: Publication,
    pub head: Head,
}

/// One operation selected under a held native gate. It cannot escape the
/// borrowed original selection, and capture retains the exact head for CAS.
pub(crate) struct VerifiedConsumerCall<'a, 'records> {
    pub(super) selected: &'a Selection<'records>,
    pub(super) registration: RegisteredCall,
}
impl VerifiedConsumerCall<'_, '_> {
    pub(crate) fn correlation(&self) -> &Correlation {
        &self.registration.correlation
    }
    pub(crate) fn kind(&self) -> CallKind {
        self.registration.kind
    }
}

pub(crate) fn roster_digest(
    rows: &[&RosterEntry],
) -> Result<crabber_core::InputFingerprint, Failure> {
    let mut ordered = rows.to_vec();
    ordered.sort_by(|a, b| a.call.0.cmp(&b.call.0));
    digest(&("crabber.initial-consumer.roster-set.v1", ordered))
}
