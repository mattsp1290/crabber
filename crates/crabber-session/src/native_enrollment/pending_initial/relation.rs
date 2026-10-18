use super::{
    Assignment, CloseOutcome, CloseRecord, Correlation, Epoch, EpochKind, Failure, Head,
    InputFingerprint, Proof, Publication, PublicationKind, RegisteredComplete, SourceKey,
    SourceSlot, bounded, digest, unknown,
};
use crate::native_enrollment::caller::initial::SelectedOriginal;

#[derive(Clone, Copy)]
pub(crate) struct Bundle<'a> {
    pub slot: Option<&'a SourceSlot>,
    pub epoch: Option<&'a Epoch>,
    pub registered: Option<&'a RegisteredComplete>,
    pub assignment: Option<&'a Assignment>,
    pub close: Option<&'a CloseRecord>,
    pub publications: &'a [Publication],
    pub head: Option<&'a Head>,
}
impl Bundle<'_> {
    pub(crate) fn empty(&self) -> bool {
        self.slot.is_none()
            && self.epoch.is_none()
            && self.registered.is_none()
            && self.assignment.is_none()
            && self.close.is_none()
            && self.publications.is_empty()
            && self.head.is_none()
    }
    pub(crate) fn verify(&self, key: &SourceKey, proof: &mut Proof) -> Result<(), Failure> {
        proof.measure(&(
            self.slot,
            self.epoch,
            self.registered,
            self.assignment,
            self.close,
            self.publications,
            self.head,
        ))?;
        if self.empty() {
            return Ok(());
        }
        let epoch = self.epoch.ok_or_else(|| unknown("missing Pending epoch"))?;
        epoch.correlation.validate(proof)?;
        if epoch.version != 1 || epoch.correlation.key != *key {
            return Err(unknown("Pending epoch route"));
        }
        let identity = &epoch.correlation.identity;
        let epoch_digest = epoch.digest()?;
        let slot = self
            .slot
            .ok_or_else(|| unknown("missing Pending source slot"))?;
        if slot.version != 1
            || slot.key != *key
            || slot.epoch != identity.epoch
            || slot.epoch_digest != epoch_digest
        {
            return Err(unknown("Pending source slot route"));
        }
        match epoch.kind {
            EpochKind::Opened => {
                let registered = self
                    .registered
                    .ok_or_else(|| unknown("missing registered Complete"))?;
                if registered.version != 1
                    || registered.key != *key
                    || registered.epoch != identity.epoch
                    || registered.call != identity.complete
                    || registered.epoch_digest != epoch_digest
                {
                    return Err(unknown("registered Complete route"));
                }
            }
            EpochKind::SealedBeforeOpen
                if self.registered.is_none()
                    && self.assignment.is_none()
                    && self.close.is_some() => {}
            EpochKind::SealedBeforeOpen => return Err(unknown("sealed-before-Open relation")),
        }
        if let Some(assignment) = self.assignment {
            bounded(assignment, proof)?;
            if assignment.version != 1
                || assignment.correlation != epoch.correlation
                || assignment.call != identity.complete
                || assignment.epoch_digest != epoch_digest
            {
                return Err(unknown("Pending assignment route"));
            }
            crate::native_parent::source_protocol::validate_scope(
                &key.session_id,
                &assignment.run_id,
            )
            .map_err(unknown)?;
        }
        if let Some(close) = self.close {
            bounded(close, proof)?;
            if close.version != 1
                || close.correlation != epoch.correlation
                || close.epoch_digest != epoch_digest
            {
                return Err(unknown("Pending close route"));
            }
            match (&close.outcome, self.assignment) {
                (CloseOutcome::NoPublication, None) => (),
                (CloseOutcome::Assigned { assignment_digest }, Some(assignment))
                    if *assignment_digest == assignment.digest()? => {}
                _ => return Err(unknown("Pending close assignment")),
            }
        }
        self.verify_publications(epoch, proof)
    }
    fn verify_publications(&self, epoch: &Epoch, proof: &mut Proof) -> Result<(), Failure> {
        let head = self.head.ok_or_else(|| unknown("missing Pending head"))?;
        if self.publications.is_empty() || self.publications.len() > 3 {
            return Err(unknown("Pending publication set"));
        }
        let expected_count = usize::from(epoch.kind == EpochKind::Opened)
            + usize::from(self.assignment.is_some())
            + usize::from(self.close.is_some());
        if self.publications.len() != expected_count {
            return Err(unknown("Pending publication membership"));
        }
        let mut previous = None;
        for (index, publication) in self.publications.iter().enumerate() {
            bounded(publication, proof)?;
            let (kind, record_digest) = if index == 0 && epoch.kind == EpochKind::Opened {
                (
                    PublicationKind::Opened,
                    digest(&(
                        "crabber.pending-initial.open.v1",
                        epoch,
                        self.slot.ok_or_else(|| unknown("missing slot"))?,
                        self.registered
                            .ok_or_else(|| unknown("missing registration"))?,
                    ))?,
                )
            } else if index == 1 && self.assignment.is_some() {
                (
                    PublicationKind::Assigned,
                    self.assignment
                        .ok_or_else(|| unknown("missing assignment"))?
                        .digest()?,
                )
            } else {
                (
                    PublicationKind::Closed,
                    digest(&(
                        "crabber.pending-initial.close.v1",
                        self.close.ok_or_else(|| unknown("missing close"))?,
                    ))?,
                )
            };
            if publication.version != 1
                || publication.key != epoch.correlation.key
                || publication.epoch != epoch.correlation.identity.epoch
                || publication.sequence != u64::try_from(index + 1).map_err(unknown)?
                || publication.kind != kind
                || publication.record_digest != record_digest
                || publication.previous_digest != previous
                || publication.digest != publication_digest(publication)?
            {
                return Err(unknown("Pending publication prefix"));
            }
            previous = Some(publication.digest.clone());
        }
        bounded(head, proof)?;
        if head.version != 1
            || head.key != epoch.correlation.key
            || head.epoch != epoch.correlation.identity.epoch
            || head.sequence != u64::try_from(self.publications.len()).map_err(unknown)?
            || head.origin_digest != self.publications[0].digest
            || Some(&head.digest) != previous.as_ref()
        {
            return Err(unknown("Pending protected head"));
        }
        Ok(())
    }
}
#[derive(Default)]
pub(crate) struct RecordDelta {
    pub epoch: Option<Epoch>,
    pub slot: Option<SourceSlot>,
    pub registered: Option<RegisteredComplete>,
    pub assignment: Option<Assignment>,
    pub close: Option<CloseRecord>,
    pub publication: Option<Publication>,
    pub head: Option<Head>,
}

pub(super) fn publication_digest(p: &Publication) -> Result<InputFingerprint, Failure> {
    digest(&(
        "crabber.pending-initial.publication.v1",
        p.version,
        &p.key,
        &p.epoch,
        p.sequence,
        p.kind,
        &p.record_digest,
        &p.previous_digest,
    ))
}

/// Same-gate verified call; constructed only by the native publication verifier.
pub(crate) struct VerifiedInitialCall<'a> {
    pub(super) expected: &'a Correlation,
    pub(super) epoch_digest: InputFingerprint,
    pub(super) selected: &'a SelectedOriginal<'a>,
}
impl VerifiedInitialCall<'_> {
    pub(crate) fn correlation(&self) -> &Correlation {
        self.expected
    }
    pub(crate) fn epoch_digest(&self) -> &InputFingerprint {
        &self.epoch_digest
    }
    pub(crate) fn selected(&self) -> &SelectedOriginal<'_> {
        self.selected
    }
}
#[derive(Clone, Copy)]
pub(crate) enum BirthDecision<'a> {
    Raw,
    PendingInitial(&'a VerifiedInitialCall<'a>),
}
