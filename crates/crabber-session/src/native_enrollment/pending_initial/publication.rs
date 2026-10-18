//! Native-only constructors; the shared relation module remains read-only.
use super::{
    Assignment, Bundle, CloseOutcome, CloseRecord, Correlation, EnrolledInitialData,
    Epoch, EpochKind, Failure, Head, InputFingerprint, OpenRecord, Proof, Publication,
    PublicationKind, RecordDelta, RegisteredComplete, SourceSlot, bounded, digest,
    prefix, unknown,
};
use super::relation::{VerifiedInitialCall, publication_digest};
use crate::native_enrollment::caller::initial::SelectedOriginal;
use crate::session_owner::preparation::PreparationCommitError;
pub(crate) enum OpenDecision {
    Publish {
        record: OpenRecord,
        delta: Box<RecordDelta>,
    },
    Replayed(OpenRecord),
    Closed(CloseRecord),
    Conflict,
}
pub(crate) enum CloseDecision {
    Publish {
        record: CloseRecord,
        delta: Box<RecordDelta>,
    },
    Replayed(CloseRecord),
    Conflict,
}
fn copied<T: serde::Serialize + Clone>(value: &T, proof: &mut Proof) -> Result<T, Failure> {
    bounded(value, proof)?;
    Ok(value.clone())
}

fn append(
    view: &Bundle<'_>,
    correlation: &Correlation,
    kind: PublicationKind,
    record_digest: InputFingerprint,
    proof: &mut Proof,
) -> Result<(Publication, Head), Failure> {
    let sequence = u64::try_from(view.publications.len())
        .map_err(unknown)?
        .checked_add(1)
        .ok_or_else(|| unknown("Pending sequence"))?;
    if sequence > 3 {
        return Err(unknown("closed Initial publication shape"));
    }
    let mut p = Publication {
        version: 1,
        key: copied(&correlation.key, proof)?,
        epoch: copied(&correlation.identity.epoch, proof)?,
        sequence,
        kind,
        record_digest,
        previous_digest: view.head.map(|h| copied(&h.digest, proof)).transpose()?,
        digest: InputFingerprint::new("0".repeat(64)).map_err(unknown)?,
    };
    bounded(&p, proof)?;
    p.digest = publication_digest(&p)?;
    let h = Head {
        version: 1,
        key: copied(&correlation.key, proof)?,
        epoch: copied(&correlation.identity.epoch, proof)?,
        sequence,
        origin_digest: match view.publications.first() {
            Some(first) => copied(&first.digest, proof)?,
            None => copied(&p.digest, proof)?,
        },
        digest: copied(&p.digest, proof)?,
    };
    bounded(&h, proof)?;
    Ok((p, h))
}
fn checked_prefix(
    prefix: &prefix::HistoricalInitialPrefix<'_>,
    expected: &Correlation,
    proof: &mut Proof,
) -> Result<(), PreparationCommitError> {
    if prefix.correlation(&expected.identity, proof)? != *expected {
        return Err(crate::session_owner::SessionOwnerError::SemanticConflict.into());
    }
    Ok(())
}
fn capacity(error: &Failure) -> PreparationCommitError {
    match error {
        Failure::Limited(_) => crate::session_owner::SessionOwnerError::Capacity.into(),
        Failure::Error(_) => crate::session_owner::SessionOwnerError::UnknownStoreOutcome.into(),
    }
}
pub(crate) fn prepare_open(
    prefix: &prefix::HistoricalInitialPrefix<'_>,
    expected: &Correlation,
    view: Bundle<'_>,
    proof: &mut Proof,
) -> Result<OpenDecision, PreparationCommitError> {
    checked_prefix(prefix, expected, proof)?;
    view.verify(&expected.key, proof)
        .map_err(|error| capacity(&error))?;
    if let Some(epoch) = view.epoch {
        if epoch.correlation != *expected {
            return Ok(OpenDecision::Conflict);
        }
        if let Some(close) = view.close {
            return Ok(OpenDecision::Closed(
                copied(close, proof).map_err(|error| capacity(&error))?,
            ));
        }
        return Ok(OpenDecision::Replayed(OpenRecord {
            epoch: copied(epoch, proof).map_err(|error| capacity(&error))?,
            slot: copied(
                view.slot
                    .ok_or_else(|| capacity(&unknown("missing slot")))?,
                proof,
            )
            .map_err(|error| capacity(&error))?,
            registered: copied(
                view.registered
                    .ok_or_else(|| capacity(&unknown("missing Complete registration")))?,
                proof,
            )
            .map_err(|error| capacity(&error))?,
        }));
    }
    let epoch = Epoch {
        version: 1,
        correlation: copied(expected, proof).map_err(|error| capacity(&error))?,
        kind: EpochKind::Opened,
    };
    let epoch_digest = epoch.digest().map_err(|error| capacity(&error))?;
    let slot = SourceSlot {
        version: 1,
        key: copied(&expected.key, proof).map_err(|error| capacity(&error))?,
        epoch: copied(&expected.identity.epoch, proof).map_err(|error| capacity(&error))?,
        epoch_digest: copied(&epoch_digest, proof).map_err(|error| capacity(&error))?,
    };
    let registered = RegisteredComplete {
        version: 1,
        key: copied(&expected.key, proof).map_err(|error| capacity(&error))?,
        epoch: copied(&expected.identity.epoch, proof).map_err(|error| capacity(&error))?,
        call: copied(&expected.identity.complete, proof).map_err(|error| capacity(&error))?,
        epoch_digest,
    };
    let record = OpenRecord {
        epoch,
        slot,
        registered,
    };
    let record_digest = digest(&(
        "crabber.pending-initial.open.v1",
        &record.epoch,
        &record.slot,
        &record.registered,
    ))
    .map_err(|error| capacity(&error))?;
    let (publication, head) = append(
        &view,
        expected,
        PublicationKind::Opened,
        record_digest,
        proof,
    )
    .map_err(|error| capacity(&error))?;
    let delta = RecordDelta {
        epoch: Some(copied(&record.epoch, proof).map_err(|error| capacity(&error))?),
        slot: Some(copied(&record.slot, proof).map_err(|error| capacity(&error))?),
        registered: Some(copied(&record.registered, proof).map_err(|error| capacity(&error))?),
        publication: Some(publication),
        head: Some(head),
        ..RecordDelta::default()
    };
    proof
        .charge(std::mem::size_of::<RecordDelta>())
        .map_err(|error| capacity(&error))?;
    Ok(OpenDecision::Publish {
        record,
        delta: Box::new(delta),
    })
}
pub(crate) fn verify_complete<'a>(
    prefix: &prefix::HistoricalInitialPrefix<'_>,
    expected: &'a Correlation,
    selected: &'a SelectedOriginal<'a>,
    view: Bundle<'_>,
    proof: &mut Proof,
) -> Result<VerifiedInitialCall<'a>, PreparationCommitError> {
    checked_prefix(prefix, expected, proof)?;
    selected
        .verify(prefix.original(), proof)
        .map_err(|error| capacity(&error))?;
    view.verify(&expected.key, proof)
        .map_err(|error| capacity(&error))?;
    let epoch = view
        .epoch
        .ok_or(crate::session_owner::SessionOwnerError::Unsupported)?;
    if epoch.correlation != *expected
        || epoch.kind != EpochKind::Opened
        || view.close.is_some()
        || view.assignment.is_some()
    {
        return Err(crate::session_owner::SessionOwnerError::InvalidTransition.into());
    }
    Ok(VerifiedInitialCall {
        expected,
        epoch_digest: epoch.digest().map_err(|error| capacity(&error))?,
        selected,
    })
}

pub(crate) fn prepare_close(
    prefix: &prefix::HistoricalInitialPrefix<'_>,
    expected: &Correlation,
    view: Bundle<'_>,
    proof: &mut Proof,
) -> Result<CloseDecision, PreparationCommitError> {
    checked_prefix(prefix, expected, proof)?;
    view.verify(&expected.key, proof)
        .map_err(|error| capacity(&error))?;
    if let Some(epoch) = view.epoch {
        if epoch.correlation != *expected {
            return Ok(CloseDecision::Conflict);
        }
        if let Some(close) = view.close {
            return Ok(CloseDecision::Replayed(
                copied(close, proof).map_err(|error| capacity(&error))?,
            ));
        }
    }
    let new_epoch = if view.epoch.is_none() {
        Some(Epoch {
            version: 1,
            correlation: copied(expected, proof).map_err(|error| capacity(&error))?,
            kind: EpochKind::SealedBeforeOpen,
        })
    } else {
        None
    };
    let epoch = view
        .epoch
        .or(new_epoch.as_ref())
        .ok_or(crate::session_owner::SessionOwnerError::UnknownStoreOutcome)?;
    let epoch_digest = epoch.digest().map_err(|error| capacity(&error))?;
    let slot = if view.slot.is_none() {
        Some(SourceSlot {
            version: 1,
            key: copied(&expected.key, proof).map_err(|error| capacity(&error))?,
            epoch: copied(&expected.identity.epoch, proof).map_err(|error| capacity(&error))?,
            epoch_digest: copied(&epoch_digest, proof).map_err(|error| capacity(&error))?,
        })
    } else {
        None
    };
    let record = CloseRecord {
        version: 1,
        correlation: copied(expected, proof).map_err(|error| capacity(&error))?,
        epoch_digest,
        outcome: match view.assignment {
            Some(a) => CloseOutcome::Assigned {
                assignment_digest: a.digest().map_err(|error| capacity(&error))?,
            },
            None => CloseOutcome::NoPublication,
        },
    };
    let record_digest =
        digest(&("crabber.pending-initial.close.v1", &record)).map_err(|error| capacity(&error))?;
    let (publication, head) = append(
        &view,
        expected,
        PublicationKind::Closed,
        record_digest,
        proof,
    )
    .map_err(|error| capacity(&error))?;
    let delta = RecordDelta {
        epoch: new_epoch,
        slot,
        close: Some(copied(&record, proof).map_err(|error| capacity(&error))?),
        publication: Some(publication),
        head: Some(head),
        ..RecordDelta::default()
    };
    proof
        .charge(std::mem::size_of::<RecordDelta>())
        .map_err(|error| capacity(&error))?;
    Ok(CloseDecision::Publish {
        record,
        delta: Box::new(delta),
    })
}
impl Assignment {
    pub(crate) fn from_actual(
        call: &VerifiedInitialCall<'_>,
        completion: &crate::session_owner::preparation::PreparationCompletion,
        first: &crate::native_enrollment::caller::initial::FirstPublication,
        proof: &mut Proof,
    ) -> Result<Self, PreparationCommitError> {
        actual_assignment(
            call.correlation(),
            call.epoch_digest(),
            call.selected(),
            completion,
            first,
            proof,
        )
    }
}
fn actual_assignment(
    correlation: &Correlation,
    epoch_digest: &InputFingerprint,
    selected: &SelectedOriginal<'_>,
    completion: &crate::session_owner::preparation::PreparationCompletion,
    first: &crate::native_enrollment::caller::initial::FirstPublication,
    proof: &mut Proof,
) -> Result<Assignment, PreparationCommitError> {
    use crate::session_owner::preparation::PreparationCompletion;
    let (admitted, receipt) = match completion {
        PreparationCompletion::Admitted(admitted) => (admitted, None),
        PreparationCompletion::Keyed(crate::KeyedAdmitOutcome::Started { receipt, admitted }) => {
            (&**admitted, Some(receipt))
        }
        _ => return Err(crate::session_owner::SessionOwnerError::Unsupported.into()),
    };
    crate::native_enrollment::caller::initial::Initial {
        input: crate::native_enrollment::caller::initial::Input::Prepared { selected, receipt },
        result: admitted,
        first,
    }
    .verify(proof)
    .map_err(|error| capacity(&error))?;
    let actual = (
        &admitted.session,
        &admitted.run,
        &admitted.fence,
        &admitted.assistant_placeholder,
        &admitted.epoch,
        &admitted.prior_history,
    );
    proof.measure(&actual).map_err(|error| capacity(&error))?;
    let result_digest = digest(&("crabber.pending-initial.actual-result.v1", actual))
        .map_err(|error| capacity(&error))?;
    let value = Assignment {
        version: 1,
        correlation: copied(correlation, proof).map_err(|error| capacity(&error))?,
        call: copied(&correlation.identity.complete, proof).map_err(|error| capacity(&error))?,
        run_id: copied(&admitted.run.id, proof).map_err(|error| capacity(&error))?,
        epoch_digest: copied(epoch_digest, proof).map_err(|error| capacity(&error))?,
        first_issuer_digest: copied(&first.source.issuance_digest, proof)
            .map_err(|error| capacity(&error))?,
        first_source_digest: copied(&first.source.chain_digest, proof)
            .map_err(|error| capacity(&error))?,
        result_digest,
        keyed_receipt_digest: receipt
            .map(|r| digest(&("crabber.pending-initial.keyed-result.v1", r)))
            .transpose()
            .map_err(|error| capacity(&error))?,
    };
    bounded(&value, proof).map_err(|error| capacity(&error))?;
    Ok(value)
}
impl EnrolledInitialData {
    /// Current-operation ACK accounting only. This never admits a native call.
    pub(crate) fn verify_returned(
        &self,
        selected: &SelectedOriginal<'_>,
        proof: &mut Proof,
    ) -> Result<(), PreparationCommitError> {
        self.assignment
            .correlation
            .validate(proof)
            .map_err(|error| capacity(&error))?;
        let epoch = Epoch {
            version: 1,
            correlation: copied(self.correlation(), proof).map_err(|error| capacity(&error))?,
            kind: EpochKind::Opened,
        };
        let epoch_digest = epoch.digest().map_err(|error| capacity(&error))?;
        let expected = actual_assignment(
            self.correlation(),
            &epoch_digest,
            selected,
            &self.completion,
            &self.first,
            proof,
        )?;
        bounded(&self.assignment, proof).map_err(|error| capacity(&error))?;
        if expected != self.assignment {
            return Err(crate::session_owner::SessionOwnerError::SemanticConflict.into());
        }
        Ok(())
    }
}
pub(crate) fn prepare_assignment(
    call: &VerifiedInitialCall<'_>,
    assignment: &Assignment,
    view: Bundle<'_>,
    proof: &mut Proof,
) -> Result<RecordDelta, PreparationCommitError> {
    view.verify(&call.correlation().key, proof)
        .map_err(|error| capacity(&error))?;
    let epoch = view
        .epoch
        .ok_or(crate::session_owner::SessionOwnerError::UnknownStoreOutcome)?;
    call.selected
        .verify(call.selected.original(), proof)
        .map_err(|error| capacity(&error))?;
    if epoch.correlation != *call.correlation()
        || epoch.kind != EpochKind::Opened
        || view.assignment.is_some()
        || view.close.is_some()
        || assignment.correlation != epoch.correlation
        || assignment.epoch_digest != epoch.digest().map_err(|error| capacity(&error))?
    {
        return Err(crate::session_owner::SessionOwnerError::InvalidTransition.into());
    }
    let (publication, head) = append(
        &view,
        call.correlation(),
        PublicationKind::Assigned,
        assignment.digest().map_err(|error| capacity(&error))?,
        proof,
    )
    .map_err(|error| capacity(&error))?;
    Ok(RecordDelta {
        assignment: Some(copied(assignment, proof).map_err(|error| capacity(&error))?),
        publication: Some(publication),
        head: Some(head),
        ..RecordDelta::default()
    })
}
