//! Ephemeral actual first publisher output; never enrollment or a grant factory.
use super::Caller;
use crate::native_parent::{RunIssuance, source_protocol::JournalSource};
use crate::public_session::v2::proof::{Failure, Proof, unknown};

#[must_use]
pub(crate) struct FirstPublication {
    pub issuer: RunIssuance,
    pub source: JournalSource,
}
impl FirstPublication {
    pub(crate) fn capture(
        issuer: &RunIssuance,
        source: &JournalSource,
        proof: &mut Proof,
    ) -> Result<Self, Failure> {
        bootstrap(Caller::Raw, issuer, source, proof)?;
        // Charge the actual source bodies before making this same-Tx owned copy.
        proof.measure(&(issuer, source))?;
        Ok(Self {
            issuer: issuer.clone(),
            source: source.clone(),
        })
    }
}
/// This validates only the actual original issuer/source bootstrap. It does
/// not observe a second initial admission, inspect a higher request, or publish.
pub(crate) fn bootstrap(
    caller: Caller,
    issuer: &RunIssuance,
    source: &JournalSource,
    proof: &mut Proof,
) -> Result<(), Failure> {
    proof.measure(&(issuer, source))?;
    match caller {
        Caller::Raw => (),
    }
    source.verify(issuer).map_err(unknown)?;
    if issuer.sequence != 1 || source.sequence != 1 || source.previous_digest.is_some() {
        return Err(unknown("actual initial source"));
    }
    Ok(())
}
#[must_use]
pub(crate) struct RunPublication {
    pub sequence: Option<u64>,
    pub first: Option<FirstPublication>,
    pub updated: Option<RecoveryPublication>,
}
impl RunPublication {
    pub(crate) fn into_sequence(self) -> Option<u64> {
        drop(self.first);
        drop(
            self.updated
                .map(|publication| (publication.issuer, publication.source)),
        );
        self.sequence
    }
}
#[must_use]
pub(crate) struct CapturedAdmission {
    pub admitted: crate::AdmitOutcome,
    pub first: FirstPublication,
}
#[must_use]
pub(crate) enum CapturedKeyedAdmission {
    Started {
        receipt: crabber_core::AdmissionReceipt,
        admitted: Box<CapturedAdmission>,
    },
    Replayed(crabber_core::AdmissionReceipt),
}

/// Exact outer input at the actual grant-returning boundary. Queue includes
/// both the genuine prepared admission and the actual Queue control result.
pub(crate) enum Input<'a> {
    Ordinary(&'a crate::AdmitRequest),
    Bound {
        request: &'a crate::AdmitRequest,
        binding: &'a crate::ExecutionBindingCapsule,
    },
    Keyed {
        request: &'a crate::KeyedAdmitRequest,
        receipt: &'a crabber_core::AdmissionReceipt,
    },
    BoundKeyed {
        request: &'a crate::KeyedAdmitRequest,
        binding: &'a crate::ExecutionBindingCapsule,
        receipt: &'a crabber_core::AdmissionReceipt,
    },
    Queue {
        request: &'a crate::QueueMutation,
        prepared: &'a crate::BoundKeyedAdmitRequest,
        receipt: &'a crabber_core::AdmissionReceipt,
        queue_receipt: &'a crate::QueueReceipt,
        selected: &'a SelectedInput<'a>,
    },
}
pub(crate) struct Initial<'a> {
    pub input: Input<'a>,
    pub result: &'a crate::AdmitOutcome,
    pub first: &'a FirstPublication,
}
impl Input<'_> {
    fn request(&self, proof: &mut Proof) -> Result<&crate::AdmitRequest, Failure> {
        Ok(match self {
            Self::Ordinary(request) => {
                proof.measure(request)?;
                request
            }
            Self::Bound { request, binding } => {
                proof.measure(&(request, binding))?;
                binding.validate(request).map_err(unknown)?;
                request
            }
            Self::Keyed { request, receipt } => {
                verify_keyed(request, receipt, proof)?;
                &request.request
            }
            Self::BoundKeyed {
                request,
                binding,
                receipt,
            } => {
                proof.measure(binding)?;
                binding.validate_keyed(request).map_err(unknown)?;
                verify_keyed(request, receipt, proof)?;
                &request.request
            }
            Self::Queue {
                request,
                prepared,
                receipt,
                queue_receipt,
                selected,
            } => {
                selected.verify(prepared, proof)?;
                proof.measure(&(request, receipt, queue_receipt))?;
                prepared
                    .binding
                    .validate_keyed(&prepared.keyed)
                    .map_err(unknown)?;
                verify_keyed_selected(&prepared.keyed, receipt, Some(selected), proof)?;
                if !matches!(request.action, crate::QueueAction::Admit { .. })
                    || queue_receipt.session_id != request.session_id
                    || queue_receipt.operation_id != request.id
                    || queue_receipt.semantic_digest != request.fingerprint().map_err(unknown)?
                    || queue_receipt.summary.state != crate::QueueState::Admitted
                    || queue_receipt.summary.admission.as_ref() != Some(*receipt)
                {
                    return Err(unknown("actual Queue initial result"));
                }
                &prepared.keyed.request
            }
        })
    }
}
fn verify_keyed(
    request: &crate::KeyedAdmitRequest,
    receipt: &crabber_core::AdmissionReceipt,
    proof: &mut Proof,
) -> Result<(), Failure> {
    verify_keyed_selected(request, receipt, None, proof)
}
fn verify_keyed_selected(
    request: &crate::KeyedAdmitRequest,
    receipt: &crabber_core::AdmissionReceipt,
    selected: Option<&SelectedInput<'_>>,
    proof: &mut Proof,
) -> Result<(), Failure> {
    input_keyed(request, selected, proof)?;
    proof.measure(receipt)?;
    if receipt.semantic_digest != request.validated_digest().map_err(unknown)?
        || receipt.fingerprint != request.options.fingerprint
        || Some(&receipt.session_id) != request.request.session_id.as_ref()
        || receipt.user_message_id != request.request.user_message.id
    {
        return Err(unknown("actual keyed initial result"));
    }
    Ok(())
}
impl<'a> Initial<'a> {
    pub(super) fn verify(&self, proof: &mut Proof) -> Result<super::verify::Scope<'a>, Failure> {
        let request = self.input.request(proof)?;
        let out = self.result;
        proof.measure(&(
            &out.session,
            &out.run,
            &out.fence,
            &out.assistant_placeholder,
            &out.epoch,
            &out.prior_history,
        ))?;
        bootstrap(Caller::Raw, &self.first.issuer, &self.first.source, proof)?;
        self.first.issuer.verify(&out.run).map_err(unknown)?;
        let receipt = match &self.input {
            Input::Keyed { receipt, .. }
            | Input::BoundKeyed { receipt, .. }
            | Input::Queue { receipt, .. } => Some(*receipt),
            Input::Ordinary(_) | Input::Bound { .. } => None,
        };
        if out.session.id != out.run.session_id
            || out.session.id != request.user_message.session_id
            || request
                .session_id
                .as_ref()
                .is_some_and(|id| *id != out.session.id)
            || out.session.workspace_id != request.workspace_id
            || out.session.directory != request.directory
            || out.run.owner != request.owner
            || out.run.config_hash != request.config_hash
            || out.run.plan_fingerprint != request.plan_fingerprint
            || out.run.status != crabber_core::RunStatus::Running
            || out.fence.run_id != out.run.id
            || out.fence.claim_token != out.run.claim_token
            || out.epoch != out.run.epoch_id
            || receipt.is_some_and(|r| r.run_id != out.run.id)
        {
            return Err(unknown("actual raw Initial result"));
        }
        Ok(super::verify::Scope {
            session: &out.run.session_id,
            run: &out.run.id,
        })
    }
}

#[must_use]
pub(crate) struct CapturedQueueAdmission {
    pub admitted: CapturedAdmission,
    pub receipt: crabber_core::AdmissionReceipt,
}

/// Only the original publisher can populate this opt-in output. It is never a
/// current-head lookup and carries no reconstructed native authority.
#[must_use]
pub(crate) struct RecoveryPublication {
    pub issuer: RunIssuance,
    pub source: Option<JournalSource>,
}
impl RecoveryPublication {
    pub(crate) fn capture(
        issuer: &RunIssuance,
        source: Option<&JournalSource>,
        proof: &mut Proof,
    ) -> Result<Self, Failure> {
        proof.measure(&(issuer, source))?;
        issuer.validate().map_err(unknown)?;
        if let Some(source) = source {
            source.verify(issuer).map_err(unknown)?;
            if issuer.sequence <= 1
                || source.sequence != issuer.sequence
                || source.previous_digest.is_none()
            {
                return Err(unknown("actual noninitial publication"));
            }
        }
        // None is only passed by the real noncreation protocol0 append branch.
        // It remains original legacy metadata and never invents a journal source.
        Ok(Self {
            issuer: issuer.clone(),
            source: source.cloned(),
        })
    }
}
#[derive(Clone, Copy)]
pub(crate) enum Capture {
    None,
    Original,
}
#[derive(Default)]
#[must_use]
pub(crate) struct SourcePublication {
    pub first: Option<FirstPublication>,
    pub updated: Option<RecoveryPublication>,
}
impl SourcePublication {
    pub(crate) fn legacy(
        mode: Capture,
        issuer: &RunIssuance,
        creation: bool,
        proof: &mut Proof,
    ) -> Result<Self, Failure> {
        match (mode, creation) {
            (Capture::Original, false) => Ok(Self {
                first: None,
                updated: Some(RecoveryPublication::capture(issuer, None, proof)?),
            }),
            _ => Ok(Self::default()),
        }
    }
    pub(crate) fn capture(
        mode: Capture,
        issuer: &RunIssuance,
        source: &JournalSource,
        creation: bool,
        proof: &mut Proof,
    ) -> Result<Self, Failure> {
        Ok(match mode {
            Capture::None => Self::default(),
            Capture::Original if creation => Self {
                first: Some(FirstPublication::capture(issuer, source, proof)?),
                updated: None,
            },
            Capture::Original => Self {
                first: None,
                updated: Some(RecoveryPublication::capture(issuer, Some(source), proof)?),
            },
        })
    }
}

/// Borrowed actual Initial/Queue preparation input before original-intent hashing.
/// Other families retain their own separate request preflight.
pub(crate) fn preparation_input(
    joined: &crate::session_owner::preparation::JoinedPreparation,
    proof: &mut Proof,
) -> Result<(), Failure> {
    use crate::session_owner::preparation::PreparedOperation;
    match joined.observed_operation() {
        PreparedOperation::ExpiredRunResume(prepared) => {
            super::recovery::measure_expired(&prepared.source, proof)?;
            proof.measure(&*prepared.binding)?;
        }
        PreparedOperation::AdmissionRecovery(recovery) => {
            crate::native_enrollment::caller::measure_recovery_source(&recovery.source, proof)?;
            proof.measure(&*recovery.binding)?;
        }
        PreparedOperation::Admit(bound) => proof.measure(&(&bound.request, &bound.binding))?,
        PreparedOperation::AdmitKeyed(bound) => proof.measure(bound)?,
        PreparedOperation::Queue { request, binding } => proof.measure(&(request, binding))?,
        _ => return Ok(()),
    }
    let grant = joined.observed_grant();
    proof.measure(&(grant.original_reservation(), joined.observed_registration()))?;
    super::measure_owner_authority(grant.owner_authority(), proof)?;
    if let Some(authority) = grant.retained_resources_authority() {
        super::measure_owner_authority(authority, proof)?;
    }
    Ok(())
}

#[cfg(test)]
impl Input<'_> {
    pub(crate) fn test_digest(&self) -> crabber_core::InputFingerprint {
        use super::contract::snapshot_digest;
        match self {
            Self::Ordinary(r) => snapshot_digest(&("ordinary", r)),
            Self::Bound { request, binding } => snapshot_digest(&("bound", request, binding)),
            Self::Keyed { request, receipt } => snapshot_digest(&("keyed", request, receipt)),
            Self::BoundKeyed {
                request,
                binding,
                receipt,
            } => snapshot_digest(&("bound-keyed", request, binding, receipt)),
            Self::Queue {
                request,
                prepared,
                receipt,
                queue_receipt,
                selected: _,
            } => snapshot_digest(&("queue", request, prepared, receipt, queue_receipt)),
        }
    }
}

#[path = "initial/selected.rs"]
mod selected;
#[cfg(any(feature = "postgres", feature = "sqlite", test))]
pub(crate) use selected::with_queue_input_async;
pub(crate) use selected::{Dispatch, SelectedInput, input_keyed, input_request, with_queue_input};
