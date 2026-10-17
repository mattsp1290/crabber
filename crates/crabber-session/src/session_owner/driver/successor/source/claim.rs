//! Only genuine immutable native operation sources can construct this evidence.
use super::{Error, RegisterSuccessorExecutionDriver};
use crate::native_parent::{CompletionOperation, RunIssuance, RunState};
use crate::session_owner::{
    SessionOwnerOperation, SessionOwnerState,
    audit::{Audit, RevisionState, verify_current},
    preparation::{ExistingWorkTarget, PreparationKey, PreparationPurpose},
};
use crabber_core::{AdmissionReceipt, RunStatus, SessionRevision};

pub(crate) enum Native<'a> {
    Expired(crate::session_owner::preparation::expired::replay::Native<'a>),
    Admission {
        init: &'a crate::admission_journal::Init,
        initial: &'a RunIssuance,
        receipt: &'a AdmissionReceipt,
    },
    Approval {
        evidence: super::approval::Evidence<'a>,
        input: super::input::Input<'a>,
    },
    Tool {
        evidence: super::tool::Evidence<'a>,
        input: super::input::Input<'a>,
    },
}
pub(crate) struct AuthenticatedClaim<'a> {
    pub next: &'a RunIssuance,
    pub audits: &'a [Audit],
    pub revision: SessionRevision,
    pub head: u64,
    pub head_digest: crabber_core::InputFingerprint,
    pub approval: Option<(crate::ContinuationTarget, crate::AttemptTarget)>,
    pub tool: Option<(crate::ContinuationTarget, crate::AttemptTarget)>,
}
impl<'a> AuthenticatedClaim<'a> {
    pub(crate) fn verify_binding(
        &self,
        binding: &crate::ExecutionBindingCapsule,
    ) -> Result<(), Error> {
        let origin = self.audits[0]
            .record
            .preparation
            .as_ref()
            .ok_or(Error::UnknownStoreOutcome)?;
        if origin.resource_contract.is_none()
            || crate::session_owner::preparation::resource_contract(&binding.runtime_semantics)?
                .as_ref()
                != origin.resource_contract.as_ref()
            || binding.behavior_fingerprint
                != self.audits[0].record.snapshot.binding.behavior_fingerprint
        {
            return Err(Error::Unsupported);
        }
        if let PreparationPurpose::ExistingRun {
            work: ExistingWorkTarget::ExpiredRun(target),
            ..
        } = &origin.purpose
            && crate::session_owner::preparation::intent::execution_binding_digest(binding)?
                != target.binding_digest
        {
            return Err(Error::SemanticConflict);
        }
        Ok(())
    }
    pub(crate) fn verify(
        previous: &RunIssuance,
        next: &'a RunIssuance,
        audits: &'a [Audit],
        native: Native<'_>,
    ) -> Result<Self, Error> {
        if audits.len() != 5 {
            return Err(Error::Unsupported);
        }
        for issuer in [previous, next] {
            issuer.validate().map_err(unknown)?;
        }
        for audit in audits {
            audit.validate()?;
        }
        let source = next.completion_source.as_ref().ok_or(Error::Unsupported)?;
        let (approval, tool) = match (source.native_operation, native) {
            (CompletionOperation::ExpiredRunResume, Native::Expired(native)) => {
                crate::session_owner::preparation::expired::history::verify(
                    previous, next, audits, native,
                )?
                .ok_or(Error::Unsupported)?;
                (None, None)
            }
            (
                CompletionOperation::AdmissionRecovery,
                Native::Admission {
                    init,
                    initial,
                    receipt,
                },
            ) => {
                admission(previous, next, audits, init, initial, receipt)?;
                (None, None)
            }
            (
                CompletionOperation::ApprovalClaim | CompletionOperation::ApprovalRecovery,
                Native::Approval { evidence, input },
            ) => {
                let facts = evidence.verify(previous, next, audits)?;
                input.verify(&next.state.session_id, &next.state.run_id, None)?;
                (Some(facts), None)
            }
            (
                CompletionOperation::ToolClaim
                | CompletionOperation::ToolRecovery
                | CompletionOperation::FrontendClaim
                | CompletionOperation::FrontendRecovery,
                Native::Tool { evidence, input },
            ) => {
                let facts = evidence.verify(previous, next, audits)?;
                input.verify(&next.state.session_id, &next.state.run_id, None)?;
                (None, Some(facts))
            }
            _ => return Err(Error::Unsupported),
        };
        let RevisionState::Present(revision) = audits[2]
            .publication
            .ok_or(Error::UnknownStoreOutcome)?
            .after
        else {
            return Err(Error::UnknownStoreOutcome);
        };
        let continuation = approval.as_ref().or(tool.as_ref());
        Ok(Self {
            next,
            audits,
            revision: continuation.map_or(revision, |a| a.revision),
            head: continuation.map_or(next.sequence, |a| a.head),
            head_digest: continuation.map_or_else(
                || crate::native_parent::issuance_digest(next).map_err(unknown),
                |a| Ok(a.head_digest.clone()),
            )?,
            approval: approval.map(|a| (a.continuation, a.attempt)),
            tool: tool.map(|a| (a.continuation, a.attempt)),
        })
    }
    pub(crate) fn matches_request(
        &self,
        request: &RegisterSuccessorExecutionDriver,
    ) -> Result<(), Error> {
        request.fingerprint()?;
        let source = self
            .next
            .completion_source
            .as_ref()
            .ok_or(Error::Unsupported)?;
        let binding = &self.audits[0].record.snapshot.binding;
        if self.next.sequence != request.claim.issuance
            || source.root_join_id != request.claim.root_join_id
            || self.next.state
                != RunState::new(
                    request.context.session_id(),
                    &request.fence.run_id,
                    self.next.state.status,
                    &request.fence.claim_token,
                )
                .map_err(unknown)?
            || binding.workspace_id != request.context.workspace_id()
            || binding.directory != request.context.directory()
        {
            return Err(Error::SemanticConflict);
        }
        Ok(())
    }
}
pub(crate) fn audit_ids(join: &Audit) -> Result<Vec<crabber_core::SessionOwnerOperationId>, Error> {
    join.validate()?;
    let origin = join.record.preparation.as_ref().ok_or(Error::Unsupported)?;
    let ids =
        crate::session_owner::preparation::identity::ids(&origin.key, &join.receipt.session_id)?;
    if join.receipt.operation_id != ids.join || join.record.retained_resources.is_none() {
        return Err(Error::Unsupported);
    }
    let link = crate::session_owner::preparation::handoff::source(&join.record)?;
    let child = link.identities()?;
    Ok(vec![
        ids.reserve,
        ids.begin,
        ids.join,
        child.reserve,
        child.begin,
    ])
}
fn admission(
    previous: &RunIssuance,
    next: &RunIssuance,
    audits: &[Audit],
    init: &crate::admission_journal::Init,
    initial: &RunIssuance,
    receipt: &AdmissionReceipt,
) -> Result<(), Error> {
    let [reserve, begin, join, child_reserve, child_begin] = audits else {
        return Err(Error::Unsupported);
    };
    verify_current(&begin.record, reserve, begin)?;
    verify_current(&join.record, reserve, join)?;
    verify_current(&child_begin.record, child_reserve, child_begin)?;
    crate::session_owner::preparation::resource_source::verify(
        &child_begin.record,
        reserve,
        begin,
    )?;
    let origin = reserve
        .record
        .preparation
        .as_ref()
        .ok_or(Error::UnknownStoreOutcome)?;
    let PreparationPurpose::ExistingRun {
        run_id,
        work: ExistingWorkTarget::AdmissionRecovery { key, issuance },
    } = &origin.purpose
    else {
        return Err(Error::SemanticConflict);
    };
    let session = &reserve.receipt.session_id;
    let ids = crate::session_owner::preparation::identity::ids(&origin.key, session)?;
    let source = next.completion_source.as_ref().ok_or(Error::Unsupported)?;
    let link = crate::session_owner::preparation::handoff::source(&join.record)?;
    let child_ids = link.identities()?;
    if origin.key
        != (PreparationKey::AdmissionRecovery {
            key: key.clone(),
            issuance: *issuance,
        })
        || reserve.receipt.operation_id != ids.reserve
        || begin.receipt.operation_id != ids.begin
        || join.receipt.operation_id != ids.join
        || begin.receipt.operation != SessionOwnerOperation::Begin
        || begin.receipt.owner.state != SessionOwnerState::Active
        || join.receipt.operation != SessionOwnerOperation::Join
        || join.receipt.owner.state != SessionOwnerState::Joined
        || join.record.retained_resources.as_ref() != Some(&child_begin.record.snapshot.target)
        || child_reserve.receipt.operation_id != child_ids.reserve
        || child_begin.receipt.operation_id != child_ids.begin
        || child_begin.receipt.operation != SessionOwnerOperation::Begin
        || previous.sequence != *issuance
        || next.sequence != issuance + 1
        || previous.state.status != RunStatus::Running
        || next.state.status != RunStatus::Running
        || previous.state.token == next.state.token
        || previous.state.run_id != *run_id
        || next.state.run_id != *run_id
        || previous.state.session_id != *session
        || next.state.session_id != *session
        || next.version != 3
        || source.native_operation != CompletionOperation::AdmissionRecovery
        || source.root_join_id != ids.join
        || source.root_join_digest != join.receipt.semantic_digest
        || source.native_semantic_digest != origin.intent
        || init.driver_key() != key
    {
        return Err(Error::SemanticConflict);
    }
    init.verify_driver_origin(receipt, initial)
        .map_err(unknown)?;
    if receipt.session_id != *session || receipt.run_id != *run_id {
        return Err(Error::SemanticConflict);
    }
    Ok(())
}
fn unknown(_: impl std::fmt::Debug) -> Error {
    Error::UnknownStoreOutcome
}
