//! Canonical retirement originals. Metadata never supplies a cleanup capability.
use super::{Correlation, Failure, Original, Proof, RegisterOriginal, Selection, digest, unknown};
use crate::session_owner::{
    MutateSessionOwner, SessionOwnerCommand, SessionOwnerOperation, SessionOwnerTermination,
    audit::Audit, owned_scope::OwnedMainDisposition, record::Record,
};
use crabber_core::SessionOwnerOperationId;
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "retirement", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum DriverRetirement {
    NoDriver,
    Activated { audit: Box<Audit> },
    Unactivated { audit: Box<Audit> },
}
impl Selection<'_> {
    pub(crate) fn registered_driver(&self) -> Option<&RegisterOriginal> {
        self.results.iter().find_map(|r| match &r.original {
            Original::Register(value) => Some(value.as_ref()),
            _ => None,
        })
    }
    pub(crate) fn activated_driver(&self) -> Option<&super::ActivateOriginal> {
        self.results.iter().find_map(|r| match &r.original {
            Original::Activate(value) => Some(value.as_ref()),
            _ => None,
        })
    }
    pub(crate) fn retirement(&self) -> Option<(&DriverRetirement, OwnedMainDisposition)> {
        self.close
            .map(|close| (&close.retirement, close.disposition))
            .or(match self.origin {
                Some(super::Origin::NoOpenedMember(value)) => {
                    Some((&value.retirement, value.disposition))
                }
                _ => None,
            })
    }
}
impl DriverRetirement {
    pub(crate) fn audit(&self) -> Option<&Audit> {
        match self {
            Self::NoDriver => None,
            Self::Activated { audit } | Self::Unactivated { audit } => Some(audit),
        }
    }
    pub(crate) fn verify(
        &self,
        view: &Selection<'_>,
        expected: &Correlation,
        disposition: OwnedMainDisposition,
        proof: &mut Proof,
    ) -> Result<(), Failure> {
        proof.measure(self)?;
        let registered = view.registered_driver();
        let activated = view.activated_driver();
        match (self, registered, activated) {
            (Self::NoDriver, None, None) => Ok(()),
            (Self::Unactivated { audit }, Some(register), None) => {
                let request = unactivated_request(expected, register, proof)?;
                verify_join(
                    audit,
                    &register.begin.record,
                    &request,
                    SessionOwnerTermination::Cancelled,
                )?;
                if audit.record.owned_join.is_some() || audit.record.owned_scope.is_some() {
                    return Err(unknown("unactivated retirement invented scope"));
                }
                Ok(())
            }
            (Self::Activated { audit }, Some(register), Some(activation)) => {
                let joined = audit
                    .record
                    .owned_join
                    .as_ref()
                    .ok_or_else(|| unknown("activated retirement missing original join"))?;
                if joined.activation != activation.request
                    || joined.disposition != disposition
                    || activation.audit.record.snapshot.target
                        != register.begin.record.snapshot.target
                {
                    return Err(unknown("retirement activation/disposition"));
                }
                let request = joined.request().map_err(unknown)?;
                verify_join(
                    audit,
                    &activation.audit.record,
                    &request,
                    match disposition {
                        OwnedMainDisposition::Returned => SessionOwnerTermination::Returned,
                        OwnedMainDisposition::Panicked => SessionOwnerTermination::Panicked,
                        OwnedMainDisposition::NoDispatch | OwnedMainDisposition::Aborted => {
                            SessionOwnerTermination::Cancelled
                        }
                    },
                )?;
                crate::session_owner::owned_scope::join_verify::verify_record(
                    joined,
                    &audit.record,
                )
                .map_err(unknown)?;
                if audit.receipt.semantic_digest != joined.fingerprint().map_err(unknown)? {
                    return Err(unknown("retirement owned Join digest"));
                }
                Ok(())
            }
            _ => Err(unknown("retirement does not match original driver members")),
        }
    }
}
/// Stable cleanup request identity; its semantic digest remains the ordinary
/// Join fingerprint. This metadata helper never authorizes a native mutation.
pub(crate) fn unactivated_request(
    expected: &Correlation,
    register: &RegisterOriginal,
    proof: &mut Proof,
) -> Result<MutateSessionOwner, Failure> {
    proof.measure(&(expected, register))?;
    let original = digest(&("crabber.initial-consumer.register-original.v1", register))?;
    let identity = digest(&(
        "crabber.initial-consumer.driver-retirement.v1",
        expected,
        &register.begin.record.snapshot.target,
        &original,
    ))?;
    let session = &expected.key.session_id;
    let target = &register.begin.record.snapshot.target;
    proof.measure(&(session, target))?;
    proof.charge("initial-retire-".len() + 64 + 2)?;
    Ok(MutateSessionOwner {
        id: SessionOwnerOperationId(format!("initial-retire-{}", identity.as_str())),
        session_id: session.clone(),
        target: target.clone(),
        command: SessionOwnerCommand::Join {
            termination: SessionOwnerTermination::Cancelled,
        },
    })
}
pub(super) fn verify_join(
    audit: &Audit,
    before: &Record,
    request: &MutateSessionOwner,
    termination: SessionOwnerTermination,
) -> Result<(), Failure> {
    audit.validate().map_err(unknown)?;
    let after = &audit.record;
    if audit.receipt.operation != SessionOwnerOperation::Join
        || audit.receipt.operation_id != request.id
        || audit.receipt.session_id != request.session_id
        || audit.receipt.owner.target != request.target
        || after.snapshot.state != crate::session_owner::SessionOwnerState::Joined
        || after.snapshot.termination != Some(termination)
        || !after.token.is_empty()
        || after.sequence
            != before
                .sequence
                .checked_add(1)
                .ok_or_else(|| unknown("retirement sequence"))?
        || !same_retained_fields(before, after)
        || after.owned_scope != before.owned_scope
        || audit.publication.is_none()
    {
        return Err(unknown("retirement native Join original"));
    }
    if after.owned_join.is_none()
        && audit.receipt.semantic_digest != request.fingerprint().map_err(unknown)?
    {
        return Err(unknown("unactivated ordinary Join digest"));
    }
    Ok(())
}
fn same_retained_fields(before: &Record, after: &Record) -> bool {
    before.version == after.version
        && before.snapshot.binding == after.snapshot.binding
        && before.snapshot.target == after.snapshot.target
        && before.snapshot.created_at == after.snapshot.created_at
        && before.snapshot.cancellation == after.snapshot.cancellation
        && before.origin == after.origin
        && before.preparation == after.preparation
        && before.resource == after.resource
        && before.service == after.service
        && before.driver == after.driver
        && before.successor_driver == after.successor_driver
        && before.retained_resources == after.retained_resources
        && before.unkeyed_input == after.unkeyed_input
        && before.cancellation == after.cancellation
        && before.parent == after.parent
        && before.owner == after.owner
        && before.lease_until == after.lease_until
        && before.ever_began == after.ever_began
        && before.resolver == after.resolver
}

native_pending_initial_items! {
    #[path="retirement/native.rs"] mod native;
    pub(crate) use native::{VerifiedUnactivatedRetirement,verify_unactivated_retirement};
}
