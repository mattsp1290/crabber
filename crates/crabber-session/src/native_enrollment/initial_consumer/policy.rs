//! Original fixed construction metadata. None of these records is a capability.
use super::{
    ConsumerClose, Correlation, Failure, InputFingerprint, Origin, Proof, digest, unknown,
};
use crate::session_owner::{audit::Audit, owned_scope::OwnedScopeActivation};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SourceKind {
    FixedMemorySystemClockLiteralScriptedV1,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct OriginalPolicy {
    pub version: u32,
    pub kind: SourceKind,
    pub preparation_activation: OwnedScopeActivation,
}
impl OriginalPolicy {
    pub(crate) fn digest(&self) -> Result<InputFingerprint, Failure> {
        digest(&("crabber.initial-consumer.original-policy.v1", self))
    }
    pub(crate) fn validate(
        &self,
        expected: &Correlation,
        proof: &mut Proof,
    ) -> Result<(), Failure> {
        proof.measure(self)?;
        self.preparation_activation.fingerprint().map_err(unknown)?;
        if self.version != 1 || self.preparation_activation.session_id() != &expected.key.session_id
        {
            return Err(unknown("fixed policy original"));
        }
        Ok(())
    }
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PolicyTerminal {
    pub version: u32,
    pub original_policy_digest: InputFingerprint,
    pub preparation_join: Box<Audit>,
}
impl PolicyTerminal {
    pub(crate) fn verify(
        &self,
        original: &OriginalPolicy,
        proof: &mut Proof,
    ) -> Result<(), Failure> {
        proof.measure(self)?;
        let audit = &self.preparation_join;
        audit.validate().map_err(unknown)?;
        let joined = audit
            .record
            .owned_join
            .as_ref()
            .ok_or_else(|| unknown("fixed policy child Join"))?;
        crate::session_owner::owned_scope::join_verify::verify_record(joined, &audit.record)
            .map_err(unknown)?;
        if self.version != 1
            || self.original_policy_digest != original.digest()?
            || joined.activation != original.preparation_activation
            || audit.receipt.operation != crate::session_owner::SessionOwnerOperation::Join
            || audit.receipt.operation_id != joined.request().map_err(unknown)?.id
            || !audit.record.token.is_empty()
            || audit.receipt.semantic_digest != joined.fingerprint().map_err(unknown)?
            || audit.publication.is_none()
        {
            return Err(unknown("fixed policy terminal original"));
        }
        Ok(())
    }
}
impl Origin {
    pub(crate) fn original_policy(&self) -> Option<&OriginalPolicy> {
        match self {
            Self::Opened(anchor) => anchor.original_policy.as_deref(),
            Self::NoOpenedMember(_) => None,
        }
    }
}
impl ConsumerClose {
    pub(crate) fn valid_policy_version(&self, origin: &Origin) -> bool {
        matches!(
            (origin, self.version, self.policy_terminal.as_ref()),
            (
                Origin::Opened(super::Anchor {
                    version: 1,
                    original_policy: None,
                    ..
                }),
                2,
                None
            ) | (
                Origin::Opened(super::Anchor {
                    version: 2,
                    original_policy: Some(_),
                    ..
                }),
                3,
                Some(_)
            )
        )
    }
    pub(crate) fn verify_policy(&self, origin: &Origin, proof: &mut Proof) -> Result<(), Failure> {
        if !self.valid_policy_version(origin) {
            return Err(unknown("fixed policy close version"));
        }
        if let (Some(original), Some(terminal)) = (origin.original_policy(), &self.policy_terminal)
        {
            terminal.verify(original, proof)?;
        }
        Ok(())
    }
}

native_pending_initial_items! {
pub(crate) fn retirement_verify_child(audit: &Audit, activated: &crate::session_owner::record::Record, proof: &mut Proof) -> Result<(), Failure> {
    proof.measure(activated)?;
    let joined = audit.record.owned_join.as_ref().ok_or_else(|| unknown("fixed child missing joined original"))?;
    let request = joined.request().map_err(unknown)?;
    let termination = match joined.disposition {
        crate::session_owner::owned_scope::OwnedMainDisposition::Returned => crate::session_owner::SessionOwnerTermination::Returned,
        crate::session_owner::owned_scope::OwnedMainDisposition::Panicked => crate::session_owner::SessionOwnerTermination::Panicked,
        crate::session_owner::owned_scope::OwnedMainDisposition::NoDispatch | crate::session_owner::owned_scope::OwnedMainDisposition::Aborted => crate::session_owner::SessionOwnerTermination::Cancelled,
    };
    super::retirement::verify_join(audit, activated, &request, termination)
}

}
