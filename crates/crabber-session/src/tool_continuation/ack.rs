//! Ephemeral integrity of the grant emitted by a native issuer, never provenance.
//! The independent Some-source inspection remains a separate requirement.
use super::{ToolContinuationError as Error, ToolContinuationGrant, ToolContinuationReceipt};
use crate::public_session::v2::proof::Proof;
use crabber_core::InputFingerprint;
use serde::Serialize;
use sha2::{Digest, Sha256};
#[path = "ack/verify.rs"]
mod verify;
#[path = "ack/view.rs"]
mod view;
const MAX_ACK_BYTES: usize = 8 << 20;
#[path = "ack/issuance.rs"]
mod issuance;
pub(crate) use issuance::{Completion, Issuance};
/// Exact native metering includes seals even though the hash excludes them.
pub(crate) fn issue(
    grant: &mut ToolContinuationGrant,
    issuance: Issuance<'_>,
) -> Result<(), Error> {
    let Issuance {
        receipt,
        token,
        run_token,
        proof,
    } = issuance;
    verify::shape(grant, receipt)?;
    let bytes = crate::snapshot::encoded_bytes(&view::ack(grant, receipt, true)?, MAX_ACK_BYTES)
        .ok_or(Error::InvalidRequest)?;
    let bytes = bytes
        .checked_add(first_seal_growth(grant, receipt)?)
        .filter(|bytes| *bytes <= MAX_ACK_BYTES)
        .ok_or(Error::InvalidRequest)?;
    proof
        .charge(bytes)
        .map_err(|_| Error::UnknownStoreOutcome)?;
    grant.seal_ack_from_store(receipt, verify::attempt(receipt)?, token, run_token)
}
fn first_seal_growth(
    grant: &ToolContinuationGrant,
    receipt: &ToolContinuationReceipt,
) -> Result<usize, Error> {
    if grant.authority.tool_ack_commitment.is_some() {
        return Ok(0);
    }
    // The actual tuple always serializes its Option field, including None.
    // Count the resulting Some field with an existing validated fingerprint:
    // every fingerprint has the same 64 lowercase ASCII wire width. Its value
    // is used only for metering; the commitment still hashes the complete ACK.
    let sealed = crate::snapshot::encoded_bytes(&Some(&receipt.semantic_digest), 128)
        .ok_or(Error::InvalidRequest)?;
    let unsealed = crate::snapshot::encoded_bytes(&grant.authority.tool_ack_commitment, 128)
        .ok_or(Error::InvalidRequest)?;
    sealed.checked_sub(unsealed).ok_or(Error::InvalidRequest)
}
impl ToolContinuationGrant {
    /// Finish a fresh native grant inside a trusted Store after its ownership
    /// decision. Requires the original private parent credentials; possessing
    /// a returned grant supplies no way to reseal changed public data.
    ///
    /// This changes no authority or stored history. Custom Tool/Frontend Store
    /// issuers must call this before emitting Applied; `from_store` stays unchanged.
    /// # Errors
    /// Rejects mismatched private credentials, scope, phase membership or bounds.
    pub fn seal_ack_from_store(
        &mut self,
        receipt: &ToolContinuationReceipt,
        parent: &crate::AttemptSnapshot,
        token: &str,
        run_token: &str,
    ) -> Result<(), Error> {
        if self.authority.snapshot != *parent
            || !self.authority.matches_store(parent, token, run_token)
        {
            return Err(Error::AuthorityMismatch);
        }
        verify::shape(self, receipt)?;
        let commitment = {
            let view = view::ack(self, receipt, false)?;
            digest(&view)?
        };
        self.authority.tool_ack_commitment = Some(commitment);
        Ok(())
    }
    /// Verify the original native ACK before dispatching work. This grants no
    /// current eligibility, stored provenance, ownership or native Begin bypass.
    /// # Errors
    /// Unsealed custom-Store grants are unsupported; substitutions or altered
    /// receipts, scope or payload fail even when public fields stay unchanged.
    pub fn verify_native_ack(&self, receipt: &ToolContinuationReceipt) -> Result<(), Error> {
        let expected = self
            .authority
            .tool_ack_commitment
            .as_ref()
            .ok_or(Error::Unsupported)?;
        verify::shape(self, receipt)?;
        if *expected != digest(&view::ack(self, receipt, false)?)? {
            return Err(Error::AuthorityMismatch);
        }
        Ok(())
    }
}
fn digest(value: &impl Serialize) -> Result<InputFingerprint, Error> {
    if crate::snapshot::encoded_bytes(value, MAX_ACK_BYTES).is_none() {
        return Err(Error::InvalidRequest);
    }
    let mut canonical = serde_json::to_value(value).map_err(|_| Error::InvalidRequest)?;
    canonical.sort_all_objects();
    InputFingerprint::new(format!(
        "{:x}",
        Sha256::digest(canonical.to_string().as_bytes())
    ))
    .map_err(|_| Error::InvalidRequest)
}
#[cfg(test)]
#[path = "ack/tests.rs"]
mod tests;
