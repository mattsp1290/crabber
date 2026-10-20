//! Same original public sealer, with enforced conservative private-byte charges.
use crate::public_session::v2::proof::Proof;
use crate::{ToolContinuationError as Error, ToolContinuationGrant, ToolContinuationReceipt};
#[path = "../../../crabber-session/src/tool_continuation/ack/issuance.rs"]
mod issuance;
pub(crate) use issuance::{Completion, Issuance};
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
    let payload = crate::session_owner::preparation::tool_driver::payload::view(grant)
        .map_err(|_| Error::InvalidRequest)?;
    let public = crate::snapshot::encoded_bytes(&(receipt, &grant.fence, payload), 5 << 20)
        .ok_or(Error::InvalidRequest)?;
    // The actual sealer enforces each private snapshot <=4096 encoded bytes,
    // each token <=512 ASCII bytes (<=1024 JSON bytes), and includes at most one
    // 64-byte optional seal per AttemptAuthority in metering. These are upper
    // bounds on complete encoded tuples, including syntax, not invented facts.
    let private = 8192usize
        .checked_add(
            (grant.phases.len() + grant.frontend.len())
                .checked_mul(16384)
                .ok_or(Error::InvalidRequest)?,
        )
        .and_then(|bytes| bytes.checked_add(512))
        .ok_or(Error::InvalidRequest)?;
    proof
        .charge(public.checked_add(private).ok_or(Error::InvalidRequest)?)
        .map_err(|_| Error::UnknownStoreOutcome)?;
    let (crate::ToolContinuationOutcome::Claimed {
        attempt: parent, ..
    }
    | crate::ToolContinuationOutcome::FrontendClaimed {
        attempt: parent, ..
    }
    | crate::ToolContinuationOutcome::ReclaimedUnstarted {
        attempt: parent, ..
    }
    | crate::ToolContinuationOutcome::FrontendReclaimedUnstarted {
        attempt: parent, ..
    }) = &receipt.outcome
    else {
        return Err(Error::InvalidRequest);
    };
    grant.seal_ack_from_store(receipt, parent, token, run_token)
}
