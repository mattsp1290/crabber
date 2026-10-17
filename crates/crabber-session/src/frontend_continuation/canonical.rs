use crate::tool_continuation::canonical::{digest, identity};
use crate::{FrontendResolverClaim, ToolContinuationError as Error};
use crabber_core::{FrontendToolResult, InputFingerprint};

/// Rehash the full original frontend request with an independently authenticated
/// embedded claim digest. No worker identity or lease is fabricated for a read.
pub(crate) fn claim_fingerprint(
    claim_digest: &InputFingerprint,
    resolver: &FrontendResolverClaim,
    supplied: &[FrontendToolResult],
) -> Result<InputFingerprint, Error> {
    identity(&resolver.resolver_id, 512)?;
    identity(&resolver.authority_claim, 4096)?;
    if supplied.is_empty() || supplied.len() > 64 {
        return Err(Error::InvalidRequest);
    }
    let mut results = supplied.iter().collect::<Vec<_>>();
    results.sort_by(|a, b| a.call_id().cmp(b.call_id()));
    if results
        .windows(2)
        .any(|pair| pair[0].call_id() == pair[1].call_id())
    {
        return Err(Error::InvalidRequest);
    }
    digest(&(
        "crabber.frontend-continuation.claim.v1",
        claim_digest,
        resolver,
        results,
    ))
}
