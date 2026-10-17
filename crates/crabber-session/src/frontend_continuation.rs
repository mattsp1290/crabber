mod origin;
mod record;
pub(crate) use record::{FrontendRecoveryRecord, recovery_semantics};
mod staging;
mod validation;
pub(crate) use origin::{
    FrontendOriginRecord, staging_receipt, verify_assistant_source, verify_declaration,
    verify_receipt as verify_staging_receipt,
};
pub use staging::{FrontendCallStaging, FrontendStagingReceipt};
// Exact external result sets share the native durable continuation authority.
use crate::tool_continuation::canonical::{digest, identity};
use crate::{AttemptTarget, PhaseTarget, ToolContinuationClaim, ToolContinuationError as Error};
use crabber_core::{
    ControlId, FrontendTool, FrontendToolResult, InputFingerprint, SessionRevision,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FrontendResolverClaim {
    /// Authenticated by the embedding host. Frontend result data is not consent.
    pub resolver_id: String,
    pub authority_claim: String,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FrontendCallOrigin {
    pub call_id: crabber_core::ToolCallId,
    pub descriptor: FrontendTool,
    pub descriptor_fingerprint: InputFingerprint,
    pub call_binding: InputFingerprint,
}
impl std::fmt::Debug for FrontendCallOrigin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("FrontendCallOrigin([redacted])")
    }
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FrozenFrontendCall {
    pub target: PhaseTarget,
    pub call_binding: InputFingerprint,
    pub descriptor: FrontendTool,
    pub descriptor_fingerprint: InputFingerprint,
    pub arguments: serde_json::Value,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FrontendCallView {
    pub call_id: crabber_core::ToolCallId,
    pub descriptor: FrontendTool,
    pub descriptor_fingerprint: InputFingerprint,
    pub arguments: serde_json::Value,
    /// Consumption may be a truthful administrative cancellation, not success.
    pub consumed: bool,
}
#[derive(Clone)]
pub struct FrontendPhaseResume {
    pub target: PhaseTarget,
    pub authority: crate::PhaseAuthority,
    pub result: FrontendToolResult,
    pub frozen: FrozenFrontendCall,
}
impl std::fmt::Debug for FrontendPhaseResume {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("FrontendPhaseResume([redacted])")
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FrontendContinuationClaim {
    pub claim: ToolContinuationClaim,
    pub resolver: FrontendResolverClaim,
    pub results: Vec<FrontendToolResult>,
}
impl FrontendContinuationClaim {
    /// Every required frontend call must appear exactly once. Set ordering has
    /// no authority; the frozen batch determines processing/settlement order.
    /// # Errors
    /// Rejects malformed identities, duplicate results or unbounded original data.
    pub fn fingerprint(&self) -> Result<InputFingerprint, Error> {
        Self::fingerprint_parts(&self.claim, &self.resolver, &self.results)
    }
    /// Hash borrowed host results before copying or retaining them.
    /// # Errors
    /// Rejects oversized sets, duplicate identities and aggregate data over 4 MiB.
    pub fn fingerprint_parts(
        claim: &ToolContinuationClaim,
        resolver: &FrontendResolverClaim,
        supplied: &[FrontendToolResult],
    ) -> Result<InputFingerprint, Error> {
        claim_fingerprint(&claim.fingerprint()?, resolver, supplied)
    }
}

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
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FrontendContinuationRecovery {
    pub id: ControlId,
    pub expected_revision: SessionRevision,
    pub original: FrontendContinuationClaim,
    pub attempt: AttemptTarget,
    pub owner: String,
    pub lease_ms: u64,
}
impl FrontendContinuationRecovery {
    /// Only an exact positively Unstarted reservation can use this descriptor.
    /// # Errors
    /// Rejects malformed scope, changed original results or invalid lease bounds.
    pub fn fingerprint(&self) -> Result<InputFingerprint, Error> {
        record::recovery_fingerprint(
            &self.id,
            self.expected_revision,
            &self.original.fingerprint()?,
            &self.attempt,
            &self.owner,
            self.lease_ms,
        )
    }
}
/// Validate the entire borrowed declaration set before materializing a plan.
/// # Errors
/// Rejects excess count, aggregate data over 4 MiB, and invalid descriptors.
pub fn validate_frontend_declarations(tools: &[FrontendTool]) -> Result<(), Error> {
    if tools.len() > 128 || crate::snapshot::encoded_bytes(&tools, 4 * 1024 * 1024).is_none() {
        return Err(Error::InvalidRequest);
    }
    for tool in tools {
        frontend_descriptor_fingerprint(tool)?;
    }
    Ok(())
}
/// Frozen declaration identity includes the optional independently named result
/// contract. Input parameters never validate returned content.
/// # Errors
/// Rejects invalid/offline-unresolvable schemas and unbounded declarations.
pub fn frontend_descriptor_fingerprint(tool: &FrontendTool) -> Result<InputFingerprint, Error> {
    identity(&tool.name, 128)?;
    if tool.description.len() > 8192
        || tool.description.contains('\0')
        || crate::snapshot::encoded_bytes(tool, 65536).is_none()
    {
        return Err(Error::InvalidRequest);
    }
    crate::ensure_storable(tool).map_err(|_| Error::InvalidRequest)?;
    jsonschema::validator_for(&tool.input_parameters).map_err(|_| Error::InvalidRequest)?;
    if let Some(schema) = &tool.result_contract {
        jsonschema::validator_for(schema).map_err(|_| Error::InvalidRequest)?;
    }
    digest(&("crabber.frontend-tool-descriptor.v1", tool))
}
/// An explicit result contract targets the canonical `ToolOutput` object with
/// ordered `content` and `is_error`; absent contracts accept any valid `ToolOutput`.
/// # Errors
/// Rejects malformed declaration or a result violating its separate contract.
pub fn validate_frontend_result(
    tool: &FrontendTool,
    result: &FrontendToolResult,
) -> Result<(), Error> {
    frontend_descriptor_fingerprint(tool)?;
    if let Some(schema) = &tool.result_contract {
        let value = serde_json::to_value(result.output()).map_err(|_| Error::InvalidRequest)?;
        jsonschema::validator_for(schema)
            .map_err(|_| Error::InvalidRequest)?
            .validate(&value)
            .map_err(|_| Error::InvalidRequest)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crabber_core::{
        ContinuationGeneration, ContinuationId, FrontendResultStatus, MessageContent, RunId,
        SessionId, ToolCallId, ToolOutput,
    };
    fn result(id: &str) -> FrontendToolResult {
        FrontendToolResult::new(
            ToolCallId::from(id),
            FrontendResultStatus::Completed,
            ToolOutput::new(
                vec![MessageContent::Text {
                    text: "external text".into(),
                }],
                false,
            )
            .unwrap(),
        )
        .unwrap()
    }
    fn claim() -> FrontendContinuationClaim {
        FrontendContinuationClaim {
            claim: ToolContinuationClaim {
                id: ControlId::new(),
                session_id: SessionId::new(),
                run_id: RunId::new(),
                continuation: crate::ContinuationTarget {
                    id: ContinuationId::new(),
                    generation: ContinuationGeneration::INITIAL,
                },
                expected_revision: SessionRevision::INITIAL,
                expected_plan: InputFingerprint::new("a".repeat(64)).unwrap(),
                owner: "worker".into(),
                lease_ms: 30_000,
            },
            resolver: FrontendResolverClaim {
                resolver_id: "verified-client".into(),
                authority_claim: "host-authenticated transport".into(),
            },
            results: vec![result("a"), result("b")],
        }
    }
    #[test]
    fn complete_result_set_hash_ignores_order_and_worker_bookkeeping_but_binds_original_data() {
        let original = claim();
        let hash = original.fingerprint().unwrap();
        let mut reordered = original.clone();
        reordered.results.reverse();
        reordered.claim.owner = "fresh-worker".into();
        reordered.claim.lease_ms = 40_000;
        assert_eq!(reordered.fingerprint().unwrap(), hash);
        let mut changed = original.clone();
        changed.resolver.authority_claim = "different-host-assertion".into();
        assert_ne!(changed.fingerprint().unwrap(), hash);
        changed = original.clone();
        changed.results.pop();
        assert_ne!(changed.fingerprint().unwrap(), hash);
        changed = original;
        changed.results[1] = changed.results[0].clone();
        assert_eq!(changed.fingerprint().unwrap_err(), Error::InvalidRequest);
    }
    #[test]
    fn input_parameters_never_become_an_output_contract() {
        let mut descriptor = FrontendTool {
            name: "frontend".into(),
            description: "External text result".into(),
            input_parameters: serde_json::json!({"type":"object","required":["location"],"properties":{"location":{"type":"string"}}}),
            result_contract: None,
        };
        let result = result("text-result");
        assert!(validate_frontend_result(&descriptor, &result).is_ok());
        let original = frontend_descriptor_fingerprint(&descriptor).unwrap();
        descriptor.result_contract = Some(
            serde_json::json!({"type":"object","properties":{"content":{"type":"array","minItems":2}}}),
        );
        assert_ne!(
            frontend_descriptor_fingerprint(&descriptor).unwrap(),
            original
        );
        assert_eq!(
            validate_frontend_result(&descriptor, &result).unwrap_err(),
            Error::InvalidRequest
        );
        descriptor.result_contract =
            Some(serde_json::json!({"$ref":"https://invalid.example/remote-schema"}));
        assert_eq!(
            frontend_descriptor_fingerprint(&descriptor).unwrap_err(),
            Error::InvalidRequest
        );
    }
}
#[cfg(test)]
mod declaration_allocation {
    use super::*;
    #[test]
    fn aggregate_declarations_are_rejected_before_schema_or_plan_materialization() {
        let tools = (0..128)
            .map(|i| FrontendTool {
                name: format!("frontend-{i}"),
                description: String::new(),
                input_parameters: serde_json::json!({"type":"object","title":"x".repeat(40_000)}),
                result_contract: None,
            })
            .collect::<Vec<_>>();
        let measured = allocation_counter::measure(|| {
            assert_eq!(
                validate_frontend_declarations(&tools).unwrap_err(),
                Error::InvalidRequest
            );
        });
        assert!(
            measured.bytes_total < 65_536,
            "declarations materialized before aggregate refusal: {measured:?}"
        );
    }
}

/// Immutable read routing, deliberately unable to represent a Run or worker grant.
#[cfg(any(feature = "postgres", feature = "sqlite"))]
pub(crate) struct FrontendReadScope {
    pub session_id: crabber_core::SessionId,
    pub id: crabber_core::RunId,
}
#[cfg(any(feature = "postgres", feature = "sqlite"))]
impl FrontendReadScope {
    pub fn new(
        session: &crabber_core::SessionId,
        run: &crabber_core::RunId,
    ) -> Result<Self, Error> {
        identity(&session.0, 512)?;
        identity(&run.0, 128)?;
        Ok(Self {
            session_id: session.clone(),
            id: run.clone(),
        })
    }
}
