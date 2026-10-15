use crate::{
    PhaseCommand, PhaseError, PhaseMutation, PhaseOperation, PhaseQuiescence,
    PhaseQuiescenceResult, PhaseReceipt, PhaseResolverClaim, PhaseSnapshot, PhaseState,
    UncertainPhaseChoice,
};
use crabber_core::InputFingerprint;
use serde::{Deserialize, Serialize};
#[path = "transition.rs"]
mod transition;
use crabber_core::{EventKind, EventRecord, Run};
use time::OffsetDateTime;
pub(crate) use transition::{PhaseRecord, call_binding};

/// Public audit information excludes output bodies and native authority tokens.
/// The semantic digest binds the original command, including supplied output.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PhaseEvent {
    version: u32,
    operation: PhaseOperation,
    semantic_digest: InputFingerprint,
    phase: PhaseSnapshot,
    resolver: Option<PhaseResolverClaim>,
}
pub(crate) fn operation(command: &PhaseCommand) -> PhaseOperation {
    match command {
        PhaseCommand::Claim { .. } => PhaseOperation::Claim,
        PhaseCommand::Begin { .. } => PhaseOperation::Begin,
        PhaseCommand::Complete { .. } => PhaseOperation::Complete,
        PhaseCommand::MarkExpiredUncertain { .. } => PhaseOperation::MarkExpiredUncertain,
        PhaseCommand::MarkStoppedUncertain { .. } => PhaseOperation::MarkStoppedUncertain,
        PhaseCommand::ResolveUncertain { choice, .. } => match choice {
            UncertainPhaseChoice::Success { .. } => PhaseOperation::ResolveSuccess,
            UncertainPhaseChoice::Failure { .. } => PhaseOperation::ResolveFailure,
            UncertainPhaseChoice::Refuse => PhaseOperation::ResolveRefuse,
        },
        PhaseCommand::RecordQuiescence { result, .. } => match result {
            PhaseQuiescenceResult::Complete => PhaseOperation::QuiescenceComplete,
            PhaseQuiescenceResult::Incomplete => PhaseOperation::QuiescenceIncomplete,
        },
    }
}
pub(crate) fn event(
    run: &Run,
    request: &PhaseMutation,
    snapshot: &PhaseSnapshot,
    now: OffsetDateTime,
    digest: InputFingerprint,
) -> Result<EventRecord, PhaseError> {
    Ok(EventRecord {
        cursor: None,
        session_id: run.session_id.clone(),
        run_id: run.id.clone(),
        turn_id: None,
        kind: EventKind::ToolPhaseChanged,
        payload: serde_json::to_value(PhaseEvent {
            version: 1,
            operation: operation(&request.command),
            semantic_digest: digest,
            phase: snapshot.clone(),
            resolver: resolver(&request.command),
        })
        .map_err(|_| PhaseError::InvalidRequest)?,
        correlation: Some(request.id.0.clone()),
        live_only: false,
        created_at: now,
    })
}
pub(crate) fn verify_receipt(
    receipt: &PhaseReceipt,
    event: &EventRecord,
) -> Result<(), PhaseError> {
    validate_snapshot(&receipt.phase).map_err(|_| PhaseError::UnknownStoreOutcome)?;
    if receipt.revision == crabber_core::SessionRevision::INITIAL || receipt.event_cursor.0 == 0 {
        return Err(PhaseError::UnknownStoreOutcome);
    }
    if event.kind != EventKind::ToolPhaseChanged
        || event.live_only
        || event.session_id != receipt.phase.session_id
        || event.run_id != receipt.phase.run_id
        || event.correlation.as_deref() != Some(&receipt.operation_id.0)
        || event.cursor.is_some_and(|v| v != receipt.event_cursor)
    {
        return Err(PhaseError::UnknownStoreOutcome);
    }
    let saved: PhaseEvent = serde_json::from_value(event.payload.clone())
        .map_err(|_| PhaseError::UnknownStoreOutcome)?;
    let resolver_required = matches!(
        saved.operation,
        PhaseOperation::MarkStoppedUncertain
            | PhaseOperation::ResolveSuccess
            | PhaseOperation::ResolveFailure
            | PhaseOperation::ResolveRefuse
    );
    let resolver_optional = matches!(
        saved.operation,
        PhaseOperation::QuiescenceComplete | PhaseOperation::QuiescenceIncomplete
    );
    if resolver_required && saved.resolver.is_none()
        || !resolver_required && !resolver_optional && saved.resolver.is_some()
    {
        return Err(PhaseError::UnknownStoreOutcome);
    }
    if let Some(claim) = &saved.resolver {
        validate_resolver(claim).map_err(|_| PhaseError::UnknownStoreOutcome)?;
    }
    let valid = match saved.operation {
        PhaseOperation::Claim => saved.phase.state == PhaseState::Claimed,
        PhaseOperation::Begin => saved.phase.state == PhaseState::Started,
        PhaseOperation::Complete => matches!(
            saved.phase.state,
            PhaseState::Completed | PhaseState::Failed
        ),
        PhaseOperation::MarkExpiredUncertain
        | PhaseOperation::MarkStoppedUncertain
        | PhaseOperation::ResolveRefuse => saved.phase.state == PhaseState::Uncertain,
        PhaseOperation::ResolveSuccess => saved.phase.state == PhaseState::Completed,
        PhaseOperation::ResolveFailure => saved.phase.state == PhaseState::Failed,
        PhaseOperation::QuiescenceComplete => saved.phase.quiescence == PhaseQuiescence::Complete,
        PhaseOperation::QuiescenceIncomplete => {
            saved.phase.quiescence == PhaseQuiescence::Incomplete
        }
    };
    if saved.version != 1
        || saved.phase != receipt.phase
        || saved.semantic_digest != receipt.semantic_digest
        || saved.operation != receipt.operation
        || saved.resolver != receipt.resolver
        || !valid
    {
        return Err(PhaseError::UnknownStoreOutcome);
    }
    Ok(())
}

pub(crate) fn validate_request(request: &PhaseMutation) -> Result<(), PhaseError> {
    let target = request.command.target();
    for id in [&request.id.0, &target.id.0, &target.attempt.id.0] {
        crate::attempt::validate_identifier(id).map_err(|_| PhaseError::InvalidRequest)?;
    }
    validate_call_id(&target.call_id.0).map_err(|_| PhaseError::InvalidRequest)?;
    let claim = match &request.command {
        PhaseCommand::MarkStoppedUncertain { claim, .. }
        | PhaseCommand::ResolveUncertain { claim, .. } => Some(claim),
        PhaseCommand::RecordQuiescence { host_claim, .. } => host_claim.as_ref(),
        _ => None,
    };
    if let Some(claim) = claim {
        validate_resolver(claim)?;
    }
    if let PhaseCommand::ResolveUncertain {
        choice: UncertainPhaseChoice::Success { output },
        ..
    } = &request.command
        && output.is_error()
    {
        return Err(PhaseError::InvalidRequest);
    }
    Ok(())
}

pub(crate) fn resolver(command: &PhaseCommand) -> Option<PhaseResolverClaim> {
    match command {
        PhaseCommand::MarkStoppedUncertain { claim, .. }
        | PhaseCommand::ResolveUncertain { claim, .. } => Some(claim.clone()),
        PhaseCommand::RecordQuiescence { host_claim, .. } => host_claim.clone(),
        _ => None,
    }
}

pub(crate) fn validate_snapshot(snapshot: &PhaseSnapshot) -> Result<(), PhaseError> {
    for id in [&snapshot.target.id.0, &snapshot.target.attempt.id.0] {
        crate::attempt::validate_identifier(id).map_err(|_| PhaseError::UnknownStoreOutcome)?;
    }
    validate_call_id(&snapshot.target.call_id.0)?;
    if snapshot.session_id.0.is_empty()
        || snapshot.session_id.0.len() > 512
        || snapshot.session_id.0.chars().any(char::is_control)
        || snapshot.run_id.0.is_empty()
        || snapshot.run_id.0.len() > 128
        || snapshot.run_id.0.chars().any(char::is_control)
        || matches!(snapshot.state, PhaseState::Claimed | PhaseState::Started)
            && snapshot.quiescence != PhaseQuiescence::Pending
    {
        return Err(PhaseError::UnknownStoreOutcome);
    }
    Ok(())
}

fn validate_resolver(claim: &PhaseResolverClaim) -> Result<(), PhaseError> {
    for (value, limit) in [(&claim.resolver_id, 128), (&claim.authority_claim, 2048)] {
        if value.trim().is_empty() || value.len() > limit || value.chars().any(char::is_control) {
            return Err(PhaseError::InvalidRequest);
        }
    }
    Ok(())
}

fn validate_call_id(id: &str) -> Result<(), PhaseError> {
    // Provider call identities are bounded opaque data, not native capability IDs.
    if id.is_empty() || id.len() > 512 || id.chars().any(char::is_control) {
        return Err(PhaseError::UnknownStoreOutcome);
    }
    Ok(())
}
