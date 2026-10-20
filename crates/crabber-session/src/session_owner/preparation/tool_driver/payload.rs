//! Canonical public grant data. Capabilities are deliberately absent.
//! Membership and order of phases, frontend results, calls and responses matter.
use crate::public_session::v2::proof::Proof;
use crate::session_owner::SessionOwnerError as Error;
use crabber_core::{InputFingerprint, InteractionState};
use serde::Serialize;
use sha2::{Digest, Sha256};
pub(crate) const CAP: usize = (4 << 20) + (32 << 10);
#[derive(Serialize)]
struct Payload<'a> {
    phases: Vec<Phase<'a>>,
    frontend: Vec<Frontend<'a>>,
    calls: Vec<Invocation<'a>>,
    source_turn_id: &'a Option<crabber_core::TurnId>,
    usage: &'a crabber_core::Usage,
    turns_used: u64,
}
#[derive(Serialize)]
struct Phase<'a> {
    target: &'a crate::PhaseTarget,
    identity: &'a crabber_core::ResumableToolIdentity,
    checkpoint: &'a crabber_core::ToolCheckpoint,
    responses: Vec<Response<'a>>,
}
#[derive(Serialize)]
struct Response<'a> {
    name: &'a str,
    state: InteractionState,
    value: Option<&'a serde_json::Value>,
}
#[derive(Serialize)]
struct Frontend<'a> {
    target: &'a crate::PhaseTarget,
    result: &'a crabber_core::FrontendToolResult,
    frozen: &'a crate::FrozenFrontendCall,
}
#[derive(Serialize)]
struct Invocation<'a> {
    target: &'a crate::PhaseTarget,
    call_binding: &'a InputFingerprint,
    decision: Decision,
}
#[derive(Serialize)]
enum Decision {
    Allow,
    Deny,
    NotRequired,
}
fn charge<T>(count: usize, proof: &mut Proof) -> Result<(), Error> {
    if count > 64 {
        return Err(Error::UnknownStoreOutcome);
    }
    proof
        .charge(
            count
                .checked_mul(std::mem::size_of::<T>())
                .ok_or(Error::UnknownStoreOutcome)?,
        )
        .map_err(|_| Error::UnknownStoreOutcome)
}
fn digest(payload: &impl Serialize, proof: &mut Proof) -> Result<InputFingerprint, Error> {
    let view = ("crabber.tool-continuation.grant-payload.v1", payload);
    let bytes = crate::snapshot::encoded_bytes(&view, CAP).ok_or(Error::UnknownStoreOutcome)?;
    proof
        .charge(bytes)
        .map_err(|_| Error::UnknownStoreOutcome)?;
    let mut canonical = serde_json::to_value(view).map_err(|_| Error::UnknownStoreOutcome)?;
    canonical.sort_all_objects();
    InputFingerprint::new(format!(
        "{:x}",
        Sha256::digest(canonical.to_string().as_bytes())
    ))
    .map_err(|_| Error::UnknownStoreOutcome)
}
#[path = "payload/actual.rs"]
mod actual;
pub(crate) fn view(grant: &crate::ToolContinuationGrant) -> Result<impl Serialize + '_, Error> {
    if grant.phases.len() > 64
        || grant.frontend.len() > 64
        || grant.calls.len() > 64
        || grant.phases.iter().any(|phase| phase.responses.len() > 64)
    {
        return Err(Error::UnknownStoreOutcome);
    }
    for phase in &grant.phases {
        for response in &phase.responses {
            if let Some(value) = &response.value {
                crabber_core::AcceptedToolResponse::validate_value(value)
                    .map_err(|_| Error::InvalidRequest)?;
            }
        }
    }
    for phase in &grant.frontend {
        json_depth(&phase.frozen.arguments, 0)?;
        json_depth(&phase.frozen.descriptor.input_parameters, 0)?;
        if let Some(schema) = &phase.frozen.descriptor.result_contract {
            json_depth(schema, 0)?;
        }
    }
    Ok(actual::View(grant))
}
// Stored native JSON is already bounded by its reader nesting limit. Check
// mutable ACK JSON before invoking a recursive serializer or canonical clone.
fn json_depth(value: &serde_json::Value, depth: usize) -> Result<(), Error> {
    if depth > 128 {
        return Err(Error::InvalidRequest);
    }
    match value {
        serde_json::Value::Array(values) => {
            for value in values {
                json_depth(value, depth + 1)?;
            }
        }
        serde_json::Value::Object(values) => {
            for value in values.values() {
                json_depth(value, depth + 1)?;
            }
        }
        _ => (),
    }
    Ok(())
}
pub(crate) fn actual(
    grant: &crate::ToolContinuationGrant,
    proof: &mut Proof,
) -> Result<InputFingerprint, Error> {
    digest(&view(grant)?, proof)
}
pub(crate) fn expected(
    batch: &crate::ToolContinuationSuspension,
    answers: &[crate::continuation_input::Input<'_>],
    claim: Option<&crate::FrontendContinuationClaim>,
    proof: &mut Proof,
) -> Result<InputFingerprint, Error> {
    charge::<Phase<'_>>(batch.phases.len(), proof)?;
    charge::<Frontend<'_>>(batch.frontend.len(), proof)?;
    charge::<Invocation<'_>>(batch.calls.len(), proof)?;
    let phases = batch
        .phases
        .iter()
        .map(|phase| {
            charge::<Response<'_>>(phase.interactions.len(), proof)?;
            let responses = phase
                .interactions
                .iter()
                .map(|required| {
                    let answer = answers
                        .iter()
                        .find_map(|input| match input {
                            crate::continuation_input::Input::ToolAnswer(answer)
                                if answer.interaction_id == required.id =>
                            {
                                Some(*answer)
                            }
                            _ => None,
                        })
                        .ok_or(Error::UnknownStoreOutcome)?;
                    Ok(Response {
                        name: required.description.name(),
                        state: crate::tool_continuation::reply_state(&answer.reply),
                        value: match &answer.reply {
                            crate::ToolInteractionReply::Answer { value } => Some(value),
                            _ => None,
                        },
                    })
                })
                .collect::<Result<_, Error>>()?;
            Ok(Phase {
                target: &phase.next,
                identity: &phase.identity,
                checkpoint: &phase.checkpoint,
                responses,
            })
        })
        .collect::<Result<_, Error>>()?;
    let frontend = batch
        .frontend
        .iter()
        .map(|frozen| {
            let result = claim
                .ok_or(Error::UnknownStoreOutcome)?
                .results
                .iter()
                .find(|result| result.call_id() == &frozen.target.call_id)
                .ok_or(Error::UnknownStoreOutcome)?;
            Ok(Frontend {
                target: &frozen.target,
                result,
                frozen,
            })
        })
        .collect::<Result<_, Error>>()?;
    let calls = batch
        .calls
        .iter()
        .map(|call| {
            Ok(Invocation {
                target: &call.target,
                call_binding: &call.call_binding,
                decision: match call.decision {
                    crate::CallApproval::Allow => Decision::Allow,
                    crate::CallApproval::Deny => Decision::Deny,
                    crate::CallApproval::NotRequired => Decision::NotRequired,
                    crate::CallApproval::Ask { .. } => return Err(Error::UnknownStoreOutcome),
                },
            })
        })
        .collect::<Result<_, Error>>()?;
    digest(
        &Payload {
            phases,
            frontend,
            calls,
            source_turn_id: &batch.source_turn_id,
            usage: &batch.usage,
            turns_used: batch.turns_used,
        },
        proof,
    )
}

#[cfg(test)]
#[path = "payload/tests.rs"]
mod tests;
