//! Borrow and authenticate exact source facts while the transaction lock is held.
use super::{
    ControlId, Decision, Error, ForkSession, ImportedHistory, OperationReceipt, State, receipt,
};
use crate::fork::history::{AdmittedCall, AdmittedResult, Selection, SourceCall};
use crabber_core::{
    ContentBlock, EventKind, Message, MessageId, Role, ToolCallRecord, ToolCallStatus,
};
use std::collections::BTreeSet;

pub(super) enum Failure {
    Limited,
    Error(Error),
}
impl From<Error> for Failure {
    fn from(error: Error) -> Self {
        Self::Error(error)
    }
}
struct Proof(usize);
impl Proof {
    fn measure(&mut self, value: &impl serde::Serialize) -> Result<(), Failure> {
        let bytes = crate::snapshot::encoded_bytes(value, self.0).ok_or(Failure::Limited)?;
        self.0 -= bytes;
        Ok(())
    }
    fn encoded(&mut self, value: &str) -> Result<(), Failure> {
        self.0 = self.0.checked_sub(value.len()).ok_or(Failure::Limited)?;
        Ok(())
    }
}
fn unknown(_: impl std::fmt::Debug) -> Failure {
    Failure::Error(Error::UnknownStoreOutcome)
}

pub(super) fn prepare(
    state: &State,
    request: &ForkSession,
    messages: &[Message],
) -> Result<Decision, Failure> {
    if let Some(limit) = raw_preflight(request, messages) {
        return Ok(Decision::Limited(limit));
    }
    let mut proof = Proof(request.max_proof_bytes);
    let mut committed = BTreeSet::new();
    let mut admitted = BTreeSet::new();
    if messages.iter().any(|message| message.run_id.is_none()) {
        imported(state, request, &mut proof, &mut committed)?;
    }
    let mut checked_runs = BTreeSet::new();
    let mut calls = Vec::new();
    for message in messages {
        crate::attempt::validate_identifier(&message.id.0).map_err(unknown)?;
        if let Some(run) = &message.run_id {
            crate::attempt::validate_identifier(&run.0).map_err(unknown)?;
            if checked_runs.insert(run) {
                original_input(
                    state,
                    request,
                    run,
                    &mut proof,
                    &mut committed,
                    &mut admitted,
                )?;
            }
            if !committed.contains(&message.id)
                && committed_event(state, request, message, &mut proof)?
            {
                committed.insert(message.id.clone());
            }
        }
        for part in &message.parts {
            if let ContentBlock::ToolCall { call_id, .. } = &part.content {
                crate::attempt::validate_identifier(&call_id.0).map_err(unknown)?;
                if calls.len() >= request.limits.tool_calls {
                    return Ok(Decision::Limited(crate::SnapshotLimit::ToolCalls));
                }
                if admitted.contains(&message.id) {
                    calls.push(admitted_call(message, &part.content, messages, &admitted)?);
                } else if let Some(call) = state.calls.get(call_id) {
                    calls.push(SourceCall::Native(call));
                } else if let Some(call) = state
                    .imported_calls
                    .for_session(&request.source_session_id)
                    .iter()
                    .find(|call| call.id == *call_id)
                {
                    calls.push(SourceCall::Imported(call));
                } else {
                    return Err(Error::Unsupported.into());
                }
            }
        }
    }
    // Bound every borrowed logical carrier before terminal proof validation can
    // materialize even its derived comparison output. No copy is made here.
    if let Selection::Limited(limit) =
        crate::fork::history::select(request, messages, &calls, &committed)?
    {
        return Ok(Decision::Limited(limit));
    }
    for source in &mut calls {
        if let SourceCall::Native(call) = source
            && matches!(
                call.status,
                ToolCallStatus::Completed | ToolCallStatus::Failed
            )
            && terminal_call(state, request, call, &mut proof)? != crate::PhaseQuiescence::Complete
        {
            *source = SourceCall::UnquiescentNative(call);
        }
    }
    match crate::fork::history::select(request, messages, &calls, &committed)? {
        Selection::Ready(plan) => plan.copy().map(Decision::Ready).map_err(Failure::from),
        Selection::Unsafe(boundary) => Ok(Decision::Unsafe(boundary)),
        Selection::Limited(limit) => Ok(Decision::Limited(limit)),
    }
}

fn original_input(
    state: &State,
    request: &ForkSession,
    run_id: &crabber_core::RunId,
    proof: &mut Proof,
    committed: &mut BTreeSet<MessageId>,
    admitted: &mut BTreeSet<MessageId>,
) -> Result<(), Failure> {
    let run = state
        .runs
        .get(run_id)
        .filter(|run| run.id == *run_id && run.session_id == request.source_session_id)
        .ok_or_else(|| unknown("source run"))?;
    proof.measure(&(
        &run.id,
        &run.session_id,
        &run.config_hash,
        &run.plan_fingerprint,
    ))?;
    let Some(encoded) = state.execution_bindings.get(run_id) else {
        return Ok(());
    };
    proof.encoded(encoded)?;
    let binding = crate::RetainedExecutionBinding::decode(encoded).map_err(unknown)?;
    let count = binding
        .capsule
        .input_envelope
        .as_ref()
        .map_or(1, |input| input.messages().len());
    let all = state.messages.for_session(&request.source_session_id);
    let start = all
        .iter()
        .position(|message| message.run_id.as_ref() == Some(run_id))
        .ok_or_else(|| unknown("input source"))?;
    let original = all
        .get(
            start
                ..start
                    .checked_add(count)
                    .ok_or_else(|| unknown("input count"))?,
        )
        .ok_or_else(|| unknown("input count"))?;
    for message in original {
        proof.measure(message)?;
    }
    binding
        .validate_messages(
            run_id,
            &request.source_session_id,
            &run.config_hash,
            &run.plan_fingerprint,
            original,
        )
        .map_err(unknown)?;
    if binding.capsule.request.workspace_id != request.workspace_id
        || binding.capsule.request.directory != request.directory
    {
        return Err(unknown("source context"));
    }
    committed.extend(original.iter().map(|message| message.id.clone()));
    admitted.extend(original.iter().map(|message| message.id.clone()));
    Ok(())
}

fn committed_event(
    state: &State,
    request: &ForkSession,
    message: &Message,
    proof: &mut Proof,
) -> Result<bool, Failure> {
    let kind = match message.role {
        Role::Assistant => EventKind::MessageCommitted,
        Role::Tool => EventKind::ToolCallSettled,
        _ => return Ok(false),
    };
    let event = state.events.iter().rev().find(|event| {
        event.session_id == request.source_session_id
            && Some(&event.run_id) == message.run_id.as_ref()
            && event.kind == kind
            && event.payload.get("message_id").and_then(|v| v.as_str()) == Some(&message.id.0)
    });
    let Some(event) = event else {
        return Ok(false);
    };
    proof.measure(event)?;
    if event.live_only
        || event.cursor.is_none()
        || (kind == EventKind::MessageCommitted
            && event.payload.get("role") != Some(&serde_json::json!(Role::Assistant)))
    {
        return Err(unknown("message closure"));
    }
    Ok(true)
}

fn terminal_call(
    state: &State,
    request: &ForkSession,
    call: &ToolCallRecord,
    proof: &mut Proof,
) -> Result<crate::PhaseQuiescence, Failure> {
    if !matches!(
        call.status,
        ToolCallStatus::Completed | ToolCallStatus::Failed
    ) {
        return Err(unknown("unclosed source"));
    }
    let run = state
        .runs
        .get(&call.run_id)
        .filter(|run| run.session_id == request.source_session_id)
        .ok_or_else(|| unknown("call run"))?;
    let phase = state
        .phases
        .values()
        .filter(|phase| phase.snapshot.run_id == run.id && phase.snapshot.target.call_id == call.id)
        .max_by_key(|phase| phase.snapshot.target.generation)
        .ok_or(Error::Unsupported)?;
    proof.measure(&(&phase.snapshot, &phase.output, &phase.token))?;
    proof.measure(call)?;
    let mut terminal = None;
    let mut sources = Vec::with_capacity(2);
    for only_terminal in [false, true] {
        let event = state
            .events
            .iter()
            .rev()
            .find(|event| {
                event.session_id == run.session_id
                    && event.run_id == run.id
                    && event.kind == EventKind::ToolPhaseChanged
                    && event
                        .payload
                        .pointer("/phase/target/call_id")
                        .and_then(|v| v.as_str())
                        == Some(&call.id.0)
                    && (!only_terminal
                        || matches!(
                            event.payload.get("operation").and_then(|v| v.as_str()),
                            Some("complete" | "resolve_success" | "resolve_failure")
                        ))
            })
            .ok_or_else(|| unknown("phase source"))?;
        proof.measure(event)?;
        let id = event
            .correlation
            .as_deref()
            .ok_or_else(|| unknown("phase receipt"))?;
        crate::attempt::validate_identifier(id).map_err(unknown)?;
        let Some(OperationReceipt::Phase(receipt)) = state
            .operation_receipts
            .get(&(run.session_id.clone(), ControlId(id.into())))
        else {
            return Err(unknown("phase receipt"));
        };
        proof.measure(receipt)?;
        sources.push((receipt, event));
        if only_terminal {
            terminal = event.cursor;
        }
    }
    let settled = state
        .events
        .iter()
        .rev()
        .find(|event| {
            event.session_id == run.session_id && event.run_id == run.id && event.cursor < terminal
        })
        .ok_or_else(|| unknown("settled source"))?;
    proof.measure(settled)?;
    crate::phase::terminal_history::verify_history_scope(
        &run.session_id,
        &run.id,
        phase,
        call,
        sources[0],
        sources[1],
        settled,
    )
    .map_err(unknown)
}

fn imported(
    state: &State,
    request: &ForkSession,
    proof: &mut Proof,
    committed: &mut BTreeSet<MessageId>,
) -> Result<(), Failure> {
    let mut sources = state
        .fork_audits
        .iter()
        .filter(|((target, _), _)| *target == request.source_session_id);
    let (_, audit) = sources.next().ok_or_else(|| unknown("import source"))?;
    if sources.next().is_some() {
        return Err(unknown("duplicate import origin"));
    }
    proof.measure(audit)?;
    proof.measure(&audit.receipt)?;
    audit.validate()?;
    if receipt(
        state,
        &request.source_session_id,
        &audit.receipt.operation_id,
    )? != Some(audit.receipt.clone())
    {
        return Err(unknown("import receipt"));
    }
    let messages = state
        .messages
        .for_session(&request.source_session_id)
        .get(..audit.receipt.messages)
        .ok_or_else(|| unknown("imported message count"))?;
    let calls = state.imported_calls.for_session(&request.source_session_id);
    let provenance = state
        .imported_provenance
        .for_session(&request.source_session_id);
    if calls.len() != audit.receipt.imported_calls
        || provenance.len() != messages.len()
        || messages.iter().any(|message| {
            message.run_id.is_some() || message.session_id != request.source_session_id
        })
    {
        return Err(unknown("imported scope"));
    }
    for message in messages {
        proof.measure(message)?;
    }
    for call in calls {
        proof.measure(call)?;
    }
    for origin in provenance {
        proof.measure(origin)?;
    }
    let history = ImportedHistory {
        messages: messages.to_vec(),
        calls: calls.to_vec(),
        provenance: provenance.to_vec(),
    };
    if history.digest()? != audit.receipt.history_digest {
        return Err(unknown("imported history digest"));
    }
    committed.extend(messages.iter().map(|message| message.id.clone()));
    Ok(())
}

// Borrowed raw carriers are bounded before scanning parts, decoding retained
// input or allocating structural identity sets. Later selection also includes
// call carriers in the same history allowance.
fn raw_preflight(request: &ForkSession, messages: &[Message]) -> Option<crate::SnapshotLimit> {
    let mut usage = crate::SnapshotUsage {
        messages: messages.len(),
        ..Default::default()
    };
    for message in messages {
        usage.parts = usage.parts.saturating_add(message.parts.len());
        if let Some(limit) = usage.exceeded(request.limits) {
            return Some(limit);
        }
        let Some(bytes) = crate::snapshot::encoded_bytes(
            message,
            request
                .limits
                .encoded_bytes
                .saturating_sub(usage.encoded_bytes),
        ) else {
            return Some(crate::SnapshotLimit::EncodedBytes);
        };
        usage.encoded_bytes += bytes;
        usage.text_bytes = message.parts.iter().fold(usage.text_bytes, |n, part| {
            n.saturating_add(crate::snapshot::text_bytes(&part.content))
        });
        if let Some(limit) = usage.exceeded(request.limits) {
            return Some(limit);
        }
    }
    None
}

/// Only exact, independently authenticated original input messages can supply
/// these facts. Absence of a native call alone never creates admitted provenance.
fn admitted_call<'a>(
    message: &'a Message,
    block: &'a ContentBlock,
    messages: &'a [Message],
    admitted: &BTreeSet<MessageId>,
) -> Result<SourceCall<'a>, Failure> {
    let ContentBlock::ToolCall {
        call_id,
        name,
        arguments,
    } = block
    else {
        return Err(unknown("admitted source kind"));
    };
    let run_id = message
        .run_id
        .as_ref()
        .ok_or_else(|| unknown("admitted run"))?;
    let mut results = messages
        .iter()
        .filter(|candidate| {
            candidate.role == Role::Tool
                && candidate.run_id.as_ref() == Some(run_id)
                && admitted.contains(&candidate.id)
        })
        .flat_map(|message| &message.parts)
        .filter_map(|part| match &part.content {
            ContentBlock::ToolResult {
                call_id: id,
                content,
                is_error,
            } if id == call_id => Some(AdmittedResult {
                content,
                is_error: *is_error,
            }),
            _ => None,
        });
    let result = results.next();
    if results.next().is_some() {
        return Err(unknown("duplicate admitted result"));
    }
    Ok(SourceCall::AdmittedInput(AdmittedCall {
        id: call_id,
        name,
        arguments,
        run_id,
        result,
    }))
}
