//! Structural validation follows adapter authentication of committed messages
//! and native terminal results. A valid structure alone grants no copy authority.
use super::{ForkBoundary, ForkError as Error, ForkSession};
use crate::{SnapshotLimit, SnapshotUsage};
use crabber_core::{
    ContentBlock, ImportedMessageProvenance, ImportedToolCall, Message, MessageId, PartId,
    PartKind, Role, RunId, ToolCallId, ToolCallRecord, ToolCallStatus, ToolResult,
    ToolResultStatus,
};
use std::collections::{BTreeMap, BTreeSet};

#[path = "history/rewrite.rs"]
mod rewrite;

#[path = "history/source.rs"]
mod source;
pub(crate) use source::{AdmittedCall, AdmittedResult, SourceCall};

#[derive(Default, serde::Serialize)]
pub(crate) struct ImportedHistory {
    pub messages: Vec<Message>,
    pub calls: Vec<ImportedToolCall>,
    pub provenance: Vec<ImportedMessageProvenance>,
}
impl ImportedHistory {
    pub(crate) fn digest(&self) -> Result<crabber_core::InputFingerprint, Error> {
        // Canonicalize only after the entire bounded prefix passed every read
        // allowance. SQL JSON object ordering must not affect source identity.
        let mut value = serde_json::to_value(self).map_err(|_| Error::UnknownStoreOutcome)?;
        value.sort_all_objects();
        crate::admission_config_hash(&serde_json::json!(["crabber.imported-history.v1", value]))
            .try_into()
            .map_err(|_| Error::UnknownStoreOutcome)
    }
}
pub(crate) enum Decision {
    Ready(ImportedHistory),
    Unsafe(ForkBoundary),
    Limited(SnapshotLimit),
}
pub(crate) enum Selection<'a> {
    Ready(CopyPlan<'a>),
    Unsafe(ForkBoundary),
    Limited(SnapshotLimit),
}
/// Borrowed structural facts. Copying requires the adapter to authenticate and
/// charge the complete terminal source proof first.
pub(crate) struct CopyPlan<'a> {
    request: &'a ForkSession,
    messages: &'a [Message],
    pairs: BTreeMap<&'a ToolCallId, Pair<'a>>,
    structured: BTreeSet<&'a ToolCallId>,
}
impl CopyPlan<'_> {
    pub(crate) fn copy(self) -> Result<ImportedHistory, Error> {
        rewrite::copy(self.request, self.messages, &self.pairs, &self.structured)
    }
}
struct Pair<'a> {
    assistant: &'a MessageId,
    result: Option<&'a MessageId>,
    source: &'a SourceCall<'a>,
}

/// Adapters supply exactly the selected prefix in durable insertion order,
/// with independently authenticated closure evidence at the captured revision.
/// All messages and call carriers must have passed pre-transfer read budgets.
pub(crate) fn select<'a>(
    request: &'a ForkSession,
    messages: &'a [Message],
    calls: &'a [SourceCall<'a>],
    committed: &BTreeSet<MessageId>,
) -> Result<Selection<'a>, Error> {
    validate_boundary(request, messages, calls)?;
    if matches!(request.boundary, ForkBoundary::Empty) {
        return Ok(Selection::Ready(CopyPlan {
            request,
            messages,
            pairs: BTreeMap::new(),
            structured: BTreeSet::new(),
        }));
    }
    let mut usage = SnapshotUsage::default();
    if messages.len() > request.limits.messages {
        return Ok(Selection::Limited(SnapshotLimit::Messages));
    }
    if calls.len() > request.limits.tool_calls {
        return Ok(Selection::Limited(SnapshotLimit::ToolCalls));
    }
    let mut source_calls = BTreeMap::new();
    if let Some(limit) = collect_calls(request, calls, &mut usage, &mut source_calls)? {
        return Ok(Selection::Limited(limit));
    }
    let mut ids = BTreeSet::new();
    let mut parts = BTreeSet::new();
    let mut pairs = BTreeMap::new();
    let mut structured = BTreeSet::new();
    let mut open = BTreeSet::new();
    let mut earlier = ForkBoundary::Empty;
    let mut unsafe_prefix = false;
    for message in messages {
        usage.messages += 1;
        usage.parts = usage.parts.saturating_add(message.parts.len());
        if let Some(limit) = usage.exceeded(request.limits) {
            return Ok(Selection::Limited(limit));
        }
        let Some(bytes) = crate::snapshot::encoded_bytes(
            message,
            request
                .limits
                .encoded_bytes
                .saturating_sub(usage.encoded_bytes),
        ) else {
            return Ok(Selection::Limited(SnapshotLimit::EncodedBytes));
        };
        usage.encoded_bytes += bytes;
        message_identity(message, &request.source_session_id, &mut ids)?;
        unsafe_prefix |= !committed.contains(&message.id);
        let mut ordinal = None;
        for part in &message.parts {
            validate_id(&part.id.0).map_err(|_| Error::UnknownStoreOutcome)?;
            if (matches!(part.content, ContentBlock::ToolCall { .. })
                && part.kind != PartKind::FunctionToolCall)
                || (matches!(part.content, ContentBlock::ToolResult { .. })
                    && part.kind != PartKind::FunctionToolResult)
                || part.message_id != message.id
                || !parts.insert(&part.id)
                || ordinal.is_some_and(|previous| previous >= part.ordinal)
            {
                return Err(Error::UnknownStoreOutcome);
            }
            ordinal = Some(part.ordinal);
            unsafe_prefix |= matches!(part.kind, PartKind::CompactionSummary);
            inspect_blocks(
                &part.content,
                message,
                &source_calls,
                &mut pairs,
                &mut structured,
                &mut open,
            )?;
        }
        usage.tool_calls = structured.len().max(calls.len());
        if let Some(limit) = usage.exceeded(request.limits) {
            return Ok(Selection::Limited(limit));
        }
        usage.text_bytes = message.parts.iter().fold(usage.text_bytes, |total, part| {
            total.saturating_add(crate::snapshot::text_bytes(&part.content))
        });
        if let Some(limit) = usage.exceeded(request.limits) {
            return Ok(Selection::Limited(limit));
        }
        if !unsafe_prefix && open.is_empty() && pairs.values().all(|pair| pair.source.closed()) {
            earlier = ForkBoundary::AfterMessage {
                message_id: message.id.clone(),
            };
        }
    }
    if earlier != request.boundary {
        return Ok(Selection::Unsafe(earlier));
    }
    if pairs.len() != source_calls.len() {
        return Err(Error::UnknownStoreOutcome);
    }
    Ok(Selection::Ready(CopyPlan {
        request,
        messages,
        pairs,
        structured,
    }))
}

#[cfg(test)]
fn prepare<'a>(
    request: &'a ForkSession,
    messages: &'a [Message],
    calls: &'a [SourceCall<'a>],
    committed: &BTreeSet<MessageId>,
) -> Result<Decision, Error> {
    match select(request, messages, calls, committed)? {
        Selection::Ready(plan) => plan.copy().map(Decision::Ready),
        Selection::Unsafe(boundary) => Ok(Decision::Unsafe(boundary)),
        Selection::Limited(limit) => Ok(Decision::Limited(limit)),
    }
}

#[cfg(test)]
#[path = "history/tests.rs"]
mod tests;

fn validate_boundary(
    request: &ForkSession,
    messages: &[Message],
    calls: &[SourceCall<'_>],
) -> Result<(), Error> {
    request.fingerprint()?;
    match &request.boundary {
        ForkBoundary::Empty if !messages.is_empty() || !calls.is_empty() => {
            Err(Error::UnknownStoreOutcome)
        }
        ForkBoundary::AfterMessage { message_id }
            if messages.last().is_none_or(|m| m.id != *message_id) =>
        {
            Err(Error::NotFound)
        }
        _ => Ok(()),
    }
}

fn message_identity<'a>(
    message: &'a Message,
    session: &crabber_core::SessionId,
    seen: &mut BTreeSet<&'a MessageId>,
) -> Result<(), Error> {
    validate_id(&message.id.0).map_err(|_| Error::UnknownStoreOutcome)?;
    if let Some(run) = &message.run_id {
        crate::attempt::validate_identifier(&run.0).map_err(|_| Error::UnknownStoreOutcome)?;
    }
    if message.session_id != *session
        || !seen.insert(&message.id)
        || message
            .parent_id
            .as_ref()
            .is_some_and(|parent| parent == &message.id || !seen.contains(parent))
    {
        return Err(Error::UnknownStoreOutcome);
    }
    Ok(())
}

fn collect_calls<'a>(
    request: &ForkSession,
    calls: &'a [SourceCall<'a>],
    usage: &mut SnapshotUsage,
    source_calls: &mut BTreeMap<&'a ToolCallId, &'a SourceCall<'a>>,
) -> Result<Option<SnapshotLimit>, Error> {
    for call in calls {
        let Some(bytes) = call.encoded_bytes(
            request
                .limits
                .encoded_bytes
                .saturating_sub(usage.encoded_bytes),
        ) else {
            return Ok(Some(SnapshotLimit::EncodedBytes));
        };
        usage.encoded_bytes += bytes;
        call.validate_shape()?;
        if let Some(result) = call.fields().3 {
            usage.text_bytes = result
                .content
                .iter()
                .fold(usage.text_bytes, |total, block| {
                    total.saturating_add(crate::snapshot::text_bytes(block))
                });
        }
        if let Some(limit) = usage.exceeded(request.limits) {
            return Ok(Some(limit));
        }
        let id = call.fields().0;
        if source_calls.insert(id, call).is_some() {
            return Err(Error::UnknownStoreOutcome);
        }
    }
    Ok(None)
}

fn inspect_blocks<'a>(
    block: &'a ContentBlock,
    message: &'a Message,
    sources: &BTreeMap<&'a ToolCallId, &'a SourceCall<'a>>,
    pairs: &mut BTreeMap<&'a ToolCallId, Pair<'a>>,
    structured: &mut BTreeSet<&'a ToolCallId>,
    open: &mut BTreeSet<&'a ToolCallId>,
) -> Result<(), Error> {
    let mut pending = vec![(block, 0)];
    while let Some((block, depth)) = pending.pop() {
        if depth > 64 {
            return Err(Error::UnknownStoreOutcome);
        }
        match block {
            ContentBlock::ToolCall {
                call_id,
                name,
                arguments,
            } => {
                validate_id(&call_id.0).map_err(|_| Error::UnknownStoreOutcome)?;
                if !structured.insert(call_id) || !open.insert(call_id) {
                    return Err(Error::UnknownStoreOutcome);
                }
                if depth == 0 {
                    if message.role != Role::Assistant {
                        return Err(Error::UnknownStoreOutcome);
                    }
                    let source = sources.get(call_id).ok_or(Error::UnknownStoreOutcome)?;
                    let (_, expected_name, expected_arguments, _, run) = source.fields();
                    if expected_name != name
                        || expected_arguments != arguments
                        || run != message.run_id.as_ref()
                    {
                        return Err(Error::UnknownStoreOutcome);
                    }
                    pairs.insert(
                        call_id,
                        Pair {
                            assistant: &message.id,
                            result: None,
                            source,
                        },
                    );
                }
            }
            ContentBlock::ToolResult {
                call_id,
                content,
                is_error,
            } => {
                if !open.remove(call_id) {
                    return Err(Error::UnknownStoreOutcome);
                }
                if depth == 0 {
                    if message.role != Role::Tool {
                        return Err(Error::UnknownStoreOutcome);
                    }
                    let pair = pairs.get_mut(call_id).ok_or(Error::UnknownStoreOutcome)?;
                    let (_, _, _, result, run) = pair.source.fields();
                    let result = result.ok_or(Error::UnknownStoreOutcome)?;
                    if pair.result.replace(&message.id).is_some()
                        || result.content != content
                        || *is_error != (result.status != ToolResultStatus::Completed)
                        || run != message.run_id.as_ref()
                    {
                        return Err(Error::UnknownStoreOutcome);
                    }
                }
                pending.extend(content.iter().rev().map(|child| (child, depth + 1)));
            }
            _ => (),
        }
    }
    Ok(())
}

/// Presentation identities follow the admitted input contract, not the native
/// authority-key grammar. Provider IDs may include Unicode and punctuation.
pub(crate) fn validate_id(id: &str) -> Result<(), Error> {
    if id.is_empty() || id.len() > 512 || id.chars().any(char::is_control) {
        return Err(Error::UnknownStoreOutcome);
    }
    Ok(())
}
