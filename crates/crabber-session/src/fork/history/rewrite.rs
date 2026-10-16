//! Rewrite only typed identities. User strings and JSON arguments stay intact.
use super::{
    BTreeMap, BTreeSet, ContentBlock, Error, ForkSession, ImportedHistory,
    ImportedMessageProvenance, ImportedToolCall, Message, MessageId, Pair, PartId, ToolCallId,
};

pub(super) fn copy(
    request: &ForkSession,
    messages: &[Message],
    pairs: &BTreeMap<&ToolCallId, Pair<'_>>,
    structured: &BTreeSet<&ToolCallId>,
) -> Result<ImportedHistory, Error> {
    let message_ids: BTreeMap<_, _> = messages
        .iter()
        .map(|message| (&message.id, MessageId::new()))
        .collect();
    let call_ids: BTreeMap<_, _> = structured
        .iter()
        .map(|id| (*id, ToolCallId::new()))
        .collect();
    let mut result = ImportedHistory::default();
    for original in messages {
        let mut message = original.clone();
        crate::public_history::redact_message(&mut message);
        message.id = message_ids
            .get(&original.id)
            .ok_or(Error::UnknownStoreOutcome)?
            .clone();
        message.session_id.clone_from(&request.target_session_id);
        message.run_id = None;
        message.parent_id = original
            .parent_id
            .as_ref()
            .map(|id| {
                message_ids
                    .get(id)
                    .cloned()
                    .ok_or(Error::UnknownStoreOutcome)
            })
            .transpose()?;
        let mut provenance = ImportedMessageProvenance {
            session_id: request.target_session_id.clone(),
            message_id: message.id.clone(),
            source_session_id: request.source_session_id.clone(),
            source_revision: request.expected_source_revision,
            source_message_id: original.id.clone(),
            source_run_id: original.run_id.clone(),
            parts: vec![],
        };
        for (ordinal, part) in message.parts.iter_mut().enumerate() {
            let source = part.id.clone();
            part.id = PartId::new();
            part.message_id.clone_from(&message.id);
            part.ordinal = u32::try_from(ordinal).map_err(|_| Error::UnknownStoreOutcome)?;
            provenance.parts.push((part.id.clone(), source));
            rewrite(&mut part.content, &call_ids)?;
        }
        result.provenance.push(provenance);
        result.messages.push(message);
    }
    for (id, pair) in pairs {
        let (_, name, arguments, output, run) = pair.source.fields();
        let mut output = output.ok_or(Error::UnknownStoreOutcome)?.clone();
        crate::public_history::redact_result(&mut output);
        for block in &mut output.content {
            rewrite(block, &call_ids)?;
        }
        result.calls.push(ImportedToolCall {
            session_id: request.target_session_id.clone(),
            id: call_ids.get(id).ok_or(Error::UnknownStoreOutcome)?.clone(),
            assistant_message_id: message_ids
                .get(pair.assistant)
                .ok_or(Error::UnknownStoreOutcome)?
                .clone(),
            result_message_id: message_ids
                .get(pair.result.ok_or(Error::UnknownStoreOutcome)?)
                .ok_or(Error::UnknownStoreOutcome)?
                .clone(),
            name: name.into(),
            arguments: arguments.clone(),
            result: output,
            source_session_id: request.source_session_id.clone(),
            source_revision: request.expected_source_revision,
            source_call_id: (*id).clone(),
            source_run_id: run.cloned(),
        });
    }
    Ok(result)
}
fn rewrite(block: &mut ContentBlock, ids: &BTreeMap<&ToolCallId, ToolCallId>) -> Result<(), Error> {
    let mut pending = vec![block];
    while let Some(block) = pending.pop() {
        match block {
            ContentBlock::ToolCall { call_id, .. } => {
                *call_id = ids.get(call_id).ok_or(Error::UnknownStoreOutcome)?.clone();
            }
            ContentBlock::ToolResult {
                call_id, content, ..
            } => {
                *call_id = ids.get(call_id).ok_or(Error::UnknownStoreOutcome)?.clone();
                pending.extend(content.iter_mut());
            }
            _ => (),
        }
    }
    Ok(())
}
