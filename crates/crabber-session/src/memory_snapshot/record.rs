//! A borrowed record is measured before it can be cloned into a page.
use super::{Boundary, SnapshotLimit, SnapshotPage, SnapshotUsage, State, StoreError};
use crate::SnapshotLimits;
use crabber_core::{
    ImportedMessageProvenance, ImportedToolCall, Message, SessionId, ToolCallRecord,
};

pub(super) enum Record<'a> {
    Message(&'a Message),
    Native(&'a ToolCallRecord),
    Imported(&'a ImportedToolCall),
    Provenance(&'a ImportedMessageProvenance),
}
pub(super) fn next<'a>(
    state: &'a State,
    session: &SessionId,
    boundary: &Boundary,
) -> Result<Option<Record<'a>>, StoreError> {
    let invalid = || StoreError::Validation("invalid captured history record".into());
    let record = if boundary.message_offset < boundary.messages {
        Record::Message(
            state
                .messages
                .for_session(session)
                .get(boundary.message_offset)
                .ok_or_else(invalid)?,
        )
    } else if boundary.call_offset < boundary.calls {
        let id = state
            .call_order
            .for_session(session)
            .get(boundary.call_offset)
            .ok_or_else(invalid)?;
        Record::Native(state.calls.get(id).ok_or_else(invalid)?)
    } else if boundary.imported_offset < boundary.imported {
        let call = state
            .imported_calls
            .for_session(session)
            .get(boundary.imported_offset)
            .filter(|call| call.session_id == *session)
            .ok_or_else(invalid)?;
        Record::Imported(call)
    } else if boundary.provenance_offset < boundary.provenance {
        let origin = state
            .imported_provenance
            .for_session(session)
            .get(boundary.provenance_offset)
            .filter(|origin| origin.session_id == *session)
            .ok_or_else(invalid)?;
        Record::Provenance(origin)
    } else {
        return Ok(None);
    };
    Ok(Some(record))
}
impl Record<'_> {
    pub(super) fn measure(
        &self,
        mut usage: SnapshotUsage,
        limits: SnapshotLimits,
    ) -> Result<SnapshotUsage, SnapshotLimit> {
        let content: &[crabber_core::ContentBlock] = match self {
            Self::Message(message) => {
                usage.messages = usage.messages.saturating_add(1);
                usage.parts = usage.parts.saturating_add(message.parts.len());
                usage.text_bytes = message.parts.iter().fold(usage.text_bytes, |n, part| {
                    n.saturating_add(crate::snapshot::text_bytes(&part.content))
                });
                &[]
            }
            Self::Native(call) => {
                usage.tool_calls = usage.tool_calls.saturating_add(1);
                call.result.as_ref().map_or(&[], |result| &result.content)
            }
            Self::Imported(call) => {
                usage.tool_calls = usage.tool_calls.saturating_add(1);
                &call.result.content
            }
            Self::Provenance(_) => &[],
        };
        usage.text_bytes = content.iter().fold(usage.text_bytes, |n, block| {
            n.saturating_add(crate::snapshot::text_bytes(block))
        });
        if let Some(limit) = usage.exceeded(limits) {
            return Err(limit);
        }
        let cap = limits.encoded_bytes.saturating_sub(usage.encoded_bytes);
        let bytes = match self {
            Self::Message(value) => crate::snapshot::encoded_bytes(value, cap),
            Self::Native(value) => crate::snapshot::encoded_bytes(value, cap),
            Self::Imported(value) => crate::snapshot::encoded_bytes(value, cap),
            Self::Provenance(value) => crate::snapshot::encoded_bytes(value, cap),
        }
        .ok_or(SnapshotLimit::EncodedBytes)?;
        usage.encoded_bytes += bytes;
        Ok(usage)
    }
    pub(super) fn append(self, page: &mut SnapshotPage, boundary: &mut Boundary) {
        match self {
            Self::Message(value) => {
                page.messages.push(value.clone());
                boundary.message_offset += 1;
            }
            Self::Native(value) => {
                page.tool_calls.push(value.clone());
                boundary.call_offset += 1;
            }
            Self::Imported(value) => {
                page.imported_calls.push(value.clone());
                boundary.imported_offset += 1;
            }
            Self::Provenance(value) => {
                page.imported_provenance.push(value.clone());
                boundary.provenance_offset += 1;
            }
        }
    }
}
