//! A replacement window, never a page in an all-history assembly.
use crate::{
    PublicHistoryPage, PublicNewestHistoryOutcome, SnapshotLimit, SnapshotLimits, SnapshotRequest,
    SnapshotUsage, StoreError,
};
use crabber_core::{
    ContentBlock, EventCursor, ImportedMessageProvenance, ImportedToolCall, Message,
    SessionRevision, ToolCallId, ToolCallRecord,
};
use serde::Serialize;
use std::collections::BTreeSet;

pub(crate) fn validate(request: &SnapshotRequest) -> Result<(), StoreError> {
    if request.continuation.is_some()
        || request.session_id.0.is_empty()
        || request.session_id.0.len() > 512
        || request.session_id.0.chars().any(char::is_control)
    {
        return Err(StoreError::Validation(
            "invalid newest history request".into(),
        ));
    }
    Ok(())
}

#[derive(Clone, Copy, Default)]
struct Lengths {
    messages: usize,
    calls: usize,
    imported: usize,
    provenance: usize,
}
pub(crate) struct Window {
    page: PublicHistoryPage,
    limits: SnapshotLimits,
    committed: Lengths,
    calls: BTreeSet<ToolCallId>,
}
impl Window {
    pub(crate) fn new(
        revision: SessionRevision,
        high_water: EventCursor,
        limits: SnapshotLimits,
    ) -> Self {
        Self {
            page: PublicHistoryPage {
                captured_revision: revision,
                high_water,
                messages: Vec::new(),
                tool_calls: Vec::new(),
                imported_calls: Vec::new(),
                imported_provenance: Vec::new(),
                read_usage: SnapshotUsage::default(),
                continuation: None,
                omitted_older_messages: false,
            },
            limits,
            committed: Lengths::default(),
            calls: BTreeSet::new(),
        }
    }
    /// Scalar preflight before a SQL payload is transferred or decoded.
    pub(crate) fn preflight(&self, cost: SnapshotUsage) -> Result<(), SnapshotLimit> {
        self.total(cost).exceeded(self.limits).map_or(Ok(()), Err)
    }
    fn total(&self, cost: SnapshotUsage) -> SnapshotUsage {
        let used = self.page.read_usage;
        SnapshotUsage {
            messages: used.messages.saturating_add(cost.messages),
            tool_calls: used.tool_calls.saturating_add(cost.tool_calls),
            parts: used.parts.saturating_add(cost.parts),
            text_bytes: used.text_bytes.saturating_add(cost.text_bytes),
            encoded_bytes: used.encoded_bytes.saturating_add(cost.encoded_bytes),
        }
    }
    fn charge<T: Serialize>(
        &mut self,
        record: &T,
        mut cost: SnapshotUsage,
        raw_cost: Option<SnapshotUsage>,
    ) -> Result<(), SnapshotLimit> {
        if let Some(raw_cost) = raw_cost {
            self.preflight(raw_cost)?;
            self.page.read_usage = self.total(raw_cost);
            return Ok(());
        }
        self.preflight(cost)?;
        cost.encoded_bytes = crate::snapshot::encoded_bytes(
            record,
            self.limits
                .encoded_bytes
                .saturating_sub(self.page.read_usage.encoded_bytes),
        )
        .ok_or(SnapshotLimit::EncodedBytes)?;
        self.preflight(cost)?;
        self.page.read_usage = self.total(cost);
        Ok(())
    }
    pub(crate) fn message(
        &mut self,
        message: &Message,
        raw_cost: Option<SnapshotUsage>,
    ) -> Result<Vec<ToolCallId>, SnapshotLimit> {
        self.charge(
            message,
            SnapshotUsage {
                messages: 1,
                parts: message.parts.len(),
                text_bytes: message.parts.iter().fold(0usize, |n, p| {
                    n.saturating_add(crate::snapshot::text_bytes(&p.content))
                }),
                ..SnapshotUsage::default()
            },
            raw_cost,
        )?;
        let mut message = message.clone();
        crate::public_history::redact_message(&mut message);
        let mut ids = BTreeSet::new();
        for part in &message.parts {
            call_ids(&part.content, &mut ids);
        }
        ids.retain(|id| self.calls.insert(id.clone()));
        self.page.messages.push(message);
        Ok(ids.into_iter().collect())
    }
    pub(crate) fn native(
        &mut self,
        call: &ToolCallRecord,
        raw_cost: Option<SnapshotUsage>,
    ) -> Result<(), SnapshotLimit> {
        self.charge(call, call_cost(call.result.as_ref()), raw_cost)?;
        let mut call = call.clone();
        if let Some(result) = &mut call.result {
            crate::public_history::redact_result(result);
        }
        self.page.tool_calls.push(call);
        Ok(())
    }
    pub(crate) fn imported(
        &mut self,
        call: &ImportedToolCall,
        raw_cost: Option<SnapshotUsage>,
    ) -> Result<(), SnapshotLimit> {
        self.charge(call, call_cost(Some(&call.result)), raw_cost)?;
        let mut call = call.clone();
        crate::public_history::redact_result(&mut call.result);
        self.page.imported_calls.push(call);
        Ok(())
    }
    pub(crate) fn provenance(
        &mut self,
        provenance: &ImportedMessageProvenance,
        raw_cost: Option<SnapshotUsage>,
    ) -> Result<(), SnapshotLimit> {
        self.charge(provenance, SnapshotUsage::default(), raw_cost)?;
        self.page.imported_provenance.push(provenance.clone());
        Ok(())
    }
    pub(crate) fn commit(&mut self) {
        self.committed = Lengths {
            messages: self.page.messages.len(),
            calls: self.page.tool_calls.len(),
            imported: self.page.imported_calls.len(),
            provenance: self.page.imported_provenance.len(),
        };
    }
    pub(crate) fn finish(mut self, limit: Option<SnapshotLimit>) -> PublicNewestHistoryOutcome {
        if let Some(limit) = limit {
            if self.committed.messages == 0 {
                return PublicNewestHistoryOutcome::Limited(limit);
            }
            self.page.omitted_older_messages = true;
        }
        self.page.messages.truncate(self.committed.messages);
        self.page.tool_calls.truncate(self.committed.calls);
        self.page.imported_calls.truncate(self.committed.imported);
        self.page
            .imported_provenance
            .truncate(self.committed.provenance);
        self.page.messages.reverse();
        self.page.tool_calls.reverse();
        self.page.imported_calls.reverse();
        self.page.imported_provenance.reverse();
        PublicNewestHistoryOutcome::Page(self.page)
    }
}
fn call_cost(result: Option<&crabber_core::ToolResult>) -> SnapshotUsage {
    SnapshotUsage {
        tool_calls: 1,
        text_bytes: result.map_or(0, |r| {
            r.content.iter().fold(0usize, |n, b| {
                n.saturating_add(crate::snapshot::text_bytes(b))
            })
        }),
        ..SnapshotUsage::default()
    }
}
fn call_ids(block: &ContentBlock, ids: &mut BTreeSet<ToolCallId>) {
    match block {
        ContentBlock::ToolCall { call_id, .. } => {
            ids.insert(call_id.clone());
        }
        ContentBlock::ToolResult {
            call_id, content, ..
        } => {
            ids.insert(call_id.clone());
            for block in content {
                call_ids(block, ids);
            }
        }
        _ => (),
    }
}
