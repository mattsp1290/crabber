use async_trait::async_trait;
use crabber_core::{
    ContextEpoch, EpochId, EventCursor, EventRecord, Message, MessageId, Part, Run, RunFence,
    RunId, RunStatus, Session, SessionId, ToolCallId, ToolCallRecord, ToolResult, Usage,
};
use std::{collections::BTreeMap, time::Duration};
use time::OffsetDateTime;

pub type StoreError = crabber_core::CoreError;

#[derive(Debug, Clone)]
pub struct AdmitRequest {
    pub session_id: Option<SessionId>,
    pub workspace_id: String,
    pub directory: String,
    pub title: String,
    pub user_message: Message,
    pub config_hash: String,
    pub plan_fingerprint: String,
    pub owner: String,
    pub lease: Duration,
}

#[derive(Debug, Clone)]
pub struct AdmitOutcome {
    pub session: Session,
    pub run: Run,
    pub fence: RunFence,
    pub assistant_placeholder: MessageId,
    pub epoch: EpochId,
    pub prior_history: Vec<Message>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InboxKind {
    Steer,
    FollowUp,
}

#[async_trait]
pub trait Store: Send + Sync {
    async fn admit_run(&self, request: AdmitRequest) -> Result<AdmitOutcome, StoreError>;
    async fn execution(&self, fence: RunFence) -> Result<Box<dyn ExecutionStore>, StoreError>;
    async fn get_session(&self, id: &SessionId) -> Result<Option<Session>, StoreError>;
    async fn get_run(&self, id: &RunId) -> Result<Option<Run>, StoreError>;
    async fn list_messages(
        &self,
        id: &SessionId,
        epoch: Option<EpochId>,
    ) -> Result<Vec<Message>, StoreError>;
    async fn list_events(
        &self,
        id: &SessionId,
        after: Option<EventCursor>,
        limit: usize,
    ) -> Result<Vec<EventRecord>, StoreError>;
    async fn list_unfinished_runs(&self) -> Result<Vec<Run>, StoreError>;
    async fn list_unfinished_tool_calls(
        &self,
        run: &RunId,
    ) -> Result<Vec<ToolCallRecord>, StoreError>;
    async fn claim_expired_run(&self, run: &RunId, owner: &str) -> Result<RunFence, StoreError>;
    async fn get_extension_state(
        &self,
        extension_id: &str,
        session: &SessionId,
    ) -> Result<BTreeMap<String, String>, StoreError>;
    async fn enqueue_inbox(
        &self,
        session: &SessionId,
        kind: InboxKind,
        message: Message,
    ) -> Result<(), StoreError>;
}

#[async_trait]
pub trait ExecutionStore: Send + Sync {
    async fn renew_lease(&self, until: OffsetDateTime) -> Result<(), StoreError>;
    async fn append_message(&self, message: Message) -> Result<(), StoreError>;
    async fn append_part(&self, part: Part) -> Result<(), StoreError>;
    async fn append_event(&self, event: EventRecord) -> Result<(), StoreError>;
    async fn create_tool_call(
        &self,
        call: ToolCallRecord,
        pending_event: EventRecord,
    ) -> Result<(), StoreError>;
    async fn claim_tool_call(
        &self,
        id: &ToolCallId,
        running_event: EventRecord,
    ) -> Result<(), StoreError>;
    async fn settle_tool_call(
        &self,
        id: &ToolCallId,
        result: ToolResult,
        result_message: Message,
        terminal_event: EventRecord,
    ) -> Result<(), StoreError>;
    async fn start_epoch(&self, epoch: ContextEpoch) -> Result<(), StoreError>;
    async fn finish_epoch(&self, id: &EpochId, summary: Message) -> Result<(), StoreError>;
    async fn pause_run(
        &self,
        checkpoint: serde_json::Value,
        event: EventRecord,
    ) -> Result<(), StoreError>;
    async fn settle_run(
        &self,
        status: RunStatus,
        error: Option<String>,
        usage: Usage,
        event: EventRecord,
    ) -> Result<(), StoreError>;
    async fn put_extension_state(
        &self,
        extension_id: &str,
        entries: Vec<(String, Option<String>)>,
    ) -> Result<(), StoreError>;
    async fn claim_inbox(&self, kind: InboxKind) -> Result<Vec<Message>, StoreError>;
    /// Claims inbox rows and appends them to run history in one transaction.
    async fn claim_inbox_into_history(&self, kind: InboxKind) -> Result<Vec<Message>, StoreError>;
}
