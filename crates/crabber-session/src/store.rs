use async_trait::async_trait;
use crabber_core::{
    AdmissionKey, AdmissionOptions, AdmissionReceipt, ContextEpoch, EpochId, EventCursor,
    EventRecord, InputFingerprint, Message, MessageId, Part, Run, RunFence, RunId, RunStatus,
    Session, SessionId, ToolCallId, ToolCallRecord, ToolResult, Usage,
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

/// Store-side keyed request. The runtime independently computes `config_hash`
/// from all reflected configuration and plan inputs. Stores MUST independently
/// hash message semantics and immutable identity using `semantic_digest`.
#[derive(Clone)]
pub struct KeyedAdmitRequest {
    pub request: AdmitRequest,
    pub options: AdmissionOptions,
}

impl std::fmt::Debug for KeyedAdmitRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("KeyedAdmitRequest([redacted])")
    }
}

impl KeyedAdmitRequest {
    /// Version 1 uses canonical JSON (sorted object keys), SHA-256, and domain
    /// separation. Excludes generated IDs, timestamps, lease, and owner. Includes
    /// ordered message parts (kind/content/ordinal), role/parent, identity/title,
    /// runtime configuration and plan hashes, and host opaque behavior version.
    /// # Errors
    /// Returns a sanitized serialization error.
    pub fn semantic_digest(&self) -> Result<InputFingerprint, StoreError> {
        use sha2::{Digest, Sha256};
        let request = &self.request;
        let parts = request
            .user_message
            .parts
            .iter()
            .map(|part| serde_json::json!([part.ordinal, part.kind, part.content]))
            .collect::<Vec<_>>();
        let mut value = serde_json::json!([
            "crabber.admission.v1",
            request.workspace_id,
            request.directory,
            request.title,
            request.user_message.role,
            request.user_message.parent_id,
            parts,
            request.config_hash,
            request.plan_fingerprint,
            self.options.behavior_fingerprint.as_str()
        ]);
        value.sort_all_objects();
        let bytes = serde_json::to_vec(&value)
            .map_err(|_| StoreError::Validation("invalid admission semantics".into()))?;
        InputFingerprint::new(format!("{:x}", Sha256::digest(bytes)))
    }
}

/// Only a newly committed admission grants execution authority.
#[derive(Clone)]
pub enum KeyedAdmitOutcome {
    Started {
        receipt: AdmissionReceipt,
        admitted: Box<AdmitOutcome>,
    },
    Replayed(AdmissionReceipt),
}

impl std::fmt::Debug for KeyedAdmitOutcome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Started { receipt, .. } => f.debug_tuple("Started").field(receipt).finish(),
            Self::Replayed(receipt) => f.debug_tuple("Replayed").field(receipt).finish(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InboxKind {
    Steer,
    FollowUp,
}

#[async_trait]
pub trait Store: Send + Sync {
    async fn admit_run(&self, request: AdmitRequest) -> Result<AdmitOutcome, StoreError>;
    /// Atomically admit a stable first/existing session ID and retain its receipt.
    /// Validate message/session identity before replay; resolve key conflict/replay
    /// before Busy. Replay MUST NOT claim/renew a lease or return execution authority.
    /// Custom stores must implement this transactionally; the default fails closed.
    async fn admit_keyed_run(
        &self,
        _request: KeyedAdmitRequest,
    ) -> Result<KeyedAdmitOutcome, StoreError> {
        Err(StoreError::AdmissionUnsupported)
    }
    /// Read immutable metadata without claiming a lease. None is not proof that
    /// an in-flight admission cannot subsequently commit.
    async fn lookup_admission(
        &self,
        _session: &SessionId,
        _key: &AdmissionKey,
    ) -> Result<Option<AdmissionReceipt>, StoreError> {
        Err(StoreError::AdmissionUnsupported)
    }
    async fn execution(&self, fence: RunFence) -> Result<Box<dyn ExecutionStore>, StoreError>;
    async fn get_session(&self, id: &SessionId) -> Result<Option<Session>, StoreError>;
    async fn get_run(&self, id: &RunId) -> Result<Option<Run>, StoreError>;
    async fn list_messages(
        &self,
        id: &SessionId,
        epoch: Option<EpochId>,
    ) -> Result<Vec<Message>, StoreError>;
    /// Returns the append-only message history, including messages hidden by
    /// context epoch projections. Used to reconcile a committed turn after a crash.
    async fn list_all_messages(&self, id: &SessionId) -> Result<Vec<Message>, StoreError>;
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
