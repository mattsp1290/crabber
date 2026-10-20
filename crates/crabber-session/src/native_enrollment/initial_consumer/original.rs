//! Complete canonical originals. Opaque live grants are never serialized here.
use super::{CallKind, Failure, InputFingerprint, Proof, ReverseKind, digest, unknown};
use crate::native_parent::{RunIssuance, source_protocol::JournalSource};
use crate::session_owner::{audit::Audit, driver::DriverLink};
use crabber_core::{EventCursor, EventRecord, Message, Run, RunFence, SessionRevision};
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DriverRequest {
    pub session: crabber_core::SessionId,
    pub workspace: String,
    pub directory: String,
    pub fence: RunFence,
}
impl DriverRequest {
    pub(crate) fn fingerprint(&self) -> Result<InputFingerprint, Failure> {
        digest(&(
            "crabber.execution-driver.initial.request.v1",
            &self.session,
            &self.workspace,
            &self.directory,
            &self.fence.run_id,
            &self.fence.claim_token,
        ))
    }
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RegisterOriginal {
    pub request: DriverRequest,
    pub receipt: crate::session_owner::driver::ExecutionDriverReceipt,
    pub source: DriverLink,
    pub reserve: Audit,
    pub begin: Audit,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ActivateOriginal {
    pub request: crate::session_owner::owned_scope::OwnedScopeActivation,
    pub receipt: crate::session_owner::SessionOwnerReceipt,
    pub audit: Audit,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativePublication {
    pub issuer: RunIssuance,
    pub source: JournalSource,
}
impl NativePublication {
    pub(crate) fn verify(&self) -> Result<(), Failure> {
        self.source.verify(&self.issuer).map_err(unknown)
    }
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct KeyedBeginOriginal {
    pub fence: RunFence,
    pub init: crate::admission_journal::Init,
    pub begin: crate::admission_journal::Begin,
    pub previous_state: crate::AdmissionExecutionState,
    pub record: crate::AdmissionExecutionRecord,
    pub revision: SessionRevision,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StoredAttempt {
    pub snapshot: crate::AttemptSnapshot,
    pub owner: String,
    pub token: String,
    pub run_token: String,
    pub lease_until: time::OffsetDateTime,
}
impl StoredAttempt {
    pub(crate) fn matches(&self, authority: &crate::AttemptAuthority) -> bool {
        authority.matches_store(&self.snapshot, &self.token, &self.run_token)
    }
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AttemptOriginal {
    pub request: crate::AttemptMutation,
    pub before: Option<StoredAttempt>,
    pub after: StoredAttempt,
    pub receipt: crate::AttemptReceipt,
    pub event: EventRecord,
    pub publication: Option<NativePublication>,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct MessageOriginal {
    pub message: Message,
    pub revision: SessionRevision,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct LifecycleOriginal {
    pub kind: InitialLifecycleKind,
    pub input: EventRecord,
    pub event: EventRecord,
    pub cursor: EventCursor,
    pub revision: SessionRevision,
}
#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum InitialLifecycleKind {
    RunAdmitted,
    RunStarted,
    TurnStarted,
    MessageCommitted,
    TurnCompleted,
}
impl InitialLifecycleKind {
    pub(crate) fn call_kind(self) -> CallKind {
        match self {
            Self::RunAdmitted => CallKind::RunAdmitted,
            Self::RunStarted => CallKind::RunStarted,
            Self::TurnStarted => CallKind::TurnStarted,
            Self::MessageCommitted => CallKind::MessageCommitted,
            Self::TurnCompleted => CallKind::TurnCompleted,
        }
    }
    pub(crate) fn event_kind(self) -> crabber_core::EventKind {
        match self {
            Self::RunAdmitted => crabber_core::EventKind::RunAdmitted,
            Self::RunStarted => crabber_core::EventKind::RunStarted,
            Self::TurnStarted => crabber_core::EventKind::TurnStarted,
            Self::MessageCommitted => crabber_core::EventKind::MessageCommitted,
            Self::TurnCompleted => crabber_core::EventKind::TurnCompleted,
        }
    }
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ConclusionOriginal {
    pub request: crate::AttemptConclusion,
    pub attempt: AttemptOriginal,
    pub before_run: Run,
    pub after_run: Run,
    pub settlement: EventRecord,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(
    tag = "operation",
    content = "original",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub(crate) enum Original {
    Register(Box<RegisterOriginal>),
    Activate(Box<ActivateOriginal>),
    KeyedBegin(Box<KeyedBeginOriginal>),
    ReserveInitial(Box<AttemptOriginal>),
    BeginInitial(Box<AttemptOriginal>),
    AssistantMessage(Box<MessageOriginal>),
    Lifecycle(Box<LifecycleOriginal>),
    Conclude(Box<ConclusionOriginal>),
}
impl Original {
    pub(crate) fn kind(&self) -> CallKind {
        match self {
            Self::Register(_) => CallKind::Register,
            Self::Activate(_) => CallKind::Activate,
            Self::KeyedBegin(_) => CallKind::KeyedBegin,
            Self::ReserveInitial(_) => CallKind::ReserveInitial,
            Self::BeginInitial(_) => CallKind::BeginInitial,
            Self::AssistantMessage(_) => CallKind::AssistantMessage,
            Self::Lifecycle(v) => v.kind.call_kind(),
            Self::Conclude(_) => CallKind::Conclude,
        }
    }
    pub(crate) fn input_digest(&self) -> Result<InputFingerprint, Failure> {
        match self {
            Self::Register(v) => v.request.fingerprint(),
            Self::Activate(v) => v.request.fingerprint().map_err(unknown),
            Self::KeyedBegin(v) => digest(&("crabber.initial-consumer.keyed-begin.v1", &v.fence)),
            Self::ReserveInitial(v) | Self::BeginInitial(v) => v
                .request
                .fingerprint(&v.after.snapshot.session_id, &v.after.snapshot.run_id)
                .map_err(unknown),
            Self::AssistantMessage(v) => {
                digest(&("crabber.initial-consumer.message-input.v1", &v.message))
            }
            Self::Lifecycle(v) => digest(&(
                "crabber.initial-consumer.lifecycle-input.v1",
                v.kind,
                &v.input,
            )),
            Self::Conclude(v) => v
                .request
                .fingerprint(&v.after_run.session_id, &v.after_run.id)
                .map_err(unknown),
        }
    }
    pub(crate) fn reverse_keys(&self) -> Result<Vec<(ReverseKind, InputFingerprint)>, Failure> {
        let key = match self {
            Self::Register(v) => (
                ReverseKind::Driver,
                digest(&(&v.receipt.session_id, &v.receipt.reserve.owner.target))?,
            ),
            Self::Activate(v) => (
                ReverseKind::Activation,
                digest(&(&v.receipt.session_id, &v.request.id))?,
            ),
            Self::KeyedBegin(v) => (
                ReverseKind::KeyedBegin,
                digest(&(&v.record.receipt.session_id, &v.record.receipt.run_id))?,
            ),
            Self::ReserveInitial(v) => (
                ReverseKind::InitialAttempt,
                digest(&(&v.after.snapshot.session_id, &v.after.snapshot.target))?,
            ),
            Self::BeginInitial(v) => (
                ReverseKind::AttemptBegin,
                digest(&(&v.after.snapshot.session_id, &v.after.snapshot.target))?,
            ),
            Self::AssistantMessage(v) => (
                ReverseKind::Message,
                digest(&(&v.message.session_id, &v.message.id))?,
            ),
            Self::Lifecycle(v) => (
                ReverseKind::Event,
                digest(&(&v.event.session_id, v.cursor))?,
            ),
            Self::Conclude(v) => (
                ReverseKind::Conclusion,
                digest(&(&v.after_run.session_id, &v.request.finish.id))?,
            ),
        };
        Ok(vec![key])
    }
    pub(crate) fn verify(
        &self,
        session: &crabber_core::SessionId,
        run: &crabber_core::RunId,
        proof: &mut Proof,
    ) -> Result<(), Failure> {
        proof.measure(self)?;
        match self {
            Self::Register(v) => verify_register(v, session, run),
            Self::Activate(v) => {
                v.audit.validate().map_err(unknown)?;
                if v.receipt.session_id != *session
                    || v.audit.receipt != v.receipt
                    || v.audit.record.owned_scope.as_ref() != Some(&v.request)
                    || v.receipt.semantic_digest != v.request.fingerprint().map_err(unknown)?
                    || v.audit.publication.is_none()
                {
                    return Err(unknown("consumer activation original"));
                }
                Ok(())
            }
            Self::KeyedBegin(v) => {
                if v.previous_state != crate::AdmissionExecutionState::Unstarted
                    || v.record.state != crate::AdmissionExecutionState::Started
                    || v.record.receipt.session_id != *session
                    || v.record.receipt.run_id != *run
                    || v.fence.run_id != *run
                    || crate::native_parent::RunState::new(
                        session,
                        run,
                        v.begin.issuance.state.status,
                        &v.fence.claim_token,
                    )
                    .map_err(unknown)?
                        != v.begin.issuance.state
                {
                    return Err(unknown("consumer keyed Begin original"));
                }
                v.init.verify(&v.record).map_err(unknown)?;
                v.begin.verify(&v.init).map_err(unknown)
            }
            Self::ReserveInitial(v) | Self::BeginInitial(v) => verify_attempt(v, session, run),
            Self::AssistantMessage(v) => {
                if v.message.session_id != *session
                    || v.message.run_id.as_ref() != Some(run)
                    || v.message.role != crabber_core::Role::Assistant
                {
                    return Err(unknown("consumer assistant message original"));
                }
                crate::ensure_storable(&v.message).map_err(unknown)
            }
            Self::Lifecycle(v) => {
                if v.event.session_id != *session
                    || v.event.run_id != *run
                    || v.event.kind != v.kind.event_kind()
                    || v.event.cursor != Some(v.cursor)
                    || v.input.cursor.is_some()
                    || v.input.session_id != v.event.session_id
                    || v.input.run_id != v.event.run_id
                    || v.input.kind != v.event.kind
                    || v.input.payload != v.event.payload
                    || v.input.turn_id != v.event.turn_id
                    || v.input.correlation != v.event.correlation
                    || v.input.live_only != v.event.live_only
                    || v.input.created_at != v.event.created_at
                {
                    return Err(unknown("consumer lifecycle original"));
                }
                Ok(())
            }
            Self::Conclude(v) => {
                verify_attempt(&v.attempt, session, run)?;
                v.request
                    .verify_replay(&v.attempt.receipt)
                    .map_err(unknown)?;
                if v.before_run.id != *run
                    || v.after_run.id != *run
                    || v.before_run.session_id != *session
                    || v.after_run.session_id != *session
                    || v.settlement.session_id != *session
                    || v.settlement.run_id != *run
                    || v.settlement.kind != crabber_core::EventKind::RunSettled
                {
                    return Err(unknown("consumer conclusion original"));
                }
                Ok(())
            }
        }
    }
}
fn verify_register(
    v: &RegisterOriginal,
    session: &crabber_core::SessionId,
    run: &crabber_core::RunId,
) -> Result<(), Failure> {
    v.reserve.validate().map_err(unknown)?;
    v.begin.validate().map_err(unknown)?;
    crate::session_owner::audit::verify_current(&v.begin.record, &v.reserve, &v.begin)
        .map_err(unknown)?;
    if v.request.session != *session
        || v.request.fence.run_id != *run
        || v.receipt.session_id != *session
        || v.receipt.run_id != *run
        || v.receipt.request_digest != v.request.fingerprint()?
        || v.receipt.reserve != v.reserve.receipt
        || v.receipt.begin != v.begin.receipt
        || v.reserve.record.driver.as_deref() != Some(&v.source)
        || v.begin.record.driver.as_deref() != Some(&v.source)
        || v.begin.publication.is_none()
        || v.reserve.publication.is_none()
    {
        return Err(unknown("consumer driver original"));
    }
    Ok(())
}
fn verify_attempt(
    v: &AttemptOriginal,
    session: &crabber_core::SessionId,
    run: &crabber_core::RunId,
) -> Result<(), Failure> {
    crate::attempt::verify_receipt(&v.receipt, &v.event).map_err(unknown)?;
    if v.after.snapshot.session_id != *session
        || v.after.snapshot.run_id != *run
        || v.after.snapshot != v.receipt.attempt
        || v.receipt.operation_id != v.request.id
        || v.after.snapshot.target != *v.request.command.target()
        || v.after.snapshot.target.generation != crabber_core::AttemptGeneration::INITIAL
    {
        return Err(unknown("consumer initial attempt original"));
    }
    match &v.request.command {
        crate::AttemptCommand::ReserveBound { .. } => {
            if v.before.is_some()
                || v.after.snapshot.state != crate::AttemptState::Unstarted
                || v.after.snapshot.cleanup != crate::AttemptCleanup::Pending
            {
                return Err(unknown("consumer initial reservation transition"));
            }
        }
        crate::AttemptCommand::Begin { .. } => {
            let before = v
                .before
                .as_ref()
                .ok_or_else(|| unknown("consumer Begin missing Reserve"))?;
            if before.snapshot.state != crate::AttemptState::Unstarted
                || v.after.snapshot.state != crate::AttemptState::Started
                || before.snapshot.target != v.after.snapshot.target
                || before.token != v.after.token
                || before.run_token != v.after.run_token
                || before.owner != v.after.owner
                || before.lease_until != v.after.lease_until
            {
                return Err(unknown("consumer initial Begin transition"));
            }
        }
        crate::AttemptCommand::Finish { outcome, .. } => {
            if v.before
                .as_ref()
                .is_none_or(|r| r.snapshot.state != crate::AttemptState::Started)
                || v.after.snapshot.state != (*outcome).into()
                || v.after.snapshot.cleanup != crate::AttemptCleanup::Complete
            {
                return Err(unknown("consumer initial conclusion transition"));
            }
        }
        _ => return Err(unknown("unsupported consumer Attempt original")),
    }
    if !matches!(v.request.command, crate::AttemptCommand::Finish { .. })
        && v.receipt.semantic_digest != v.request.fingerprint(session, run).map_err(unknown)?
    {
        return Err(unknown("consumer Attempt input digest"));
    }
    if let Some(p) = &v.publication {
        p.verify()?;
    }
    Ok(())
}
