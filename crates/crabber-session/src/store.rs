use async_trait::async_trait;
use crabber_core::{
    AdmissionKey, AdmissionOptions, AdmissionReceipt, ContextEpoch, EpochId, EventCursor,
    EventRecord, InputFingerprint, Message, MessageId, Part, Run, RunFence, RunId, RunStatus,
    Session, SessionId, ToolCallId, ToolCallRecord, ToolResult, Usage,
};
use std::{collections::BTreeMap, time::Duration};
use time::OffsetDateTime;

pub type StoreError = crabber_core::CoreError;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct AdmitRequest {
    /// Optional guard for a host's previously validated coherent snapshot.
    /// Checked under the admission transaction after identity and receipt replay.
    /// This legacy text request guard is not itself an idempotency semantic;
    /// typed envelopes additionally retain their revision in canonical input.
    pub expected_revision: Option<crabber_core::SessionRevision>,
    /// Ordered records following the admission's first message. This is a
    /// trusted persistence API, not role/history authorization for wire input.
    pub additional_messages: Vec<Message>,
    /// Original bounded semantic input; generated persistence IDs are separate.
    pub input_envelope: Option<crabber_core::InputEnvelope>,
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

impl AdmitRequest {
    /// Check the complete input batch before any write. Runtime-owned generated
    /// IDs and host-supplied provenance are validated against the envelope separately.
    /// # Errors
    /// Rejects oversized batches, duplicate IDs, foreign scope or pre-owned typed
    /// records. The legacy single raw-message path normalizes its run ID.
    pub fn validate_messages(&self) -> Result<(), StoreError> {
        let session = self
            .session_id
            .as_ref()
            .unwrap_or(&self.user_message.session_id);
        if self.additional_messages.len() >= crabber_core::input::MAX_INPUT_MESSAGES {
            return Err(StoreError::Validation(
                "invalid admission message batch".into(),
            ));
        }
        if let Some(input) = &self.input_envelope {
            if !matches!(
                input.admission_kind(),
                crabber_core::AdmissionKind::Ordinary
            ) {
                return Err(StoreError::HostUnsupported);
            }
            if self.expected_revision != Some(input.expected_revision()) {
                return Err(StoreError::AdmissionConflict);
            }
            input.validate_message_projection(
                session,
                None,
                std::iter::once(&self.user_message).chain(&self.additional_messages),
            )?;
        }
        let mut messages = std::collections::BTreeSet::new();
        let mut parts = std::collections::BTreeSet::new();
        for message in std::iter::once(&self.user_message).chain(&self.additional_messages) {
            if message.session_id != *session
                || (message.run_id.is_some()
                    && (self.input_envelope.is_some() || !self.additional_messages.is_empty()))
                || !messages.insert(&message.id)
            {
                return Err(StoreError::Validation(
                    "invalid admission message batch".into(),
                ));
            }
            for part in &message.parts {
                if part.message_id != message.id || !parts.insert(&part.id) {
                    return Err(StoreError::Validation(
                        "invalid admission message batch".into(),
                    ));
                }
            }
        }
        Ok(())
    }
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
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct KeyedAdmitRequest {
    pub request: AdmitRequest,
    pub options: AdmissionOptions,
    pub execution: Option<crate::AdmissionExecutionCapsule>,
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
        if self.request.input_envelope.is_some() {
            return crate::typed_admission::semantic_digest(self);
        }
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

impl KeyedAdmitRequest {
    /// Validate immutable message identity and optional original execution
    /// evidence before taking admission locks, then independently hash semantics.
    /// Custom adapters must also check persisted identity and replay under their
    /// transaction lock. This pure validation grants no execution authority.
    /// # Errors
    /// Rejects inconsistent scope, original evidence or serialization.
    pub fn validated_digest(&self) -> Result<InputFingerprint, StoreError> {
        let request = &self.request;
        let session = request.session_id.as_ref().ok_or_else(|| {
            StoreError::Validation("keyed admission requires a stable session ID".into())
        })?;
        if request.user_message.session_id != *session
            || request.user_message.run_id.is_some()
            || request
                .user_message
                .parts
                .iter()
                .any(|part| part.message_id != request.user_message.id)
        {
            return Err(StoreError::Validation(
                "invalid admission message identity".into(),
            ));
        }
        request.validate_messages()?;
        // Legacy keyed capsules cannot attest a multi-message input. The typed
        // capsule/digest must be installed before this path can grant authority.
        if !request.additional_messages.is_empty() && request.input_envelope.is_none() {
            return Err(StoreError::HostUnsupported);
        }
        let digest = self.semantic_digest()?;
        if let Some(capsule) = &self.execution {
            capsule.validate(self)?;
        }
        Ok(digest)
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

/// Persistence boundary for durable session records. For legacy records,
/// implementations must
/// reject, before changing state, any record their readers cannot decode or
/// which contains a NUL character, using [`StoreError::Validation`]. Bundled
/// stores use [`crate::ensure_storable`]; custom stores should run
/// [`crate::storetest::run_storable_record_contract`].
/// Typed public state follows its separate bounded, lossless JSON contract.
#[async_trait]
pub trait Store: Send + Sync {
    /// V2 profile-bound public history. Custom adapters explicitly opt in.
    async fn public_history_v2(
        &self,
        _request: crate::SnapshotRequest,
    ) -> Result<crate::PublicHistoryOutcome, StoreError> {
        Err(StoreError::HostUnsupported)
    }
    /// Coherent fixed aggregate V2 metadata and history; no execution authority.
    async fn public_session_snapshot_v2(
        &self,
        request: crate::PublicSessionRequestV2,
    ) -> Result<crate::PublicSessionOutcomeV2, crate::PublicSessionError> {
        crate::public_session::v2::compose::snapshot(self, request).await
    }
    /// Fixed aggregate V2 metadata. Custom adapters explicitly opt in.
    async fn public_control_header_v2(
        &self,
        _request: crate::PublicHeaderRequestV2,
    ) -> Result<crate::PublicHeaderOutcomeV2, crate::PublicSessionError> {
        Err(crate::PublicSessionError::Unsupported)
    }
    /// Bounded native metadata, with private approval proofs preflighted before decoding.
    async fn public_control_header(
        &self,
        _request: crate::PublicHeaderRequest,
    ) -> Result<crate::PublicHeaderOutcome, crate::PublicSessionError> {
        Err(crate::PublicSessionError::Unsupported)
    }
    /// Public history and metadata at one captured revision. No dispatch authority.
    async fn public_session_snapshot(
        &self,
        request: crate::PublicSessionRequest,
    ) -> Result<crate::PublicSessionOutcome, crate::PublicSessionError> {
        crate::public_session::compose::snapshot(self, request).await
    }

    /// Whether this adapter durably implements targeted cancel and steer.
    fn supports_run_controls(&self) -> bool {
        false
    }
    /// Coherent exact host target binding. Does not infer workspace authorization.
    async fn run_control_binding(
        &self,
        _session: &SessionId,
        _run: &RunId,
    ) -> Result<crate::RunControlBinding, crate::RunControlError> {
        Err(crate::RunControlError::Unsupported)
    }
    /// Apply an already accepted Cancel to an authentically quiescent pause.
    /// A settled original returns metadata before CAS. It never grants dispatch.
    async fn settle_waiting_cancellation(
        &self,
        _request: crate::WaitingCancellation,
    ) -> Result<crate::RunControlSummary, crate::RunControlError> {
        Err(crate::RunControlError::Unsupported)
    }
    /// Exact-target durable cancel/steer acceptance. Unsupported before acceptance.
    async fn control_run(
        &self,
        _request: crate::RunControlRequest,
    ) -> Result<crate::RunControlOutcome, crate::RunControlError> {
        Err(crate::RunControlError::Unsupported)
    }
    /// Immutable original receipt, reconciled before current revision or ingress.
    async fn lookup_run_control(
        &self,
        _session: &SessionId,
        _id: &crabber_core::ControlId,
    ) -> Result<Option<crate::RunControlReceipt>, crate::RunControlError> {
        Err(crate::RunControlError::Unsupported)
    }
    /// Coherent bounded status, with no private steer input or worker authority.
    async fn run_controls(
        &self,
        _target: &crate::RunControlTarget,
    ) -> Result<crate::RunControlPage, crate::RunControlError> {
        Err(crate::RunControlError::Unsupported)
    }

    /// Revoke a lost/stopped parent without manufacturing child outcomes.
    async fn mark_attempt_uncertain(
        &self,
        _request: crate::AttemptUncertainty,
    ) -> Result<crate::AttemptControlOutcome, crate::AttemptError> {
        Err(crate::AttemptError::Unsupported)
    }
    /// Select failure/interruption only after truthful result and cleanup reconciliation.
    async fn retire_attempt(
        &self,
        _request: crate::AttemptRetirement,
    ) -> Result<crate::AttemptControlOutcome, crate::AttemptError> {
        Err(crate::AttemptError::Unsupported)
    }

    /// Durable future-run FIFO capability. Unsupported adapters must refuse before accepting input.
    fn supports_future_queue(&self) -> bool {
        false
    }
    /// Coherent preparation defaults and revision; does not infer workspace authorization.
    /// Queue implementations must preserve valid lazy-session scope verbatim.
    async fn queue_preparation_snapshot(
        &self,
        _session: &SessionId,
    ) -> Result<crate::SessionSummary, StoreError> {
        Err(StoreError::HostUnsupported)
    }
    /// Keyed session control; only a fresh claim/admission returns authority.
    async fn mutate_queue(
        &self,
        _request: crate::QueueMutation,
    ) -> Result<crate::QueueOutcome, StoreError> {
        Err(StoreError::HostUnsupported)
    }
    /// Original safe receipt, including after cancellation or admission. Never dispatches.
    async fn lookup_queue(
        &self,
        _session: &SessionId,
        _id: &crabber_core::ControlId,
    ) -> Result<Option<crate::QueueReceipt>, StoreError> {
        Err(StoreError::HostUnsupported)
    }
    /// Bounded coherent metadata; continuation requires the original revision.
    async fn list_queue(
        &self,
        _request: crate::QueuePageRequest,
    ) -> Result<crate::QueuePage, StoreError> {
        Err(StoreError::HostUnsupported)
    }
    /// Safe exact item metadata; contains no input or worker authority.
    async fn get_queue(
        &self,
        _session: &SessionId,
        _id: &crabber_core::QueueId,
    ) -> Result<crate::QueueSummary, StoreError> {
        Err(StoreError::HostUnsupported)
    }
    /// Explicit private host evidence read; never authorizes dispatch.
    async fn retained_queue(
        &self,
        _session: &SessionId,
        _id: &crabber_core::QueueId,
    ) -> Result<crate::RetainedQueue, StoreError> {
        Err(StoreError::HostUnsupported)
    }

    /// Whether the adapter implements the owner ledger. This does not certify
    /// that runtime/factory dispatch has adopted complete ownership coverage.
    fn supports_session_owners(&self) -> bool {
        false
    }
    /// Reserve before resource/factory dispatch, even before session creation.
    /// Ownership operation IDs have a distinct typed namespace because these
    /// receipts may precede the ordinary session/Run receipt FK graph.
    async fn reserve_session_owner(
        &self,
        _request: crate::session_owner::ReserveSessionOwner,
        _parent: Option<crate::session_owner::SessionOwnerParentAuthority>,
    ) -> Result<crate::session_owner::SessionOwnerOutcome, crate::session_owner::SessionOwnerError>
    {
        Err(crate::session_owner::SessionOwnerError::Unsupported)
    }
    /// Whether atomic preparation relinquishment/native publication is implemented.
    fn supports_preparation_protocol(&self) -> bool {
        false
    }
    /// Pure protected original unkeyed admission inspection is implemented.
    /// This capability is required before factories on the owned initial path.
    fn supports_initial_preparation_admission_inspection(&self) -> bool {
        false
    }
    /// Authenticate the exact original owned unkeyed preparation association.
    /// Reads only original sources; never completes preparation or returns a fence.
    async fn inspect_initial_preparation_admission(
        &self,
        _original: &crate::session_owner::preparation::JoinedPreparation,
    ) -> Result<
        Option<crate::session_owner::preparation::PreparationAdmissionReceipt>,
        crate::session_owner::preparation::PreparationCommitError,
    > {
        Err(crate::session_owner::SessionOwnerError::Unsupported.into())
    }
    /// Atomically capture the original CAS before granting preparation capacity.
    /// Exact retries return metadata and never replacement dispatch authority.
    async fn reserve_preparation(
        &self,
        _request: crate::session_owner::preparation::ReservePreparation,
    ) -> Result<crate::session_owner::SessionOwnerOutcome, crate::session_owner::SessionOwnerError>
    {
        Err(crate::session_owner::SessionOwnerError::Unsupported)
    }
    /// Original preparation correlation, selected by immutable stable identity.
    /// No current owner, live lease, factory or authority is read or granted.
    async fn lookup_preparation(
        &self,
        _session: &SessionId,
        _key: &crate::session_owner::preparation::PreparationKey,
    ) -> Result<
        Option<crate::session_owner::preparation::PreparationReconciliation>,
        crate::session_owner::SessionOwnerError,
    > {
        Err(crate::session_owner::SessionOwnerError::Unsupported)
    }
    /// Whether the native adapter can own an initial outer execution worker.
    fn supports_execution_drivers(&self) -> bool {
        false
    }
    /// Atomically register and begin an exact first prepared-admission driver.
    /// Original replay returns metadata and never a dispatch authority.
    async fn register_execution_driver(
        &self,
        _request: crate::session_owner::driver::RegisterExecutionDriver,
    ) -> Result<
        crate::session_owner::driver::ExecutionDriverOutcome,
        crate::session_owner::SessionOwnerError,
    > {
        Err(crate::session_owner::SessionOwnerError::Unsupported)
    }
    /// Whether exact prepared noninitial native claims can own distinct workers.
    fn supports_successor_execution_drivers(&self) -> bool {
        false
    }
    /// Pure origin support, suitable for checking before factories. Fresh native
    /// registration still independently authenticates every original source.
    fn supports_successor_execution_driver_origin(
        &self,
        origin: crate::session_owner::driver::SuccessorExecutionDriverOrigin,
    ) -> bool {
        use crate::session_owner::driver::SuccessorExecutionDriverOrigin as Origin;
        matches!(origin, Origin::AdmissionRecovery | Origin::ExpiredRun)
            && self.supports_successor_execution_drivers()
    }
    /// Atomically Reserve/Begin one actual noninitial native claim's worker.
    /// Original receipt replay is metadata only and never remints dispatch authority.
    async fn register_successor_execution_driver(
        &self,
        _request: crate::session_owner::driver::RegisterSuccessorExecutionDriver,
    ) -> Result<
        crate::session_owner::driver::SuccessorExecutionDriverOutcome,
        crate::session_owner::SessionOwnerError,
    > {
        Err(crate::session_owner::SessionOwnerError::Unsupported)
    }
    /// Whether independently linked preparation resource capacity is durable.
    fn supports_preparation_resources(&self) -> bool {
        false
    }

    /// Exact preparation-linked session dependency registration/access. Legacy
    /// adapters must refuse before a service factory or generation exposure.
    fn supports_session_service_dependencies(&self) -> bool {
        false
    }

    async fn register_session_service_dependency(
        &self,
        _request: crate::session_owner::service::RegisterSessionServiceDependency,
    ) -> Result<
        crate::session_owner::service::SessionServiceDependencyOutcome,
        crate::session_owner::SessionOwnerError,
    > {
        Err(crate::session_owner::SessionOwnerError::Unsupported)
    }

    /// Pure coherent validation of a later preparation and an already-live
    /// original dependency. Success grants no authority or fresh dispatch.
    async fn check_session_service_dependency_access(
        &self,
        _request: crate::session_owner::service::SessionServiceDependencyAccess,
    ) -> Result<(), crate::session_owner::SessionOwnerError> {
        Err(crate::session_owner::SessionOwnerError::Unsupported)
    }
    /// Atomically authenticate the exact root and reserve/begin its independent
    /// resource owner before factory dispatch. An identical retry grants nothing.
    async fn retain_preparation_resources(
        &self,
        _grant: crate::session_owner::preparation::PreparationGrant,
    ) -> Result<
        crate::session_owner::preparation::PreparationResourceOutcome,
        crate::session_owner::SessionOwnerError,
    > {
        Err(crate::session_owner::SessionOwnerError::Unsupported)
    }
    /// Commit a joined preparation and its native successor atomically. The
    /// original operation input, digest and revision remain unchanged.
    async fn complete_preparation(
        &self,
        _joined: crate::session_owner::preparation::JoinedPreparation,
    ) -> Result<
        crate::session_owner::preparation::PreparationCompletion,
        crate::session_owner::preparation::PreparationCommitError,
    > {
        Err(StoreError::HostUnsupported.into())
    }
    /// Durable signal request/observation support, independent of local registries.
    fn supports_session_owner_cancellation(&self) -> bool {
        false
    }
    /// Record one exact existing-owner cancellation intent. No execution grant.
    async fn request_session_owner_cancel(
        &self,
        _request: crate::session_owner::RequestSessionOwnerCancel,
    ) -> Result<crate::session_owner::SessionOwnerOutcome, crate::session_owner::SessionOwnerError>
    {
        Err(crate::session_owner::SessionOwnerError::Unsupported)
    }
    /// Acknowledge the actual local signal; expiry alone cannot imply observation.
    async fn observe_session_owner_cancel(
        &self,
        _request: crate::session_owner::ObserveSessionOwnerCancel,
        _authority: crate::session_owner::SessionOwnerAuthority,
    ) -> Result<crate::session_owner::SessionOwnerOutcome, crate::session_owner::SessionOwnerError>
    {
        Err(crate::session_owner::SessionOwnerError::Unsupported)
    }
    /// Authenticated current intent only, including an already observed request.
    /// An expired unchanged authority can read it without renewing/reviving work.
    async fn pending_session_owner_cancel(
        &self,
        _authority: &crate::session_owner::SessionOwnerAuthority,
    ) -> Result<
        Option<crate::session_owner::SessionOwnerCancellation>,
        crate::session_owner::SessionOwnerError,
    > {
        Err(crate::session_owner::SessionOwnerError::Unsupported)
    }
    /// Typed registered dispatch and observed-resource-join support. Legacy
    /// owner receipts alone do not advertise this stronger protocol.
    fn supports_owned_scopes(&self) -> bool {
        false
    }
    /// Activate only an already registered exact local gate. Fresh Applied is
    /// the sole source of a dispatch permit; replay and unknown ACK never grant.
    async fn activate_owned_scope(
        &self,
        request: crate::session_owner::owned_scope::OwnedScopeActivation,
        authority: crate::session_owner::SessionOwnerAuthority,
        parent: Option<crate::session_owner::owned_scope::OwnedScopeParentAuthority>,
    ) -> Result<
        crate::session_owner::owned_scope::OwnedScopeActivationOutcome,
        crate::session_owner::SessionOwnerError,
    > {
        use crate::session_owner::owned_scope::{OwnedDispatchPermit, OwnedScopeActivationOutcome};
        use crate::session_owner::{
            SessionOwnerError as Error, SessionOwnerOperation, SessionOwnerOutcome,
            SessionOwnerState,
        };
        if !self.supports_owned_scopes() {
            return Err(Error::Unsupported);
        }
        let digest = request.fingerprint()?;
        let result = self
            .publish_owned_scope_activation(request.clone(), authority, parent)
            .await?;
        let (receipt, fresh) = match result {
            SessionOwnerOutcome::Applied {
                receipt,
                authority: None,
            } => (receipt, true),
            SessionOwnerOutcome::Replayed(receipt) => (receipt, false),
            SessionOwnerOutcome::Applied { .. } => return Err(Error::UnknownStoreOutcome),
        };
        if receipt.operation != SessionOwnerOperation::ActivateScope
            || receipt.operation_id != *request.operation_id()
            || receipt.session_id != *request.session_id()
            || receipt.owner.target != *request.target()
            || receipt.owner.state != SessionOwnerState::Active
            || receipt.semantic_digest != digest
        {
            return Err(Error::UnknownStoreOutcome);
        }
        Ok(if fresh {
            OwnedScopeActivationOutcome::Applied {
                receipt,
                permit: Box::new(OwnedDispatchPermit::applied(request)),
            }
        } else {
            OwnedScopeActivationOutcome::Replayed(receipt)
        })
    }
    /// Trusted Store adapter SPI: atomically authenticate original Reserve/Begin,
    /// exact current authority/lease/cancellation and native parent, then publish
    /// activation and its public revision. Only the actual fresh commit returns
    /// Applied. Replays are metadata; uncertain commits must return an error.
    /// The default activation wrapper alone mints a fresh local dispatch permit.
    async fn publish_owned_scope_activation(
        &self,
        _request: crate::session_owner::owned_scope::OwnedScopeActivation,
        _authority: crate::session_owner::SessionOwnerAuthority,
        _parent: Option<crate::session_owner::owned_scope::OwnedScopeParentAuthority>,
    ) -> Result<crate::session_owner::SessionOwnerOutcome, crate::session_owner::SessionOwnerError>
    {
        Err(crate::session_owner::SessionOwnerError::Unsupported)
    }
    /// Report exact observed main and sealed resource joins. Receipt replay
    /// never grants work and no lease expiry, timeout or Drop supplies proof.
    async fn join_owned_scope(
        &self,
        _proof: crate::session_owner::owned_scope::JoinedOwnedScope,
        _authority: crate::session_owner::SessionOwnerAuthority,
    ) -> Result<crate::session_owner::SessionOwnerOutcome, crate::session_owner::SessionOwnerError>
    {
        Err(crate::session_owner::SessionOwnerError::Unsupported)
    }
    async fn mutate_session_owner(
        &self,
        _request: crate::session_owner::MutateSessionOwner,
        _authority: Option<crate::session_owner::SessionOwnerAuthority>,
    ) -> Result<crate::session_owner::SessionOwnerOutcome, crate::session_owner::SessionOwnerError>
    {
        Err(crate::session_owner::SessionOwnerError::Unsupported)
    }
    async fn get_session_owner(
        &self,
        _session: &SessionId,
        _id: &crabber_core::SessionOwnerId,
    ) -> Result<crate::session_owner::SessionOwnerSnapshot, crate::session_owner::SessionOwnerError>
    {
        Err(crate::session_owner::SessionOwnerError::Unsupported)
    }
    /// Original metadata only; never dispatches or replaces private authority.
    async fn lookup_session_owner(
        &self,
        _session: &SessionId,
        _id: &crabber_core::SessionOwnerOperationId,
    ) -> Result<
        Option<crate::session_owner::SessionOwnerReceipt>,
        crate::session_owner::SessionOwnerError,
    > {
        Err(crate::session_owner::SessionOwnerError::Unsupported)
    }

    /// Pure guarded erasure. Refusal does not cancel or drain any work. Native
    /// implementations must prove ownership coverage and physical joins before
    /// purging history atomically with a permanent, minimal deletion receipt.
    async fn delete_session(
        &self,
        _request: crate::DeleteSession,
    ) -> Result<crate::DeletionOutcome, crate::DeletionError> {
        Err(crate::DeletionError::Unsupported)
    }
    /// Original minimal deletion metadata, including after host shutdown.
    async fn lookup_session_deletion(
        &self,
        _session: &SessionId,
        _id: &crabber_core::ControlId,
    ) -> Result<Option<crate::DeletionReceipt>, crate::DeletionError> {
        Err(crate::DeletionError::Unsupported)
    }
    /// Distinguish authoritative erasure from absence without reading history.
    async fn session_disposition(
        &self,
        _session: &SessionId,
    ) -> Result<crate::SessionDisposition, crate::DeletionError> {
        Err(crate::DeletionError::Unsupported)
    }

    /// Atomically copy an explicitly selected closed conversation prefix.
    /// Imported calls are presentation records and never acquire execution
    /// authority. Custom stores must implement the complete guarded contract.
    async fn fork_session(
        &self,
        _request: crate::ForkSession,
    ) -> Result<crate::ForkOutcome, crate::ForkError> {
        Err(crate::ForkError::Unsupported)
    }
    /// Original fork metadata only, including after source archive or erasure.
    async fn lookup_session_fork(
        &self,
        _target: &SessionId,
        _id: &crabber_core::ControlId,
    ) -> Result<Option<crate::ForkReceipt>, crate::ForkError> {
        Err(crate::ForkError::Unsupported)
    }

    /// Pure host metadata control; never initializes runtime/provider/tool code.
    async fn manage_session(
        &self,
        _request: crate::ManageSession,
    ) -> Result<crate::ManagementReceipt, crate::ManagementError> {
        Err(crate::ManagementError::Unsupported)
    }
    async fn lookup_session_management(
        &self,
        _session: &SessionId,
        _id: &crabber_core::ControlId,
    ) -> Result<Option<crate::ManagementReceipt>, crate::ManagementError> {
        Err(crate::ManagementError::Unsupported)
    }
    async fn session_summary(
        &self,
        _session: &SessionId,
    ) -> Result<crate::SessionSummary, crate::ManagementError> {
        Err(crate::ManagementError::Unsupported)
    }
    /// Latest run in native admission order with a captured public revision.
    /// Bounds the complete metadata JSON before materialization, capped at 8 KiB.
    /// SQL formatting overhead may conservatively require a larger byte budget.
    /// Native stores bound projection before transfer/cloning and never decode
    /// a private Run for this read. Custom stores fail closed by default.
    async fn public_run_state(
        &self,
        _session: &SessionId,
        _max_bytes: usize,
    ) -> Result<crate::PublicRunOutcome, StoreError> {
        Err(StoreError::SnapshotUnsupported)
    }
    async fn list_sessions(
        &self,
        _request: crate::ListSessions,
    ) -> Result<crate::SessionListPage, crate::ManagementError> {
        Err(crate::ManagementError::Unsupported)
    }

    /// Bounded, coherent current approval metadata; never a dispatch capability.
    async fn approval_snapshot(
        &self,
        _session: &SessionId,
        _run: &RunId,
    ) -> Result<Option<crate::ApprovalSnapshot>, crate::ApprovalError> {
        Err(crate::ApprovalError::Unsupported)
    }
    /// Reconcile an original control key without restoring a plan or owner.
    /// Authenticate the exact original frozen plan without reading a mutable Run,
    /// current answers, checkpoints, owner tokens, or initializing factories.
    async fn original_approval_plan(
        &self,
        _session: &SessionId,
        _run: &RunId,
        _target: &crate::ContinuationTarget,
    ) -> Result<crabber_core::InputFingerprint, crate::ApprovalError> {
        Err(crate::ApprovalError::Unsupported)
    }
    async fn lookup_approval(
        &self,
        _session: &SessionId,
        _id: &crabber_core::ControlId,
    ) -> Result<Option<crate::ApprovalReceipt>, crate::ApprovalError> {
        Err(crate::ApprovalError::Unsupported)
    }
    /// Host-authorized response. Identity/policy assertion is recorded, not authenticated.
    async fn answer_approval(
        &self,
        _answer: crate::ApprovalAnswer,
    ) -> Result<crate::ApprovalReceipt, crate::ApprovalError> {
        Err(crate::ApprovalError::Unsupported)
    }
    /// Reclaim only a positively Unstarted consumed approval after both leases expire.
    /// A fresh application issues new private authority; replay is metadata only.
    async fn reclaim_approval_continuation(
        &self,
        _request: crate::ApprovalContinuationRecovery,
    ) -> Result<crate::ApprovalClaimOutcome, crate::ApprovalError> {
        Err(crate::ApprovalError::Unsupported)
    }
    /// Explicitly cancel or expire a fully quiescent waiting pre-effect batch.
    /// Atomically records selected error results, terminal run outcome and barrier
    /// release. Claimed, started or uncertain work is ineligible. No dispatch.
    async fn terminate_approval(
        &self,
        _termination: crate::ApprovalTermination,
    ) -> Result<crate::ApprovalReceipt, crate::ApprovalError> {
        Err(crate::ApprovalError::Unsupported)
    }
    /// Atomically consume the complete accepted set and reserve its next attempt.
    /// Only a fresh result grants native ownership; replay returns metadata.
    async fn claim_approval_continuation(
        &self,
        _claim: crate::ApprovalContinuationClaim,
    ) -> Result<crate::ApprovalClaimOutcome, crate::ApprovalError> {
        Err(crate::ApprovalError::Unsupported)
    }

    /// Pure capability check for native attempt reservation, one-shot Begin and
    /// atomic logical run conclusion. Custom adapters must opt in only when all
    /// of those operations (including `ReserveBound`'s atomic pristine-admission
    /// guard) and metadata reads implement the native contract.
    fn supports_owned_attempts(&self) -> bool {
        false
    }
    /// Pure capability declaration for native phase claim/start/settlement and
    /// receipt reads. Never opens a transaction or probes database health.
    fn supports_owned_tool_phases(&self) -> bool {
        false
    }
    async fn frontend_call_origin(
        &self,
        _session: &SessionId,
        _run: &RunId,
        _call: &ToolCallId,
    ) -> Result<Option<crate::FrontendCallOrigin>, crate::ToolContinuationError> {
        Err(crate::ToolContinuationError::Unsupported)
    }
    async fn lookup_frontend_staging(
        &self,
        _session: &SessionId,
        _id: &crabber_core::ControlId,
    ) -> Result<Option<crate::FrontendStagingReceipt>, crate::ToolContinuationError> {
        Err(crate::ToolContinuationError::Unsupported)
    }
    /// Explicit external frontend result continuations require authoritative
    /// staging, complete-set claims and the shared native ownership contract.
    fn supports_frontend_tools(&self) -> bool {
        false
    }
    /// Atomically consume the complete authenticated external result set and
    /// reserve fresh native result-processing authority. Replays are metadata.
    async fn claim_frontend_continuation(
        &self,
        _claim: crate::FrontendContinuationClaim,
    ) -> Result<crate::ToolContinuationClaimOutcome, crate::ToolContinuationError> {
        Err(crate::ToolContinuationError::Unsupported)
    }
    /// Rotate authority only while every reserved result phase is still Claimed
    /// and the parent is positively Unstarted. Started protection is uncertain.
    async fn recover_frontend_continuation(
        &self,
        _request: crate::FrontendContinuationRecovery,
    ) -> Result<crate::ToolContinuationClaimOutcome, crate::ToolContinuationError> {
        Err(crate::ToolContinuationError::Unsupported)
    }
    /// Pure declaration for durable prepared-batch approvals and their controls,
    /// including bounded original-plan provenance reads for pure reconciliation.
    fn supports_tool_continuations(&self) -> bool {
        false
    }
    async fn tool_continuation_snapshot(
        &self,
        _session: &SessionId,
        _run: &RunId,
    ) -> Result<Option<crate::ToolContinuationSnapshot>, crate::ToolContinuationError> {
        Err(crate::ToolContinuationError::Unsupported)
    }
    /// Authenticate the exact original frozen plan without reading a mutable Run,
    /// current answers, checkpoints, owner tokens, or initializing factories.
    async fn original_tool_continuation_plan(
        &self,
        _session: &SessionId,
        _run: &RunId,
        _target: &crate::ContinuationTarget,
    ) -> Result<crabber_core::InputFingerprint, crate::ToolContinuationError> {
        Err(crate::ToolContinuationError::Unsupported)
    }
    async fn lookup_tool_continuation(
        &self,
        _session: &SessionId,
        _id: &crabber_core::ControlId,
    ) -> Result<Option<crate::ToolContinuationReceipt>, crate::ToolContinuationError> {
        Err(crate::ToolContinuationError::Unsupported)
    }
    async fn answer_tool_interaction(
        &self,
        _answer: crate::ToolInteractionAnswer,
    ) -> Result<crate::ToolContinuationReceipt, crate::ToolContinuationError> {
        Err(crate::ToolContinuationError::Unsupported)
    }
    async fn recover_tool_continuation(
        &self,
        _request: crate::ToolContinuationRecovery,
    ) -> Result<crate::ToolContinuationClaimOutcome, crate::ToolContinuationError> {
        Err(crate::ToolContinuationError::Unsupported)
    }
    async fn claim_tool_continuation(
        &self,
        _claim: crate::ToolContinuationClaim,
    ) -> Result<crate::ToolContinuationClaimOutcome, crate::ToolContinuationError> {
        Err(crate::ToolContinuationError::Unsupported)
    }
    fn supports_pre_effect_approvals(&self) -> bool {
        false
    }

    /// Atomic admission plus retained provider-binding evidence. No fallback to
    /// legacy admission is safe: custom adapters must implement this capability,
    /// enforce an expected revision under the same mutation lock, and preserve
    /// every typed message and original envelope. Unsupported codecs must fail.
    async fn admit_bound_run(
        &self,
        _request: crate::BoundAdmitRequest,
    ) -> Result<AdmitOutcome, StoreError> {
        Err(StoreError::HostUnsupported)
    }
    /// Matching replay checks the original capsule before current admission guards.
    async fn admit_bound_keyed_run(
        &self,
        _request: crate::BoundKeyedAdmitRequest,
    ) -> Result<KeyedAdmitOutcome, StoreError> {
        Err(StoreError::HostUnsupported)
    }
    /// Scoped private evidence, with no execution authority or factory restoration.
    async fn execution_binding(
        &self,
        _session: &SessionId,
        _run: &RunId,
    ) -> Result<Option<crate::RetainedExecutionBinding>, StoreError> {
        Err(StoreError::HostUnsupported)
    }

    /// Experimental native phase metadata; contains no worker authority or output.
    async fn tool_phase(
        &self,
        _run: &RunId,
        _id: &crabber_core::PhaseId,
    ) -> Result<crate::VersionedPhase, crate::PhaseError> {
        Err(crate::PhaseError::Unsupported)
    }
    /// Mark an exact phase uncertain without acquiring worker authority.
    async fn mark_phase_uncertain(
        &self,
        _request: crate::PhaseUncertainty,
    ) -> Result<crate::PhaseControlOutcome, crate::PhaseError> {
        Err(crate::PhaseError::Unsupported)
    }
    /// Reconcile exact resource cleanup; supplied evidence is a host assertion.
    async fn record_phase_cleanup(
        &self,
        _request: crate::PhaseCleanup,
    ) -> Result<crate::PhaseControlOutcome, crate::PhaseError> {
        Err(crate::PhaseError::Unsupported)
    }
    /// Read bounded coherent phase metadata without output bodies or tokens.
    async fn list_tool_phases(
        &self,
        _request: crate::PhasePageRequest,
    ) -> Result<crate::PhasePage, crate::PhaseError> {
        Err(crate::PhaseError::Unsupported)
    }
    /// Reconcile an already uncertain external outcome using the host's asserted
    /// authority and protected output. Preserves cleanup and ownership, including
    /// after lease expiry; grants no worker authority. Identical retries replay.
    async fn resolve_uncertain_phase(
        &self,
        _resolution: crate::PhaseResolution,
    ) -> Result<crate::PhaseResolutionOutcome, crate::PhaseError> {
        Err(crate::PhaseError::Unsupported)
    }
    /// Reconcile the original phase operation without granting dispatch authority.
    async fn lookup_phase_mutation(
        &self,
        _session: &SessionId,
        _id: &crabber_core::ControlId,
    ) -> Result<Option<crate::PhaseReceipt>, crate::PhaseError> {
        Err(crate::PhaseError::Unsupported)
    }
    /// Experimental coherent attempt metadata; never returns worker authority.
    async fn execution_attempt(
        &self,
        _run: &RunId,
        _id: &crabber_core::AttemptId,
    ) -> Result<crate::VersionedAttempt, crate::AttemptError> {
        Err(crate::AttemptError::Unsupported)
    }
    /// Read the latest attempt and coherent public revision, without authority.
    /// None means no reservation was visible; it does not disprove an in-flight
    /// mutation. A missing run is `NotFound`. Custom stores fail closed.
    async fn latest_execution_attempt(
        &self,
        _run: &RunId,
    ) -> Result<Option<crate::VersionedAttempt>, crate::AttemptError> {
        Err(crate::AttemptError::Unsupported)
    }
    /// Metadata-only reconciliation of the original operation key.
    /// Absence does not prove an in-flight operation cannot later commit.
    async fn lookup_attempt_mutation(
        &self,
        _session: &SessionId,
        _id: &crabber_core::ControlId,
    ) -> Result<Option<crate::AttemptReceipt>, crate::AttemptError> {
        Err(crate::AttemptError::Unsupported)
    }

    /// Read one coherent bounded public state/revision snapshot. Experimental.
    async fn public_state(
        &self,
        _session: &SessionId,
    ) -> Result<crate::PublicStateSnapshot, crate::PublicStateError> {
        Err(crate::PublicStateError::Unsupported)
    }
    /// Metadata-only reconciliation, without granting execution authority.
    /// Absence does not prove an in-flight operation cannot subsequently commit.
    async fn lookup_public_state_mutation(
        &self,
        _session: &SessionId,
        _id: &crabber_core::ControlId,
    ) -> Result<Option<crate::PublicStateReceipt>, crate::PublicStateError> {
        Err(crate::PublicStateError::Unsupported)
    }

    /// Read the opaque revision of public session state/history. Values may have
    /// gaps; never infer an operation count. Lease/token refresh is private and
    /// does not advance it. Unstable until durable-host qualification.
    /// This scalar alone does not validate a separately captured history snapshot.
    /// Custom adapters must implement/forward the full mutation contract.
    async fn session_revision(
        &self,
        _session: &SessionId,
    ) -> Result<crabber_core::SessionRevision, StoreError> {
        Err(StoreError::HostUnsupported)
    }
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
    /// Loads sensitive adapter evidence; never expose it through metadata lookup.
    async fn load_admission_execution(
        &self,
        _session: &SessionId,
        _key: &AdmissionKey,
    ) -> Result<Option<crate::AdmissionExecutionRecord>, crate::AdmissionExecutionError> {
        Err(crate::AdmissionExecutionError::Unsupported)
    }
    /// Reads retained execution state without granting ownership. Adapters retaining
    /// admission evidence and wrappers around them must implement/forward this
    /// classification so generic recovery can skip Unstarted before acquiring a plan.
    async fn admission_execution_state(
        &self,
        _run: &RunId,
    ) -> Result<Option<crate::AdmissionExecutionState>, StoreError> {
        Ok(None)
    }
    /// Original generic-claim metadata only, reconciled by exact original
    /// issuer sequence before current CAS/lease checks. Authenticates the actual
    /// successor and immutable preparation/native sources; never returns a fence.
    async fn lookup_expired_run_preparation(
        &self,
        _session: &SessionId,
        _run: &RunId,
        _issuance: u64,
    ) -> Result<
        Option<crate::session_owner::preparation::ExpiredRunReceipt>,
        crate::session_owner::SessionOwnerError,
    > {
        Err(crate::session_owner::SessionOwnerError::Unsupported)
    }
    /// Coherently authenticates an expired generic Run's original suspension
    /// or protected keyed execution source, current native issuer and frozen
    /// binding before factories. This observation grants no execution authority.
    /// Reservation and completion must revalidate the exact original source.
    /// Unsupported adapters must fail before any restore or factory effect.
    async fn inspect_expired_run_preparation(
        &self,
        _request: crate::session_owner::preparation::InspectExpiredRunPreparation,
    ) -> Result<
        crate::session_owner::preparation::ExpiredRunPreparation,
        crate::session_owner::SessionOwnerError,
    > {
        Err(crate::session_owner::SessionOwnerError::Unsupported)
    }
    /// Coherent original recovery source/eligibility inspection, with no grant.
    /// Authenticate original admission Init/receipt/capsule and actual current
    /// native issuance/fence, then capture public revision under the same gate.
    /// Store-clock expiry and Unstarted eligibility precede any factory work;
    /// reservation/completion must revalidate this exact source. Never derive
    /// an issuance sequence from caller labels or silently fall back to legacy.
    async fn inspect_admission_recovery(
        &self,
        _request: crate::ClaimUnstartedAdmissionRequest,
    ) -> Result<
        crate::session_owner::preparation::AdmissionRecoveryPreparation,
        crate::AdmissionExecutionError,
    > {
        Err(crate::AdmissionExecutionError::Unsupported)
    }
    /// Claims only retained Unstarted evidence under the run ownership lock.
    async fn claim_unstarted_admission(
        &self,
        _request: crate::ClaimUnstartedAdmissionRequest,
    ) -> Result<crate::ClaimedAdmission, crate::AdmissionExecutionError> {
        Err(crate::AdmissionExecutionError::Unsupported)
    }
    /// Legacy-only settlement: native attempts/phases and active approval barriers
    /// require explicit reconciliation controls and are refused without mutation.
    /// Atomically revoke the expected fence and interrupt every unfinished tool
    /// and the run, retaining history, usage, receipts and unrelated inbox rows.
    /// Expiry must be checked under the transaction lock using the store clock.
    /// A host-stopped assertion must match both owner and token. Retain durable
    /// abandonment evidence for identical-request replay, distinct from ordinary
    /// interruption. An error never implies settlement succeeded. Custom stores
    /// fail closed until they implement the complete transactional contract.
    async fn abandon_run(
        &self,
        _request: crabber_core::AbandonRequest,
    ) -> Result<crabber_core::AbandonOutcome, crabber_core::AbandonError> {
        Err(crabber_core::AbandonError::Unsupported)
    }
    async fn execution(&self, fence: RunFence) -> Result<Box<dyn ExecutionStore>, StoreError>;
    async fn get_session(&self, id: &SessionId) -> Result<Option<Session>, StoreError>;
    async fn get_run(&self, id: &RunId) -> Result<Option<Run>, StoreError>;
    /// Bounded all-history snapshot, in message append order then tool creation
    /// order. Includes hidden epochs and pending/settled tool records unchanged.
    /// Relations may span pages; preserve IDs, parent IDs and call IDs and assemble
    /// all pages before projecting. A successful complete read has one immutable
    /// high-water, captured atomically with history. Reads must measure limits
    /// before cloning/materializing records, never load the whole history first.
    /// Concurrent appends remain outside the snapshot; mutable captured records
    /// must be frozen or cause explicit Invalidated/restart, never silently change.
    /// Custom stores fail closed until they implement this contract.
    async fn snapshot(
        &self,
        _request: crate::SnapshotRequest,
    ) -> Result<crate::SnapshotOutcome, StoreError> {
        Err(StoreError::SnapshotUnsupported)
    }
    /// Public history with provider state and reasoning recursively omitted.
    /// Uses the raw snapshot's pre-allocation limits and authenticated history
    /// boundary. It does not capture session/control metadata; use the captured
    /// public revision when assembling a coherent aggregate view.
    async fn public_history(
        &self,
        request: crate::SnapshotRequest,
    ) -> Result<crate::PublicHistoryOutcome, StoreError> {
        Ok(self.snapshot(request).await?.into())
    }
    /// Unbounded runtime context projection. Embeddings should use `snapshot`.
    async fn list_messages(
        &self,
        id: &SessionId,
        epoch: Option<EpochId>,
    ) -> Result<Vec<Message>, StoreError>;
    /// Unbounded: returns the append-only message history, including messages hidden by
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
    /// Bundled stores return [`StoreError::SessionDeleted`] after permanent
    /// erasure, while missing sessions have empty extension state.
    async fn get_extension_state(
        &self,
        extension_id: &str,
        session: &SessionId,
    ) -> Result<BTreeMap<String, String>, StoreError>;
    /// Bundled stores reject permanently erased scopes before validating the
    /// message. A missing session returns [`StoreError::NotFound`].
    async fn enqueue_inbox(
        &self,
        session: &SessionId,
        kind: InboxKind,
        message: Message,
    ) -> Result<(), StoreError>;
}

/// Fenced write operations. Legacy record operations follow the storable-record
/// contract documented on [`Store`]. Typed public state uses its own bounded,
/// lossless JSON contract, including embedded NUL values.
#[async_trait]
pub trait ExecutionStore: Send + Sync {
    async fn stage_frontend_call(
        &self,
        _request: crate::FrontendCallStaging,
        _authority: crate::AttemptAuthority,
    ) -> Result<crate::FrontendStagingReceipt, crate::ToolContinuationError> {
        Err(crate::ToolContinuationError::Unsupported)
    }

    /// Commit explicit checkpoints only after the actual phase/attempt resource joins.
    async fn suspend_tool_continuation(
        &self,
        _batch: crate::ToolContinuationSuspension,
        _authority: crate::ToolSuspensionAuthority,
    ) -> Result<crate::ToolContinuationReceipt, crate::ToolContinuationError> {
        Err(crate::ToolContinuationError::Unsupported)
    }

    /// Observe cancellation under the live worker fence. With a turn, atomically
    /// append all accepted steers before constructing the next provider context.
    async fn observe_run_controls(
        &self,
        _turn: Option<&crabber_core::TurnId>,
    ) -> Result<crate::RunControlBoundary, crate::RunControlError> {
        Err(crate::RunControlError::Unsupported)
    }

    /// Atomically acknowledge native phase Begin and its logical running source.
    /// Only a fresh Applied result permits dispatch under the original authority.
    async fn start_tool_phase(
        &self,
        _start: crate::PhaseStart,
        _authority: Option<crate::PhaseAuthority>,
    ) -> Result<crate::PhaseMutationOutcome, crate::PhaseError> {
        Err(crate::PhaseError::Unsupported)
    }
    /// Freeze the whole prepared batch and its pending approvals, then conclude
    /// this attempt as suspended in the SAME transaction. Caller first joins
    /// its resources; no effectful phase in this batch may have started.
    async fn suspend_for_approval(
        &self,
        _batch: crate::ApprovalSuspension,
        _authority: crate::AttemptAuthority,
    ) -> Result<crate::ApprovalReceipt, crate::ApprovalError> {
        Err(crate::ApprovalError::Unsupported)
    }

    /// Atomically conclude an owned attempt and its terminal/suspended logical
    /// run, with complete cleanup, events and receipt. The owner must first join
    /// all attempt resources. `PendingInput` leaves every part unchanged.
    async fn conclude_attempt(
        &self,
        _conclusion: crate::AttemptConclusion,
        _authority: Option<crate::AttemptAuthority>,
    ) -> Result<crate::AttemptMutationOutcome, crate::AttemptError> {
        Err(crate::AttemptError::Unsupported)
    }

    /// Atomically complete a started owned phase, record the worker's assertion
    /// of quiescence, settle its logical call, append protected result history
    /// and events, and retain its receipt. Invoke only after all phase resources
    /// have actually joined. Replay grants no authority and appends nothing.
    async fn settle_tool_phase(
        &self,
        _settlement: crate::PhaseSettlement,
        _authority: Option<crate::PhaseAuthority>,
    ) -> Result<crate::PhaseMutationOutcome, crate::PhaseError> {
        Err(crate::PhaseError::Unsupported)
    }

    /// Experimental atomic phase transition. Only a fresh Applied Begin permits
    /// dispatch. Outcome persistence does not imply logical call settlement or
    /// prove worker quiescence. Cleanup requires actual owned-task completion.
    async fn mutate_phase(
        &self,
        _request: crate::PhaseMutation,
        _authority: Option<crate::PhaseWriteAuthority>,
    ) -> Result<crate::PhaseMutationOutcome, crate::PhaseError> {
        Err(crate::PhaseError::Unsupported)
    }
    /// Experimental atomic attempt transition with event, revision and receipt.
    /// `ReserveBound` verifies the retained typed input and absence of execution
    /// history in this transaction; a metadata read of None is not permission.
    /// Later generations additionally recheck current run-wide quiescence.
    /// Only a fresh Applied Begin permits attempt work. Tool dispatch additionally
    /// requires independent phase authority. Replay never grants dispatch.
    async fn mutate_attempt(
        &self,
        _request: crate::AttemptMutation,
        _authority: Option<crate::AttemptAuthority>,
    ) -> Result<crate::AttemptMutationOutcome, crate::AttemptError> {
        Err(crate::AttemptError::Unsupported)
    }

    /// Atomically replace public state/activity, commit its event/revision/receipt.
    /// Exact receipt replay precedes current ownership and revision checks.
    /// Experimental. `UnknownStoreOutcome` requires receipt reconciliation using
    /// the original operation ID and semantics; never invent a new ID to retry.
    async fn mutate_public_state(
        &self,
        _mutation: crate::PublicStateMutation,
    ) -> Result<crate::PublicStateReceipt, crate::PublicStateError> {
        Err(crate::PublicStateError::Unsupported)
    }

    /// One-shot fenced start, committed before any execution effects. An unknown
    /// response must never be retried as permission to execute.
    async fn begin_admission_execution(&self) -> Result<(), crate::AdmissionExecutionError> {
        Err(crate::AdmissionExecutionError::Unsupported)
    }
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
    /// Legacy boundary for runs without native attempt/phase evidence or a
    /// continuation barrier. Native workers must use `conclude_attempt`.
    async fn pause_run(
        &self,
        checkpoint: serde_json::Value,
        event: EventRecord,
    ) -> Result<(), StoreError>;
    /// Legacy terminal boundary; refuses native execution evidence and
    /// continuation barriers instead of bypassing their result/cleanup proof.
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
