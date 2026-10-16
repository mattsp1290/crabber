//! Physical task completion is carried with its actual returned native intent.
use super::ReservePreparation;
use crate::session_owner::{SessionOwnerAuthority, SessionOwnerError};
use crate::{AdmitOutcome, BoundAdmitRequest, BoundKeyedAdmitRequest, KeyedAdmitOutcome};

/// Fresh private preparation capacity. A Store rechecks Active state, token,
/// generation, original reservation and actual revision lineage at completion.
#[derive(Clone)]
pub struct PreparationGrant {
    pub(crate) reservation: ReservePreparation,
    pub(crate) authority: SessionOwnerAuthority,
    pub(crate) resources: Option<SessionOwnerAuthority>,
}
impl PreparationGrant {
    /// Original immutable reservation for trusted adapter verification. This
    /// grants no replacement authority and cannot alter an observed operation.
    #[must_use]
    pub fn original_reservation(&self) -> &ReservePreparation {
        &self.reservation
    }

    /// Original operation correlation with the actual current generation.
    /// # Errors
    /// Rejects malformed source identity. This returns metadata only.
    pub fn reconciliation(&self) -> Result<super::PreparationReconciliation, SessionOwnerError> {
        let mut metadata = self.reservation.reconciliation()?;
        metadata.owner = self.authority.target().clone();
        Ok(metadata)
    }
    /// Private retained capacity authority, never available from receipt replay.
    #[must_use]
    pub fn owner_authority(&self) -> &SessionOwnerAuthority {
        &self.authority
    }
    /// The independent resource authority, available only from a fresh linked
    /// Store commit. Receipt replay cannot reconstruct it.
    #[must_use]
    pub fn retained_resources_authority(&self) -> Option<&SessionOwnerAuthority> {
        self.resources.as_ref()
    }
    /// Trusted adapter bridge after the fresh linked child Reserve/Begin commit.
    /// This does not authenticate storage and cannot manufacture replay authority.
    /// # Errors
    /// Refuses a child for another preparation generation or session.
    pub fn with_retained_resources(
        mut self,
        authority: SessionOwnerAuthority,
    ) -> Result<Self, SessionOwnerError> {
        let link = super::PreparationResourceLink::for_grant(&self)?;
        if authority.session_id() != &link.session_id
            || authority.target().id != link.identities()?.owner
            || authority.target().generation != crabber_core::SessionOwnerGeneration::INITIAL
        {
            return Err(SessionOwnerError::InvalidRequest);
        }
        self.resources = Some(authority);
        Ok(self)
    }
    /// Trusted runtime bridge, used only after fresh Reserve and fresh Begin.
    /// Metadata replay must never be passed here as a fresh dispatch grant.
    /// This constructor does not authorize storage or execution by itself.
    /// # Errors
    /// Rejects a grant for another owner/session or an invalid reservation.
    pub fn from_fresh_begin(
        reservation: ReservePreparation,
        authority: SessionOwnerAuthority,
    ) -> Result<Self, SessionOwnerError> {
        reservation.fingerprint()?;
        if authority.session_id() != &reservation.owner.binding.session_id
            || authority.target().id != reservation.owner.binding.id
        {
            return Err(SessionOwnerError::InvalidRequest);
        }
        Ok(Self {
            reservation,
            authority,
            resources: None,
        })
    }
}

/// Native intent produced inside the retained preparation task. No native
/// operation is dispatched until the task has physically joined and its owner
/// lineage is authenticated in the committing transaction.
#[derive(Clone)]
pub enum PreparedOperation {
    Admit(Box<BoundAdmitRequest>),
    AdmitKeyed(Box<BoundKeyedAdmitRequest>),
    Queue {
        request: crate::QueueMutation,
        binding: Option<Box<BoundKeyedAdmitRequest>>,
    },
    ApprovalClaim(crate::ApprovalContinuationClaim),
    ToolClaim(crate::ToolContinuationClaim),
    FrontendClaim(crate::FrontendContinuationClaim),
    FrontendRecovery(crate::FrontendContinuationRecovery),
    ApprovalRecovery(crate::ApprovalContinuationRecovery),
    ToolRecovery(crate::ToolContinuationRecovery),
}

/// Original unkeyed admission metadata. It grants no dispatch authority.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparationAdmissionReceipt {
    pub session_id: crabber_core::SessionId,
    pub run_id: crabber_core::RunId,
    pub operation_id: crabber_core::ControlId,
}

pub enum PreparationCompletion {
    Admitted(AdmitOutcome),
    AdmissionReplayed(PreparationAdmissionReceipt),
    Keyed(KeyedAdmitOutcome),
    Queue(crate::QueueOutcome),
    Approval(crate::ApprovalClaimOutcome),
    ToolContinuation(crate::ToolContinuationClaimOutcome),
}

impl PreparationCompletion {
    /// Whether this outcome is metadata only and grants no dispatch authority.
    #[must_use]
    pub fn is_replayed(&self) -> bool {
        match self {
            Self::Admitted(_) => false,
            Self::AdmissionReplayed(_) => true,
            Self::Keyed(outcome) => matches!(outcome, KeyedAdmitOutcome::Replayed(_)),
            Self::Queue(outcome) => matches!(outcome, crate::QueueOutcome::Replayed(_)),
            Self::Approval(outcome) => matches!(outcome, crate::ApprovalClaimOutcome::Replayed(_)),
            Self::ToolContinuation(outcome) => {
                matches!(outcome, crate::ToolContinuationClaimOutcome::Replayed(_))
            }
        }
    }
}

/// Safe native-operation category; no capsule or dispatch authority.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PreparationOperationKind {
    Admission,
    KeyedAdmission,
    Queue,
    ApprovalClaim,
    ApprovalRecovery,
    ToolClaim,
    FrontendClaim,
    FrontendRecovery,
    ToolRecovery,
}

/// Opaque, non-serializable evidence of an observed task join. Its operation is
/// taken from the actual joined task result and cannot be replaced afterwards.
/// Returned resources remain retained by the supervisor until native successor
/// ownership commits, or until actual failure cleanup has joined.
#[derive(Clone)]
pub struct JoinedPreparation {
    pub(crate) grant: PreparationGrant,
    pub(crate) operation: PreparedOperation,
    registered: Option<crate::session_owner::owned_scope::OwnedScopeActivation>,
}
impl JoinedPreparation {
    /// Transfer an intent returned by the actual registered preparation child
    /// task. The registration retains its physical main-task evidence for
    /// later resource cleanup; no replacement task or reconstructed join is
    /// used. The returned value must remain retained through native completion
    /// and any independent resource handoff or cleanup.
    /// # Errors
    /// Refuses a different original child source before taking its output.
    /// A non-returning main disposition or previously transferred value grants
    /// no preparation proof. Business errors remain the original inner value.
    pub async fn observe_registered<T: Send + 'static, E: Send + 'static>(
        grant: PreparationGrant,
        task: &mut crate::session_owner::owned_scope::OwnedTaskRegistration<
            Result<(T, PreparedOperation), E>,
        >,
    ) -> Result<Result<(T, Self), E>, SessionOwnerError> {
        super::owned::verify_child_source(&grant, &task.activation().source)?;
        let registered = task.activation().clone();
        let value = task
            .take_value()
            .await?
            .ok_or(SessionOwnerError::InvalidTransition)?;
        Ok(value.map(|(retained, operation)| {
            (
                retained,
                Self {
                    grant,
                    operation,
                    registered: Some(registered),
                },
            )
        }))
    }

    /// Authenticate the exact task-returned intent against its original
    /// reservation before native replay or transactional lineage validation.
    /// Custom adapters must call this before consuming the read-only view.
    /// # Errors
    /// Rejects changed keys, scope, purpose, input, options or frozen bindings.
    pub fn verify_original_intent(&self) -> Result<(), super::PreparationCommitError> {
        if self
            .grant
            .original_reservation()
            .resource_contract
            .is_some()
        {
            let registered = self
                .registered
                .as_ref()
                .ok_or(SessionOwnerError::InvalidTransition)?;
            super::owned::verify_child_source(&self.grant, &registered.source)?;
        }
        super::intent::validate(&self.grant, &self.operation)
    }

    /// Exact local registration whose actual returned intent was observed.
    /// Metadata only; this cannot construct another joined observation.
    #[must_use]
    pub fn observed_registration(
        &self,
    ) -> Option<&crate::session_owner::owned_scope::OwnedScopeActivation> {
        self.registered.as_ref()
    }

    /// Read-only original grant for trusted storage adapters.
    #[must_use]
    pub fn observed_grant(&self) -> &PreparationGrant {
        &self.grant
    }
    /// Read-only exact operation returned by the actually joined task.
    #[must_use]
    pub fn observed_operation(&self) -> &PreparedOperation {
        &self.operation
    }

    /// Safe operation classification for adapter policy and observability.
    #[must_use]
    pub fn operation_kind(&self) -> PreparationOperationKind {
        match &self.operation {
            PreparedOperation::Admit(_) => PreparationOperationKind::Admission,
            PreparedOperation::AdmitKeyed(_) => PreparationOperationKind::KeyedAdmission,
            PreparedOperation::Queue { .. } => PreparationOperationKind::Queue,
            PreparedOperation::ApprovalClaim(_) => PreparationOperationKind::ApprovalClaim,
            PreparedOperation::ApprovalRecovery(_) => PreparationOperationKind::ApprovalRecovery,
            PreparedOperation::ToolClaim(_) => PreparationOperationKind::ToolClaim,
            PreparedOperation::FrontendClaim(_) => PreparationOperationKind::FrontendClaim,
            PreparedOperation::FrontendRecovery(_) => PreparationOperationKind::FrontendRecovery,
            PreparedOperation::ToolRecovery(_) => PreparationOperationKind::ToolRecovery,
        }
    }
    /// Trusted adapter lease policy. Lease bookkeeping is excluded from native
    /// request identity; all frozen input, binding and operation IDs stay exact.
    pub fn set_admission_lease(&mut self, lease: std::time::Duration) {
        match &mut self.operation {
            PreparedOperation::Admit(bound) => bound.request.lease = lease,
            PreparedOperation::AdmitKeyed(bound) => bound.keyed.request.lease = lease,
            PreparedOperation::Queue { .. }
            | PreparedOperation::ApprovalClaim(_)
            | PreparedOperation::FrontendClaim(_)
            | PreparedOperation::FrontendRecovery(_)
            | PreparedOperation::ToolClaim(_)
            | PreparedOperation::ApprovalRecovery(_)
            | PreparedOperation::ToolRecovery(_) => (),
        }
    }
    /// Observe the actual owned task join, preserving its returned resources.
    /// # Errors
    /// Returns a panic/cancellation as `JoinError`; business failure remains the
    /// inner error and grants no native dispatch authority.
    pub async fn observe<T, E>(
        grant: PreparationGrant,
        task: tokio::task::JoinHandle<Result<(T, PreparedOperation), E>>,
    ) -> Result<Result<(T, Self), E>, tokio::task::JoinError> {
        Ok(task.await?.map(|(retained, operation)| {
            (
                retained,
                Self {
                    grant,
                    operation,
                    registered: None,
                },
            )
        }))
    }
}
macro_rules! redacted {
    ($($ty:ty),* $(,)?) => {$(impl std::fmt::Debug for $ty {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str(concat!(stringify!($ty), "([redacted])"))
        }
    })*};
}
redacted!(
    PreparationGrant,
    PreparedOperation,
    PreparationCompletion,
    JoinedPreparation
);
