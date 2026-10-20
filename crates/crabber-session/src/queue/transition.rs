use crate::queue::{
    BoundKeyedAdmitRequest, Claim, QueueAction, QueueAuthority, QueueId, QueueMutation, QueueState,
    QueueSummary, Record, SessionId, StoreError, invalid,
};
use time::OffsetDateTime;
/// Constructed only after queue authority, FIFO and latest revision verification.
/// Native admission still checks scope, archive, barriers and active runs.
pub(crate) struct VerifiedAdmission<'a> {
    bound: &'a BoundKeyedAdmitRequest,
    owner: String,
    lease: std::time::Duration,
}
#[derive(Clone, Copy)]
pub(crate) enum AdmissionGuard<'a> {
    Ordinary,
    Queue(&'a VerifiedAdmission<'a>),
    PreparedQueue {
        queue: &'a VerifiedAdmission<'a>,
        preparation: &'a crate::session_owner::preparation::proof::VerifiedPreparation<'a>,
    },
    Preparation(&'a crate::session_owner::preparation::proof::VerifiedPreparation<'a>),
}
impl<'a> AdmissionGuard<'a> {
    pub(crate) fn owner_and_lease<'r>(
        &'r self,
        request: &'r crate::AdmitRequest,
    ) -> (&'r str, std::time::Duration) {
        match self {
            Self::Queue(proof) | Self::PreparedQueue { queue: proof, .. } => {
                (&proof.owner, proof.lease)
            }
            _ => (&request.owner, request.lease),
        }
    }
    pub fn is_ordinary(self) -> bool {
        !matches!(self, Self::Queue(_) | Self::PreparedQueue { .. })
    }
    pub(crate) fn preparation(
        self,
    ) -> Option<&'a crate::session_owner::preparation::proof::VerifiedPreparation<'a>> {
        match self {
            Self::Preparation(proof)
            | Self::PreparedQueue {
                preparation: proof, ..
            } => Some(proof),
            _ => None,
        }
    }
    #[cfg(any(feature = "postgres", feature = "sqlite"))]
    pub(crate) fn has_owned_resources(self) -> bool {
        self.preparation()
            .is_some_and(|proof| proof.resource_handoff().is_some())
    }
    pub fn expected_revision(
        self,
        original: Option<crabber_core::SessionRevision>,
    ) -> Option<crabber_core::SessionRevision> {
        match self {
            Self::Preparation(proof) => Some(proof.effective_revision),
            _ => original,
        }
    }
    pub fn verify(self, request: &crate::AdmitRequest) -> Result<(), StoreError> {
        if let Self::Preparation(proof) = self {
            proof.verify_admission(request)?;
        }
        if let Self::Queue(proof) | Self::PreparedQueue { queue: proof, .. } = self {
            let original = &proof.bound.keyed.request;
            if original.session_id != request.session_id
                || original.expected_revision != request.expected_revision
                || original.config_hash != request.config_hash
                || original.plan_fingerprint != request.plan_fingerprint
                || original.input_envelope != request.input_envelope
                || original.user_message != request.user_message
                || original.additional_messages != request.additional_messages
            {
                return Err(StoreError::AdmissionConflict);
            }
        }
        Ok(())
    }
}
impl VerifiedAdmission<'_> {
    pub fn bound_ref(&self) -> &BoundKeyedAdmitRequest {
        self.bound
    }
}
pub(crate) struct Change<'a> {
    pub record: Record,
    pub authority: Option<QueueAuthority>,
    pub admission: Option<VerifiedAdmission<'a>>,
}
fn live(
    record: &Record,
    authority: &QueueAuthority,
    now: OffsetDateTime,
) -> Result<(), StoreError> {
    let claim = record.claim.as_ref().ok_or(StoreError::Conflict)?;
    if record.summary.state != QueueState::Claimed
        || claim.authority != *authority
        || claim.expires <= now
    {
        return Err(StoreError::Conflict);
    }
    Ok(())
}
pub(crate) fn apply_guarded<'a>(
    request: &QueueMutation,
    summary: &crate::SessionSummary,
    head: Option<&QueueSummary>,
    original: Option<(Record, Option<&'a BoundKeyedAdmitRequest>)>,
    sequence: u64,
    now: OffsetDateTime,
    preparation: Option<&'a crate::session_owner::preparation::proof::VerifiedPreparation<'a>>,
) -> Result<Change<'a>, StoreError> {
    let expected = if let Some(proof) = preparation {
        proof.verify_queue(request)?;
        proof.effective_revision
    } else {
        request.expected_revision
    };
    if expected != summary.revision {
        return Err(StoreError::AdmissionRevisionConflict);
    }
    if summary.archived && !matches!(request.action, QueueAction::Cancel { .. }) {
        return Err(StoreError::Conflict);
    }
    let mut authority = None;
    let mut admission = None;
    let record = match &request.action {
        QueueAction::Enqueue { queue_id, input } => {
            if original.is_some() {
                return Err(StoreError::AdmissionConflict);
            }
            enqueue_record(request, summary, queue_id, input, sequence)?
        }
        action => {
            let (mut record, prepared_original) = original.ok_or(StoreError::NotFound)?;
            if !matches!(action, QueueAction::Cancel { .. }) && head != Some(&record.summary) {
                return Err(StoreError::Busy);
            }
            match action {
                QueueAction::Claim { owner, lease } => {
                    authority = Some(claim(
                        &mut record,
                        &request.session_id,
                        &request.id,
                        owner,
                        *lease,
                        now,
                    )?);
                }
                QueueAction::Prepare {
                    authority,
                    prepared,
                } => {
                    live(&record, authority, now)?;
                    if record.prepared.is_some() {
                        return Err(StoreError::Conflict);
                    }
                    record.verify_prepared(prepared)?;
                    record.prepare_operation = Some(request.id.clone());
                    record.summary.prepared = true;
                }
                QueueAction::Admit {
                    authority,
                    owner,
                    lease,
                } => {
                    live(&record, authority, now)?;
                    let bound = prepared_original.ok_or(StoreError::Conflict)?;
                    if record.prepared.is_none() {
                        return Err(StoreError::Conflict);
                    }
                    record.verify_prepared(bound)?;
                    admission = Some(VerifiedAdmission {
                        bound,
                        owner: owner.clone(),
                        lease: *lease,
                    });
                }
                QueueAction::Cancel { .. } => {
                    if record.summary.state == QueueState::Cancelled {
                        return Err(StoreError::Conflict);
                    }
                    if record.summary.state != QueueState::Admitted {
                        record.summary.state = QueueState::Cancelled;
                        record.claim = None;
                    }
                }
                QueueAction::Enqueue { .. } => unreachable!(),
            }
            record
        }
    };
    Ok(Change {
        record,
        authority,
        admission,
    })
}
fn enqueue_record(
    request: &QueueMutation,
    summary: &crate::SessionSummary,
    queue_id: &QueueId,
    input: &crate::QueueInput,
    sequence: u64,
) -> Result<Record, StoreError> {
    if input.request.workspace_id != summary.workspace_id
        || input.request.directory != summary.directory
    {
        return Err(StoreError::SessionIdentityMismatch);
    }
    Ok(Record {
        version: 2,
        enqueue_operation: request.id.clone(),
        prepare_operation: None,
        summary: QueueSummary {
            session_id: request.session_id.clone(),
            queue_id: queue_id.clone(),
            sequence,
            state: QueueState::Queued,
            prepared: false,
            claim_generation: 0,
            admission: None,
        },
        input: input.clone(),
        claim: None,
        prepared: None,
    })
}

fn claim(
    record: &mut Record,
    session: &SessionId,
    operation: &crabber_core::ControlId,
    owner: &str,
    lease: std::time::Duration,
    now: OffsetDateTime,
) -> Result<QueueAuthority, StoreError> {
    if record
        .claim
        .as_ref()
        .is_some_and(|claim| claim.expires > now)
    {
        return Err(StoreError::Busy);
    }
    if !matches!(
        record.summary.state,
        QueueState::Queued | QueueState::Claimed
    ) {
        return Err(StoreError::Conflict);
    }
    let generation = record.claim.as_ref().map_or(Ok(1), |claim| {
        claim
            .authority
            .generation
            .checked_add(1)
            .ok_or_else(invalid)
    })?;
    let fresh = QueueAuthority {
        session_id: session.clone(),
        queue_id: record.summary.queue_id.clone(),
        generation,
        token: uuid::Uuid::new_v4().to_string(),
    };
    let expires = now
        .checked_add(time::Duration::try_from(lease).map_err(|_| invalid())?)
        .ok_or_else(invalid)?;
    record.summary.claim_generation = generation;
    record.claim = Some(Claim {
        operation: operation.clone(),
        authority: fresh.clone(),
        owner: owner.into(),
        expires,
    });
    record.summary.state = QueueState::Claimed;
    Ok(fresh)
}
pub(crate) fn complete(
    change: &mut Change<'_>,
    admitted: &crate::AdmitOutcome,
    receipt: crabber_core::AdmissionReceipt,
) {
    debug_assert_eq!(admitted.run.id, receipt.run_id);
    change.record.summary.state = QueueState::Admitted;
    change.record.summary.admission = Some(receipt);
    change.record.claim = None;
}
pub(crate) fn target(action: &QueueAction) -> Option<&QueueId> {
    match action {
        QueueAction::Enqueue { queue_id, .. } | QueueAction::Cancel { queue_id } => Some(queue_id),
        QueueAction::Prepare { authority, .. } | QueueAction::Admit { authority, .. } => {
            Some(&authority.queue_id)
        }
        QueueAction::Claim { .. } => None,
    }
}
