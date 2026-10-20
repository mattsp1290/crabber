//! Native credentials select attribution; public payloads never select an owner.
use super::{PublicStateError as Error, PublicStateReceipt};
use crate::{AttemptAuthority, PhaseAuthority, session_owner::SessionOwnerAuthority};
use crabber_core::{
    ControlId, EventKind, EventRecord, InputFingerprint, PublicAttemptIdentity,
    PublicInvocationIdentity, PublicSourceChange, PublicSourceEvent, PublicSourceIdentity,
    PublicStateContents, Run, SessionId, SessionRevision,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use time::OffsetDateTime;

/// Ephemeral native ownership. Receipt replay never grants another writer.
#[derive(Debug, Clone)]
pub enum PublicSourceAuthority {
    Attempt(AttemptAuthority),
    Phase(PhaseAuthority),
    Invocation(SessionOwnerAuthority),
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublicSourceMutation {
    pub id: ControlId,
    pub expected_revision: SessionRevision,
    pub change: PublicSourceChange,
}
impl PublicSourceMutation {
    /// Validate the borrowed complete request before selecting or copying state.
    /// # Errors
    /// Refuses malformed operation identity or unbounded payloads and patches.
    pub fn validate(&self) -> Result<(), Error> {
        super::validate_id(&self.id)?;
        self.change.validate().map_err(|_| Error::InvalidRequest)
    }
}
impl PublicSourceAuthority {
    pub(crate) fn session(&self) -> &SessionId {
        match self {
            Self::Attempt(authority) => &authority.snapshot.session_id,
            Self::Phase(authority) => &authority.public_source_scope().0.session_id,
            Self::Invocation(authority) => authority.session_id(),
        }
    }
    pub(crate) fn run(&self) -> Option<&crabber_core::RunId> {
        match self {
            Self::Attempt(a) => Some(&a.snapshot.run_id),
            Self::Phase(a) => Some(&a.public_source_scope().0.run_id),
            Self::Invocation(_) => None,
        }
    }
    pub(crate) fn attempt(&self) -> Option<&crate::AttemptTarget> {
        match self {
            Self::Attempt(authority) => Some(&authority.snapshot.target),
            Self::Phase(authority) => Some(&authority.public_source_scope().0.target.attempt),
            Self::Invocation(_) => None,
        }
    }
    pub(crate) fn phase(&self) -> Option<&crate::PhaseTarget> {
        match self {
            Self::Phase(authority) => Some(&authority.public_source_scope().0.target),
            Self::Attempt(_) | Self::Invocation(_) => None,
        }
    }
    pub(crate) fn identity(
        &self,
        workspace: &str,
        run: &Run,
        owner: Option<&crate::session_owner::record::Record>,
        proof: &mut crate::public_session::v2::proof::Proof,
    ) -> Result<PublicSourceIdentity, Error> {
        if self.session() != &run.session_id {
            return Err(Error::StaleOwner);
        }
        let (target, call, invocation) = match self {
            Self::Attempt(authority) => {
                if authority.snapshot.run_id != run.id {
                    return Err(Error::StaleOwner);
                }
                (&authority.snapshot.target, None, None)
            }
            Self::Phase(authority) => {
                let snapshot = authority.public_source_scope().0;
                if snapshot.run_id != run.id {
                    return Err(Error::StaleOwner);
                }
                (
                    &snapshot.target.attempt,
                    Some(&snapshot.target.call_id),
                    None,
                )
            }
            Self::Invocation(authority) => {
                let record = owner.ok_or(Error::StaleOwner)?;
                let binding = &record.snapshot.binding;
                let parent = binding.parent.as_ref().ok_or(Error::UnknownStoreOutcome)?;
                let phase = parent.phase.as_ref().ok_or(Error::UnknownStoreOutcome)?;
                if binding.kind != crate::session_owner::SessionOwnerKind::DetachedInvocation
                    || binding.session_id != run.session_id
                    || parent.run_id != run.id
                    || &record.snapshot.target != authority.target()
                    || parent.call_id.as_ref() != Some(&phase.call_id)
                {
                    return Err(Error::StaleOwner);
                }
                (
                    &phase.attempt,
                    Some(&phase.call_id),
                    Some(InvocationIdentityView {
                        id: &authority.target().id.0,
                        generation: authority.target().generation,
                        parent_invocation: parent
                            .invocation
                            .as_ref()
                            .map(|target| target.id.0.as_str()),
                        parent_generation: parent
                            .invocation
                            .as_ref()
                            .map(|target| target.generation),
                        parent_call_id: parent.call_id.as_ref(),
                    }),
                )
            }
        };
        let view = SourceIdentityView {
            workspace_id: workspace,
            session_id: &run.session_id,
            run_id: &run.id,
            attempt: AttemptIdentityView {
                id: &target.id,
                generation: target.generation,
            },
            call_id: call,
            invocation,
        };
        proof.measure(&view).map_err(|_| Error::InvalidRequest)?;
        Ok(PublicSourceIdentity {
            workspace_id: workspace.to_owned(),
            session_id: run.session_id.clone(),
            run_id: run.id.clone(),
            attempt: PublicAttemptIdentity {
                id: target.id.clone(),
                generation: target.generation,
            },
            call_id: call.cloned(),
            invocation: view.invocation.map(|invocation| PublicInvocationIdentity {
                id: crabber_core::InvocationId::from(invocation.id.to_owned()),
                generation: invocation.generation,
                parent_invocation: invocation
                    .parent_invocation
                    .map(|id| crabber_core::InvocationId::from(id.to_owned())),
                parent_generation: invocation.parent_generation,
                parent_call_id: invocation.parent_call_id.cloned(),
            }),
        })
    }
    pub(crate) fn verify_current(
        &self,
        run: &Run,
        attempt: Option<&crate::attempt::AttemptRecord>,
        phase: Option<&crate::phase::PhaseRecord>,
        owner: Option<&crate::session_owner::record::Record>,
        now: OffsetDateTime,
        proof: &mut crate::public_session::v2::proof::Proof,
    ) -> Result<(), Error> {
        match self {
            Self::Attempt(authority) => {
                crate::native_enrollment::caller::measure_authority(Some(authority), proof)
            }
            Self::Phase(authority) => {
                crate::native_enrollment::caller::measure_phase_authority(Some(authority), proof)
            }
            Self::Invocation(authority) => {
                crate::native_enrollment::caller::measure_owner_authority(authority, proof)
            }
        }
        .map_err(|_| Error::UnknownStoreOutcome)?;
        if let Self::Invocation(authority) = self {
            let record = owner.ok_or(Error::StaleOwner)?;
            if !authority.authenticates(
                &record.snapshot.binding.session_id,
                &record.snapshot.target,
                &record.token,
            ) || record.snapshot.state != crate::session_owner::SessionOwnerState::Active
                || record.lease_until <= now
            {
                return Err(Error::StaleOwner);
            }
            return Ok(());
        }
        let record = attempt.ok_or(Error::StaleOwner)?;
        record.validate().map_err(|_| Error::UnknownStoreOutcome)?;
        if record.snapshot.session_id != run.session_id
            || record.snapshot.run_id != run.id
            || record.snapshot.state != crate::AttemptState::Started
            || record.lease_until <= now
            || record.run_token != run.claim_token
            || run.status != crabber_core::RunStatus::Running
            || run.lease_until <= now
            || self.attempt() != Some(&record.snapshot.target)
        {
            return Err(Error::StaleOwner);
        }
        match self {
            Self::Attempt(authority)
                if authority.matches_store(&record.snapshot, &record.token, &record.run_token) =>
            {
                Ok(())
            }
            Self::Phase(authority) => {
                let phase = phase.ok_or(Error::StaleOwner)?;
                phase.validate().map_err(|_| Error::UnknownStoreOutcome)?;
                if phase.snapshot.state == crate::PhaseState::Started
                    && self.phase() == Some(&phase.snapshot.target)
                    && phase.snapshot.session_id == run.session_id
                    && phase.snapshot.run_id == run.id
                    && authority.matches_store(&phase.snapshot, &phase.token)
                    && authority.matches_attempt_store(
                        &record.snapshot,
                        &record.token,
                        &record.run_token,
                    )
                {
                    Ok(())
                } else {
                    Err(Error::StaleOwner)
                }
            }
            Self::Attempt(_) => Err(Error::StaleOwner),
            Self::Invocation(_) => unreachable!(),
        }
    }
}
fn charge(
    proof: &mut crate::public_session::v2::proof::Proof,
    value: &impl Serialize,
) -> Result<(), Error> {
    proof.measure(value).map_err(|_| Error::UnknownStoreOutcome)
}
fn decode(
    event: &EventRecord,
    proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<PublicSourceEvent, Error> {
    charge(proof, &event.payload)?;
    PublicSourceEvent::deserialize(&event.payload).map_err(|_| Error::UnknownStoreOutcome)
}
fn kind(change: &PublicSourceChange) -> EventKind {
    match change {
        PublicSourceChange::Presentation { .. } => EventKind::PublicPresentation,
        PublicSourceChange::StateSnapshot { .. } => EventKind::PublicStateSnapshot,
        PublicSourceChange::StateDelta { .. } => EventKind::PublicStateDelta,
        PublicSourceChange::ActivitySnapshot { .. } => EventKind::ActivitySnapshot,
        PublicSourceChange::ActivityDelta { .. } => EventKind::ActivityDelta,
    }
}
fn digest(
    event: &PublicSourceEvent,
    proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<InputFingerprint, Error> {
    let input = ("crabber.public-source.v2", event);
    charge(proof, &input)?;
    let mut value = serde_json::to_value(input).map_err(|_| Error::InvalidRequest)?;
    value.sort_all_objects();
    charge(proof, &value)?;
    InputFingerprint::new(format!(
        "{:x}",
        Sha256::digest(value.to_string().as_bytes())
    ))
    .map_err(|_| Error::InvalidRequest)
}
pub(crate) fn source_event(
    source: PublicSourceIdentity,
    mutation: &PublicSourceMutation,
    revision: SessionRevision,
    contents: PublicStateContents,
    now: OffsetDateTime,
    proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<(EventRecord, InputFingerprint), Error> {
    charge(proof, &mutation.change)?;
    let saved = PublicSourceEvent {
        version: 2,
        source,
        revision,
        change: mutation.change.clone(),
        contents,
        before: None,
        before_event_cursor: None,
    };
    let digest = digest(&saved, proof)?;
    charge(proof, &saved)?;
    let event = EventRecord {
        cursor: None,
        session_id: saved.source.session_id.clone(),
        run_id: saved.source.run_id.clone(),
        turn_id: None,
        kind: kind(&saved.change),
        payload: serde_json::to_value(saved).map_err(|_| Error::InvalidRequest)?,
        correlation: Some(mutation.id.0.clone()),
        live_only: false,
        created_at: now,
    };
    Ok((event, digest))
}
pub(crate) fn verify_source_event(
    receipt: &PublicStateReceipt,
    event: &EventRecord,
) -> Result<(), Error> {
    verify_source_event_with_proof(
        receipt,
        event,
        &mut crate::public_session::v2::proof::Proof::bounded(32 << 20),
    )
}
pub(crate) fn verify_source_event_with_proof(
    receipt: &PublicStateReceipt,
    event: &EventRecord,
    proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<(), Error> {
    let saved = decode(event, proof)?;
    let is_delta = matches!(
        &saved.change,
        PublicSourceChange::StateDelta { .. } | PublicSourceChange::ActivityDelta { .. }
    );
    if saved.version != 2
        || saved.revision.get() == 0
        || is_delta != saved.before.is_some()
        || is_delta != saved.before_event_cursor.is_some()
        || saved
            .before_event_cursor
            .is_some_and(|cursor| cursor.0 >= receipt.event_cursor.0)
    {
        return Err(Error::UnknownStoreOutcome);
    }
    if let Some(before) = &saved.before {
        charge(proof, before)?;
        charge(proof, &saved.change)?;
        if saved
            .change
            .apply(before)
            .map_err(|_| Error::UnknownStoreOutcome)?
            != saved.contents
        {
            return Err(Error::UnknownStoreOutcome);
        }
    }
    if kind(&saved.change) != event.kind
        || digest(&saved, proof)? != receipt.semantic_digest
        || saved.source.session_id != event.session_id
        || saved.source.run_id != event.run_id
        || receipt.revision != saved.revision
        || event.live_only
        || event.session_id != receipt.session_id
        || event.run_id != receipt.run_id
        || event.correlation.as_deref() != Some(receipt.operation_id.0.as_str())
        || event
            .cursor
            .is_some_and(|cursor| cursor != receipt.event_cursor)
    {
        return Err(Error::UnknownStoreOutcome);
    }
    Ok(())
}
pub(crate) fn attach_before(
    event: &mut EventRecord,
    before: PublicStateContents,
    cursor: crabber_core::EventCursor,
    proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<InputFingerprint, Error> {
    let mut saved = decode(event, proof)?;
    saved.before = Some(before);
    saved.before_event_cursor = Some(cursor);
    let result = digest(&saved, proof)?;
    charge(proof, &saved)?;
    event.payload = serde_json::to_value(saved).map_err(|_| Error::UnknownStoreOutcome)?;
    Ok(result)
}
pub(crate) fn before_event(
    event: &EventRecord,
) -> Result<Option<crabber_core::EventCursor>, Error> {
    if event
        .payload
        .get("version")
        .and_then(serde_json::Value::as_u64)
        != Some(2)
    {
        return Ok(None);
    }
    Option::<crabber_core::EventCursor>::deserialize(
        event
            .payload
            .get("before_event_cursor")
            .ok_or(Error::UnknownStoreOutcome)?,
    )
    .map_err(|_| Error::UnknownStoreOutcome)
}
pub(crate) fn verify_before_event(event: &EventRecord, before: &EventRecord) -> Result<(), Error> {
    verify_before_event_with_proof(
        event,
        before,
        &mut crate::public_session::v2::proof::Proof::bounded(32 << 20),
    )
}
pub(crate) fn verify_before_event_with_proof(
    event: &EventRecord,
    before: &EventRecord,
    proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<(), Error> {
    let saved = decode(event, proof)?;
    let prior = decode(before, proof)?;
    let contents = saved.before.as_ref().ok_or(Error::UnknownStoreOutcome)?;
    if before.cursor != saved.before_event_cursor
        || before.kind != EventKind::PublicStateSnapshot
        || prior.version != 2
        || prior.source != saved.source
        || prior.revision.next().ok() != Some(saved.revision)
        || prior.contents != *contents
        || prior.before.is_some()
        || prior.before_event_cursor.is_some()
        || !matches!(&prior.change,PublicSourceChange::StateSnapshot{state} if state==contents.state())
        || before.session_id != event.session_id
        || before.run_id != event.run_id
        || before.correlation != event.correlation
        || before.live_only
    {
        return Err(Error::UnknownStoreOutcome);
    }
    Ok(())
}
pub(crate) fn source_matches(
    event: &EventRecord,
    source: &PublicSourceIdentity,
    mutation: &PublicSourceMutation,
    proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<(), Error> {
    if event
        .payload
        .get("version")
        .and_then(serde_json::Value::as_u64)
        != Some(2)
    {
        return Err(Error::SemanticConflict);
    }
    let saved = decode(event, proof)?;
    if saved.version != 2
        || saved.source != *source
        || saved.change != mutation.change
        || mutation.expected_revision.next().ok() != Some(saved.revision)
    {
        return Err(Error::SemanticConflict);
    }
    Ok(())
}

#[derive(Serialize)]
struct SourceIdentityView<'a> {
    workspace_id: &'a str,
    session_id: &'a SessionId,
    run_id: &'a crabber_core::RunId,
    attempt: AttemptIdentityView<'a>,
    call_id: Option<&'a crabber_core::ToolCallId>,
    invocation: Option<InvocationIdentityView<'a>>,
}
#[derive(Serialize)]
struct AttemptIdentityView<'a> {
    id: &'a crabber_core::AttemptId,
    generation: crabber_core::AttemptGeneration,
}
#[derive(Serialize)]
struct InvocationIdentityView<'a> {
    id: &'a str,
    generation: crabber_core::SessionOwnerGeneration,
    parent_invocation: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    parent_generation: Option<crabber_core::SessionOwnerGeneration>,
    parent_call_id: Option<&'a crabber_core::ToolCallId>,
}
