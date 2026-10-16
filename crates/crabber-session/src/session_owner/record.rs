//! Pure transitions. Adapters authenticate the independent latest audit before
//! calling these methods, then publish record, receipt and revision atomically.
#[path = "record/resources.rs"]
mod resources;
use crate::session_owner::{
    MutateSessionOwner, ReserveSessionOwner, SessionOwnerAuthority, SessionOwnerCommand,
    SessionOwnerError as Error, SessionOwnerOperation, SessionOwnerReceipt, SessionOwnerResolver,
    SessionOwnerSnapshot, SessionOwnerState, SessionOwnerTarget, SessionOwnerTermination,
    identifier,
};
use crabber_core::{InputFingerprint, SessionOwnerGeneration, SessionOwnerOperationId};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Record {
    pub version: u32,
    pub snapshot: SessionOwnerSnapshot,
    pub origin: SessionOwnerOperationId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preparation: Option<super::preparation::Origin>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resource: Option<super::preparation::PreparationResourceLink>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service: Option<Box<super::service::SessionServiceLink>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub driver: Option<Box<super::driver::DriverLink>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retained_resources: Option<SessionOwnerTarget>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unkeyed_input: Option<InputFingerprint>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cancellation: Option<Box<crate::session_owner::cancellation::Cancellation>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owned_scope: Option<super::owned_scope::OwnedScopeActivation>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owned_join: Option<super::owned_scope::JoinSource>,
    pub parent: Option<crate::native_parent::Witness>,
    pub sequence: u64,
    pub owner: String,
    pub token: String,
    pub lease_until: OffsetDateTime,
    pub ever_began: bool,
    pub resolver: Option<SessionOwnerResolver>,
}
impl Record {
    pub fn reserve(
        request: &ReserveSessionOwner,
        parent: Option<crate::native_parent::Witness>,
        now: OffsetDateTime,
    ) -> Result<Self, Error> {
        request.fingerprint()?;
        Ok(Self {
            version: 2,
            snapshot: SessionOwnerSnapshot {
                binding: request.binding.clone(),
                target: SessionOwnerTarget {
                    id: request.binding.id.clone(),
                    generation: SessionOwnerGeneration::INITIAL,
                },
                state: SessionOwnerState::ReservedUnstarted,
                termination: None,
                cancellation: None,
                created_at: now,
                updated_at: now,
            },
            origin: request.id.clone(),
            preparation: None,
            resource: None,
            service: None,
            driver: None,
            retained_resources: None,
            unkeyed_input: None,
            cancellation: None,
            parent,
            owned_scope: None,
            owned_join: None,
            sequence: 1,
            owner: request.owner.clone(),
            token: uuid::Uuid::new_v4().to_string(),
            lease_until: deadline(now, request.lease_ms)?,
            ever_began: false,
            resolver: None,
        })
    }
    pub fn validate(&self) -> Result<(), Error> {
        self.snapshot
            .binding
            .validate()
            .map_err(|_| Error::UnknownStoreOutcome)?;
        identifier(&self.origin.0, 128, false).map_err(|_| Error::UnknownStoreOutcome)?;
        identifier(&self.owner, 512, false).map_err(|_| Error::UnknownStoreOutcome)?;
        if !matches!(self.version, 1 | 2)
            || self.sequence == 0
            || self.sequence > i64::MAX as u64
            || self.snapshot.target.id != self.snapshot.binding.id
            || (self.snapshot.state == SessionOwnerState::Joined)
                != self.snapshot.termination.is_some()
            || (self.snapshot.state == SessionOwnerState::ReservedUnstarted && self.ever_began)
            || (self.snapshot.state == SessionOwnerState::Active && !self.ever_began)
        {
            return Err(Error::UnknownStoreOutcome);
        }
        if let Some(preparation) = &self.preparation
            && (self.version != 2
                || preparation.operation != self.origin
                || self.snapshot.binding.kind
                    != crate::session_owner::SessionOwnerKind::Preparation)
        {
            return Err(Error::UnknownStoreOutcome);
        }
        resources::validate(self)?;
        if let Some(scope) = &self.owned_scope {
            super::owned_scope::source::matches_record(&scope.source, self)?;
            scope
                .fingerprint()
                .map_err(|_| Error::UnknownStoreOutcome)?;
            if scope.source.session != self.snapshot.binding.session_id
                || scope.source.target != self.snapshot.target
                || !self.ever_began
            {
                return Err(Error::UnknownStoreOutcome);
            }
        }
        if let Some(source) = &self.owned_join {
            super::owned_scope::join_verify::verify_record(source, self)?;
        }
        if let Some(cancellation) = &self.cancellation {
            cancellation
                .validate(self)
                .map_err(|_| Error::UnknownStoreOutcome)?;
        }
        if self.snapshot.cancellation != self.cancellation.as_ref().map(|value| value.summary()) {
            return Err(Error::UnknownStoreOutcome);
        }
        match (&self.snapshot.binding.parent, &self.parent) {
            (None, None) => (),
            (Some(binding), None)
                if self.driver.as_ref().is_some_and(|source| {
                    source.run_id() == &binding.run_id
                        && binding.call_id.is_none()
                        && binding.phase.is_none()
                }) => {}
            (Some(binding), Some(witness)) => {
                witness.validate().map_err(|_| Error::UnknownStoreOutcome)?;
                if witness.source.session_id != self.snapshot.binding.session_id
                    || witness.source.run_id != binding.run_id
                    || witness.source.phase != binding.phase
                    || witness.source.phase.as_ref().map(|p| &p.call_id) != binding.call_id.as_ref()
                {
                    return Err(Error::UnknownStoreOutcome);
                }
            }
            _ => return Err(Error::UnknownStoreOutcome),
        }
        if let Some(claim) = &self.resolver {
            crate::session_owner::resolver(claim).map_err(|_| Error::UnknownStoreOutcome)?;
        }
        if self.snapshot.state == SessionOwnerState::Joined {
            if !self.token.is_empty() {
                return Err(Error::UnknownStoreOutcome);
            }
        } else if self.token.is_empty()
            || self.token.len() > 512
            || !self.token.bytes().all(|b| b.is_ascii_graphic())
        {
            return Err(Error::UnknownStoreOutcome);
        }
        if crate::snapshot::encoded_bytes(self, 32768).is_none() {
            return Err(Error::UnknownStoreOutcome);
        }
        Ok(())
    }
    pub fn grant(&self) -> Result<SessionOwnerAuthority, Error> {
        self.validate()?;
        if self.snapshot.state != SessionOwnerState::ReservedUnstarted {
            return Err(Error::InvalidTransition);
        }
        SessionOwnerAuthority::from_store(
            self.snapshot.binding.session_id.clone(),
            self.snapshot.target.clone(),
            self.token.clone(),
        )
    }
    fn authorized(&self, authority: Option<&SessionOwnerAuthority>) -> bool {
        authority.is_some_and(|value| {
            value.authenticates(
                &self.snapshot.binding.session_id,
                &self.snapshot.target,
                &self.token,
            )
        })
    }
    pub fn mutate(
        &mut self,
        request: &MutateSessionOwner,
        authority: Option<&SessionOwnerAuthority>,
        now: OffsetDateTime,
    ) -> Result<bool, Error> {
        self.validate()?;
        request.fingerprint()?;
        if request.session_id != self.snapshot.binding.session_id {
            return Err(Error::NotFound);
        }
        if request.target != self.snapshot.target {
            return Err(Error::GenerationConflict);
        }
        if self.snapshot.state == SessionOwnerState::Joined {
            return Err(Error::InvalidTransition);
        }
        let authorized = self.authorized(authority);
        if let Some(preparation) = &self.preparation
            && matches!(request.command, SessionOwnerCommand::Begin)
            && request.id
                != crate::session_owner::preparation::identity::ids(
                    &preparation.key,
                    &self.snapshot.binding.session_id,
                )?
                .begin
        {
            return Err(Error::InvalidRequest);
        }
        let public_change = match &request.command {
            SessionOwnerCommand::Begin => {
                if !authorized || self.lease_until <= now {
                    return Err(Error::StaleOwner);
                }
                if self.snapshot.state != SessionOwnerState::ReservedUnstarted || self.ever_began {
                    return Err(Error::InvalidTransition);
                }
                self.snapshot.state = SessionOwnerState::Active;
                self.ever_began = true;
                true
            }
            SessionOwnerCommand::Renew { lease_ms } => {
                if !authorized || self.lease_until <= now {
                    return Err(Error::StaleOwner);
                }
                if self.snapshot.state == SessionOwnerState::Uncertain {
                    return Err(Error::InvalidTransition);
                }
                self.lease_until = deadline(now, *lease_ms)?;
                false
            }
            SessionOwnerCommand::Join { termination } => {
                // Cleanup evidence cannot execute effects. An expired unchanged
                // owner may report its actual join; rotated generations cannot.
                if !authorized {
                    return Err(Error::StaleOwner);
                }
                self.join(*termination);
                true
            }
            SessionOwnerCommand::MarkUncertain { claim } => {
                if !authorized && self.lease_until > now {
                    return Err(Error::LiveOwner);
                }
                self.snapshot.state = SessionOwnerState::Uncertain;
                self.resolver = Some(claim.clone());
                true
            }
            SessionOwnerCommand::ReclaimUnstarted { owner, lease_ms } => {
                if self.cancellation.is_some() {
                    return Err(Error::CancellationPending);
                }
                if self.lease_until > now {
                    return Err(Error::LiveOwner);
                }
                if self.snapshot.state != SessionOwnerState::ReservedUnstarted || self.ever_began {
                    return Err(Error::InvalidTransition);
                }
                self.snapshot.target.generation = self
                    .snapshot
                    .target
                    .generation
                    .next()
                    .map_err(|_| Error::GenerationConflict)?;
                self.owner.clone_from(owner);
                self.token = uuid::Uuid::new_v4().to_string();
                self.lease_until = deadline(now, *lease_ms)?;
                true
            }
            SessionOwnerCommand::ResolveJoined { termination, claim } => {
                if self.lease_until > now {
                    return Err(Error::LiveOwner);
                }
                self.join(*termination);
                self.resolver = Some(claim.clone());
                true
            }
        };
        self.publish_change(now, public_change)?;
        Ok(public_change)
    }
    pub(crate) fn publish_change(
        &mut self,
        now: OffsetDateTime,
        public_change: bool,
    ) -> Result<(), Error> {
        self.version = 2;
        self.sequence = self
            .sequence
            .checked_add(1)
            .filter(|value| i64::try_from(*value).is_ok())
            .ok_or(Error::UnknownStoreOutcome)?;
        if public_change {
            self.snapshot.updated_at = now;
        }
        self.validate()
    }
    fn join(&mut self, termination: SessionOwnerTermination) {
        self.snapshot.state = SessionOwnerState::Joined;
        self.snapshot.termination = Some(termination);
        self.token.clear();
    }
    pub fn receipt(
        &self,
        id: SessionOwnerOperationId,
        digest: InputFingerprint,
        operation: SessionOwnerOperation,
    ) -> SessionOwnerReceipt {
        SessionOwnerReceipt {
            session_id: self.snapshot.binding.session_id.clone(),
            operation_id: id,
            semantic_digest: digest,
            operation,
            owner: self.snapshot.clone(),
        }
    }
}
fn deadline(now: OffsetDateTime, milliseconds: u64) -> Result<OffsetDateTime, Error> {
    if milliseconds == 0 || milliseconds > 900_000 {
        return Err(Error::InvalidRequest);
    }
    now.checked_add(time::Duration::milliseconds(
        i64::try_from(milliseconds).map_err(|_| Error::InvalidRequest)?,
    ))
    .ok_or(Error::InvalidRequest)
}
