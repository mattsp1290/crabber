//! Independent source facts are not a resource-ownership certificate.
use crate::native_enrollment::initial_consumer::InitialConsumption;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Fact {
    Authenticated,
    Unproven,
}
impl Fact {
    pub(crate) fn from_verified(verified: bool) -> Self {
        if verified {
            Self::Authenticated
        } else {
            Self::Unproven
        }
    }
}
/// Payload-free report. Every independent source check must finish even when
/// another fact is unproven. No fact confers execution authority.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RunHistoryReport {
    pub(crate) initial_source: Fact,
    pub(crate) initial_consumption: InitialConsumption,
    pub(crate) every_noninitial_claim: Fact,
    pub(crate) retained_noninitial_sources: Fact,
    pub(crate) terminal_source: Fact,
    pub(crate) main_join: Fact,
    pub(crate) resource_policy: Fact,
}
impl RunHistoryReport {
    pub(crate) fn sources(initial: bool, claims: ClaimHistory, terminal: bool) -> Self {
        Self {
            initial_source: Fact::from_verified(initial),
            initial_consumption: InitialConsumption::Unproven,
            every_noninitial_claim: claims.complete,
            retained_noninitial_sources: claims.retained_sources,
            terminal_source: Fact::from_verified(terminal),
            // No verifier here authenticates a complete per-Run join inventory
            // or a fixed whole-Run library resource policy.
            main_join: Fact::Unproven,
            resource_policy: Fact::Unproven,
        }
    }
    pub(crate) fn with_initial_consumption(mut self, value: InitialConsumption) -> Self {
        self.initial_consumption = value;
        self
    }
    pub(crate) fn complete(self) -> bool {
        self.initial_consumption == InitialConsumption::CompletedSource
            && [
                self.initial_source,
                self.every_noninitial_claim,
                self.retained_noninitial_sources,
                self.terminal_source,
                self.main_join,
                self.resource_policy,
            ]
            .into_iter()
            .all(|fact| fact == Fact::Authenticated)
    }
}
/// Retained associations and complete historical membership are independent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ClaimHistory {
    pub(crate) retained_sources: Fact,
    pub(crate) complete: Fact,
}
impl ClaimHistory {
    pub(crate) fn with_membership(mut self, protected: bool) -> Self {
        self.complete = if protected {
            self.retained_sources
        } else {
            Fact::Unproven
        };
        self
    }
}
/// Streaming retained-source fold, keeping only the last authenticated cursor.
/// Present rows cannot prove that the tail or an entire claim was never erased.
#[derive(Debug, Clone, Copy)]
pub(crate) struct NoninitialClaims {
    retained: Fact,
    last_control: Option<crabber_core::EventCursor>,
}
impl Default for NoninitialClaims {
    fn default() -> Self {
        Self {
            retained: Fact::Authenticated,
            last_control: None,
        }
    }
}
impl NoninitialClaims {
    /// `control` comes only from the independently authenticated original native
    /// receipt/event after `completion_history::verify_control` succeeds.
    pub(crate) fn observe(
        &mut self,
        previous: Option<&crate::native_parent::RunIssuance>,
        current: &crate::native_parent::RunIssuance,
        control: Option<crabber_core::EventCursor>,
    ) -> Result<(), crate::native_parent::Error> {
        use crate::native_parent::Error;
        if crate::snapshot::encoded_bytes(current, 2048).is_none() {
            return Err(Error);
        }
        current.validate()?;
        let Some(previous) = previous else {
            return if current.sequence == 1 && control.is_none() {
                Ok(())
            } else {
                Err(Error)
            };
        };
        previous.validate()?;
        if current.sequence != previous.sequence.checked_add(1).ok_or(Error)?
            || current.state.session_id != previous.state.session_id
            || current.state.run_id != previous.state.run_id
            || current.state == previous.state
        {
            return Err(Error);
        }
        if current.state.token == previous.state.token {
            // Status-only publications preserve the existing fence. They cannot
            // acquire or repeat a preparation/native claim association.
            return if current.completion_source.is_none() && control.is_none() {
                Ok(())
            } else {
                Err(Error)
            };
        }
        if let Some(cursor) = control {
            if current.completion_source.is_none()
                || self.last_control.is_some_and(|last| cursor <= last)
            {
                return Err(Error);
            }
            self.last_control = Some(cursor);
        } else {
            self.retained = Fact::Unproven;
        }
        Ok(())
    }
    pub(crate) fn finish(self, initial_present: bool) -> ClaimHistory {
        ClaimHistory {
            retained_sources: if initial_present {
                self.retained
            } else {
                Fact::Unproven
            },
            // No independent monotonic membership/head protocol exists yet.
            // Even no retained rotations cannot authenticate their absence.
            complete: Fact::Unproven,
        }
    }
}
