//! Independent source facts are not a resource-ownership certificate.
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
    pub(crate) every_noninitial_claim: Fact,
    pub(crate) terminal_source: Fact,
    pub(crate) main_join: Fact,
    pub(crate) resource_policy: Fact,
}
impl RunHistoryReport {
    pub(crate) fn sources(initial: bool, claims: Fact, terminal: bool) -> Self {
        Self {
            initial_source: Fact::from_verified(initial),
            every_noninitial_claim: claims,
            terminal_source: Fact::from_verified(terminal),
            // No independently source-bound post-worker join or fixed whole-Run
            // library policy is published by the current Runtime.
            main_join: Fact::Unproven,
            resource_policy: Fact::Unproven,
        }
    }
    pub(crate) fn complete(self) -> bool {
        [
            self.initial_source,
            self.every_noninitial_claim,
            self.terminal_source,
            self.main_join,
            self.resource_policy,
        ]
        .into_iter()
        .all(|fact| fact == Fact::Authenticated)
    }
}
/// Streaming fold; no history-sized collection. Noninitial issuances require
/// their original native claim and qualified preparation completion reader.
#[derive(Debug, Clone, Copy)]
pub(crate) struct NoninitialClaims(Fact);
impl Default for NoninitialClaims {
    fn default() -> Self {
        Self(Fact::Authenticated)
    }
}
impl NoninitialClaims {
    pub(crate) fn observe(&mut self, issuance: &crate::native_parent::RunIssuance) {
        if issuance.sequence > 1 {
            self.0 = Fact::Unproven;
        }
    }
    pub(crate) fn finish(self, initial_present: bool) -> Fact {
        if initial_present {
            self.0
        } else {
            Fact::Unproven
        }
    }
}
