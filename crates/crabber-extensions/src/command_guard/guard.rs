use super::{Options, Outcome, Policy};
use async_trait::async_trait;
use crabber::extension::{Extension, ExtensionError, GuardDecision, Registrar, ToolGuard};
use serde_json::Value;
use std::{
    fmt,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
};

/// Extension and guard identity used in run-plan fingerprints.
pub const GUARD_ID: &str = "crabber-extensions/command-guard";

/// Point-in-time counts for bound tools; unbound checks are not counted.
/// Capacity is counted before analysis; internal means a caught analysis panic.
/// Fields are read independently, so concurrent snapshots are not atomic.
/// Individual counters wrap at `u64::MAX`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Stats {
    pub abstain: u64,
    pub rule_match: u64,
    pub invalid_command: u64,
    pub unanalysable: u64,
    pub analysis_limit: u64,
    pub capacity: u64,
    pub internal: u64,
}
impl Stats {
    /// Saturating sum of the six denial counters.
    pub fn denials(&self) -> u64 {
        self.rule_match
            .saturating_add(self.invalid_command)
            .saturating_add(self.unanalysable)
            .saturating_add(self.analysis_limit)
            .saturating_add(self.capacity)
            .saturating_add(self.internal)
    }
}

/// Deny-only Crabber guard around a frozen [`Policy`]. Hosts own mount scope
/// and permission decisions. A denial persists as Crabber's fixed
/// `permission denied`, with no per-call reason. Re-analyze stored normalized
/// arguments through [`Self::policy`] for the analysis class; capacity and
/// internal failures appear only in [`Self::stats`].
///
/// Use one instance per registry or tenant: mounting the same instance twice
/// is permitted and shares capacity and counters, including across registries.
/// Checks run synchronously and block their runtime worker. Use a multi-thread
/// runtime with worker stacks of at least 2 MiB and keep `max_in_flight` at or
/// below the workers the host can afford to block; saturation does not unblock
/// an analysis already running.
///
/// Analysis panics are caught with unwinding enabled. The host-owned panic
/// hook runs first and may print the payload. Stack overflow, allocation failure
/// and `panic = "abort"` are not contained. No input is retained by this guard.
pub struct CommandGuard {
    guard: Arc<Guard>,
    hash: String,
}
impl CommandGuard {
    /// Validate and freeze options through [`Policy::new`].
    pub fn new(options: Options) -> Result<Self, ExtensionError> {
        Ok(Self::from_guard(Guard::new(Policy::new(options)?)))
    }
    fn from_guard(guard: Guard) -> Self {
        let hash = crate::config_hash(&(
            "command-guard-extension-v1",
            GUARD_ID,
            guard.policy.config_hash(),
        ));
        Self {
            guard: Arc::new(guard),
            hash,
        }
    }
    /// Frozen policy for host re-analysis of stored normalized arguments.
    pub fn policy(&self) -> &Policy {
        &self.guard.policy
    }
    /// Read independent, relaxed counter snapshots.
    pub fn stats(&self) -> Stats {
        self.guard.stats()
    }
    #[cfg(test)]
    fn with_analyzer(options: Options, analyze: Analyzer) -> Result<Self, ExtensionError> {
        Ok(Self::from_guard(Guard::with_analyzer(
            Policy::new(options)?,
            analyze,
        )))
    }
    #[cfg(test)]
    fn guard(&self) -> &Arc<Guard> {
        &self.guard
    }
}
impl fmt::Debug for CommandGuard {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CommandGuard")
            .field("policy", self.policy())
            .field("hash", &self.hash)
            .finish()
    }
}
#[async_trait]
impl Extension for CommandGuard {
    fn id(&self) -> &str {
        GUARD_ID
    }
    fn version(&self) -> &str {
        env!("CARGO_PKG_VERSION")
    }
    fn config_hash(&self) -> String {
        self.hash.clone()
    }
    async fn install(&self, registrar: &mut Registrar) -> Result<(), ExtensionError> {
        registrar.guard(self.guard.clone());
        Ok(())
    }
}

#[derive(Default)]
struct Counters {
    abstain: AtomicU64,
    rule_match: AtomicU64,
    invalid_command: AtomicU64,
    unanalysable: AtomicU64,
    analysis_limit: AtomicU64,
    capacity: AtomicU64,
    internal: AtomicU64,
}
type Analyzer = Arc<dyn Fn(&Policy, &str, &Value) -> Outcome + Send + Sync>;
struct Guard {
    policy: Policy,
    max_in_flight: usize,
    in_flight: AtomicUsize,
    counters: Counters,
    analyze: Analyzer,
}
impl Guard {
    fn new(policy: Policy) -> Self {
        Self::build(policy, Arc::new(Policy::analyze))
    }
    fn build(policy: Policy, analyze: Analyzer) -> Self {
        Self {
            max_in_flight: policy.limits().max_in_flight,
            policy,
            in_flight: AtomicUsize::new(0),
            counters: Counters::default(),
            analyze,
        }
    }
    #[cfg(test)]
    fn with_analyzer(policy: Policy, analyze: Analyzer) -> Self {
        Self::build(policy, analyze)
    }
    #[cfg(test)]
    fn in_flight(&self) -> usize {
        self.in_flight.load(Ordering::Acquire)
    }
    #[cfg(test)]
    fn preload_permits(&self, count: usize) {
        self.in_flight.store(count, Ordering::Release);
    }
    fn stats(&self) -> Stats {
        let c = &self.counters;
        Stats {
            abstain: c.abstain.load(Ordering::Relaxed),
            rule_match: c.rule_match.load(Ordering::Relaxed),
            invalid_command: c.invalid_command.load(Ordering::Relaxed),
            unanalysable: c.unanalysable.load(Ordering::Relaxed),
            analysis_limit: c.analysis_limit.load(Ordering::Relaxed),
            capacity: c.capacity.load(Ordering::Relaxed),
            internal: c.internal.load(Ordering::Relaxed),
        }
    }
    fn decide(&self, name: &str, arguments: &Value) -> GuardDecision {
        if self.policy.binding(name).is_none() {
            return GuardDecision::Abstain;
        }
        let mut current = self.in_flight.load(Ordering::Acquire);
        loop {
            let Some(next) = current.checked_add(1).filter(|&n| n <= self.max_in_flight) else {
                self.counters.capacity.fetch_add(1, Ordering::Relaxed);
                return GuardDecision::Deny;
            };
            match self.in_flight.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(observed) => current = observed,
            }
        }
        let _permit = Permit(&self.in_flight);
        let outcome = catch_unwind(AssertUnwindSafe(|| {
            (self.analyze)(&self.policy, name, arguments)
        }));
        let (counter, decision) = match outcome {
            Ok(Outcome::Abstain) => (&self.counters.abstain, GuardDecision::Abstain),
            Ok(Outcome::RuleMatch) => (&self.counters.rule_match, GuardDecision::Deny),
            Ok(Outcome::InvalidCommand) => (&self.counters.invalid_command, GuardDecision::Deny),
            Ok(Outcome::Unanalysable) => (&self.counters.unanalysable, GuardDecision::Deny),
            Ok(Outcome::AnalysisLimit) => (&self.counters.analysis_limit, GuardDecision::Deny),
            Err(_) => (&self.counters.internal, GuardDecision::Deny),
        };
        counter.fetch_add(1, Ordering::Relaxed);
        decision
    }
}
impl ToolGuard for Guard {
    fn id(&self) -> &str {
        GUARD_ID
    }
    fn check(&self, name: &str, arguments: &Value) -> GuardDecision {
        self.decide(name, arguments)
    }
}
struct Permit<'a>(&'a AtomicUsize);
impl Drop for Permit<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}
#[cfg(test)]
mod tests;
