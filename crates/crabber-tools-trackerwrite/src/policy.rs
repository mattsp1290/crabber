//! Explicit bn executable, hub, actor, workflow and child environment.
use crabber::ExtensionError;
use crabber_tools_core::{EnvPolicy, validate_binary};
use serde::Serialize;
use std::{collections::BTreeSet, path::PathBuf};
/// Host-owned policy for the bn CLI backend.
#[derive(Clone, Serialize, Debug, PartialEq, Eq)]
pub struct TrackerPolicy {
    /// Absolute bn executable; resolve once on the host.
    pub bn_binary: PathBuf,
    /// Explicit hub project; never inferred from cwd.
    pub project: String,
    /// Absolute UTF-8 hub directory passed as BEANS_HUB.
    pub hub: PathBuf,
    /// Audit actor passed as BN_ACTOR.
    pub actor: String,
    /// Configured workflow states admitted for transitions.
    pub statuses: BTreeSet<String>,
    /// Additional replacement environment, e.g. explicit PATH/HOME for git.
    /// BEANS_HUB and BN_ACTOR are reserved; nothing is inherited implicitly.
    pub environment: Vec<(String, String)>,
}
impl TrackerPolicy {
    /// Reject invalid executables, namespaces, statuses or environment entries.
    pub fn validate(&self) -> Result<(), ExtensionError> {
        validate_binary(&self.bn_binary)?;
        let valid_label =
            |s: &str| !s.is_empty() && s.len() <= 256 && !s.starts_with('-') && !s.contains('\0');
        if self.project.is_empty()
            || self.project.len() > 64
            || !self
                .project
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
            || self.project.starts_with('-')
            || !self.hub.is_absolute()
            || !self.hub.is_dir()
            || self.hub.to_str().is_none_or(|s| s.contains('\0'))
            || self.actor.trim().is_empty()
            || !valid_label(&self.actor)
            || self.statuses.is_empty()
            || !self.statuses.iter().all(|s| valid_label(s))
        {
            return Err(ExtensionError::Plan("tracker policy".into()));
        }
        let mut keys = BTreeSet::new();
        if self
            .environment
            .iter()
            .any(|(key, _)| matches!(key.as_str(), "BEANS_HUB" | "BN_ACTOR") || !keys.insert(key))
        {
            return Err(ExtensionError::Plan("tracker environment".into()));
        }
        self.env().validate()
    }
    pub(crate) fn env(&self) -> EnvPolicy {
        let mut set = self.environment.clone();
        set.push(("BEANS_HUB".into(), self.hub.to_string_lossy().into_owned()));
        set.push(("BN_ACTOR".into(), self.actor.clone()));
        EnvPolicy::Replace(set)
    }
}
