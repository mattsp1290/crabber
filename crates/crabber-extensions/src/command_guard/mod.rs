//! Pure, bounded, deny-only shell syntax analysis for trusted host commands.
//!
//! Hosts supply exact tool bindings and basename/positional-prefix rules. Paths
//! are never resolved. Unsupported syntax denies; an abstention is not approval.
//! `CommandGuard` registers the policy as a Crabber `ToolGuard` with bounded
//! in-flight capacity and per-class counters.
//!
//! Analysis supports the admitted POSIX/Bash grammar, simple parameter words,
//! substitutions, conditionals, loops, cases, redirects and bounded delegation
//! through `env`, `command`, `exec`, `sudo`, `timeout`, `sh` and `bash`. It walks
//! every branch and executable substitution, including commands that would not
//! execute on the current shell's path. Arithmetic, functions, arrays, opaque
//! builtin operand interpretation and unsupported wrapper options deny. This
//! structural inventory is an analysis boundary, not a danger catalog.
//!
//! Basename/prefix rules cannot recognize `git -C . push`, execution via
//! `nohup`, `nice`, `ionice`, `xargs`, `find -exec`, `busybox`, `ssh`, `python -c`,
//! `perl -e`, `node -e`, `make`, or host-defined functions and aliases. This is
//! trusted syntax inspection, not a sandbox. Hosts own trust, permissions,
//! credentials, provisioning and execution environment. A configuration hash
//! includes canonical bindings/rules, all limits and versioned behavior.
//!
//! There is no raw JSON byte channel: serde_json has resolved duplicate keys
//! and UTF-8 before analysis. JSON key/string work has a separate
//! `max_analysis_bytes` cap; script budgets are shared across nested shell and
//! decoded backquote scripts. Depth is shared with wrappers and hard-capped
//! at 32, measured on a 512 KiB debug thread. The reference's comment-continuation
//! and POSIX Bash ambiguities are denied conservatively. Unquoted heredoc
//! continuations deny because supported shells disagree on joined delimiters. See the repository's
//! extension-parity document and reference fixture instructions for differences.
#![deny(
    clippy::indexing_slicing,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::arithmetic_side_effects,
    clippy::unreachable,
    clippy::todo
)]
mod ast;
mod budget;
mod builtins;
mod config;
mod guard;
mod input;
mod matching;
mod parser;
mod variables;
mod walk;
mod words;
mod wrappers;

pub use config::*;
pub use guard::{CommandGuard, GUARD_ID, Stats};
use serde_json::Value;

/// Fixed outcomes contain no command text or rule identifiers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// No deny rule or structural boundary was found; never an approval.
    Abstain,
    /// A host deny rule matched.
    RuleMatch,
    /// Input is not a valid sentence of the supported grammar.
    InvalidCommand,
    /// A recognized construct prevents sound bounded analysis.
    Unanalysable,
    /// A configured resource budget was exhausted.
    AnalysisLimit,
}
impl Outcome {
    /// Whether a host should deny this outcome.
    pub fn denies(self) -> bool {
        self != Self::Abstain
    }
    /// Fixed host diagnostic code; no command-derived text is returned.
    pub fn code(self) -> Option<&'static str> {
        match self {
            Self::Abstain => None,
            Self::RuleMatch => Some("rule-match"),
            Self::InvalidCommand => Some("invalid-command"),
            Self::Unanalysable => Some("unanalysable-command"),
            Self::AnalysisLimit => Some("analysis-limit"),
        }
    }
}
impl Policy {
    /// Analyze an exact bound tool's parsed arguments. Unbound tools abstain
    /// without inspecting arguments. Object keys are visited in serde_json map
    /// order; multi-fault outcome classes may differ from wire-order readers.
    /// Duplicate keys and UTF-8 validation have already been resolved upstream.
    /// Aggregate JSON key/string bytes are capped by `max_analysis_bytes` in
    /// a separate extraction budget before scanning for NUL.
    pub fn analyze(&self, tool_name: &str, arguments: &Value) -> Outcome {
        let Some(binding) = self.binding(tool_name) else {
            return Outcome::Abstain;
        };
        match input::extract(arguments, &binding.command_field, &self.limits) {
            Ok(command) => self.analyze_script(&command, binding.dialect),
            Err(outcome) => outcome,
        }
    }
    /// Analyze a script without accessing processes, files, environment or
    /// credentials. Analysis is deterministic and retains no input.
    pub fn analyze_script(&self, script: &str, dialect: Dialect) -> Outcome {
        let mut analysis = walk::Analysis {
            policy: self,
            budget: budget::Budget::new(&self.limits),
        };
        analysis.script(script, dialect, parser::Context::Top, 0, 0)
    }
}
