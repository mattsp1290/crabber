#[cfg(unix)]
mod bindings;
#[cfg(unix)]
mod limits;
#[cfg(unix)]
mod paths;
use crabber::ExtensionError;
use serde::Serialize;
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    path::PathBuf,
    time::Duration,
};

/// Release whose four pipe filters are supported.
pub const RTK_VERSION: &str = "0.51.0";
/// Conventional ordinary-phase transform order.
pub const DEFAULT_ORDER: i32 = 1000;

/// Fixed text filters. Hosts assert the input format; no auto-detection occurs.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub enum Filter {
    /// Cargo test output; an addition beyond the Go reference.
    CargoTest,
    /// Go test output.
    GoTest,
    /// Git diff output.
    GitDiff,
    /// Repetitive logs.
    Log,
}
impl Filter {
    /// Argument passed after `rtk pipe --filter`.
    pub fn cli_name(self) -> &'static str {
        match self {
            Self::CargoTest => "cargo-test",
            Self::GoTest => "go-test",
            Self::GitDiff => "git-diff",
            Self::Log => "log",
        }
    }
}
/// One selected top-level string member.
#[derive(Clone, Debug, Serialize)]
pub struct Field {
    /// Nonempty member name without NUL.
    pub name: String,
    /// Host-asserted text format.
    pub filter: Filter,
}
/// Optional exact comparison against a normalized input string member.
#[derive(Clone, Debug)]
pub struct InputMatch {
    /// Nonempty top-level input member name without NUL.
    pub field: String,
    /// Distinct allowed values, compared byte for byte.
    pub equals: Vec<String>,
}
/// Selected output representation; nested paths are unsupported.
#[derive(Clone, Debug, Serialize)]
pub enum Selection {
    /// A result that is one JSON string.
    Text(Filter),
    /// Named top-level string members, processed in canonical name order.
    JsonFields(Vec<Field>),
}
/// One binding per tool name.
#[derive(Clone, Debug)]
pub struct Binding {
    /// Exact tool name.
    pub tool_name: String,
    /// Optional exact normalized-input condition.
    pub match_input: Option<InputMatch>,
    /// Output selection.
    pub selection: Selection,
}
/// Positive resource bounds, all included in the configuration fingerprint.
#[derive(Clone, Debug, Serialize)]
pub struct Limits {
    /// Bindings per instance; 1–256.
    pub max_bindings: usize,
    /// Members per JSON selection; 1–256.
    pub max_fields_per_binding: usize,
    /// Allowed values per input match; 1–256.
    pub max_input_matches: usize,
    /// Sum of binding text bytes; 1–1 MiB.
    pub max_config_bytes: usize,
    /// Matched input string UTF-8 bytes; 1–1 MiB.
    pub max_input_bytes: usize,
    /// Selected string UTF-8 bytes; 1–1 MiB.
    pub max_field_bytes: usize,
    /// Concurrent transform leases; 1–64. Capacity never queues.
    pub max_concurrent: usize,
    /// Child stdout bytes; 1–1 MiB, at most max_field_bytes.
    pub max_stdout_bytes: usize,
    /// One deadline across every field in a transform; positive, at most 60 s.
    pub timeout: Duration,
    /// Reaping bound after KILL; positive, at most 60 s.
    pub kill_wait: Duration,
    /// Close bound; positive, at most 120 s, at least timeout + kill_wait.
    pub shutdown_grace: Duration,
}
/// Host-owned executable and temporary root. Construction never executes RTK.
#[derive(Clone)]
pub struct Options {
    /// Explicit ordinary-phase order.
    pub order: i32,
    /// Absolute normalized path, canonicalized before execution. The host must
    /// keep its target immutable throughout this mount's lifetime.
    pub executable_path: PathBuf,
    /// Exactly 64 lowercase SHA-256 hex characters. Verified at construction
    /// only; this proves identity, not provenance or execution-time integrity.
    pub executable_sha256: String,
    /// Existing absolute private root, owned by the host euid and without
    /// group/other write permission. Ancestors are not inspected.
    /// At most sixteen instance roots per process ID can coexist here; stale
    /// roots count. Exhaustion quarantines new instances until the host removes
    /// stale roots or chooses another root and constructs a replacement.
    pub temp_root: PathBuf,
    /// Nonempty exact tool bindings.
    pub bindings: Vec<Binding>,
    /// Resource bounds.
    pub limits: Limits,
}
impl fmt::Debug for Options {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Options")
            .field("order", &self.order)
            .field("bindings", &self.bindings.len())
            .field("limits", &self.limits)
            .finish()
    }
}
pub(super) const PRIVATE_KEYS: [(&str, &str); 7] = [
    ("HOME", "home"),
    ("XDG_CONFIG_HOME", "config"),
    ("XDG_DATA_HOME", "data"),
    ("XDG_STATE_HOME", "state"),
    ("XDG_CACHE_HOME", "cache"),
    ("CLAUDE_CONFIG_DIR", "claude"),
    ("TMPDIR", "tmp"),
];
pub(super) const FIXED_ENV: [(&str, &str); 5] = [
    ("RTK_TELEMETRY_DISABLED", "1"),
    ("NO_COLOR", "1"),
    ("TERM", "dumb"),
    ("LANG", "C"),
    ("LC_ALL", "C"),
];
#[derive(Clone)]
pub(super) struct CanonicalBinding {
    pub(super) match_input: Option<(String, BTreeSet<String>)>,
    pub(super) selection: Selection,
}
#[derive(Clone)]
pub(super) struct Configuration {
    pub(super) executable: PathBuf,
    pub(super) temp_root: PathBuf,
    pub(super) bindings: BTreeMap<String, CanonicalBinding>,
    pub(super) limits: Limits,
    pub(super) order: i32,
}
pub(super) fn validate(options: Options) -> Result<(Configuration, String), ExtensionError> {
    #[cfg(not(unix))]
    {
        let _ = options;
        Err(crate::config_error("unsupported-platform"))
    }
    #[cfg(unix)]
    {
        validate_unix(options)
    }
}
#[cfg(unix)]
fn validate_unix(options: Options) -> Result<(Configuration, String), ExtensionError> {
    let Options {
        order,
        executable_path,
        executable_sha256,
        temp_root,
        bindings,
        limits,
    } = options;
    let l = &limits;
    limits::validate(l)?;
    if executable_sha256.len() != 64
        || !executable_sha256
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(crate::config_error("executable-digest"));
    }
    let bindings = bindings::canonicalize(bindings, l)?;
    let executable = paths::executable(executable_path, &executable_sha256)?;
    let temp_root = paths::temp_root(temp_root)?;
    let canonical: Vec<_> = bindings
        .iter()
        .map(|(name, b)| (name, &b.match_input, &b.selection))
        .collect();
    let hash = crate::config_hash(&(
        "rtk-reducer-v1",
        RTK_VERSION,
        "private-env-v1",
        "marker-v1",
        PRIVATE_KEYS,
        FIXED_ENV,
        [
            Filter::CargoTest,
            Filter::GoTest,
            Filter::GitDiff,
            Filter::Log,
        ]
        .map(Filter::cli_name),
        executable_sha256,
        order,
        canonical,
        l,
    ));
    Ok((
        Configuration {
            executable,
            temp_root,
            bindings,
            limits,
            order,
        },
        hash,
    ))
}
#[cfg(unix)]
pub(super) fn validate_owner(uid: u32, expected: u32) -> Result<(), ExtensionError> {
    if uid != expected {
        return Err(crate::config_error("temp-root-owner"));
    }
    Ok(())
}

#[cfg(all(test, not(unix)))]
mod unsupported_tests {
    use super::*;
    #[test]
    fn unsupported_platform_rejects_first() {
        let o = Options {
            order: 0,
            executable_path: PathBuf::new(),
            executable_sha256: String::new(),
            temp_root: PathBuf::new(),
            bindings: vec![],
            limits: Limits {
                max_bindings: 0,
                max_fields_per_binding: 0,
                max_input_matches: 0,
                max_config_bytes: 0,
                max_input_bytes: 0,
                max_field_bytes: 0,
                max_concurrent: 0,
                max_stdout_bytes: 0,
                timeout: Duration::ZERO,
                kill_wait: Duration::ZERO,
                shutdown_grace: Duration::ZERO,
            },
        };
        assert!(
            validate(o)
                .err()
                .unwrap()
                .to_string()
                .ends_with("unsupported-platform")
        );
    }
}
