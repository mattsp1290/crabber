#[cfg(unix)]
use super::runner;
use super::runner::Bounds;
use crabber::ExtensionError;
use serde::Serialize;
#[cfg(unix)]
use std::path::Path;
use std::{collections::BTreeMap, fmt, path::PathBuf, time::Duration};

/// How host environment entries are frozen at construction.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub enum EnvironmentMode {
    /// Pass only overrides and per-owner private directories.
    ExplicitOnly,
    /// Freeze the host environment, then apply overrides and private directories.
    InheritAndOverride,
}
/// Host-owned environment policy. Rotate the identity when values change.
#[derive(Clone)]
pub struct Environment {
    /// Environment capture mode.
    pub mode: EnvironmentMode,
    /// UTF-8 entries; the nine private-directory keys cannot be overridden.
    /// OS argument/environment limits also apply at spawn. On Linux, a single
    /// key + equals + value + NUL must fit within 128 KiB; an otherwise valid
    /// configuration exceeding that limit fails with `runner-start`.
    pub overrides: BTreeMap<String, String>,
    /// Nonempty policy identity, at most 256 bytes without controls.
    pub identity: String,
}
impl fmt::Debug for Environment {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Environment")
            .field("mode", &self.mode)
            .field("entries", &self.overrides.len())
            .field("identity", &self.identity)
            .finish()
    }
}
/// Finite execution, protocol and cleanup bounds. Every field is fingerprinted.
#[derive(Clone, Debug, Serialize)]
pub struct Limits {
    /// Owner admissions over the instance lifetime; 1–256.
    pub max_sessions: usize,
    /// Queued operations per owner, excluding the holder; 1–64.
    pub max_queued_per_session: usize,
    /// Source UTF-8 bytes; 1–1 MiB.
    pub max_code_bytes: usize,
    /// Retained UTF-8 bytes per output stream; 1–100 KiB.
    pub max_output_bytes_per_stream: usize,
    /// Trailing expression representation bytes; 1–100 KiB.
    pub max_result_bytes: usize,
    /// Exception text bytes; 1–100 KiB.
    pub max_exception_bytes: usize,
    /// Frozen host entries, excluding private-directory keys; 1–1024.
    pub max_environment_entries: usize,
    /// Frozen key + equals + value bytes; 1–256 KiB.
    pub max_environment_bytes: usize,
    /// Positive whole seconds, at most max_timeout. Measured from request write;
    /// queue wait and runner startup are additional.
    pub default_timeout: Duration,
    /// Maximum whole seconds; 1 second–24 hours. Measured from request write;
    /// queue wait and runner startup are additional.
    pub max_timeout: Duration,
    /// Positive readiness bound, at most 60 seconds.
    pub runner_start_timeout: Duration,
    /// Positive TERM grace, at most 60 seconds; ends early on leader exit.
    pub terminate_grace: Duration,
    /// Positive reaping bound, at most 60 seconds.
    pub kill_wait: Duration,
    /// Positive close/join bound, at most 120 seconds; at least grace + kill_wait.
    /// Applies separately to close and cleanup join, so shutdown can take twice
    /// this bound.
    pub shutdown_grace: Duration,
}
impl Limits {
    /// Conservative bound for the inline tool-result JSON text, including escaping.
    /// Embedding this text again as a JSON string in events or snapshots requires
    /// additional space for that outer escaping.
    pub fn worst_case_execute_bytes(&self) -> usize {
        1024usize.saturating_add(
            6usize.saturating_mul(
                self.max_output_bytes_per_stream
                    .saturating_mul(2)
                    .saturating_add(self.max_result_bytes.max(self.max_exception_bytes)),
            ),
        )
    }
    /// Conservative inline clear-result JSON bound.
    pub fn worst_case_clear_bytes() -> usize {
        64
    }
}
/// Host-provisioned Python and trusted temporary root. Construction never spawns.
#[derive(Clone)]
pub struct Options {
    /// Absolute normalized executable path. This path is executed unchanged;
    /// its canonical target is fingerprinted, preserving venv/symlink semantics.
    /// Hosts must not repoint the path while this instance is live.
    pub python_path: PathBuf,
    /// Nonempty interpreter policy identity, at most 256 bytes without controls.
    pub python_identity: String,
    /// Absolute normalized existing directory for private owner directories.
    pub temp_root: PathBuf,
    /// Frozen environment policy; keys and values are not fingerprinted.
    pub environment: Environment,
    /// Resource bounds.
    pub limits: Limits,
}
impl fmt::Debug for Options {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Options")
            .field("python_identity", &self.python_identity)
            .field("environment", &self.environment)
            .field("limits", &self.limits)
            .finish()
    }
}

pub(super) const PRIVATE_KEYS: [(&str, &str); 9] = [
    ("HOME", "home"),
    ("XDG_CACHE_HOME", "cache"),
    ("XDG_CONFIG_HOME", "config"),
    ("XDG_DATA_HOME", "data"),
    ("XDG_STATE_HOME", "state"),
    ("XDG_RUNTIME_DIR", "runtime"),
    ("TMPDIR", "tmp"),
    ("TMP", "tmp"),
    ("TEMP", "tmp"),
];
#[derive(Clone)]
pub(super) struct Configuration {
    pub(super) python: PathBuf,
    #[cfg(unix)]
    pub(super) temp_root: PathBuf,
    pub(super) environment: Vec<(String, String)>,
    pub(super) limits: Limits,
    pub(super) bounds: Bounds,
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
fn normalized(path: &Path) -> bool {
    use std::{os::unix::ffi::OsStrExt, path::Component};
    path.is_absolute()
        && !path.as_os_str().as_bytes().contains(&0)
        && path.components().collect::<PathBuf>().as_os_str() == path.as_os_str()
        && !path.components().any(|c| c == Component::ParentDir)
}
#[cfg(unix)]
fn validate_unix(options: Options) -> Result<(Configuration, String), ExtensionError> {
    use std::os::unix::{ffi::OsStrExt, fs::PermissionsExt};
    let l = &options.limits;
    for (value, cap) in [
        (l.max_sessions, 256),
        (l.max_queued_per_session, 64),
        (l.max_code_bytes, 1024 * 1024),
        (l.max_output_bytes_per_stream, 100 * 1024),
        (l.max_result_bytes, 100 * 1024),
        (l.max_exception_bytes, 100 * 1024),
        (l.max_environment_entries, 1024),
        (l.max_environment_bytes, 256 * 1024),
    ] {
        if value == 0 || value > cap {
            return Err(crate::config_error("limits"));
        }
    }
    if l.default_timeout < Duration::from_secs(1)
        || l.default_timeout > l.max_timeout
        || l.default_timeout.subsec_nanos() != 0
        || l.max_timeout < Duration::from_secs(1)
        || l.max_timeout > Duration::from_secs(86400)
        || l.max_timeout.subsec_nanos() != 0
    {
        return Err(crate::config_error("execution-timeout"));
    }
    if [l.runner_start_timeout, l.terminate_grace, l.kill_wait]
        .iter()
        .any(|d| d.is_zero() || *d > Duration::from_secs(60))
        || l.shutdown_grace.is_zero()
        || l.shutdown_grace > Duration::from_secs(120)
    {
        return Err(crate::config_error("termination-bounds"));
    }
    if !crate::valid_identity(&options.python_identity) {
        return Err(crate::config_error("python-identity"));
    }
    if !crate::valid_identity(&options.environment.identity) {
        return Err(crate::config_error("environment-identity"));
    }
    if !normalized(&options.python_path) {
        return Err(crate::config_error("python-path"));
    }
    let python = options
        .python_path
        .canonicalize()
        .map_err(|_| crate::config_error("python-executable"))?;
    let metadata = python
        .metadata()
        .map_err(|_| crate::config_error("python-executable"))?;
    if !metadata.is_file() || metadata.permissions().mode() & 0o111 == 0 {
        return Err(crate::config_error("python-executable"));
    }
    if !normalized(&options.temp_root) {
        return Err(crate::config_error("temp-root"));
    }
    let temp_root = options
        .temp_root
        .canonicalize()
        .map_err(|_| crate::config_error("temp-root"))?;
    if !temp_root.is_dir() {
        return Err(crate::config_error("temp-root"));
    }
    if PRIVATE_KEYS
        .iter()
        .any(|(key, _)| options.environment.overrides.contains_key(*key))
    {
        return Err(crate::config_error("environment-reserved-key"));
    }
    let mut environment = BTreeMap::new();
    if options.environment.mode == EnvironmentMode::InheritAndOverride {
        for (key, value) in std::env::vars_os() {
            let key = key
                .into_string()
                .map_err(|_| crate::config_error("environment-entry"))?;
            if PRIVATE_KEYS.iter().any(|(k, _)| *k == key) {
                continue;
            }
            let value = value
                .into_string()
                .map_err(|_| crate::config_error("environment-entry"))?;
            environment.insert(key, value);
        }
    }
    environment.extend(options.environment.overrides.clone());
    let mut bytes = 0usize;
    for (key, value) in &environment {
        if key.is_empty() || key.contains(['=', '\0']) || value.contains('\0') {
            return Err(crate::config_error("environment-entry"));
        }
        bytes = bytes
            .saturating_add(key.len())
            .saturating_add(1)
            .saturating_add(value.len());
    }
    if environment.len() > l.max_environment_entries {
        return Err(crate::config_error("environment-entries"));
    }
    if bytes > l.max_environment_bytes {
        return Err(crate::config_error("environment-bytes"));
    }
    if l.shutdown_grace < l.terminate_grace + l.kill_wait {
        return Err(crate::config_error("shutdown-grace-below-termination"));
    }
    let bounds = Bounds {
        request: u32::try_from(128 + 6 * l.max_code_bytes)
            .map_err(|_| crate::config_error("limits"))?,
        response: u32::try_from(
            512 + 6
                * (2 * l.max_output_bytes_per_stream + l.max_result_bytes + l.max_exception_bytes),
        )
        .map_err(|_| crate::config_error("limits"))?,
        output: l.max_output_bytes_per_stream,
        result: l.max_result_bytes,
        exception: l.max_exception_bytes,
    };
    let hash = crate::config_hash(&(
        "python-repl-v2",
        [super::EXECUTE_TOOL, super::CLEAR_TOOL],
        [super::PERMISSION_EXECUTE, super::PERMISSION_MANAGE],
        runner::RUNNER_PROTOCOL,
        runner::runner_digest(),
        runner::INTERPRETER_FLAGS,
        runner::arguments(runner::RUNNER_SOURCE, bounds),
        PRIVATE_KEYS,
        (bounds.request, bounds.response, runner::READY_MAX),
        python.as_os_str().as_bytes(),
        &options.python_identity,
        l,
        options.environment.mode,
        &options.environment.identity,
    ));
    Ok((
        Configuration {
            python: options.python_path,
            temp_root,
            environment: environment.into_iter().collect(),
            limits: options.limits,
            bounds,
        },
        hash,
    ))
}

#[cfg(all(test, not(unix)))]
mod unsupported_tests {
    use super::*;
    #[test]
    fn unsupported_platform_rejects_before_paths_or_limits() {
        let options = Options {
            python_path: PathBuf::new(),
            python_identity: String::new(),
            temp_root: PathBuf::new(),
            environment: Environment {
                mode: EnvironmentMode::ExplicitOnly,
                overrides: BTreeMap::new(),
                identity: String::new(),
            },
            limits: Limits {
                max_sessions: 0,
                max_queued_per_session: 0,
                max_code_bytes: 0,
                max_output_bytes_per_stream: 0,
                max_result_bytes: 0,
                max_exception_bytes: 0,
                max_environment_entries: 0,
                max_environment_bytes: 0,
                default_timeout: Duration::ZERO,
                max_timeout: Duration::ZERO,
                runner_start_timeout: Duration::ZERO,
                terminate_grace: Duration::ZERO,
                kill_wait: Duration::ZERO,
                shutdown_grace: Duration::ZERO,
            },
        };
        assert!(
            validate(options)
                .err()
                .unwrap()
                .to_string()
                .ends_with("unsupported-platform")
        );
    }
}
