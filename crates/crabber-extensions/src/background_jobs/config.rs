use super::*;
#[cfg(unix)]
use crate::process;
use serde::Serialize;
#[cfg(unix)]
use std::path::Component;
use std::{collections::BTreeMap, fmt, path::PathBuf, time::Duration};

/// How the environment is frozen at construction time.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub enum EnvironmentMode {
    /// Pass only explicitly supplied entries to the child.
    ExplicitOnly,
    /// Read the host environment once, then apply explicit overrides.
    InheritAndOverride,
}
/// Host-owned process environment. Values are excluded from Debug and hashes.
/// Rotate `identity` whenever effective values or routing policy change.
#[derive(Clone)]
pub struct Environment {
    /// Freeze mode.
    pub mode: EnvironmentMode,
    /// UTF-8 key/value entries. NUL and keys containing `=` are rejected.
    pub overrides: BTreeMap<String, String>,
    /// Nonempty policy identity, at most 256 bytes, without control characters.
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
/// Finite bounds shared across all owners/mounts of one instance.
/// Size output and tracked-job limits together: retained tails can reach 80 MiB.
#[derive(Clone, Debug, Serialize)]
pub struct Limits {
    /// Simultaneously starting/running/hidden jobs; 1–64.
    pub max_running: usize,
    /// Tracked jobs including retained finished jobs; 1–256, at least max_running.
    pub max_tracked: usize,
    /// Command UTF-8 bytes; 1–64 KiB.
    pub max_command_bytes: usize,
    /// Relative working-directory bytes; 1–4 KiB.
    pub max_working_directory_bytes: usize,
    /// Retained raw bytes per output stream; 1–160 KiB.
    pub max_output_bytes_per_stream: usize,
    /// Frozen environment entries; 1–1024.
    pub max_environment_entries: usize,
    /// Frozen key + equals + value bytes; 1–256 KiB.
    pub max_environment_bytes: usize,
    /// Whole-second default, zero disables automatic timeout; at most max_timeout.
    pub default_timeout: Duration,
    /// Maximum admitted whole-second timeout; 1 second–24 hours.
    pub max_timeout: Duration,
    /// TERM-to-KILL grace; positive and at most 60 seconds.
    pub terminate_grace: Duration,
    /// Reaping/output-drain wait per attempt; positive and at most 60 seconds.
    pub kill_wait: Duration,
    /// Close/join bound; positive and at most 120 seconds.
    pub shutdown_grace: Duration,
}
impl Limits {
    /// Conservative encoded status-result bound, including lossy UTF-8 and JSON
    /// escaping. Hosts size Crabber snapshot/text budgets with this bound.
    pub fn worst_case_status_bytes(&self) -> usize {
        1024usize.saturating_add(self.max_output_bytes_per_stream.saturating_mul(12))
    }
    /// Conservative encoded list-result bound for every tracked job.
    pub fn worst_case_list_bytes(&self) -> usize {
        16usize.saturating_add(self.max_tracked.saturating_mul(512))
    }
    /// Maximum raw bytes retained in both tails across tracked jobs; excludes
    /// snapshot strings, allocator overhead and in-flight result copies.
    pub fn worst_case_retained_bytes(&self) -> usize {
        self.max_tracked
            .saturating_mul(2)
            .saturating_mul(self.max_output_bytes_per_stream)
    }
}
/// Host-provisioned shell, environment and limits. Debug omits shell paths/values.
#[derive(Clone)]
pub struct Options {
    /// Absolute normalized executable path; symlinks resolve once at construction.
    pub shell_path: PathBuf,
    /// Nonempty shell policy identity, at most 256 bytes, without controls.
    pub shell_identity: String,
    /// Frozen environment policy; rotate its identity after behavior changes.
    pub environment: Environment,
    /// Resource bounds, all fingerprinted.
    pub limits: Limits,
}
impl fmt::Debug for Options {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Options")
            .field("shell_identity", &self.shell_identity)
            .field("environment", &self.environment)
            .finish()
    }
}

#[derive(Clone)]
pub(super) struct Configuration {
    pub(super) shell: PathBuf,
    pub(super) environment: Vec<(String, String)>,
    pub(super) limits: Limits,
}

pub(super) fn validate(options: Options) -> Result<(Configuration, String), ExtensionError> {
    #[cfg(not(unix))]
    {
        let _ = options;
        Err(crate::config_error("unsupported-platform"))
    }
    #[cfg(unix)]
    validate_unix(options)
}

#[cfg(unix)]
fn validate_unix(options: Options) -> Result<(Configuration, String), ExtensionError> {
    use std::os::unix::{ffi::OsStrExt, fs::PermissionsExt};
    let limits = &options.limits;
    for (value, cap) in [
        (limits.max_running, 64),
        (limits.max_tracked, 256),
        (limits.max_command_bytes, 64 * 1024),
        (limits.max_working_directory_bytes, 4096),
        (limits.max_output_bytes_per_stream, 160 * 1024),
        (limits.max_environment_entries, 1024),
        (limits.max_environment_bytes, 256 * 1024),
    ] {
        if value == 0 || value > cap {
            return Err(crate::config_error("limits"));
        }
    }
    if limits.max_tracked < limits.max_running {
        return Err(crate::config_error("tracked-below-running"));
    }
    if limits.max_timeout < Duration::from_secs(1)
        || limits.max_timeout > Duration::from_secs(86400)
        || limits.max_timeout.subsec_nanos() != 0
    {
        return Err(crate::config_error("max-timeout"));
    }
    if limits.default_timeout > limits.max_timeout || limits.default_timeout.subsec_nanos() != 0 {
        return Err(crate::config_error("default-timeout"));
    }
    if limits.terminate_grace.is_zero()
        || limits.terminate_grace > Duration::from_secs(60)
        || limits.kill_wait.is_zero()
        || limits.kill_wait > Duration::from_secs(60)
        || limits.shutdown_grace.is_zero()
        || limits.shutdown_grace > Duration::from_secs(120)
    {
        return Err(crate::config_error("termination-bounds"));
    }
    if !crate::valid_identity(&options.shell_identity) {
        return Err(crate::config_error("shell-identity"));
    }
    if !crate::valid_identity(&options.environment.identity) {
        return Err(crate::config_error("environment-identity"));
    }
    let path = &options.shell_path;
    let normalized: PathBuf = path.components().collect();
    if !path.is_absolute()
        || path.as_os_str().as_bytes().contains(&0)
        || normalized.as_os_str() != path.as_os_str()
        || path.components().any(|c| c == Component::ParentDir)
    {
        return Err(crate::config_error("shell-path"));
    }
    let shell = std::fs::canonicalize(path).map_err(|_| crate::config_error("shell-path"))?;
    let metadata =
        std::fs::metadata(&shell).map_err(|_| crate::config_error("shell-executable"))?;
    if !metadata.is_file() || metadata.permissions().mode() & 0o111 == 0 {
        return Err(crate::config_error("shell-executable"));
    }
    let hash = fingerprint(
        &options,
        &shell,
        [PERMISSION_START, PERMISSION_READ, PERMISSION_KILL],
        process::SUPERVISOR_PROTOCOL,
        &process::supervisor_digest(),
    );
    let mut environment = BTreeMap::new();
    if options.environment.mode == EnvironmentMode::InheritAndOverride {
        for (key, value) in std::env::vars_os() {
            let key = key
                .into_string()
                .map_err(|_| crate::config_error("environment-entry"))?;
            let value = value
                .into_string()
                .map_err(|_| crate::config_error("environment-entry"))?;
            environment.insert(key, value);
        }
    }
    environment.extend(options.environment.overrides);
    if environment.len() > limits.max_environment_entries {
        return Err(crate::config_error("environment-entries"));
    }
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
    if bytes > limits.max_environment_bytes {
        return Err(crate::config_error("environment-bytes"));
    }
    Ok((
        Configuration {
            shell,
            environment: environment.into_iter().collect(),
            limits: options.limits,
        },
        hash,
    ))
}

#[cfg(unix)]
pub(super) fn fingerprint(
    options: &Options,
    shell: &std::path::Path,
    permissions: [&str; 3],
    protocol: &str,
    script_digest: &str,
) -> String {
    use std::os::unix::ffi::OsStrExt;
    crate::config_hash(&(
        "background-jobs-v2",
        [START_TOOL, STATUS_TOOL, LIST_TOOL, KILL_TOOL],
        permissions,
        shell.as_os_str().as_bytes(),
        &options.shell_identity,
        protocol,
        script_digest,
        &options.limits,
        options.environment.mode,
        &options.environment.identity,
    ))
}

#[cfg(all(test, not(unix)))]
mod unsupported_tests {
    use super::*;
    #[test]
    fn unsupported_platform_rejects_construction_before_host_validation() {
        let options = Options {
            shell_path: PathBuf::new(),
            shell_identity: String::new(),
            environment: Environment {
                mode: EnvironmentMode::ExplicitOnly,
                overrides: BTreeMap::new(),
                identity: String::new(),
            },
            limits: Limits {
                max_running: 1,
                max_tracked: 1,
                max_command_bytes: 1,
                max_working_directory_bytes: 1,
                max_output_bytes_per_stream: 1,
                max_environment_entries: 1,
                max_environment_bytes: 1,
                default_timeout: Duration::ZERO,
                max_timeout: Duration::from_secs(1),
                terminate_grace: Duration::from_secs(1),
                kill_wait: Duration::from_secs(1),
                shutdown_grace: Duration::from_secs(1),
            },
        };
        assert!(
            BackgroundJobs::new(options)
                .err()
                .unwrap()
                .to_string()
                .ends_with("unsupported-platform")
        );
    }
}
