//! Host path exclusions, resolved through the admitted directory capability.
use crate::{ToolError, WorkspaceRoot, category, category_for_io, glob_bounds};
use crabber::ExtensionError;
use globset::{GlobBuilder, GlobSet, GlobSetBuilder};
use serde::{Serialize, Serializer};
use std::{
    io,
    path::{Component, Path, PathBuf},
    sync::Arc,
};

/// Sorted host globs denying paths and their descendants.
#[derive(Clone, Debug)]
pub struct PathDenyPolicy {
    patterns: Vec<String>,
    set: Arc<GlobSet>,
}

impl PartialEq for PathDenyPolicy {
    fn eq(&self, other: &Self) -> bool {
        self.patterns == other.patterns
    }
}
impl Eq for PathDenyPolicy {}
impl Serialize for PathDenyPolicy {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.patterns.serialize(serializer)
    }
}

impl PathDenyPolicy {
    /// An empty policy, preserving ordinary tool behavior without filesystem checks.
    #[must_use]
    pub fn none() -> Self {
        Self {
            patterns: Vec::new(),
            set: Arc::new(GlobSet::empty()),
        }
    }

    /// Compile at most 256 bounded workspace-relative globs.
    /// Basename globs apply at every depth; slash-containing globs are root-relative.
    ///
    /// # Errors
    /// Returns `Plan("deny")` for an invalid, absolute, traversing, or overly complex glob.
    pub fn new(patterns: impl IntoIterator<Item = String>) -> Result<Self, ExtensionError> {
        let invalid = || ExtensionError::Plan("deny".into());
        let mut normalized = Vec::new();
        for pattern in patterns {
            if normalized.len() == 256
                || pattern.is_empty()
                || pattern.contains(['\0', '[', '!'])
                || pattern.starts_with('/')
                || pattern.starts_with("./")
                || pattern
                    .split('/')
                    .any(|part| part.is_empty() || part == "..")
            {
                return Err(invalid());
            }
            glob_bounds::validate(&pattern).map_err(|_| invalid())?;
            normalized.push(pattern);
        }
        normalized.sort();
        normalized.dedup();
        let mut builder = GlobSetBuilder::new();
        for pattern in &normalized {
            builder.add(
                GlobBuilder::new(pattern)
                    .literal_separator(true)
                    .build()
                    .map_err(|_| invalid())?,
            );
            if !pattern.contains('/') {
                builder.add(
                    GlobBuilder::new(&format!("**/{pattern}"))
                        .literal_separator(true)
                        .build()
                        .map_err(|_| invalid())?,
                );
            }
        }
        Ok(Self {
            patterns: normalized,
            set: Arc::new(builder.build().map_err(|_| invalid())?),
        })
    }

    /// Sorted, deduplicated globs, also used for configuration identity.
    #[must_use]
    pub fn patterns(&self) -> &[String] {
        &self.patterns
    }

    /// Whether this normalized relative path or a proper ancestor is denied.
    #[must_use]
    pub fn matches(&self, relative: &Path) -> bool {
        relative.ancestors().any(|path| {
            !path.as_os_str().is_empty() && path != Path::new(".") && self.set.is_match(path)
        })
    }
}

/// Original and capability-resolved names, without exposing either in errors.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResolvedPath {
    /// The normalized path requested by the caller.
    pub requested: PathBuf,
    /// Existing symlink prefixes resolved, followed by any missing tail.
    pub resolved: PathBuf,
}

fn denied() -> ToolError {
    ToolError::new(category::PATH_DENIED, "path denied by host policy")
}
fn check_name(policy: &PathDenyPolicy, path: &Path) -> Result<(), ToolError> {
    if policy.matches(path) {
        Err(denied())
    } else {
        Ok(())
    }
}

/// Check the lexical name and each resolved symlink prefix before a tool operates.
/// An empty policy performs no I/O. Missing tails are allowed for writers.
///
/// # Errors
/// Returns `path_denied` for denied names or unresolved I/O, preserving capability
/// escapes as `path_escape` and non-directory ancestors as `not_directory`.
pub fn check(
    root: &WorkspaceRoot,
    policy: &PathDenyPolicy,
    relative: &Path,
) -> Result<ResolvedPath, ToolError> {
    let requested = relative.to_path_buf();
    if policy.patterns.is_empty() {
        return Ok(ResolvedPath {
            resolved: requested.clone(),
            requested,
        });
    }
    check_name(policy, relative)?;
    let mut resolved = PathBuf::new();
    let mut missing = false;
    for component in relative.components() {
        match component {
            Component::CurDir => continue,
            Component::Normal(name) => resolved.push(name),
            _ => {
                return Err(ToolError::new(
                    category::PATH_ESCAPE,
                    "path must stay inside workspace",
                ));
            }
        }
        check_name(policy, &resolved)?;
        if missing {
            continue;
        }
        match root.dir().symlink_metadata(&resolved) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                resolved = root
                    .dir()
                    .canonicalize(&resolved)
                    .map_err(resolution_error)?;
                check_name(policy, &resolved)?;
            }
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => missing = true,
            Err(error) => return Err(resolution_error(error)),
        }
    }
    if resolved.as_os_str().is_empty() {
        resolved.push(".");
    }
    Ok(ResolvedPath {
        requested,
        resolved,
    })
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "Owned error callback matches Result::map_err."
)]
fn resolution_error(error: io::Error) -> ToolError {
    match category_for_io(&error) {
        category::PATH_ESCAPE => {
            ToolError::new(category::PATH_ESCAPE, "path must stay inside workspace")
        }
        category::NOT_DIRECTORY => {
            ToolError::new(category::NOT_DIRECTORY, "filesystem operation failed")
        }
        _ => denied(),
    }
}

/// Recheck an opened read handle before reading any bytes.
/// On Linux, `/proc` identifies the actual opened target even after a symlink swap.
/// Other Unix targets rely on the capability-resolved pre-open check.
///
/// # Errors
/// Returns `path_denied` if Linux cannot identify a live, allowed in-root target.
pub fn verify_open(
    root: &WorkspaceRoot,
    policy: &PathDenyPolicy,
    file: &cap_std::fs::File,
) -> Result<(), ToolError> {
    if policy.patterns.is_empty() {
        return Ok(());
    }
    #[cfg(target_os = "linux")]
    {
        use std::os::fd::AsRawFd;
        use std::os::unix::ffi::OsStrExt;
        let target = std::fs::read_link(format!("/proc/self/fd/{}", file.as_raw_fd()))
            .map_err(|_| denied())?;
        if target.as_os_str().as_bytes().ends_with(b" (deleted)") {
            return Err(denied());
        }
        let relative = target.strip_prefix(root.path()).map_err(|_| denied())?;
        check_name(policy, relative)?;
    }
    #[cfg(not(target_os = "linux"))]
    let _ = (root, file);
    Ok(())
}
