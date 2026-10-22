//! Capability-rooted workspace admission and syntactic path validation.
use crate::{ToolError, category};
use cap_std::{ambient_authority, fs::Dir};
use crabber::ExtensionError;
use std::{
    io,
    path::{Component, Path, PathBuf},
    sync::Arc,
};

/// An admitted directory inode; reopen after the host replaces the directory.
pub struct WorkspaceRoot {
    root: PathBuf,
    dir: Dir,
}
impl WorkspaceRoot {
    /// Admit an existing absolute directory. Invalid admission yields Plan("workspace").
    ///
    /// # Errors
    /// Returns a plan error for a relative, missing, inaccessible, or non-directory workspace.
    pub fn open(path: &Path) -> Result<Arc<Self>, ExtensionError> {
        let fail = || ExtensionError::Plan("workspace".into());
        if !path.is_absolute() {
            return Err(fail());
        }
        let root = path.canonicalize().map_err(|_| fail())?;
        let dir = Dir::open_ambient_dir(&root, ambient_authority()).map_err(|_| fail())?;
        Ok(Arc::new(Self { root, dir }))
    }
    /// Canonical absolute path used for cwd, root locks, and identity.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.root
    }
    /// All file operations must use this directory capability.
    #[must_use]
    pub fn dir(&self) -> &Dir {
        &self.dir
    }
    /// Missing, relative or different session directories fail closed.
    ///
    /// # Errors
    /// Returns `workspace_mismatch` for absent, relative, inaccessible, or different directories.
    pub fn matches_session_directory(&self, directory: Option<&str>) -> Result<(), ToolError> {
        let Some(d) = directory else {
            return Err(ToolError::new(
                category::WORKSPACE_MISMATCH,
                "session workspace unavailable",
            ));
        };
        if Path::new(d).is_absolute()
            && Path::new(d).canonicalize().ok().as_ref() == Some(&self.root)
        {
            Ok(())
        } else {
            Err(ToolError::new(
                category::WORKSPACE_MISMATCH,
                "session directory differs from admitted workspace",
            ))
        }
    }
}
/// A normalized workspace-relative name with no parent traversal or NUL.
#[derive(Clone, Debug)]
pub struct RelPath(PathBuf);
impl RelPath {
    /// Empty and dot paths are root only when `allow_empty` is true. Never expands tilde.
    ///
    /// # Errors
    /// Returns `validation` for NULs or a required empty path, and `path_escape` for traversal or absolute paths.
    pub fn parse(value: &str, allow_empty: bool) -> Result<Self, ToolError> {
        if value.contains('\0') {
            return Err(ToolError::new(category::VALIDATION, "path contains NUL"));
        }
        let mut path = PathBuf::new();
        for c in Path::new(value).components() {
            match c {
                Component::Normal(p) => path.push(p),
                Component::CurDir => {}
                _ => {
                    return Err(ToolError::new(
                        category::PATH_ESCAPE,
                        "path must stay inside workspace",
                    ));
                }
            }
        }
        if path.as_os_str().is_empty() && !allow_empty {
            if !value.is_empty() {
                return Ok(Self(PathBuf::from(".")));
            }
            return Err(ToolError::new(category::VALIDATION, "path is required"));
        }
        Ok(Self(path))
    }
    /// Capability-relative path (dot represents the root).
    #[must_use]
    pub fn as_path(&self) -> &Path {
        if self.is_root() {
            Path::new(".")
        } else {
            &self.0
        }
    }
    /// Whether this names the admitted root.
    #[must_use]
    pub fn is_root(&self) -> bool {
        self.0.as_os_str().is_empty() || self.0 == Path::new(".")
    }
}
/// Map filesystem failures without exposing the operating-system error message.
#[must_use]
pub fn category_for_io(error: &io::Error) -> &'static str {
    if error
        .to_string()
        .contains("a path led outside of the filesystem")
    {
        return category::PATH_ESCAPE;
    }
    match error.kind() {
        io::ErrorKind::NotFound => category::NOT_FOUND,
        io::ErrorKind::NotADirectory => category::NOT_DIRECTORY,
        io::ErrorKind::IsADirectory => category::IS_DIRECTORY,
        _ => category::IO,
    }
}

/// Host-owned authorization of persisted workspace routing identity.
/// Implementations must refuse missing identity and never derive authority from
/// an unconfigured session directory or the process working directory.
pub trait WorkspaceAdmission: Send + Sync {
    /// Admit a configured workspace, or return `WorkspaceUnavailable` / Plan.
    /// Factories invoke this on the blocking pool; implementations may perform I/O.
    ///
    /// # Errors
    /// Returns `WorkspaceUnavailable` for absent routing identity or a plan error when admission is refused.
    fn admit(
        &self,
        workspace: &crabber::extension::WorkspaceContext,
    ) -> Result<Arc<WorkspaceRoot>, ExtensionError>;
}

/// An explicit host table of workspace IDs and exact absolute directory strings.
/// Persist canonical paths: even a trailing slash differs from the stored name.
pub struct ExactRoots(std::collections::BTreeMap<String, PathBuf>);
impl ExactRoots {
    /// Validate a host admission table. Duplicate or empty IDs are rejected.
    ///
    /// # Errors
    /// Returns a plan error for duplicate or empty IDs, or paths that are not absolute UTF-8.
    pub fn new(
        entries: impl IntoIterator<Item = (String, PathBuf)>,
    ) -> Result<Self, ExtensionError> {
        let mut roots = std::collections::BTreeMap::new();
        for (id, path) in entries {
            if id.is_empty() {
                return Err(ExtensionError::Plan("workspace id empty".into()));
            }
            if !path.is_absolute() || path.to_str().is_none() {
                return Err(ExtensionError::Plan(
                    "workspace path must be absolute UTF-8".into(),
                ));
            }
            if roots.insert(id, path).is_some() {
                return Err(ExtensionError::Plan("duplicate workspace id".into()));
            }
        }
        Ok(Self(roots))
    }
}
impl WorkspaceAdmission for ExactRoots {
    fn admit(
        &self,
        workspace: &crabber::extension::WorkspaceContext,
    ) -> Result<Arc<WorkspaceRoot>, ExtensionError> {
        let id = workspace
            .workspace_id()
            .ok_or(ExtensionError::WorkspaceUnavailable)?;
        let directory = workspace
            .directory()
            .ok_or(ExtensionError::WorkspaceUnavailable)?;
        let path = self
            .0
            .get(id)
            .filter(|path| path.to_str() == Some(directory))
            .ok_or_else(|| ExtensionError::Plan("workspace".into()))?;
        let root = WorkspaceRoot::open(path)?;
        if path.canonicalize().ok().as_ref() != Some(&root.path().to_path_buf()) {
            return Err(ExtensionError::Plan("workspace".into()));
        }
        Ok(root)
    }
}
