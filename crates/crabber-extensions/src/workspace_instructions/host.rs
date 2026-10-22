//! Optional host adapters. Ambient path admission happens only in constructors.
use super::{chain, valid_file_name};
use cap_fs_ext::{DirExt, FollowSymlinks, OpenOptionsFollowExt};
use cap_std::{
    ambient_authority,
    fs::{Dir, OpenOptions},
};
use crabber::extension::{
    ExtensionError, WorkspaceContext, WorkspaceReadError, WorkspaceReadErrorKind as Kind,
    WorkspaceReader, WorkspaceReaderResolver,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    io::Read,
    path::{Path, PathBuf},
    sync::Arc,
};

/// A boundary-rooted reader restricted to one admitted workspace's ancestor chain.
pub struct BoundaryReader {
    directory: PathBuf,
    root: Arc<Dir>,
    allowed: BTreeSet<String>,
}
impl BoundaryReader {
    /// Admit canonical absolute directories once. Sibling paths remain denied.
    pub fn for_workspace(
        boundary: &Path,
        workspace_directory: &Path,
    ) -> Result<Self, ExtensionError> {
        let boundary = canonical(boundary)?;
        let directory = canonical(workspace_directory)?;
        let allowed = chain::derive(&[path_text(&boundary)?], &path_text(&directory)?, 64)
            .map_err(|_| invalid_host())?
            .ok_or_else(invalid_host)?
            .into_iter()
            .collect();
        let root = Arc::new(
            Dir::open_ambient_dir(boundary, ambient_authority()).map_err(|_| invalid_host())?,
        );
        Ok(Self {
            directory,
            root,
            allowed,
        })
    }
    /// Canonical identity the host should persist as the workspace directory.
    pub fn directory(&self) -> &Path {
        &self.directory
    }
}
#[async_trait::async_trait]
impl WorkspaceReader for BoundaryReader {
    async fn read_limited(
        &self,
        path: &str,
        max_bytes: usize,
    ) -> Result<Vec<u8>, WorkspaceReadError> {
        let (directory, name) = split(path)?;
        if !self.allowed.contains(directory) {
            return Err(error(Kind::Denied));
        }
        read(
            self.root.clone(),
            directory.to_owned(),
            name.to_owned(),
            max_bytes,
        )
        .await
    }
}
/// A workspace-rooted reader accepting only root basenames.
pub struct WorkspaceRootReader {
    directory: PathBuf,
    root: Arc<Dir>,
}
impl WorkspaceRootReader {
    pub fn open(workspace_directory: &Path) -> Result<Self, ExtensionError> {
        let directory = canonical(workspace_directory)?;
        let root = Arc::new(
            Dir::open_ambient_dir(&directory, ambient_authority()).map_err(|_| invalid_host())?,
        );
        Ok(Self { directory, root })
    }
    pub fn directory(&self) -> &Path {
        &self.directory
    }
}
#[async_trait::async_trait]
impl WorkspaceReader for WorkspaceRootReader {
    async fn read_limited(
        &self,
        path: &str,
        max_bytes: usize,
    ) -> Result<Vec<u8>, WorkspaceReadError> {
        let (directory, name) = split(path)?;
        if !directory.is_empty() {
            return Err(error(Kind::Denied));
        }
        read(self.root.clone(), String::new(), name.to_owned(), max_bytes).await
    }
}
/// Exact identity mapping; custom hosts may implement the public trait directly.
pub struct AllowListResolver {
    readers: BTreeMap<(String, String), Arc<dyn WorkspaceReader>>,
}
impl AllowListResolver {
    pub fn new(
        entries: Vec<(String, String, Arc<dyn WorkspaceReader>)>,
    ) -> Result<Self, ExtensionError> {
        let mut readers = BTreeMap::new();
        for (workspace, directory, reader) in entries {
            if !crate::valid_identity(&workspace)
                || !chain::absolute(&directory)
                || readers.insert((workspace, directory), reader).is_some()
            {
                return Err(invalid_host());
            }
        }
        Ok(Self { readers })
    }
}
#[async_trait::async_trait]
impl WorkspaceReaderResolver for AllowListResolver {
    async fn resolve(
        &self,
        context: &WorkspaceContext,
    ) -> Result<Arc<dyn WorkspaceReader>, WorkspaceReadError> {
        let (Some(id), Some(directory)) = (context.workspace_id(), context.directory()) else {
            return Err(error(Kind::Denied));
        };
        self.readers
            .get(&(id.into(), directory.into()))
            .cloned()
            .ok_or_else(|| error(Kind::Denied))
    }
}
fn canonical(path: &Path) -> Result<PathBuf, ExtensionError> {
    if !path.is_absolute() {
        return Err(invalid_host());
    }
    let path = path.canonicalize().map_err(|_| invalid_host())?;
    if !chain::absolute(&path_text(&path)?) {
        return Err(invalid_host());
    }
    Ok(path)
}
fn path_text(path: &Path) -> Result<String, ExtensionError> {
    path.to_str().map(str::to_owned).ok_or_else(invalid_host)
}
fn invalid_host() -> ExtensionError {
    crate::config_error("instructions-host")
}
fn error(kind: Kind) -> WorkspaceReadError {
    WorkspaceReadError::new(kind)
}
fn split(path: &str) -> Result<(&str, &str), WorkspaceReadError> {
    if path.len() > 4096 || path.starts_with('/') {
        return Err(error(Kind::InvalidPath));
    }
    let (directory, name) = path.rsplit_once('/').unwrap_or(("", path));
    if !valid_file_name(name)
        || (!directory.is_empty() && !directory.split('/').all(chain::segment))
    {
        return Err(error(Kind::InvalidPath));
    }
    Ok((directory, name))
}
fn io(error_value: std::io::Error) -> WorkspaceReadError {
    #[cfg(unix)]
    if error_value.raw_os_error() == Some(libc::ELOOP) {
        return error(Kind::InvalidPath);
    }
    error(if error_value.kind() == std::io::ErrorKind::NotFound {
        Kind::NotFound
    } else {
        Kind::Io
    })
}
async fn read(
    root: Arc<Dir>,
    directory: String,
    name: String,
    max_bytes: usize,
) -> Result<Vec<u8>, WorkspaceReadError> {
    let read_limit = max_bytes
        .checked_add(1)
        .and_then(|n| u64::try_from(n).ok())
        .ok_or_else(|| error(Kind::TooLarge))?;
    tokio::task::spawn_blocking(move || {
        let mut current = root.try_clone().map_err(io)?;
        if !directory.is_empty() {
            for component in directory.split('/') {
                let metadata = current.symlink_metadata(component).map_err(io)?;
                if !metadata.is_dir() {
                    return Err(error(Kind::InvalidPath));
                }
                current = current.open_dir_nofollow(component).map_err(io)?;
            }
        }
        if !current.symlink_metadata(&name).map_err(io)?.is_file() {
            return Err(error(Kind::InvalidPath));
        }
        let mut options = OpenOptions::new();
        options.read(true).follow(FollowSymlinks::No);
        #[cfg(unix)]
        {
            use cap_std::fs::OpenOptionsExt;
            options.custom_flags(libc::O_NONBLOCK);
        }
        let file = current.open_with(name, &options).map_err(io)?;
        if !file.metadata().map_err(io)?.is_file() {
            return Err(error(Kind::InvalidPath));
        }
        let mut bytes = Vec::new();
        file.take(read_limit).read_to_end(&mut bytes).map_err(io)?;
        if bytes.len() > max_bytes {
            return Err(error(Kind::TooLarge));
        }
        Ok(bytes)
    })
    .await
    .map_err(|_| error(Kind::Io))?
}
