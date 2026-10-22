//! Temp siblings, preserved modes and ownership, and atomic rename through capabilities.
use crate::NEW_FILE_MODE;
use cap_std::fs::Dir;
use cap_std::fs::PermissionsExt;
use std::{io, path::Path};

pub(crate) fn write(dir: &Dir, target: &Path, bytes: &[u8]) -> io::Result<bool> {
    let target = match dir.symlink_metadata(target) {
        Ok(meta) if meta.file_type().is_symlink() => dir.canonicalize(target)?,
        Ok(_) => target.to_owned(),
        Err(e) if e.kind() == io::ErrorKind::NotFound => target.to_owned(),
        Err(e) => return Err(e),
    };
    let parent = target
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let parent = dir.open_dir(parent)?;
    let name = target
        .file_name()
        .ok_or_else(|| io::Error::from(io::ErrorKind::IsADirectory))?;
    let existing = match parent.metadata(name) {
        Ok(m) => Some(m),
        Err(e) if e.kind() == io::ErrorKind::NotFound => None,
        Err(e) => return Err(e),
    };
    if existing.as_ref().is_some_and(|m| m.is_dir()) {
        return Err(io::Error::from(io::ErrorKind::IsADirectory));
    }
    if existing.as_ref().is_some_and(|m| !m.is_file()) {
        return Err(io::Error::other("not a regular file"));
    }
    let owner = existing.clone().unwrap_or(parent.dir_metadata()?);
    let mode = existing
        .as_ref()
        .map(|m| m.permissions().mode())
        .unwrap_or(NEW_FILE_MODE);
    crabber_tools_core::atomic::write_sibling(
        &parent,
        name,
        bytes,
        &owner,
        mode,
        crabber_tools_core::atomic::Install::Replace,
    )?;
    Ok(existing.is_none())
}
