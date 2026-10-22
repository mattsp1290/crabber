//! Atomic sibling installation through an already admitted parent capability.
use cap_std::fs::{Dir, Metadata, MetadataExt, OpenOptions, Permissions, PermissionsExt};
use std::{
    ffi::OsStr,
    io::{self, Write},
    path::{Component, Path},
};
/// Whether installation may replace an existing destination.
#[derive(Clone, Copy)]
pub enum Install {
    /// Atomic rename, replacing an existing non-directory name.
    Replace,
    /// Atomic hard-link installation that fails if the destination exists.
    CreateNew,
}
/// Write, sync, and install a temporary sibling. `name` must be one basename.
/// Mode and conditional ownership are supplied by the tool's preflight. Root
/// preserves the source owner; its owner may preserve the group. Cleanup errors
/// can occur after installation, so callers must account for partial effects.
pub fn write_sibling(
    parent: &Dir,
    name: &OsStr,
    bytes: &[u8],
    owner: &Metadata,
    mode: u32,
    install: Install,
) -> io::Result<()> {
    let mut components = Path::new(name).components();
    if !matches!(components.next(), Some(Component::Normal(_))) || components.next().is_some() {
        return Err(io::Error::from(io::ErrorKind::InvalidInput));
    }
    let temp = crate::temp_name(".crabber-tools-tmp-")?;
    let mut file = parent.open_with(&temp, OpenOptions::new().write(true).create_new(true))?;
    let result = (|| {
        let uid = rustix::process::geteuid();
        if uid.is_root() || uid.as_raw() == owner.uid() {
            rustix::fs::fchown(
                &file,
                Some(rustix::fs::Uid::from_raw(owner.uid())),
                Some(rustix::fs::Gid::from_raw(owner.gid())),
            )?;
        }
        file.set_permissions(Permissions::from_mode(mode))?;
        file.write_all(bytes)?;
        file.sync_all()?;
        match install {
            Install::Replace => parent.rename(&temp, parent, name),
            Install::CreateNew => {
                parent.hard_link(&temp, parent, name)?;
                parent.remove_file(&temp)
            }
        }
    })();
    if result.is_err() {
        let _ = parent.remove_file(&temp);
    }
    result
}
