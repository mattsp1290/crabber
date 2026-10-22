//! Private-directory creation and host environment augmentation.
use super::{config::PRIVATE_KEYS, failure};
use crabber::ExtensionError;
use std::path::Path;

pub(super) fn environment(
    frozen: &[(String, String)],
    root: &Path,
) -> Result<Vec<(String, String)>, ExtensionError> {
    let mut environment = frozen.to_vec();
    for (key, child) in PRIVATE_KEYS {
        environment.push((
            key.into(),
            root.join(child)
                .to_str()
                .ok_or_else(|| failure("private-dirs"))?
                .into(),
        ));
    }
    Ok(environment)
}
#[cfg(unix)]
pub(super) fn create_root(root: &Path) -> std::io::Result<()> {
    use std::os::unix::fs::DirBuilderExt;
    std::fs::DirBuilder::new().mode(0o700).create(root)
}
#[cfg(unix)]
pub(super) fn prepare(root: &Path) -> Result<(), ExtensionError> {
    use std::os::unix::fs::PermissionsExt;
    // The manager owns root before this runs, including partial setup failures.
    // Retaining it lets bounded close retry removal rather than orphaning it.
    let result = (|| -> std::io::Result<()> {
        std::fs::set_permissions(root, std::fs::Permissions::from_mode(0o700))?;
        for child in ["home", "cache", "config", "data", "state", "runtime", "tmp"] {
            let path = root.join(child);
            match create_root(&path) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists && path.is_dir() => {}
                Err(e) => return Err(e),
            }
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))?;
        }
        Ok(())
    })();
    result.map_err(|_| failure("private-dirs"))
}
#[cfg(not(unix))]
pub(super) fn prepare(_: &Path) -> Result<(), ExtensionError> {
    Err(failure("private-dirs"))
}
