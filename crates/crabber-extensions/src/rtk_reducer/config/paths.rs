use super::*;
use sha2::{Digest, Sha256};
use std::{
    io::Read,
    os::unix::fs::{MetadataExt, PermissionsExt},
};
fn normalized(path: &std::path::Path) -> bool {
    use std::{os::unix::ffi::OsStrExt, path::Component};
    path.is_absolute()
        && path.as_os_str().len() <= 4096
        && !path.as_os_str().as_bytes().contains(&0)
        && path.components().collect::<PathBuf>().as_os_str() == path.as_os_str()
        && !path.components().any(|c| c == Component::ParentDir)
}
pub(super) fn executable(path: PathBuf, expected: &str) -> Result<PathBuf, ExtensionError> {
    if !normalized(&path) {
        return Err(crate::config_error("executable-path"));
    }
    let executable = path
        .canonicalize()
        .map_err(|_| crate::config_error("executable-path"))?;
    let metadata = executable
        .metadata()
        .map_err(|_| crate::config_error("executable-file"))?;
    check_executable(&metadata)?;
    let mut file =
        std::fs::File::open(&executable).map_err(|_| crate::config_error("executable-file"))?;
    check_executable(
        &file
            .metadata()
            .map_err(|_| crate::config_error("executable-file"))?,
    )?;
    const MAX_EXECUTABLE: u64 = 128 * 1024 * 1024;
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; 32 * 1024];
    let mut total = 0u64;
    loop {
        let n = file
            .read(&mut buffer)
            .map_err(|_| crate::config_error("executable-file"))?;
        if n == 0 {
            break;
        }
        total += n as u64;
        if total > MAX_EXECUTABLE {
            return Err(crate::config_error("executable-file"));
        }
        hasher.update(&buffer[..n]);
    }
    if format!("{:x}", hasher.finalize()) != expected {
        return Err(crate::config_error("executable-digest-mismatch"));
    }
    Ok(executable)
}
pub(super) fn temp_root(path: PathBuf) -> Result<PathBuf, ExtensionError> {
    if !normalized(&path) {
        return Err(crate::config_error("temp-root"));
    }
    let temp_root = path
        .canonicalize()
        .map_err(|_| crate::config_error("temp-root"))?;
    let metadata = temp_root
        .metadata()
        .map_err(|_| crate::config_error("temp-root"))?;
    if !metadata.is_dir() {
        return Err(crate::config_error("temp-root"));
    }
    if metadata.permissions().mode() & 0o022 != 0 {
        return Err(crate::config_error("temp-root-mode"));
    }
    super::validate_owner(metadata.uid(), rustix::process::geteuid().as_raw())?;
    Ok(temp_root)
}

fn check_executable(metadata: &std::fs::Metadata) -> Result<(), ExtensionError> {
    const MAX_EXECUTABLE: u64 = 128 * 1024 * 1024;
    if !metadata.is_file()
        || metadata.permissions().mode() & 0o111 == 0
        || metadata.len() > MAX_EXECUTABLE
    {
        return Err(crate::config_error("executable-file"));
    }
    Ok(())
}
