use crate::{
    MAX_BODY_BYTES,
    fetch::{text, too_large},
};
use cap_std::fs::{OpenOptions, OpenOptionsExt};
use crabber_tools_core::{RelPath, ToolError, WorkspaceRoot, category_for_io};
use std::{io::Read, time::Instant};
use tokio_util::sync::CancellationToken;
use url::Url;
pub(crate) fn read(
    root: &WorkspaceRoot,
    url: &Url,
    cancel: &CancellationToken,
    deadline: Instant,
) -> Result<String, ToolError> {
    if url.host_str().is_some()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
    {
        return Err(ToolError::new(
            "validation",
            "file URL must name a local file without a query",
        ));
    }
    let absolute = url
        .to_file_path()
        .map_err(|_| ToolError::new("validation", "invalid file URL"))?;
    let relative = absolute
        .strip_prefix(root.path())
        .map_err(|_| ToolError::new("path_escape", "file URL must stay inside workspace"))?;
    let relative = relative
        .to_str()
        .ok_or_else(|| ToolError::new("validation", "file path must be UTF-8"))?;
    let path = RelPath::parse(relative, true)?;
    let io_error =
        |e: std::io::Error| ToolError::new(category_for_io(&e), "could not read workspace file");
    let mut file = root
        .dir()
        .open_with(
            path.as_path(),
            OpenOptions::new().read(true).custom_flags(libc::O_NONBLOCK),
        )
        .map_err(io_error)?;
    if !file.metadata().map_err(io_error)?.is_file() {
        return Err(ToolError::new("io", "resource must be a regular file"));
    }
    let mut bytes = Vec::new();
    let mut buffer = [0; 16 * 1024];
    loop {
        if cancel.is_cancelled() {
            return Err(ToolError::new("unknown", "cancelled"));
        }
        if Instant::now() >= deadline {
            return Err(ToolError::new("timeout", "URL fetch timed out"));
        }
        let count = file.read(&mut buffer).map_err(io_error)?;
        if count == 0 {
            return text(bytes);
        }
        if count > MAX_BODY_BYTES - bytes.len() {
            return Err(too_large());
        }
        bytes.extend_from_slice(&buffer[..count]);
    }
}
