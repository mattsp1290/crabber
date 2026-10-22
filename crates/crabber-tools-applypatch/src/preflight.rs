use crate::{
    MAX_FILE_BYTES, MAX_PLAN_BYTES,
    parser::{self, Kind, Operation},
    result::FileResult,
};
use cap_std::fs::{Metadata, OpenOptions, OpenOptionsExt, PermissionsExt};
use crabber_tools_core::{RelPath, ToolError, WorkspaceRoot, category, category_for_io};
use std::{
    collections::BTreeSet,
    io::Read,
    path::{Path, PathBuf},
};
use tokio_util::sync::CancellationToken;
pub(crate) struct Change {
    pub(crate) op: Operation,
    pub(crate) src: PathBuf,
    pub(crate) dst: PathBuf,
    pub(crate) bytes: Vec<u8>,
    pub(crate) owner: Option<Metadata>,
    pub(crate) mode: u32,
}
pub(crate) struct Plan {
    pub(crate) changes: Vec<Change>,
    pub(crate) files: Vec<FileResult>,
}
pub(crate) fn error(e: std::io::Error) -> ToolError {
    ToolError::new(category_for_io(&e), "patch filesystem operation failed")
}
pub(crate) fn check(cancel: &CancellationToken) -> Result<(), ToolError> {
    if cancel.is_cancelled() {
        Err(ToolError::new(category::UNKNOWN, "cancelled"))
    } else {
        Ok(())
    }
}
fn mark(seen: &mut BTreeSet<PathBuf>, path: &Path) -> Result<(), ToolError> {
    crate::targets::record(seen, path)
}

fn target(root: &WorkspaceRoot, path: &str) -> Result<PathBuf, ToolError> {
    let path = RelPath::parse(path, false)?;
    if path.is_root() {
        return Err(ToolError::new(
            category::IS_DIRECTORY,
            "target is a directory",
        ));
    }
    let mut ancestor = path.as_path().to_owned();
    let mut missing = Vec::new();
    loop {
        match root.dir().symlink_metadata(&ancestor) {
            Ok(_) => {
                let resolved = root.dir().canonicalize(&ancestor).map_err(error)?;
                let mut result = resolved;
                for part in missing.iter().rev() {
                    result.push(part);
                }
                return Ok(result);
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                missing.push(
                    ancestor
                        .file_name()
                        .ok_or_else(|| ToolError::new(category::VALIDATION, "invalid target"))?
                        .to_owned(),
                );
                ancestor = ancestor
                    .parent()
                    .filter(|p| !p.as_os_str().is_empty())
                    .unwrap_or(Path::new("."))
                    .to_owned();
            }
            Err(e) => return Err(error(e)),
        }
    }
}
fn source(root: &WorkspaceRoot, path: &str) -> Result<(PathBuf, Metadata), ToolError> {
    let path = RelPath::parse(path, false)?;
    // Resolve first to classify an escaping leaf as path_escape, then reject
    // all leaf symlinks (including in-root links) as unsupported.
    let resolved = root.dir().canonicalize(path.as_path()).map_err(error)?;
    let meta = root.dir().symlink_metadata(path.as_path()).map_err(error)?;
    if meta.file_type().is_symlink() {
        return Err(ToolError::new(
            "unsupported",
            "patch source must not be a symlink",
        ));
    }
    if meta.is_dir() {
        return Err(ToolError::new(
            category::IS_DIRECTORY,
            "patch source is a directory",
        ));
    }
    if !meta.is_file() {
        return Err(ToolError::new(
            "unsupported",
            "patch source must be a regular file",
        ));
    }
    Ok((resolved, meta))
}
fn must_be_absent(root: &WorkspaceRoot, path: &Path) -> Result<(), ToolError> {
    match root.dir().symlink_metadata(path) {
        Ok(_) => Err(ToolError::new(
            category::VALIDATION,
            "patch destination already exists",
        )),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(error(e)),
    }
}
fn text(
    root: &WorkspaceRoot,
    path: &Path,
    cancel: &CancellationToken,
) -> Result<String, ToolError> {
    let mut file = root
        .dir()
        .open_with(
            path,
            OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_NONBLOCK | libc::O_NOFOLLOW),
        )
        .map_err(error)?;
    if !file.metadata().map_err(error)?.is_file() {
        return Err(ToolError::new(
            "unsupported",
            "patch source must be a regular file",
        ));
    }
    let mut bytes = Vec::new();
    let mut chunk = [0; 8192];
    loop {
        check(cancel)?;
        let n = file.read(&mut chunk).map_err(error)?;
        if n == 0 {
            break;
        }
        if bytes.len() + n > MAX_FILE_BYTES {
            return Err(ToolError::new(
                category::TOO_LARGE,
                "patch source exceeds 16 MiB",
            ));
        }
        bytes.extend_from_slice(&chunk[..n]);
    }
    if bytes.contains(&0) {
        return Err(ToolError::new(
            category::BINARY,
            "patch source is not UTF-8 text",
        ));
    }
    String::from_utf8(bytes)
        .map_err(|_| ToolError::new(category::BINARY, "patch source is not UTF-8 text"))
}
fn update(
    mut content: String,
    op: &Operation,
    cancel: &CancellationToken,
) -> Result<Vec<u8>, ToolError> {
    let crlf = content.contains("\r\n");
    content = parser::normalize(&content);
    for h in &op.hunks {
        check(cancel)?;
        let position = crate::matching::unique_line_match(&content, &h.old, cancel)?;
        let length = content.len() - h.old.len() + h.new.len();
        if length > MAX_FILE_BYTES {
            return Err(ToolError::new(
                category::TOO_LARGE,
                "patch result exceeds 16 MiB",
            ));
        }
        content.replace_range(position..position + h.old.len(), &h.new);
    }
    if crlf {
        content = content.replace('\n', "\r\n");
    }
    if content.len() > MAX_FILE_BYTES {
        return Err(ToolError::new(
            category::TOO_LARGE,
            "patch result exceeds 16 MiB",
        ));
    }
    Ok(content.into_bytes())
}
fn change(
    root: &WorkspaceRoot,
    op: Operation,
    cancel: &CancellationToken,
    syntactic: &mut BTreeSet<PathBuf>,
    resolved: &mut BTreeSet<PathBuf>,
) -> Result<Change, ToolError> {
    for path in std::iter::once(&op.path).chain(op.new_path.iter()) {
        mark(syntactic, RelPath::parse(path, false)?.as_path())?;
    }
    let (src, owner, bytes, mode) = match op.kind {
        Kind::Add => {
            let dst = target(root, &op.path)?;
            must_be_absent(root, &dst)?;
            (dst, None, op.content.as_bytes().to_vec(), 0o644)
        }
        Kind::Update => {
            let (src, meta) = source(root, &op.path)?;
            let bytes = update(text(root, &src, cancel)?, &op, cancel)?;
            let mode = meta.permissions().mode();
            (src, Some(meta), bytes, mode)
        }
        Kind::Delete => {
            let (src, meta) = source(root, &op.path)?;
            (src, Some(meta), vec![], 0)
        }
    };
    mark(resolved, &src)?;
    let dst = if let Some(path) = &op.new_path {
        let dst = target(root, path)?;
        must_be_absent(root, &dst)?;
        mark(resolved, &dst)?;
        dst
    } else {
        src.clone()
    };
    Ok(Change {
        op,
        src,
        dst,
        owner,
        bytes,
        mode,
    })
}
pub(crate) fn plan(
    root: &WorkspaceRoot,
    operations: Vec<Operation>,
    cancel: &CancellationToken,
) -> Result<Plan, (ToolError, Vec<FileResult>)> {
    let mut changes = vec![];
    let mut files = vec![];
    let mut syntactic = BTreeSet::new();
    let mut resolved = BTreeSet::new();
    let mut retained = 0usize;
    for op in operations {
        files.push(FileResult::new(&op));
        let next =
            check(cancel).and_then(|_| change(root, op, cancel, &mut syntactic, &mut resolved));
        match next {
            Ok(next) => {
                retained += next.bytes.len();
                if retained > MAX_PLAN_BYTES {
                    return Err((
                        ToolError::new(category::TOO_LARGE, "patch plan exceeds 64 MiB"),
                        files,
                    ));
                }
                changes.push(next);
            }
            Err(e) => return Err((e, files)),
        }
    }
    Ok(Plan { changes, files })
}
