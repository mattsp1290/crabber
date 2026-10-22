//! The `file_move` tool definition.
use crate::{FileTool, Kind, Options};
use crabber::{ToolDefinition, core::ToolInfo};
use std::sync::Arc;
/// Deterministic model-facing schema and advisory safety metadata.
#[must_use]
pub fn info() -> ToolInfo {
    ToolInfo { name: "file_move".into(), description: "Move or rename one workspace-relative regular file to a new workspace-relative path that does not exist yet. Optional create_dirs=true creates the destination parent chain. The file keeps its mode and ownership. Fails with conflict if the destination exists.".into(), parameters: serde_json::json!({"type": "object", "additionalProperties": false, "required": ["path", "new_path"], "properties": {"path": {"type": "string", "minLength": 1, "description": "Workspace-relative source file. Must be a regular file."}, "new_path": {"type": "string", "minLength": 1, "description": "Workspace-relative destination. Must not exist."}, "create_dirs": {"type": "boolean", "description": "If true, mkdir -p the destination parent chain. Default false."}}}), retry_safe: false, required_permissions: vec![crabber_tools_core::permission::FS_WRITE.into()] }
}
/// Build a context-required executor sharing the mount's capacity.
#[must_use]
pub fn definition(options: Arc<Options>) -> Arc<ToolDefinition> {
    Arc::new(ToolDefinition {
        info: info(),
        executor: Arc::new(FileTool {
            options,
            kind: Kind::Move,
        }),
    })
}
// Platform facts for capability-confined hard-link moves.

#[cfg(test)]
mod tests {
    use cap_std::{ambient_authority, fs::Dir};
    use std::os::unix::fs::symlink;

    #[test]
    fn hard_link_platform_facts() {
        let root = tempfile::tempdir().unwrap();
        let dir = Dir::open_ambient_dir(root.path(), ambient_authority()).unwrap();
        dir.write("source", b"contents").unwrap();
        dir.hard_link("source", &dir, "destination").unwrap();
        assert!(dir.symlink_metadata("destination").unwrap().is_file());
        assert_eq!(dir.read("destination").unwrap(), b"contents");

        symlink("source", root.path().join("symlink")).unwrap();
        dir.hard_link("symlink", &dir, "linked-symlink").unwrap();
        let metadata = dir.symlink_metadata("linked-symlink").unwrap();
        eprintln!(
            "linked source symlink: is_symlink={}, is_file={}",
            metadata.is_symlink(),
            metadata.is_file()
        );
        // cap-std links the name itself on the supported Unix platforms.
        assert!(metadata.is_symlink());
        assert_eq!(
            dir.read_link("linked-symlink").unwrap(),
            std::path::Path::new("source")
        );

        if let Ok(other) = tempfile::tempdir_in("/dev/shm") {
            use std::os::unix::fs::MetadataExt;
            if std::fs::metadata(root.path()).unwrap().dev()
                != std::fs::metadata(other.path()).unwrap().dev()
            {
                let destination = Dir::open_ambient_dir(other.path(), ambient_authority()).unwrap();
                let error = dir
                    .hard_link("source", &destination, "cross-device")
                    .unwrap_err();
                assert_eq!(error.raw_os_error(), Some(libc::EXDEV));
                assert!(!other.path().join("cross-device").exists());
                assert_eq!(dir.read("source").unwrap(), b"contents");
            }
        }
    }
}

use crate::operations::{MoveArgs, Request, io_error};
use crabber_tools_core::{ToolError, category, failed};
use serde_json::{Value, json};
use std::io;
#[cfg(test)]
thread_local! {
    static BEFORE_LINK: std::cell::RefCell<Option<Box<dyn FnOnce()>>> = const { std::cell::RefCell::new(None) };
    static BEFORE_UNLINK: std::cell::RefCell<Option<Box<dyn FnOnce()>>> = const { std::cell::RefCell::new(None) };
}
fn link_error(error: io::Error) -> ToolError {
    if error.kind() == io::ErrorKind::AlreadyExists {
        return ToolError::new(category::CONFLICT, "destination exists");
    }
    match error.raw_os_error() {
        Some(libc::EXDEV) => {
            ToolError::new(category::IO, "destination is on a different filesystem")
        }
        Some(code)
            if [libc::EPERM, libc::ENOTSUP, libc::EOPNOTSUPP, libc::EMLINK].contains(&code) =>
        {
            ToolError::new(category::IO, "filesystem refused the link")
        }
        _ => io_error(error),
    }
}
impl Request {
    pub(crate) fn relocate(&self, options: &Options, a: &MoveArgs) -> Result<Value, ToolError> {
        self.require_regular_source(options)?;
        let dir = options.root.dir();
        let destination = a.new_path.as_path();
        match dir.symlink_metadata(destination) {
            Ok(_) => return Err(ToolError::new(category::CONFLICT, "destination exists")),
            Err(e) if e.kind() == io::ErrorKind::NotFound => (),
            Err(e) => return Err(io_error(e)),
        }
        if a.create_dirs
            && let Some(parent) = destination.parent().filter(|p| !p.as_os_str().is_empty())
        {
            dir.create_dir_all(parent).map_err(io_error)?;
        }
        #[cfg(test)]
        BEFORE_LINK.with(|slot| {
            if let Some(hook) = slot.borrow_mut().take() {
                hook();
            }
        });
        dir.hard_link(self.path.as_path(), dir, destination)
            .map_err(link_error)?;
        // Supported platforms link the source name itself, including a raced-in symlink.
        match dir.symlink_metadata(destination) {
            Ok(metadata) if metadata.is_file() => (),
            Ok(_) => {
                dir.remove_file(destination).map_err(io_error)?;
                return Err(ToolError::new(category::VALIDATION, "not a regular file"));
            }
            Err(e) => {
                let _ = dir.remove_file(destination);
                return Err(io_error(e));
            }
        }
        #[cfg(test)]
        BEFORE_UNLINK.with(|slot| {
            if let Some(hook) = slot.borrow_mut().take() {
                hook();
            }
        });
        // Catch the public host hook here so its panic cannot strand an unreported link.
        let removal = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            if let Some(hook) = &options.pre_write {
                hook();
            }
            dir.remove_file(self.path.as_path())
        }));
        let (category, rollback_message, partial_message) = match removal {
            Ok(Ok(())) => return Ok(json!({"outcome":"succeeded"})),
            Ok(Err(_)) => (
                category::IO,
                "move could not remove source",
                "move installed destination but could not remove source",
            ),
            Err(_) => (
                category::UNKNOWN,
                "move hook panicked; destination rolled back",
                "move hook panicked; destination could not be rolled back",
            ),
        };
        if dir.remove_file(destination).is_ok() {
            return Err(ToolError::new(category, rollback_message));
        }
        let mut out = failed(category, partial_message);
        out["partial"] = json!(true);
        Ok(out)
    }
}

#[cfg(test)]
mod fault_tests {
    use super::*;
    use crabber_tools_core::{Capacity, Limits, RelPath, WorkspaceRoot};
    use std::os::unix::fs::{PermissionsExt, symlink};
    use std::time::Duration;

    fn fixture() -> (tempfile::TempDir, Options, Request, MoveArgs) {
        let temp = tempfile::tempdir().unwrap();
        std::fs::create_dir(temp.path().join("src")).unwrap();
        std::fs::create_dir(temp.path().join("dst")).unwrap();
        std::fs::write(temp.path().join("src/file"), b"original").unwrap();
        let limits = Limits {
            max_in_flight: 1,
            max_blocking_wait: Duration::from_secs(1),
        };
        let options = Options {
            root: WorkspaceRoot::open(temp.path()).unwrap(),
            capacity: Capacity::new(&limits).unwrap(),
            limits,
            pre_write: None,
        };
        let request =
            Request::parse(Kind::Move, json!({"path":"src/file","new_path":"dst/new"})).unwrap();
        let args = MoveArgs {
            path: "src/file".into(),
            requested_new_path: "dst/new".into(),
            new_path: RelPath::parse("dst/new", false).unwrap(),
            create_dirs: false,
        };
        (temp, options, request, args)
    }
    #[test]
    fn destination_race_never_overwrites() {
        let (temp, options, request, args) = fixture();
        let destination = temp.path().join("dst/new");
        BEFORE_LINK.with(|slot| {
            *slot.borrow_mut() = Some(Box::new(move || {
                std::fs::write(destination, b"external").unwrap();
            }));
        });
        assert_eq!(
            request.relocate(&options, &args).unwrap_err().category,
            category::CONFLICT
        );
        assert_eq!(
            std::fs::read(temp.path().join("src/file")).unwrap(),
            b"original"
        );
        assert_eq!(
            std::fs::read(temp.path().join("dst/new")).unwrap(),
            b"external"
        );
    }
    #[test]
    fn symlink_source_race_is_removed() {
        let (temp, options, request, args) = fixture();
        let source = temp.path().join("src/file");
        std::fs::write(temp.path().join("src/target"), b"safe").unwrap();
        BEFORE_LINK.with(|slot| {
            *slot.borrow_mut() = Some(Box::new(move || {
                std::fs::remove_file(&source).unwrap();
                symlink("target", source).unwrap();
            }));
        });
        assert_eq!(
            request.relocate(&options, &args).unwrap_err().category,
            category::VALIDATION
        );
        assert!(std::fs::symlink_metadata(temp.path().join("dst/new")).is_err());
        assert!(
            std::fs::symlink_metadata(temp.path().join("src/file"))
                .unwrap()
                .file_type()
                .is_symlink()
        );
        assert_eq!(
            std::fs::read(temp.path().join("src/target")).unwrap(),
            b"safe"
        );
    }
    struct RestorePermissions(Vec<std::path::PathBuf>);
    impl Drop for RestorePermissions {
        fn drop(&mut self) {
            for path in &self.0 {
                std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)).unwrap();
            }
        }
    }
    #[test]
    fn unlink_failure_rolls_back_or_reports_partial() {
        if rustix::process::geteuid().is_root() {
            assert!(
                std::env::var_os("CI").is_none(),
                "unlink_failure_rolls_back_or_reports_partial requires a non-root CI runner"
            );
            // Root bypasses directory write permissions, so these injections cannot fail.
            return;
        }
        for partial in [false, true] {
            let (temp, options, request, args) = fixture();
            let source = temp.path().join("src");
            let destination = temp.path().join("dst");
            let _restore = RestorePermissions(vec![source.clone(), destination.clone()]);
            BEFORE_UNLINK.with(|slot| {
                *slot.borrow_mut() = Some(Box::new(move || {
                    std::fs::set_permissions(source, std::fs::Permissions::from_mode(0o500))
                        .unwrap();
                    if partial {
                        std::fs::set_permissions(
                            destination,
                            std::fs::Permissions::from_mode(0o500),
                        )
                        .unwrap();
                    }
                }));
            });
            if partial {
                let value = request.relocate(&options, &args).unwrap();
                assert_eq!(value["partial"], true);
                assert_eq!(value["error"]["category"], "io");
                assert!(temp.path().join("dst/new").exists());
                assert_eq!(
                    request.relocate(&options, &args).unwrap_err().category,
                    category::CONFLICT
                );
            } else {
                assert_eq!(
                    request.relocate(&options, &args).unwrap_err().category,
                    category::IO
                );
                assert!(!temp.path().join("dst/new").exists());
            }
            assert_eq!(
                std::fs::read(temp.path().join("src/file")).unwrap(),
                b"original"
            );
        }
    }
    #[test]
    fn link_failures_are_sanitized() {
        for code in [libc::EPERM, libc::ENOTSUP, libc::EOPNOTSUPP, libc::EMLINK] {
            let error = link_error(io::Error::from_raw_os_error(code));
            assert_eq!(error.category, category::IO);
            assert_eq!(error.message, "filesystem refused the link");
        }
        assert_eq!(
            link_error(io::Error::from_raw_os_error(libc::EXDEV)).message,
            "destination is on a different filesystem"
        );
    }
    #[test]
    fn panicking_host_hook_rolls_back_or_reports_partial() {
        for partial in [false, true] {
            if partial && rustix::process::geteuid().is_root() {
                assert!(
                    std::env::var_os("CI").is_none(),
                    "panicking_host_hook_rolls_back_or_reports_partial requires a non-root CI runner"
                );
                // Root bypasses directory write permissions; always retain successful rollback coverage.
                continue;
            }
            let (temp, mut options, request, _) = fixture();
            let destination = temp.path().join("dst");
            let _restore = RestorePermissions(vec![destination.clone()]);
            options.pre_write = Some(Arc::new(move || {
                if partial {
                    std::fs::set_permissions(&destination, std::fs::Permissions::from_mode(0o500))
                        .unwrap();
                }
                panic!("injected host hook panic");
            }));
            let value = request.run(&options, &tokio_util::sync::CancellationToken::new());
            assert_eq!(value["error"]["category"], "unknown");
            assert_eq!(value["path"], "src/file");
            assert_eq!(value["new_path"], "dst/new");
            assert_eq!(value.get("partial").is_some(), partial);
            if partial {
                assert_eq!(value["partial"], true);
            }
            assert_eq!(temp.path().join("dst/new").exists(), partial);
            assert_eq!(
                std::fs::read(temp.path().join("src/file")).unwrap(),
                b"original"
            );
        }
    }
}
