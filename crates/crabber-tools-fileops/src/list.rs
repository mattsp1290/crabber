//! The file_list tool definition.
use crate::{FileTool, Kind, Options};
use crabber::{ToolDefinition, core::ToolInfo};
use std::sync::Arc;
/// Deterministic model-facing schema and advisory safety metadata.
pub fn info() -> ToolInfo {
    ToolInfo {name: "file_list".into(), description: "List directory entries under a workspace-relative path (empty or \".\" lists the workspace root). Output is sorted and capped at 5000 entries; oversize results set truncated=true.".into(), parameters: serde_json::json!({"properties": {"path": {"minLength": 1, "description": "Workspace-relative directory. Use '.' for the workspace root. Omit the field entirely to default to '.'.", "type": "string"}, "recursive": {"description": "If true, walk descendants. Default false (immediate children only).", "type": "boolean"}}, "additionalProperties": false, "type": "object"}), retry_safe: true, required_permissions: vec![crabber_tools_core::permission::FS_READ.into()]}
}
/// Build a context-required executor sharing the mount's capacity.
pub fn definition(options: Arc<Options>) -> Arc<ToolDefinition> {
    Arc::new(ToolDefinition {
        info: info(),
        executor: Arc::new(FileTool {
            options,
            kind: Kind::List,
        }),
    })
}

use crate::{
    MAX_LIST_ENTRIES,
    operations::{ListArgs, Request, io_error},
};
use crabber_tools_core::{ToolError, category};
use serde_json::{Value, json};
use std::path::Path;
use tokio_util::sync::CancellationToken;

impl Request {
    pub(crate) fn list(
        &self,
        options: &Options,
        a: &ListArgs,
        cancel: &CancellationToken,
    ) -> Result<Value, ToolError> {
        let dir = options
            .root
            .dir()
            .open_dir(self.path.as_path())
            .map_err(io_error)?;
        let mut entries = vec![];
        let truncated = walk(
            &dir,
            if self.path.is_root() {
                Path::new("")
            } else {
                self.path.as_path()
            },
            a.recursive,
            cancel,
            &mut entries,
        )?;
        Ok(if truncated {
            json!({"outcome":"succeeded","entries":entries,"truncated":true})
        } else {
            json!({"outcome":"succeeded","entries":entries})
        })
    }
}

fn walk(
    dir: &cap_std::fs::Dir,
    prefix: &Path,
    recursive: bool,
    cancel: &CancellationToken,
    out: &mut Vec<Value>,
) -> Result<bool, ToolError> {
    // Path ordering compares components, placing a/b before a.txt. One
    // global frontier keeps the earliest remaining pre-order entries without
    // retaining one heap or one open directory per level.
    let mut pending = std::collections::BTreeSet::new();
    enqueue(dir, Path::new(""), MAX_LIST_ENTRIES, cancel, &mut pending)?;
    while let Some(relative) = pending.pop_first() {
        check_walk_cancelled(cancel)?;
        let is_dir = dir.symlink_metadata(&relative).map_err(io_error)?.is_dir();
        out.push(json!({"path":prefix.join(&relative).to_string_lossy(),"is_dir":is_dir}));
        if out.len() == MAX_LIST_ENTRIES {
            return Ok(true);
        }
        if recursive && is_dir {
            enqueue(
                dir,
                &relative,
                MAX_LIST_ENTRIES - out.len(),
                cancel,
                &mut pending,
            )?;
        }
    }
    Ok(false)
}

fn check_walk_cancelled(cancel: &CancellationToken) -> Result<(), ToolError> {
    if cancel.is_cancelled() {
        Err(ToolError::new(category::UNKNOWN, "cancelled"))
    } else {
        Ok(())
    }
}

fn enqueue(
    root: &cap_std::fs::Dir,
    relative: &Path,
    budget: usize,
    cancel: &CancellationToken,
    pending: &mut std::collections::BTreeSet<std::path::PathBuf>,
) -> Result<(), ToolError> {
    let dir = root
        .open_dir(if relative.as_os_str().is_empty() {
            Path::new(".")
        } else {
            relative
        })
        .map_err(io_error)?;
    for entry in dir.entries().map_err(io_error)? {
        check_walk_cancelled(cancel)?;
        pending.insert(relative.join(entry.map_err(io_error)?.file_name()));
        if pending.len() > budget {
            pending.pop_last();
        }
        debug_assert!(pending.len() <= budget);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn deep_wide_frontier_stays_within_global_budget() {
        let temp = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(temp.path().join("a/a")).unwrap();
        for parent in ["", "a", "a/a"] {
            for i in 0..30 {
                std::fs::write(temp.path().join(parent).join(format!("z{i:02}")), "").unwrap();
            }
        }
        let root =
            cap_std::fs::Dir::open_ambient_dir(temp.path(), cap_std::ambient_authority()).unwrap();
        let mut pending = std::collections::BTreeSet::new();
        let cancel = CancellationToken::new();
        enqueue(&root, Path::new(""), 10, &cancel, &mut pending).unwrap();
        assert_eq!(pending.len(), 10);
        assert_eq!(pending.pop_first().unwrap(), Path::new("a"));
        enqueue(&root, Path::new("a"), 9, &cancel, &mut pending).unwrap();
        assert_eq!(pending.len(), 9);
        assert_eq!(pending.pop_first().unwrap(), Path::new("a/a"));
        enqueue(&root, Path::new("a/a"), 8, &cancel, &mut pending).unwrap();
        assert_eq!(pending.len(), 8);
        assert_eq!(pending.pop_first().unwrap(), Path::new("a/a/z00"));
    }
}
