//! Capability navigation distinguishes caller creates from existing link targets.
use super::routes::{LinkScope, RouteViews};
use super::{
    PathDenyPolicy, ToolError, WorkspaceRoot, category, denied, path_escape, resolution_error,
};
use std::{
    collections::{BTreeSet, VecDeque},
    ffi::OsString,
    io,
    path::{Component, Path, PathBuf},
};

#[derive(Clone, Copy, PartialEq, Eq)]
enum Origin {
    Caller,
    SymlinkTarget,
}

#[derive(PartialEq, Eq)]
enum Prefix {
    Directory,
    File,
    MissingCallerTail,
}

enum Step {
    Component(OsString, Origin),
    CompleteLink(LinkScope),
}

pub(super) fn resolve(
    root: &WorkspaceRoot,
    policy: &PathDenyPolicy,
    path: &Path,
) -> Result<PathBuf, ToolError> {
    Resolver {
        root,
        policy,
        pending: components(path, Origin::Caller)?,
        resolved: PathBuf::new(),
        prefix: Prefix::Directory,
        followed: 0,
        views: RouteViews::default(),
    }
    .run()
}

struct Resolver<'a> {
    root: &'a WorkspaceRoot,
    policy: &'a PathDenyPolicy,
    pending: VecDeque<Step>,
    resolved: PathBuf,
    prefix: Prefix,
    followed: usize,
    views: RouteViews,
}

impl Resolver<'_> {
    fn run(mut self) -> Result<PathBuf, ToolError> {
        while let Some(step) = self.pending.pop_front() {
            match step {
                Step::CompleteLink(scope) => {
                    self.views.complete_link(&self.resolved, scope)?;
                    self.views.check(self.policy, &self.resolved)?;
                }
                Step::Component(name, origin) => self.component(&name, origin)?,
            }
        }
        if self.resolved.as_os_str().is_empty() {
            self.resolved.push(".");
        }
        Ok(self.resolved)
    }

    fn component(&mut self, name: &std::ffi::OsStr, origin: Origin) -> Result<(), ToolError> {
        // Match complete spellings before another link erases their descendants.
        let mut spelling = self.resolved.join(name);
        for step in &self.pending {
            if let Step::Component(name, _) = step {
                spelling.push(name);
            }
        }
        self.views.check(self.policy, &spelling)?;
        if self.prefix == Prefix::File {
            return Err(ToolError::new(
                category::NOT_DIRECTORY,
                "filesystem operation failed",
            ));
        }
        match Path::new(name).components().next() {
            Some(Component::CurDir) => return Ok(()),
            Some(Component::ParentDir) => {
                if self.prefix == Prefix::MissingCallerTail {
                    return Err(denied());
                }
                if !self.resolved.pop() {
                    return Err(path_escape());
                }
                self.views.leave(&self.resolved);
                self.views.check(self.policy, &self.resolved)?;
                return Ok(());
            }
            Some(Component::Normal(name)) => self.resolved.push(name),
            _ => return Err(path_escape()),
        }
        let names = self.views.check(self.policy, &self.resolved)?;
        if self.prefix == Prefix::MissingCallerTail {
            return Ok(());
        }
        match self.root.dir().symlink_metadata(&self.resolved) {
            Ok(meta) if meta.file_type().is_symlink() => self.follow_link(names)?,
            Ok(meta) => {
                self.prefix = if meta.is_dir() {
                    Prefix::Directory
                } else {
                    Prefix::File
                }
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound && origin == Origin::Caller => {
                self.prefix = Prefix::MissingCallerTail;
            }
            Err(error) => return Err(resolution_error(error)),
        }
        Ok(())
    }

    fn follow_link(&mut self, names: BTreeSet<PathBuf>) -> Result<(), ToolError> {
        if self.followed == 40 {
            return Err(denied());
        }
        self.followed += 1;
        let target = self
            .root
            .dir()
            .read_link(&self.resolved)
            .map_err(resolution_error)?;
        if target.as_os_str().is_empty() {
            return Err(denied());
        }
        let mut pending = components(&target, Origin::SymlinkTarget)?;
        // Components discard trailing directory syntax; the OS does not.
        let bytes = target.as_os_str().as_encoded_bytes();
        if bytes.ends_with(b"/") || bytes.ends_with(b"/.") {
            pending.push_back(Step::Component(".".into(), Origin::SymlinkTarget));
        }
        pending.push_back(Step::CompleteLink(
            self.views.start_link(self.followed, names),
        ));
        self.resolved.pop();
        pending.append(&mut self.pending);
        self.pending = pending;
        Ok(())
    }
}

fn components(path: &Path, origin: Origin) -> Result<VecDeque<Step>, ToolError> {
    path.components()
        .map(|part| match part {
            Component::Normal(_) | Component::CurDir => {
                Ok(Step::Component(part.as_os_str().to_owned(), origin))
            }
            Component::ParentDir if origin == Origin::SymlinkTarget => {
                Ok(Step::Component("..".into(), origin))
            }
            _ => Err(path_escape()),
        })
        .collect()
}
