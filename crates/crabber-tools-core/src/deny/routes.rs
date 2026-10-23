//! Logical alias views live only within the resolved target they name.
use super::{PathDenyPolicy, ToolError, check_name, denied};
use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
};

const MAX_ROUTE_NAMES: usize = 256;

struct Frame {
    id: usize,
    anchor: PathBuf,
    names: BTreeSet<PathBuf>,
    protected: usize,
}

/// A link target is one navigation step for previously active views.
pub(super) struct LinkScope {
    id: usize,
    names: BTreeSet<PathBuf>,
    protected: Vec<usize>,
}

#[derive(Default)]
pub(super) struct RouteViews {
    frames: Vec<Frame>,
}

impl RouteViews {
    pub(super) fn check(
        &self,
        policy: &PathDenyPolicy,
        path: &Path,
    ) -> Result<BTreeSet<PathBuf>, ToolError> {
        let mut names = BTreeSet::from([path.to_path_buf()]);
        for frame in &self.frames {
            if let Ok(suffix) = path.strip_prefix(&frame.anchor) {
                names.extend(frame.names.iter().map(|alias| alias.join(suffix)));
            }
        }
        for name in &names {
            check_name(policy, name)?;
        }
        Ok(names)
    }

    pub(super) fn start_link(&mut self, id: usize, names: BTreeSet<PathBuf>) -> LinkScope {
        let protected = self
            .frames
            .iter_mut()
            .map(|frame| {
                frame.protected += 1;
                frame.id
            })
            .collect();
        LinkScope {
            id,
            names,
            protected,
        }
    }

    pub(super) fn complete_link(
        &mut self,
        resolved: &Path,
        mut scope: LinkScope,
    ) -> Result<(), ToolError> {
        for frame in &mut self.frames {
            if scope.protected.contains(&frame.id) {
                frame.protected -= 1;
            }
        }
        self.leave(resolved);
        // Deduplicate (anchor, spelling) pairs, including repeated root returns.
        let existing: BTreeSet<_> = self
            .frames
            .iter()
            .filter(|frame| frame.anchor == resolved)
            .flat_map(|frame| frame.names.iter().cloned())
            .collect();
        scope.names.retain(|name| !existing.contains(name));
        let retained: usize = self.frames.iter().map(|frame| frame.names.len()).sum();
        if retained + scope.names.len() > MAX_ROUTE_NAMES {
            return Err(denied());
        }
        if !scope.names.is_empty() {
            self.frames.push(Frame {
                id: scope.id,
                anchor: resolved.to_path_buf(),
                names: scope.names,
                protected: 0,
            });
        }
        Ok(())
    }

    pub(super) fn leave(&mut self, resolved: &Path) {
        self.frames
            .retain(|frame| frame.protected > 0 || resolved.starts_with(&frame.anchor));
    }
}
