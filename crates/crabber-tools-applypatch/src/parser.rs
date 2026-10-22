//! Iterative port of the pinned Go structured-patch grammar.
use crabber_tools_core::{ToolError, category};
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum Kind {
    Add,
    Update,
    Delete,
}
impl Kind {
    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::Add => "add",
            Self::Update => "update",
            Self::Delete => "delete",
        }
    }
}
pub(crate) struct Operation {
    pub(crate) kind: Kind,
    pub(crate) path: String,
    pub(crate) new_path: Option<String>,
    pub(crate) content: String,
    pub(crate) hunks: Vec<Hunk>,
    pub(crate) additions: usize,
    pub(crate) deletions: usize,
}
pub(crate) struct Hunk {
    pub(crate) old: String,
    pub(crate) new: String,
}
fn invalid() -> ToolError {
    ToolError::new(category::VALIDATION, "invalid structured patch")
}
pub(crate) fn normalize(text: &str) -> String {
    text.replace("\r\n", "\n").replace('\r', "\n")
}
const HEADERS: [(&str, Kind); 3] = [
    ("*** Add File: ", Kind::Add),
    ("*** Update File: ", Kind::Update),
    ("*** Delete File: ", Kind::Delete),
];
fn header(line: &str) -> Option<(Kind, &str)> {
    HEADERS
        .iter()
        .find_map(|(prefix, kind)| line.strip_prefix(prefix).map(|path| (*kind, path)))
}
pub(crate) fn parse(text: &str) -> Result<Vec<Operation>, ToolError> {
    let text = normalize(text);
    let mut lines: Vec<_> = text.split('\n').collect();
    while lines.last() == Some(&"") {
        lines.pop();
    }
    if lines.first() != Some(&"*** Begin Patch")
        || lines.last() != Some(&"*** End Patch")
        || lines.len() < 3
    {
        return Err(invalid());
    }
    let lines = &lines[1..lines.len() - 1];
    let mut i = 0;
    let mut operations = Vec::new();
    while i < lines.len() {
        let (kind, path) = header(lines[i]).ok_or_else(invalid)?;
        let mut op = Operation {
            kind,
            path: path.trim().into(),
            new_path: None,
            content: String::new(),
            hunks: Vec::new(),
            additions: 0,
            deletions: 0,
        };
        i += 1;
        match kind {
            Kind::Add => {
                while i < lines.len() && header(lines[i]).is_none() {
                    let content = lines[i].strip_prefix('+').ok_or_else(invalid)?;
                    op.content.push_str(content);
                    op.content.push('\n');
                    op.additions += 1;
                    i += 1;
                }
                if op.additions == 0 {
                    return Err(invalid());
                }
            }
            Kind::Update => {
                if i < lines.len()
                    && let Some(path) = lines[i].strip_prefix("*** Move to: ")
                {
                    if !path.trim().is_empty() {
                        op.new_path = Some(path.trim().into());
                    }
                    i += 1;
                }
                while i < lines.len() && header(lines[i]).is_none() {
                    if !lines[i].starts_with("@@") {
                        return Err(invalid());
                    }
                    i += 1;
                    let mut h = Hunk {
                        old: String::new(),
                        new: String::new(),
                    };
                    while i < lines.len()
                        && header(lines[i]).is_none()
                        && !lines[i].starts_with("@@")
                    {
                        let line = lines[i];
                        match line.as_bytes().first() {
                            Some(b' ') => {
                                h.old.push_str(&line[1..]);
                                h.old.push('\n');
                                h.new.push_str(&line[1..]);
                                h.new.push('\n');
                            }
                            Some(b'-') => {
                                h.old.push_str(&line[1..]);
                                h.old.push('\n');
                                op.deletions += 1;
                            }
                            Some(b'+') => {
                                h.new.push_str(&line[1..]);
                                h.new.push('\n');
                                op.additions += 1;
                            }
                            _ => return Err(invalid()),
                        }
                        i += 1;
                    }
                    if h.old.is_empty() {
                        return Err(invalid());
                    }
                    op.hunks.push(h);
                }
                if op.hunks.is_empty() && op.new_path.is_none() {
                    return Err(invalid());
                }
            }
            Kind::Delete => {}
        }
        operations.push(op);
    }
    if operations.is_empty() {
        return Err(invalid());
    }
    Ok(operations)
}
