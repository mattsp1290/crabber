//! Bounded rg JSON records including base64 paths and context attachment.
use crate::{MAX_LINE_BYTES, MAX_RESULT_BYTES};
use base64::{Engine, engine::general_purpose::STANDARD};
use crabber_tools_core::{PathDenyPolicy, WorkspaceRoot};
use serde_json::{Value, json};
use std::{collections::VecDeque, sync::Arc};
fn decode(v: &Value) -> String {
    if let Some(text) = v["text"].as_str() {
        return text.to_owned();
    }
    v["bytes"]
        .as_str()
        .and_then(|s| STANDARD.decode(s).ok())
        .map(|b| String::from_utf8_lossy(&b).into_owned())
        .unwrap_or_default()
}
fn allowed_path(root: &WorkspaceRoot, deny: &PathDenyPolicy, value: &Value) -> bool {
    use std::{
        ffi::OsString,
        os::unix::ffi::{OsStrExt, OsStringExt},
        path::{Component, Path, PathBuf},
    };
    let path = if let Some(text) = value["text"].as_str() {
        PathBuf::from(text)
    } else if let Some(bytes) = value["bytes"]
        .as_str()
        .and_then(|bytes| STANDARD.decode(bytes).ok())
    {
        PathBuf::from(OsString::from_vec(bytes))
    } else {
        return false;
    };
    if path.as_os_str().as_bytes().contains(&0)
        || !path
            .components()
            .any(|part| matches!(part, Component::Normal(_)))
        || path
            .components()
            .any(|part| !matches!(part, Component::Normal(_) | Component::CurDir))
    {
        return false;
    }
    // Resolve each emitted name, not just the caller's search root: an allowed
    // directory alias can contain descendants denied by their real spelling.
    crabber_tools_core::deny::check(
        root,
        deny,
        path.strip_prefix(Path::new(".")).unwrap_or(&path),
    )
    .is_ok()
}

#[cfg(test)]
#[path = "parse_deny_tests.rs"]
mod deny_tests;

fn cut(text: &str) -> (&str, bool) {
    let mut n = text.len().min(MAX_LINE_BYTES);
    while !text.is_char_boundary(n) {
        n -= 1;
    }
    (&text[..n], n < text.len())
}
fn line(data: &Value) -> Value {
    let text = decode(&data["lines"]);
    let (text, truncated) = cut(text.trim_end_matches('\n'));
    let mut v = json!({"line_number":data["line_number"],"line":text});
    if truncated {
        v["line_truncated"] = json!(true);
    }
    v
}
pub(crate) struct Parser {
    pub matches: Vec<Value>,
    deny: Option<(Arc<WorkspaceRoot>, PathDenyPolicy)>,
    pub reason: Option<&'static str>,
    pub malformed: bool,
    before: VecDeque<Value>,
    context: usize,
    limit: usize,
    include_submatches: bool,
}
impl Parser {
    pub fn new(context: usize, limit: usize, include_submatches: bool) -> Self {
        Self {
            matches: vec![],
            deny: None,
            reason: None,
            malformed: false,
            before: VecDeque::new(),
            context,
            limit,
            include_submatches,
        }
    }
    pub(crate) fn with_deny(mut self, root: Arc<WorkspaceRoot>, deny: PathDenyPolicy) -> Self {
        if !deny.patterns().is_empty() {
            self.deny = Some((root, deny));
        }
        self
    }
    pub fn feed(&mut self, record: &[u8]) {
        let record: Value = if let Ok(v) = serde_json::from_slice(record) {
            v
        } else {
            self.malformed = true;
            return;
        };
        let data = &record["data"];
        if let Some((root, deny)) = &self.deny
            && matches!(record["type"].as_str(), Some("match" | "context"))
            && !allowed_path(root, deny, &data["path"])
        {
            self.before.clear();
            return;
        }
        match record["type"].as_str() {
            Some("begin" | "end") => {
                self.before.clear();
            }
            Some("context") => {
                let context = line(data);
                let path = decode(&data["path"]);
                let path = path.strip_prefix("./").unwrap_or(&path);
                if let Some(last) = self.matches.last_mut() {
                    let n = context["line_number"].as_u64().unwrap_or(0);
                    let last_n = last["line_number"].as_u64().unwrap_or(0);
                    if last["path"] == path && n > last_n && n - last_n <= self.context as u64 {
                        if last.get("after").is_none() {
                            last["after"] = json!([]);
                        }
                        last["after"].as_array_mut().unwrap().push(context.clone());
                    }
                }
                if self.context > 0 {
                    self.before.push_back(context);
                    if self.before.len() > self.context {
                        self.before.pop_front();
                    }
                }
            }
            Some("match") => {
                let path = decode(&data["path"]);
                let path = path.strip_prefix("./").unwrap_or(&path);
                let mut v = line(data);
                v["path"] = json!(path);
                if let Some(subs) = crate::submatches::retained(
                    data,
                    v["line"].as_str().unwrap(),
                    self.include_submatches,
                ) {
                    v["submatches"] = json!(subs);
                } else {
                    self.reason = Some("bytes");
                    return;
                }
                if !self.before.is_empty() {
                    v["before"] = json!(self.before);
                    self.before.clear();
                }
                self.matches.push(v);
                if serde_json::to_vec(&self.matches).unwrap().len() > MAX_RESULT_BYTES {
                    self.matches.pop();
                    self.reason = Some("bytes");
                    return;
                }
                if self.matches.len() >= self.limit {
                    self.reason = Some("matches");
                }
            }
            Some("summary") => {}
            _ => {
                self.malformed = true;
            }
        }
        if serde_json::to_vec(&self.matches).unwrap().len() > MAX_RESULT_BYTES {
            // Context may grow the final match after it was accepted.
            self.matches.pop();
            self.reason = Some("bytes");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_base64_paths_and_lines_lossily() {
        let mut parser = Parser::new(0, 10, true);
        let record = json!({
            "type": "match",
            "data": {
                "path": {"bytes": STANDARD.encode(b"./bad\xff.txt")},
                "lines": {"bytes": STANDARD.encode(b"match \xff\n")},
                "line_number": 1,
                "submatches": []
            }
        });
        parser.feed(&serde_json::to_vec(&record).unwrap());
        assert!(!parser.malformed);
        assert_eq!(parser.matches.len(), 1);
        assert_eq!(parser.matches[0]["path"], "bad\u{fffd}.txt");
        assert_eq!(parser.matches[0]["line"], "match \u{fffd}");
    }
}
