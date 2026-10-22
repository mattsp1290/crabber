//! Bounded rg JSON records including base64 paths and context attachment.
use crate::{MAX_LINE_BYTES, MAX_RESULT_BYTES};
use base64::{Engine, engine::general_purpose::STANDARD};
use serde_json::{Value, json};
use std::collections::VecDeque;
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
            reason: None,
            malformed: false,
            before: VecDeque::new(),
            context,
            limit,
            include_submatches,
        }
    }
    pub fn feed(&mut self, record: &[u8]) {
        let record: Value = match serde_json::from_slice(record) {
            Ok(v) => v,
            Err(_) => {
                self.malformed = true;
                return;
            }
        };
        let data = &record["data"];
        match record["type"].as_str() {
            Some("begin") | Some("end") => {
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
                match crate::submatches::retained(
                    data,
                    v["line"].as_str().unwrap(),
                    self.include_submatches,
                ) {
                    Some(subs) => v["submatches"] = json!(subs),
                    None => {
                        self.reason = Some("bytes");
                        return;
                    }
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
