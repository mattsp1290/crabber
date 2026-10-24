//! Bounded pure framing; byte acquisition belongs to the host reader.
const OPEN: &str = "<workspace_instructions version=\"crabber-v1\">\n";
const CLOSE: &str = "</workspace_instructions>";
const TOO_LARGE: &str = "[omitted: file exceeds the configured limit]";
const OMITTED: &str =
    "[omitted: remaining instruction files exceed the configured section limit]\n";
pub(super) enum Entry {
    Content(String),
    TooLarge,
}
pub(super) struct Frame {
    output: String,
    checkpoints: Vec<usize>,
    max_bytes: usize,
    full: bool,
}
impl Frame {
    pub(super) fn new(max_bytes: usize) -> Self {
        Self {
            output: String::new(),
            checkpoints: vec![],
            max_bytes,
            full: false,
        }
    }
    /// False means the section is full and further reads can be omitted.
    pub(super) fn push(&mut self, display: &str, entry: Entry) -> bool {
        if self.full {
            return false;
        }
        let (content, truncated) = match entry {
            Entry::Content(content) => (content, false),
            Entry::TooLarge => (TOO_LARGE.into(), true),
        };
        let file = format!(
            "<file path=\"{}\"{}>\n{}\n</file>\n",
            escape(display),
            if truncated { " truncated=\"true\"" } else { "" },
            content
        );
        if self.output.is_empty() {
            self.output.push_str(OPEN);
        }
        if self.output.len() + file.len() + CLOSE.len() > self.max_bytes {
            while self.output.len() + OMITTED.len() + CLOSE.len() > self.max_bytes {
                self.output.truncate(
                    self.checkpoints
                        .pop()
                        .expect("minimum section bound fits omission"),
                );
            }
            self.output.push_str(OMITTED);
            self.full = true;
            return false;
        }
        self.checkpoints.push(self.output.len());
        self.output.push_str(&file);
        true
    }
    pub(super) fn finish(mut self) -> Option<String> {
        if self.output.is_empty() {
            None
        } else {
            self.output.push_str(CLOSE);
            Some(self.output)
        }
    }
}
fn escape(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('"', "&quot;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn preserves_frame_paths_and_marks_oversized_files_without_partial_content() {
        let mut f = Frame::new(4096);
        assert!(f.push("../AGENTS.md", Entry::Content("parent".into())));
        assert!(f.push("./AGENTS.md", Entry::TooLarge));
        assert_eq!(
            f.finish().unwrap(),
            format!(
                "{OPEN}<file path=\"../AGENTS.md\">\nparent\n</file>\n<file path=\"./AGENTS.md\" truncated=\"true\">\n{TOO_LARGE}\n</file>\n{CLOSE}"
            )
        );
    }
    #[test]
    fn omission_keeps_whole_files_and_respects_minimum_bound() {
        let mut f = Frame::new(256);
        assert!(f.push("./first", Entry::Content("x".repeat(130))));
        assert!(!f.push("./second", Entry::Content("y".repeat(256))));
        let output = f.finish().unwrap();
        assert!(output.len() <= 256 && output.ends_with(CLOSE) && output.contains(OMITTED));
        assert!(!output.contains("<file"));
        assert!(Frame::new(256).finish().is_none());
    }
}
