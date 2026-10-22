use super::{Limits, Outcome};

pub(super) struct Budget<'l> {
    pub(super) limits: &'l Limits,
    bytes: usize,
    nodes: usize,
    words: usize,
}
impl<'l> Budget<'l> {
    pub(super) fn new(limits: &'l Limits) -> Self {
        Self {
            limits,
            bytes: 0,
            nodes: 0,
            words: 0,
        }
    }
    pub(super) fn admit_script(&mut self, len: usize) -> Result<(), Outcome> {
        let total = self.bytes.checked_add(len).ok_or(Outcome::AnalysisLimit)?;
        if len > self.limits.max_command_bytes || total > self.limits.max_analysis_bytes {
            return Err(Outcome::AnalysisLimit);
        }
        self.bytes = total;
        Ok(())
    }
    pub(super) fn node(&mut self) -> Result<(), Outcome> {
        if self.nodes >= self.limits.max_ast_nodes {
            return Err(Outcome::AnalysisLimit);
        }
        self.nodes = self.nodes.saturating_add(1);
        Ok(())
    }
    pub(super) fn word(&mut self) -> Result<(), Outcome> {
        if self.words >= self.limits.max_words {
            return Err(Outcome::AnalysisLimit);
        }
        self.words = self.words.saturating_add(1);
        self.node()
    }
    pub(super) fn depth(&self, depth: usize) -> Result<(), Outcome> {
        if depth >= self.limits.max_ast_depth {
            Err(Outcome::AnalysisLimit)
        } else {
            Ok(())
        }
    }
}
