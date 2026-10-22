use super::{
    Dialect, Outcome, Policy,
    ast::*,
    budget::Budget,
    parser::{self, Context},
    words,
};

/// Fresh per-call state. Nested scripts share byte, node and word budgets;
/// depth checks precede every recursive cycle. No external access is performed.
/// The first postorder denial wins; later nodes are not analyzed.
pub(super) struct Analysis<'p> {
    pub(super) policy: &'p Policy,
    pub(super) budget: Budget<'p>,
}
impl Analysis<'_> {
    pub(super) fn script(
        &mut self,
        script: &str,
        dialect: Dialect,
        context: Context,
        depth: usize,
        wrappers: usize,
    ) -> Outcome {
        let result = (|| {
            self.budget.depth(depth)?;
            self.budget.admit_script(script.len())?;
            if script.contains('\0') {
                return Err(Outcome::InvalidCommand);
            }
            let ast = parser::parse(script, dialect, context, depth, &mut self.budget)?;
            self.walk_script(&ast, &ast.heredocs, dialect, depth, wrappers)?;
            Ok(Outcome::Abstain)
        })();
        result.unwrap_or_else(|o| o)
    }
    fn walk_script(
        &mut self,
        script: &Script<'_>,
        heredocs: &[Heredoc<'_>],
        dialect: Dialect,
        depth: usize,
        wrappers: usize,
    ) -> Result<(), Outcome> {
        self.budget.depth(depth)?;
        for item in &script.items {
            let _background = item.background;
            for pipeline in
                std::iter::once(&item.and_or.first).chain(item.and_or.rest.iter().map(|(_, p)| p))
            {
                let _negated = pipeline.negated;
                for command in &pipeline.commands {
                    self.walk_command(command, heredocs, dialect, depth, wrappers)?;
                }
            }
        }
        Ok(())
    }
    fn nested(
        &mut self,
        script: &Script<'_>,
        heredocs: &[Heredoc<'_>],
        dialect: Dialect,
        depth: usize,
        wrappers: usize,
    ) -> Result<(), Outcome> {
        let next = depth.saturating_add(1);
        self.budget.depth(next)?;
        self.walk_script(script, heredocs, dialect, next, wrappers)
    }
    fn parts(
        &mut self,
        parts: &[WordPart<'_>],
        heredocs: &[Heredoc<'_>],
        dialect: Dialect,
        depth: usize,
        wrappers: usize,
    ) -> Result<(), Outcome> {
        for part in parts {
            match part {
                WordPart::CmdSubst(script) => {
                    self.nested(script, heredocs, dialect, depth, wrappers)?;
                }
                WordPart::ProcSubst { dir, script } => {
                    let _direction = dir;
                    self.nested(script, heredocs, dialect, depth, wrappers)?;
                }
                WordPart::Param(name) => {
                    let _parameter = name;
                }
                WordPart::Backquote(body) => {
                    let next = depth.saturating_add(1);
                    self.budget.depth(next)?;
                    let outcome = self.script(body, dialect, Context::Substitution, next, wrappers);
                    if outcome.denies() {
                        return Err(outcome);
                    }
                }
                WordPart::DblQuoted(parts) | WordPart::LocaleQuoted(parts) => {
                    // Quote parts contain no nested quotes; recursion into a
                    // script still passes through nested's depth check.
                    self.parts(parts, heredocs, dialect, depth, wrappers)?;
                }
                _ => {}
            }
        }
        Ok(())
    }
    fn word(
        &mut self,
        word: &Word<'_>,
        heredocs: &[Heredoc<'_>],
        dialect: Dialect,
        depth: usize,
        wrappers: usize,
    ) -> Result<words::Word, Outcome> {
        self.parts(&word.parts, heredocs, dialect, depth, wrappers)?;
        words::decode(word, self.policy.limits.max_word_bytes)
    }
    fn walk_command(
        &mut self,
        command: &Command<'_>,
        heredocs: &[Heredoc<'_>],
        dialect: Dialect,
        depth: usize,
        wrappers: usize,
    ) -> Result<(), Outcome> {
        for redirect in &command.redirects {
            let _syntax = (&redirect.fd, &redirect.op);
            match &redirect.target {
                RedirectTarget::Word(word) => {
                    self.word(word, heredocs, dialect, depth, wrappers)?;
                }
                RedirectTarget::Heredoc(id) => {
                    let body = heredocs.get(*id).ok_or(Outcome::Unanalysable)?;
                    if !body.quoted {
                        let next = depth.saturating_add(1);
                        self.budget.depth(next)?;
                        self.parts(&body.parts, heredocs, dialect, next, wrappers)?;
                    }
                }
            }
        }
        match &command.kind {
            CommandKind::Simple {
                assignments,
                words: raw,
            } => {
                for assignment in assignments {
                    let _target = &assignment.name;
                    self.word(&assignment.value, heredocs, dialect, depth, wrappers)?;
                }
                let mut decoded = vec![];
                for word in raw {
                    decoded.push(self.word(word, heredocs, dialect, depth, wrappers)?);
                }
                if !decoded.is_empty() {
                    let outcome = self.command(&decoded, depth, wrappers);
                    if outcome.denies() {
                        return Err(outcome);
                    }
                }
            }
            CommandKind::Subshell(body) | CommandKind::Brace(body) => {
                self.nested(body, heredocs, dialect, depth, wrappers)?
            }
            CommandKind::If { arms, else_ } => {
                for (cond, body) in arms {
                    self.nested(cond, heredocs, dialect, depth, wrappers)?;
                    self.nested(body, heredocs, dialect, depth, wrappers)?;
                }
                if let Some(body) = else_ {
                    self.nested(body, heredocs, dialect, depth, wrappers)?;
                }
            }
            CommandKind::While { cond, body } | CommandKind::Until { cond, body } => {
                self.nested(cond, heredocs, dialect, depth, wrappers)?;
                self.nested(body, heredocs, dialect, depth, wrappers)?;
            }
            CommandKind::For { name, words, body } => {
                let _target = name;
                if let Some(words) = words {
                    for word in words {
                        self.word(word, heredocs, dialect, depth, wrappers)?;
                    }
                }
                self.nested(body, heredocs, dialect, depth, wrappers)?;
            }
            CommandKind::Case { subject, items } => {
                self.word(subject, heredocs, dialect, depth, wrappers)?;
                for item in items {
                    let _terminator = &item.terminator;
                    for word in &item.patterns {
                        self.word(word, heredocs, dialect, depth, wrappers)?;
                    }
                    self.nested(&item.body, heredocs, dialect, depth, wrappers)?;
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::arithmetic_side_effects)]
mod tests {
    use super::*;
    #[test]
    fn reparsed_script_entry_checks_shared_depth() {
        let limits = super::super::Limits {
            max_bindings: 8,
            max_rules: 32,
            max_rule_bytes: 2048,
            max_prefix_args: 16,
            max_json_depth: 16,
            max_json_nodes: 256,
            max_command_bytes: 4096,
            max_analysis_bytes: 8192,
            max_ast_nodes: 2048,
            max_ast_depth: super::super::MAX_AST_DEPTH,
            max_words: 512,
            max_word_bytes: 4096,
            max_wrapper_depth: 8,
            max_in_flight: 4,
        };
        let p = Policy::new(super::super::Options {
            bindings: super::super::default_bindings(),
            rules: vec![super::super::Rule {
                id: "r".into(),
                executable: "blocked".into(),
                arg_prefix: vec![],
            }],
            limits,
        })
        .unwrap();
        for s in ["echo `echo $(echo ok)`", "sh -c 'echo $(echo ok)'"] {
            let mut a = Analysis {
                policy: &p,
                budget: Budget::new(p.limits()),
            };
            assert_eq!(
                a.script(
                    s,
                    Dialect::Posix,
                    Context::Top,
                    super::super::MAX_AST_DEPTH - 2,
                    0
                ),
                Outcome::AnalysisLimit
            );
        }
    }
}
