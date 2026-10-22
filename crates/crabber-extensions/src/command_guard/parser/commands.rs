use super::*;

impl<'a> Parser<'a, '_, '_> {
    pub(super) fn command(&mut self, depth: usize) -> Result<Command<'a>, Outcome> {
        self.budget.node()?;
        let kind = if self.is_op(Op::Left, depth)? {
            self.take(depth)?;
            let body = self.nested(depth, &[], &[Op::Right])?;
            if body.items.is_empty() {
                return Err(Outcome::InvalidCommand);
            }
            self.expect_op(Op::Right, depth)?;
            CommandKind::Subshell(body)
        } else if self.is_word("{", depth)? {
            self.take(depth)?;
            let body = self.nested(depth, &["}"], &[])?;
            if body.items.is_empty() {
                return Err(Outcome::InvalidCommand);
            }
            self.expect_word("}", depth)?;
            CommandKind::Brace(body)
        } else if self.is_word("if", depth)? {
            self.if_command(depth)?
        } else if self.is_word("while", depth)? || self.is_word("until", depth)? {
            let until = self.is_word("until", depth)?;
            self.take(depth)?;
            let cond = self.nested(depth, &["do"], &[])?;
            self.expect_word("do", depth)?;
            let body = self.nested(depth, &["done"], &[])?;
            self.expect_word("done", depth)?;
            if cond.items.is_empty() || body.items.is_empty() {
                return Err(Outcome::InvalidCommand);
            }
            if until {
                CommandKind::Until { cond, body }
            } else {
                CommandKind::While { cond, body }
            }
        } else if self.is_word("for", depth)? {
            self.for_command(depth)?
        } else if self.is_word("case", depth)? {
            self.case_command(depth)?
        } else {
            match self.look(depth)? {
                Token::Word(w) => match w.literal() {
                    Some("time" | "coproc" | "select" | "function" | "[[") => {
                        return Err(Outcome::Unanalysable);
                    }
                    Some(
                        "then" | "elif" | "else" | "fi" | "do" | "done" | "in" | "esac" | "}" | "!",
                    ) => return Err(Outcome::InvalidCommand),
                    _ => {}
                },
                Token::Op(_) => return Err(Outcome::InvalidCommand),
                _ => {}
            }
            return self.simple(depth);
        };
        self.budget.node()?; // CommandKind
        let mut redirects = vec![];
        while matches!(self.look(depth)?, Token::Redirect(..)) {
            redirects.push(self.redirect(depth)?);
        }
        Ok(Command { kind, redirects })
    }
    pub(super) fn if_command(&mut self, depth: usize) -> Result<CommandKind<'a>, Outcome> {
        self.expect_word("if", depth)?;
        let mut arms = vec![];
        loop {
            let cond = self.nested(depth, &["then"], &[])?;
            self.expect_word("then", depth)?;
            let body = self.nested(depth, &["elif", "else", "fi"], &[])?;
            if cond.items.is_empty() || body.items.is_empty() {
                return Err(Outcome::InvalidCommand);
            }
            arms.push((cond, body));
            if !self.is_word("elif", depth)? {
                break;
            }
            self.take(depth)?;
        }
        let else_ = if self.is_word("else", depth)? {
            self.take(depth)?;
            let body = self.nested(depth, &["fi"], &[])?;
            if body.items.is_empty() {
                return Err(Outcome::InvalidCommand);
            }
            Some(body)
        } else {
            None
        };
        self.expect_word("fi", depth)?;
        Ok(CommandKind::If { arms, else_ })
    }
    pub(super) fn for_command(&mut self, depth: usize) -> Result<CommandKind<'a>, Outcome> {
        self.expect_word("for", depth)?;
        let word = self.word_token(depth)?;
        let literal = word.literal().ok_or(Outcome::Unanalysable)?;
        if literal.len() > self.budget.limits.max_word_bytes {
            return Err(Outcome::AnalysisLimit);
        }
        let name = literal.to_owned();
        if !variables::variable_name(&name) || variables::opaque_variable_target(&name) {
            return Err(Outcome::Unanalysable);
        }
        let newline = self.is_op(Op::Newline, depth)?;
        self.newlines(depth)?;
        let words = if self.is_word("in", depth)? {
            self.take(depth)?;
            let mut words = vec![];
            while matches!(self.look(depth)?, Token::Word(_)) {
                words.push(self.word_token(depth)?);
            }
            if !self.is_op(Op::Semi, depth)? && !self.is_op(Op::Newline, depth)? {
                return Err(Outcome::InvalidCommand);
            }
            self.take(depth)?;
            self.newlines(depth)?;
            Some(words)
        } else {
            if self.is_op(Op::Semi, depth)? {
                self.take(depth)?;
                self.newlines(depth)?;
            } else if !newline {
                return Err(Outcome::InvalidCommand);
            }
            None
        };
        self.expect_word("do", depth)?;
        let body = self.nested(depth, &["done"], &[])?;
        if body.items.is_empty() {
            return Err(Outcome::InvalidCommand);
        }
        self.expect_word("done", depth)?;
        Ok(CommandKind::For {
            name: Cow::Owned(name),
            words,
            body,
        })
    }
    pub(super) fn case_command(&mut self, depth: usize) -> Result<CommandKind<'a>, Outcome> {
        self.expect_word("case", depth)?;
        let subject = self.word_token(depth)?;
        self.newlines(depth)?;
        self.expect_word("in", depth)?;
        self.newlines(depth)?;
        let mut items = vec![];
        while !self.is_word("esac", depth)? {
            if self.is_op(Op::Left, depth)? {
                self.take(depth)?;
            }
            let mut patterns = vec![self.word_token(depth)?];
            while self.is_op(Op::Pipe, depth)? {
                self.take(depth)?;
                patterns.push(self.word_token(depth)?);
            }
            self.expect_op(Op::Right, depth)?;
            let body = self.nested(depth, &["esac"], &[Op::DSemi, Op::SemiAnd, Op::DSemiAnd])?;
            let terminator = match self.look(depth)? {
                Token::Op(Op::DSemi) => CaseEnd::Break,
                Token::Op(Op::SemiAnd) => CaseEnd::FallThrough,
                Token::Op(Op::DSemiAnd) => CaseEnd::Continue,
                Token::Word(w) if w.literal() == Some("esac") => CaseEnd::None,
                _ => return Err(Outcome::InvalidCommand),
            };
            let end = matches!(terminator, CaseEnd::None);
            if !end {
                self.take(depth)?;
            }
            self.budget.node()?;
            items.push(CaseItem {
                patterns,
                body,
                terminator,
            });
            self.newlines(depth)?;
            if end {
                break;
            }
        }
        self.expect_word("esac", depth)?;
        Ok(CommandKind::Case { subject, items })
    }
    pub(super) fn simple(&mut self, depth: usize) -> Result<Command<'a>, Outcome> {
        let mut assignments = vec![];
        let mut words = vec![];
        let mut redirects = vec![];
        loop {
            if matches!(self.look(depth)?, Token::Redirect(..)) {
                redirects.push(self.redirect(depth)?);
                continue;
            }
            if !matches!(self.look(depth)?, Token::Word(_)) {
                break;
            }
            let mut word = self.word_token(depth)?;
            let adjacent_redirect = matches!(self.logical_at(self.pos).1, Some('<' | '>'));
            if self.is_op(Op::Left, depth)? {
                self.take(depth)?;
                if self.is_op(Op::Right, depth)? || word.literal().is_some_and(|s| s.ends_with('='))
                {
                    return Err(Outcome::Unanalysable);
                }
                return Err(Outcome::InvalidCommand);
            }
            if words.is_empty()
                && let Some(WordPart::Lit(text)) = word.parts.first()
            {
                match word.literal().unwrap_or("") {
                    "time" | "coproc" | "select" | "function" | "[[" => {
                        return Err(Outcome::Unanalysable);
                    }
                    "if" | "then" | "elif" | "else" | "fi" | "while" | "until" | "do" | "done"
                    | "for" | "in" | "case" | "esac" | "{" | "}" | "!" => {
                        return Err(Outcome::InvalidCommand);
                    }
                    _ => {}
                }
                if text.split_once('[').is_some_and(|(name, suffix)| {
                    variables::variable_name(name) && !suffix.contains(']')
                }) {
                    return Err(Outcome::InvalidCommand);
                }

                if text.split_once('[').is_some_and(|(name, suffix)| {
                    variables::variable_name(name) && suffix.contains("]=")
                }) || text
                    .split_once("+=")
                    .is_some_and(|(name, _)| variables::variable_name(name))
                {
                    return Err(Outcome::Unanalysable);
                }
                if let Some((name, value)) = text.split_once('=')
                    && variables::variable_name(name)
                {
                    if variables::opaque_variable_target(name) {
                        return Err(Outcome::Unanalysable);
                    }
                    let name = Cow::Owned(name.to_owned());
                    let value = Cow::Owned(value.to_owned());
                    if let Some(first) = word.parts.first_mut() {
                        *first = WordPart::Lit(value);
                    }
                    self.budget.node()?;
                    assignments.push(Assignment { name, value: word });
                    continue;
                }
            }
            if adjacent_redirect
                && word
                    .literal()
                    .is_some_and(|s| s.starts_with('{') && s.ends_with('}'))
                && matches!(self.look(depth)?, Token::Redirect(..))
            {
                return Err(Outcome::Unanalysable);
            }
            words.push(word);
        }
        if assignments.is_empty() && words.is_empty() && redirects.is_empty() {
            return Err(Outcome::InvalidCommand);
        }
        self.budget.node()?;
        Ok(Command {
            kind: CommandKind::Simple { assignments, words },
            redirects,
        })
    }
}
