use super::*;

impl<'a> Parser<'a, '_, '_> {
    pub(super) fn redirect(&mut self, depth: usize) -> Result<Redirect<'a>, Outcome> {
        let (fd, op) = match self.take(depth)? {
            Token::Redirect(fd, op) => (fd, op),
            _ => return Err(Outcome::InvalidCommand),
        };
        self.budget.node()?;
        // A redirect target is a word even when digits abut another operator.
        // Lexing it as a general token would misclassify `2>&1>f`'s target.
        while matches!(self.peek(), Some(' ' | '\t')) {
            self.bump();
        }
        if self.peek() == Some('#') {
            return Err(Outcome::InvalidCommand);
        }
        let word = self.word(depth)?;
        if word
            .literal()
            .is_some_and(|s| !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit()))
            && matches!(self.peek(), Some('<' | '>'))
        {
            return Err(Outcome::InvalidCommand);
        }
        if op == RedirectOp::DupIn
            && word
                .literal()
                .is_some_and(|s| s != "-" && !s.bytes().all(|b| b.is_ascii_digit()))
        {
            return Err(Outcome::InvalidCommand);
        }
        let target = if matches!(op, RedirectOp::Here | RedirectOp::HereTabs) {
            if self.context == Context::Substitution {
                return Err(Outcome::Unanalysable);
            }
            let (delimiter, quoted) = self.delimiter(&word)?;
            let id = self.pending.len();
            self.pending.push(Pending {
                delimiter,
                quoted,
                tabs: op == RedirectOp::HereTabs,
            });
            RedirectTarget::Heredoc(id)
        } else {
            RedirectTarget::Word(word)
        };
        self.budget.node()?;
        Ok(Redirect { fd, op, target })
    }
    pub(super) fn delimiter(&self, word: &Word<'a>) -> Result<(String, bool), Outcome> {
        let decoded = super::super::words::decode(word, self.budget.limits.max_word_bytes)?;
        let mut quoted = false;
        for part in &word.parts {
            match part {
                WordPart::Lit(s) => quoted |= s.contains('\\'),
                WordPart::SglQuoted(_) => quoted = true,
                WordPart::DblQuoted(parts) => {
                    quoted = true;
                    if parts.iter().any(|part| !matches!(part, WordPart::Lit(_))) {
                        return Err(Outcome::Unanalysable);
                    }
                }
                _ => return Err(Outcome::Unanalysable),
            }
        }
        Ok((decoded.text, quoted))
    }
    pub(super) fn read_heredocs(&mut self, depth: usize) -> Result<(), Outcome> {
        while self.consumed < self.pending.len() {
            let pending = self
                .pending
                .get(self.consumed)
                .ok_or(Outcome::InvalidCommand)?;
            let start = self.pos;
            let mut line_start = self.pos;
            let body_end;
            loop {
                let tail = self
                    .input
                    .get(line_start..)
                    .ok_or(Outcome::InvalidCommand)?;
                let len = tail.find('\n').unwrap_or(tail.len());
                let line_end = line_start.saturating_add(len);
                let line = self.text(line_start, line_end)?;
                let compare = if pending.tabs {
                    line.trim_start_matches('\t')
                } else {
                    line
                };
                if compare == pending.delimiter {
                    body_end = line_start;
                    self.pos = line_end.saturating_add(usize::from(len < tail.len()));
                    break;
                }
                if len == tail.len() {
                    return Err(Outcome::InvalidCommand);
                }
                // Bash joins lines before delimiter matching, while dash can
                // interpret joined delimiters differently. Deny the ambiguity
                // before treating any later executable line as body data.
                if !pending.quoted
                    && line.bytes().rev().take_while(|b| *b == b'\\').count() % 2 == 1
                {
                    return Err(Outcome::Unanalysable);
                }
                line_start = line_end.saturating_add(1);
            }
            let quoted = pending.quoted;
            let strip_tabs = pending.tabs;
            let body = self.text(start, body_end)?;
            let parts = if quoted {
                vec![]
            } else {
                let next_depth = depth.saturating_add(1);
                self.budget.depth(next_depth)?;
                let mut p = Parser {
                    input: body,
                    pos: 0,
                    dialect: self.dialect,
                    context: Context::Substitution,
                    budget: self.budget,
                    look: None,
                    pending: vec![],
                    heredocs: vec![],
                    consumed: 0,
                    strip_tabs,
                };
                p.parts(next_depth, PartContext::Heredoc)?
            };
            self.budget.node()?;
            self.heredocs.push(Heredoc { quoted, parts });
            self.consumed = self.consumed.saturating_add(1);
        }
        Ok(())
    }
}
