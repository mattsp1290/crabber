use super::*;

impl<'a> Parser<'a, '_, '_> {
    pub(super) fn word(&mut self, depth: usize) -> Result<Word<'a>, Outcome> {
        self.budget.word()?;
        let parts = self.parts(depth, PartContext::Word)?;
        if parts.is_empty() {
            return Err(Outcome::InvalidCommand);
        }
        Ok(Word { parts })
    }
    pub(super) fn parts(
        &mut self,
        depth: usize,
        context: PartContext,
    ) -> Result<Vec<WordPart<'a>>, Outcome> {
        let mut parts = vec![];
        let mut start = self.pos;
        loop {
            let c = self.peek();
            if c == Some('\n') && context != PartContext::Word && self.consumed < self.pending.len()
            {
                return Err(Outcome::Unanalysable);
            }
            let boundary = match context {
                PartContext::Heredoc => c.is_none(),
                PartContext::DoubleQuoted => c.is_none() || c == Some('"'),
                PartContext::Word => {
                    c.is_none()
                        || matches!(
                            c,
                            Some(' ' | '\t' | '\n' | ';' | '&' | '|' | '(' | ')' | '<' | '>')
                        ) && !(matches!(c, Some('<' | '>')) && self.next_char() == Some('('))
                }
            };
            if boundary {
                if self.pos > start {
                    let s = self.literal(start, self.pos)?;
                    self.push_part(&mut parts, WordPart::Lit(s))?;
                }
                break;
            }
            if c == Some('\\') {
                self.bump();
                // Backslash protects the next logical byte. It stays in Lit for
                // context-sensitive quote removal in the decoder.
                if self.raw().is_some() {
                    self.raw_bump();
                }
                continue;
            }
            let special = c == Some('$')
                || c == Some('`')
                || context == PartContext::Word && matches!(c, Some('\'' | '"' | '<' | '>'));
            if context == PartContext::Word && c == Some('(') {
                return Err(Outcome::InvalidCommand);
            }
            if context == PartContext::Word
                && matches!(c, Some('@' | '?' | '*' | '+' | '!'))
                && self.next_char() == Some('(')
            {
                return Err(Outcome::Unanalysable);
            }
            if !special {
                self.bump();
                continue;
            }
            if self.pos > start {
                let s = self.literal(start, self.pos)?;
                self.push_part(&mut parts, WordPart::Lit(s))?;
            }
            let part = match c {
                Some('\'') => {
                    self.bump();
                    let from = self.pos;
                    while !matches!(self.raw(), None | Some('\'')) {
                        if self.raw() == Some('\n') && self.consumed < self.pending.len() {
                            return Err(Outcome::Unanalysable);
                        }
                        self.raw_bump();
                    }
                    let text = self.raw_literal(from, self.pos)?;
                    if self.raw_bump() != Some('\'') {
                        return Err(Outcome::InvalidCommand);
                    }
                    WordPart::SglQuoted(text)
                }
                Some('"') => {
                    self.bump();
                    let inner = self.parts(depth, PartContext::DoubleQuoted)?;
                    if self.bump() != Some('"') {
                        return Err(Outcome::InvalidCommand);
                    }
                    WordPart::DblQuoted(inner)
                }
                Some('`') => {
                    WordPart::Backquote(self.backquote(context == PartContext::DoubleQuoted)?)
                }
                Some('<' | '>') => {
                    if self.dialect == Dialect::Posix {
                        return Err(Outcome::InvalidCommand);
                    }
                    let dir = if self.bump() == Some('<') {
                        Direction::In
                    } else {
                        Direction::Out
                    };
                    if self.bump() != Some('(') {
                        return Err(Outcome::InvalidCommand);
                    }
                    WordPart::ProcSubst {
                        dir,
                        script: self.substitution(depth)?,
                    }
                }
                Some('$') => self.dollar(depth, context != PartContext::Word)?,
                _ => return Err(Outcome::InvalidCommand),
            };
            self.push_part(&mut parts, part)?;
            start = self.pos;
        }
        Ok(parts)
    }
    pub(super) fn dollar(&mut self, depth: usize, quoted: bool) -> Result<WordPart<'a>, Outcome> {
        self.bump();
        match self.peek() {
            Some('(') => {
                self.bump();
                if self.peek() == Some('(') {
                    return Err(Outcome::Unanalysable);
                }
                Ok(WordPart::CmdSubst(self.substitution(depth)?))
            }
            Some('[') => Err(Outcome::Unanalysable),
            Some('{') => {
                self.bump();
                let start = self.pos;
                while !matches!(self.peek(), None | Some('}')) {
                    self.bump();
                }
                let name = self.literal(start, self.pos)?;
                if self.bump() != Some('}') {
                    return Err(Outcome::InvalidCommand);
                }
                if !variables::variable_name(&name)
                    && !(!name.is_empty() && name.bytes().all(|b| b.is_ascii_digit()))
                    && !(name.len() == 1 && name.bytes().all(|b| b"@*#?-$!".contains(&b)))
                {
                    return Err(Outcome::Unanalysable);
                }
                Ok(WordPart::Param(name))
            }
            Some('\'') if !quoted => {
                self.bump();
                let start = self.pos;
                while !matches!(self.raw(), None | Some('\'')) {
                    if self.raw() == Some('\\') {
                        return Err(Outcome::Unanalysable);
                    }
                    self.raw_bump();
                }
                let s = self.raw_literal(start, self.pos)?;
                if self.raw_bump() != Some('\'') {
                    return Err(Outcome::InvalidCommand);
                }
                Ok(WordPart::AnsiQuoted(s))
            }
            Some('"') if !quoted => {
                self.bump();
                let parts = self.parts(depth, PartContext::DoubleQuoted)?;
                if self.bump() != Some('"') {
                    return Err(Outcome::InvalidCommand);
                }
                Ok(WordPart::LocaleQuoted(parts))
            }
            Some(c) if c == '_' || c.is_ascii_alphabetic() => {
                let start = self.pos;
                self.bump();
                while self
                    .peek()
                    .is_some_and(|c| c == '_' || c.is_ascii_alphanumeric())
                {
                    self.bump();
                }
                Ok(WordPart::Param(self.literal(start, self.pos)?))
            }
            Some(c) if c.is_ascii_digit() || "@*#?-$!".contains(c) => {
                let start = self.pos;
                self.bump();
                Ok(WordPart::Param(self.literal(start, self.pos)?))
            }
            _ => Ok(WordPart::Lit(Cow::Borrowed("$"))),
        }
    }
    pub(super) fn substitution(&mut self, depth: usize) -> Result<Script<'a>, Outcome> {
        let old = self.context;
        self.context = Context::Substitution;
        let script = self.nested(depth, &[], &[Op::Right])?;
        self.expect_op(Op::Right, depth)?;
        self.context = old;
        Ok(script)
    }
    pub(super) fn backquote(&mut self, quoted: bool) -> Result<String, Outcome> {
        self.bump();
        let mut out = String::new();
        loop {
            match self.raw_bump() {
                None => return Err(Outcome::InvalidCommand),
                Some('`') => return Ok(out),
                Some('\\') => match self.raw() {
                    Some('\n') => {
                        self.raw_bump();
                    }
                    Some(c) if "`\\$".contains(c) || quoted && c == '"' => {
                        self.raw_bump();
                        out.push(c);
                    }
                    _ => out.push('\\'),
                },
                Some(c) => out.push(c),
            }
        }
    }
}
