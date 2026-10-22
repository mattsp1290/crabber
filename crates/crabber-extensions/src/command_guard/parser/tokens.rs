use super::*;

impl<'a> Parser<'a, '_, '_> {
    pub(super) fn lex(&mut self, depth: usize) -> Result<Token<'a>, Outcome> {
        loop {
            while matches!(self.peek(), Some(' ' | '\t')) {
                self.bump();
            }
            if self.peek() != Some('#') {
                break;
            }
            self.pos = self.logical_at(self.pos).0;
            while !matches!(self.raw(), None | Some('\n')) {
                self.raw_bump();
            }
        }
        let Some(c) = self.peek() else {
            return Ok(Token::Op(Op::Eof));
        };
        if c == '\n' {
            if self.context == Context::Substitution && self.consumed < self.pending.len() {
                return Err(Outcome::Unanalysable);
            }
            self.bump();
            self.read_heredocs(depth)?;
            return Ok(Token::Op(Op::Newline));
        }
        if matches!(c, '<' | '>') && self.next_char() == Some('(') {
            return Ok(Token::Word(self.word(depth)?));
        }
        if matches!(c, '<' | '>') || c == '&' && self.next_char() == Some('>') {
            return self.redirect_token(None);
        }
        let op = match c {
            '(' => Some(Op::Left),
            ')' => Some(Op::Right),
            ';' => Some(Op::Semi),
            '&' => Some(Op::Amp),
            '|' => Some(Op::Pipe),
            _ => None,
        };
        if let Some(mut op) = op {
            self.bump();
            if c == '(' && self.peek() == Some('(') {
                return Err(Outcome::Unanalysable);
            }
            if c == ';' && self.peek() == Some(';') {
                self.bump();
                op = Op::DSemi;
                if self.peek() == Some('&') {
                    self.bump();
                    op = Op::DSemiAnd;
                }
            } else if c == ';' && self.peek() == Some('&') {
                self.bump();
                op = Op::SemiAnd;
            } else if c == '&' && self.peek() == Some('&') {
                self.bump();
                op = Op::And;
            } else if c == '|' && self.peek() == Some('|') {
                self.bump();
                op = Op::Or;
            } else if c == '|' && self.peek() == Some('&') {
                self.bump();
                if self.dialect == Dialect::Posix {
                    return Err(Outcome::InvalidCommand);
                }
                op = Op::PipeAmp;
            }
            if self.dialect == Dialect::Posix && matches!(op, Op::SemiAnd | Op::DSemiAnd) {
                return Err(Outcome::Unanalysable);
            }
            return Ok(Token::Op(op));
        }
        // File descriptor text is not converted to an integer.
        if c.is_ascii_digit() {
            let start = self.pos;
            while self.peek().is_some_and(|c| c.is_ascii_digit()) {
                self.bump();
            }
            if matches!(self.peek(), Some('<' | '>')) && self.next_char() != Some('(') {
                let fd = self.literal(start, self.pos)?;
                return self.redirect_token(Some(fd));
            }
            self.pos = start;
        }
        Ok(Token::Word(self.word(depth)?))
    }
    pub(super) fn redirect_token(
        &mut self,
        fd: Option<Cow<'a, str>>,
    ) -> Result<Token<'a>, Outcome> {
        let c = self.bump().ok_or(Outcome::InvalidCommand)?;
        let mut op = match c {
            '<' => RedirectOp::In,
            '>' => RedirectOp::Out,
            '&' => {
                if self.bump() != Some('>') {
                    return Err(Outcome::InvalidCommand);
                }
                RedirectOp::Both
            }
            _ => return Err(Outcome::InvalidCommand),
        };
        match (c, self.peek()) {
            ('<', Some('<')) => {
                self.bump();
                op = RedirectOp::Here;
                if self.peek() == Some('-') {
                    self.bump();
                    op = RedirectOp::HereTabs;
                } else if self.peek() == Some('<') {
                    self.bump();
                    op = RedirectOp::HereString;
                }
            }
            ('>', Some('>')) => {
                self.bump();
                op = RedirectOp::Append;
            }
            ('<', Some('&')) => {
                self.bump();
                op = RedirectOp::DupIn;
            }
            ('>', Some('&')) => {
                self.bump();
                op = RedirectOp::DupOut;
            }
            ('<', Some('>')) => {
                self.bump();
                op = RedirectOp::ReadWrite;
            }
            ('>', Some('|')) => {
                self.bump();
                op = RedirectOp::Clobber;
            }
            ('&', Some('>')) => {
                self.bump();
                op = RedirectOp::BothAppend;
            }
            _ => {}
        }
        if self.dialect == Dialect::Posix
            && matches!(
                op,
                RedirectOp::Both | RedirectOp::BothAppend | RedirectOp::HereString
            )
        {
            return Err(Outcome::Unanalysable);
        }
        Ok(Token::Redirect(fd, op))
    }
}
