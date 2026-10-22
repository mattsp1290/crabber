//! Recursive cycles enter a nested list only after checking `depth + 1`.
//! Words have an iterative part lexer; quoted parts do not nest quotes. Every
//! substitution reuses this parser and charges the shared syntactic depth.
use super::{Dialect, Outcome, ast::*, budget::Budget, variables};
use std::borrow::Cow;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Context {
    Top,
    Substitution,
}
#[derive(Clone, Copy, PartialEq, Eq)]
enum PartContext {
    Word,
    DoubleQuoted,
    Heredoc,
}
#[derive(Clone, Copy, PartialEq, Eq)]
enum Op {
    Newline,
    Semi,
    Amp,
    Pipe,
    PipeAmp,
    And,
    Or,
    DSemi,
    SemiAnd,
    DSemiAnd,
    Left,
    Right,
    Eof,
}
enum Token<'a> {
    Word(Word<'a>),
    Op(Op),
    Redirect(Option<Cow<'a, str>>, RedirectOp),
}
struct Pending {
    delimiter: String,
    quoted: bool,
    tabs: bool,
}
struct Parser<'a, 'b, 'l> {
    input: &'a str,
    pos: usize,
    dialect: Dialect,
    context: Context,
    budget: &'b mut Budget<'l>,
    look: Option<Token<'a>>,
    pending: Vec<Pending>,
    heredocs: Vec<Heredoc<'a>>,
    consumed: usize,
    strip_tabs: bool,
}
pub(super) fn parse<'a>(
    input: &'a str,
    dialect: Dialect,
    context: Context,
    depth: usize,
    budget: &mut Budget<'_>,
) -> Result<Script<'a>, Outcome> {
    let mut p = Parser {
        input,
        pos: 0,
        dialect,
        context,
        budget,
        look: None,
        pending: vec![],
        heredocs: vec![],
        consumed: 0,
        strip_tabs: false,
    };
    let mut script = p.list(depth, &[], &[Op::Eof])?;
    p.expect_op(Op::Eof, depth)?;
    if p.consumed != p.pending.len() {
        return Err(Outcome::InvalidCommand);
    }
    script.heredocs = p.heredocs;
    Ok(script)
}
mod commands;
mod cursor;
mod heredocs;
mod tokens;
mod words;

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::panic, clippy::indexing_slicing)]
mod tests;

impl<'a> Parser<'a, '_, '_> {
    fn take(&mut self, depth: usize) -> Result<Token<'a>, Outcome> {
        match self.look.take() {
            Some(t) => Ok(t),
            None => self.lex(depth),
        }
    }
    fn look(&mut self, depth: usize) -> Result<&Token<'a>, Outcome> {
        if self.look.is_none() {
            self.look = Some(self.lex(depth)?);
        }
        self.look.as_ref().ok_or(Outcome::InvalidCommand)
    }
    fn is_op(&mut self, op: Op, depth: usize) -> Result<bool, Outcome> {
        Ok(matches!(self.look(depth)?, Token::Op(o) if *o == op))
    }
    fn is_word(&mut self, word: &str, depth: usize) -> Result<bool, Outcome> {
        Ok(matches!(self.look(depth)?, Token::Word(w) if w.literal() == Some(word)))
    }
    fn expect_op(&mut self, op: Op, depth: usize) -> Result<(), Outcome> {
        if self.is_op(op, depth)? {
            self.take(depth)?;
            Ok(())
        } else {
            Err(Outcome::InvalidCommand)
        }
    }
    fn expect_word(&mut self, s: &str, depth: usize) -> Result<(), Outcome> {
        if self.is_word(s, depth)? {
            self.take(depth)?;
            Ok(())
        } else {
            Err(Outcome::InvalidCommand)
        }
    }
    fn word_token(&mut self, depth: usize) -> Result<Word<'a>, Outcome> {
        match self.take(depth)? {
            Token::Word(w) => Ok(w),
            _ => Err(Outcome::InvalidCommand),
        }
    }
    fn newlines(&mut self, depth: usize) -> Result<(), Outcome> {
        while self.is_op(Op::Newline, depth)? {
            self.take(depth)?;
        }
        Ok(())
    }
    fn nested(&mut self, depth: usize, words: &[&str], ops: &[Op]) -> Result<Script<'a>, Outcome> {
        let next = depth.saturating_add(1);
        self.budget.depth(next)?;
        self.list(next, words, ops)
    }
    fn list(
        &mut self,
        depth: usize,
        stop_words: &[&str],
        stop_ops: &[Op],
    ) -> Result<Script<'a>, Outcome> {
        self.budget.depth(depth)?;
        self.budget.node()?;
        let mut items = vec![];
        self.newlines(depth)?;
        loop {
            let stop = match self.look(depth)? {
                Token::Op(o) => stop_ops.contains(o),
                Token::Word(w) => w.literal().is_some_and(|s| stop_words.contains(&s)),
                _ => false,
            };
            if stop {
                break;
            }
            let and_or = self.and_or(depth)?;
            let background = self.is_op(Op::Amp, depth)?;
            self.budget.node()?;
            items.push(ListItem { and_or, background });
            if background || self.is_op(Op::Semi, depth)? || self.is_op(Op::Newline, depth)? {
                self.take(depth)?;
                self.newlines(depth)?;
            } else {
                // Only ')' / EOF can close a list without a preceding separator.
                if self.is_op(Op::Right, depth)? && stop_ops.contains(&Op::Right)
                    || self.is_op(Op::Eof, depth)? && stop_ops.contains(&Op::Eof)
                    || stop_ops
                        .iter()
                        .any(|o| matches!(o, Op::DSemi | Op::SemiAnd | Op::DSemiAnd))
                        && matches!(
                            self.look(depth)?,
                            Token::Op(Op::DSemi | Op::SemiAnd | Op::DSemiAnd)
                        )
                {
                    break;
                }
                return Err(Outcome::InvalidCommand);
            }
        }
        Ok(Script {
            items,
            heredocs: vec![],
        })
    }
    fn and_or(&mut self, depth: usize) -> Result<AndOr<'a>, Outcome> {
        self.budget.node()?;
        let first = self.pipeline(depth)?;
        let mut rest = vec![];
        loop {
            let op = if self.is_op(Op::And, depth)? {
                AndOrOp::And
            } else if self.is_op(Op::Or, depth)? {
                AndOrOp::Or
            } else {
                break;
            };
            self.take(depth)?;
            self.newlines(depth)?;
            rest.push((op, self.pipeline(depth)?));
        }
        Ok(AndOr { first, rest })
    }
    fn pipeline(&mut self, depth: usize) -> Result<Pipeline<'a>, Outcome> {
        self.budget.node()?;
        let negated = self.is_word("!", depth)?;
        if negated {
            self.take(depth)?;
        }
        let mut commands = vec![self.command(depth)?];
        while self.is_op(Op::Pipe, depth)? || self.is_op(Op::PipeAmp, depth)? {
            self.take(depth)?;
            self.newlines(depth)?;
            commands.push(self.command(depth)?);
        }
        Ok(Pipeline { negated, commands })
    }
}
