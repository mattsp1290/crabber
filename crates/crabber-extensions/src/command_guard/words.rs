use super::{Outcome, ast};

pub(super) struct Word {
    pub(super) text: String,
    pub(super) known: bool,
}
struct Decoder {
    word: Word,
    cap: usize,
    bracket: Option<usize>,
}
impl Decoder {
    fn append(&mut self, text: &str) -> Result<(), Outcome> {
        if self
            .word
            .text
            .len()
            .checked_add(text.len())
            .is_none_or(|n| n > self.cap)
        {
            return Err(Outcome::AnalysisLimit);
        }
        self.word.text.push_str(text);
        Ok(())
    }
    fn parts(&mut self, parts: &[ast::WordPart<'_>], quoted: bool) -> Result<(), Outcome> {
        for part in parts {
            match part {
                ast::WordPart::Lit(s) => {
                    if !quoted && s.contains('{') {
                        self.word.known = false;
                    }
                    let mut chars = s.chars().peekable();
                    while let Some(c) = chars.next() {
                        if c == '\\' {
                            if chars
                                .peek()
                                .is_some_and(|c| !quoted || "\\$`\"".contains(*c))
                            {
                                if let Some(c) = chars.next() {
                                    self.append(c.encode_utf8(&mut [0; 4]))?;
                                }
                            } else {
                                self.append("\\")?;
                            }
                            continue;
                        }
                        if !quoted {
                            if matches!(c, '*' | '?') || c == '~' && self.word.text.is_empty() {
                                self.word.known = false;
                            }
                            if c == '[' && self.bracket.is_none() {
                                self.bracket = Some(self.word.text.len());
                            }
                        }
                        self.append(c.encode_utf8(&mut [0; 4]))?;
                    }
                }
                ast::WordPart::SglQuoted(s) => self.append(s)?,
                ast::WordPart::DblQuoted(parts) => self.parts(parts, true)?,
                ast::WordPart::LocaleQuoted(parts) => {
                    self.parts(parts, true)?;
                    self.word.known = false;
                }
                ast::WordPart::AnsiQuoted(s) => {
                    self.append(s)?;
                    self.word.known = false;
                }
                ast::WordPart::Param(_)
                | ast::WordPart::CmdSubst(_)
                | ast::WordPart::Backquote(_)
                | ast::WordPart::ProcSubst { .. } => self.word.known = false,
            }
        }
        Ok(())
    }
}
pub(super) fn decode(word: &ast::Word<'_>, cap: usize) -> Result<Word, Outcome> {
    let mut d = Decoder {
        word: Word {
            text: String::new(),
            known: true,
        },
        cap,
        bracket: None,
    };
    d.parts(&word.parts, false)?;
    if d.bracket
        .is_some_and(|pos| d.word.text.get(pos..).is_some_and(|s| s.contains(']')))
    {
        d.word.known = false;
    }
    Ok(d.word)
}
