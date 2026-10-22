use super::*;

impl<'a> Parser<'a, '_, '_> {
    pub(super) fn tabs_at(&self, mut pos: usize) -> usize {
        if self.strip_tabs && (pos == 0 || self.input.get(..pos).is_some_and(|s| s.ends_with('\n')))
        {
            while self.input.get(pos..).is_some_and(|s| s.starts_with('\t')) {
                pos = pos.saturating_add(1);
            }
        }
        pos
    }
    pub(super) fn logical_at(&self, mut pos: usize) -> (usize, Option<char>) {
        pos = self.tabs_at(pos);
        while self.input.get(pos..).is_some_and(|s| s.starts_with("\\\n")) {
            pos = self.tabs_at(pos.saturating_add(2));
        }
        (pos, self.input.get(pos..).and_then(|s| s.chars().next()))
    }
    pub(super) fn peek(&self) -> Option<char> {
        self.logical_at(self.pos).1
    }
    pub(super) fn next_char(&self) -> Option<char> {
        let (p, c) = self.logical_at(self.pos);
        self.logical_at(p.saturating_add(c?.len_utf8())).1
    }
    pub(super) fn bump(&mut self) -> Option<char> {
        let (p, c) = self.logical_at(self.pos);
        self.pos = p.saturating_add(c.map_or(0, char::len_utf8));
        c
    }
    pub(super) fn raw(&self) -> Option<char> {
        self.input.get(self.tabs_at(self.pos)..)?.chars().next()
    }
    pub(super) fn raw_bump(&mut self) -> Option<char> {
        let c = self.raw()?;
        self.pos = self.tabs_at(self.pos).saturating_add(c.len_utf8());
        Some(c)
    }
    pub(super) fn text(&self, start: usize, end: usize) -> Result<&'a str, Outcome> {
        self.input.get(start..end).ok_or(Outcome::InvalidCommand)
    }
    pub(super) fn raw_literal(&self, start: usize, end: usize) -> Result<Cow<'a, str>, Outcome> {
        let text = self.text(start, end)?;
        if !self.strip_tabs {
            return Ok(Cow::Borrowed(text));
        }
        let mut out = String::new();
        let mut beginning =
            start == 0 || self.input.get(..start).is_some_and(|s| s.ends_with('\n'));
        for c in text.chars() {
            if beginning && c == '\t' {
                continue;
            }
            out.push(c);
            beginning = c == '\n';
        }
        if out == text {
            Ok(Cow::Borrowed(text))
        } else {
            Ok(Cow::Owned(out))
        }
    }
    pub(super) fn literal(&self, start: usize, end: usize) -> Result<Cow<'a, str>, Outcome> {
        let s = self.raw_literal(start, end)?;
        if !s.contains("\\\n") {
            return Ok(s);
        }
        let mut out = String::new();
        let mut chars = s.chars().peekable();
        while let Some(c) = chars.next() {
            if c == '\\' {
                if chars.peek() == Some(&'\n') {
                    chars.next();
                    continue;
                }
                out.push(c);
                if let Some(next) = chars.next() {
                    out.push(next);
                }
            } else {
                out.push(c);
            }
        }
        Ok(Cow::Owned(out))
    }
    pub(super) fn push_part(
        &mut self,
        parts: &mut Vec<WordPart<'a>>,
        part: WordPart<'a>,
    ) -> Result<(), Outcome> {
        self.budget.node()?;
        parts.push(part);
        Ok(())
    }
}
