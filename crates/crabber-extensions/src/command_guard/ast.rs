use std::borrow::Cow;

pub(super) struct Script<'a> {
    pub(super) items: Vec<ListItem<'a>>,
    // Pending heredocs on a line can belong to different commands. The parser
    // fills their indexed bodies at that line's newline, in lexical order.
    pub(super) heredocs: Vec<Heredoc<'a>>,
}
pub(super) struct ListItem<'a> {
    pub(super) and_or: AndOr<'a>,
    pub(super) background: bool,
}
pub(super) struct AndOr<'a> {
    pub(super) first: Pipeline<'a>,
    pub(super) rest: Vec<(AndOrOp, Pipeline<'a>)>,
}
pub(super) enum AndOrOp {
    And,
    Or,
}
pub(super) struct Pipeline<'a> {
    pub(super) negated: bool,
    pub(super) commands: Vec<Command<'a>>,
}
pub(super) struct Command<'a> {
    pub(super) kind: CommandKind<'a>,
    pub(super) redirects: Vec<Redirect<'a>>,
}
pub(super) enum CommandKind<'a> {
    Simple {
        assignments: Vec<Assignment<'a>>,
        words: Vec<Word<'a>>,
    },
    Subshell(Script<'a>),
    Brace(Script<'a>),
    If {
        arms: Vec<(Script<'a>, Script<'a>)>,
        else_: Option<Script<'a>>,
    },
    While {
        cond: Script<'a>,
        body: Script<'a>,
    },
    Until {
        cond: Script<'a>,
        body: Script<'a>,
    },
    For {
        name: Cow<'a, str>,
        words: Option<Vec<Word<'a>>>,
        body: Script<'a>,
    },
    Case {
        subject: Word<'a>,
        items: Vec<CaseItem<'a>>,
    },
}
pub(super) struct CaseItem<'a> {
    pub(super) patterns: Vec<Word<'a>>,
    pub(super) body: Script<'a>,
    pub(super) terminator: CaseEnd,
}
pub(super) enum CaseEnd {
    Break,
    FallThrough,
    Continue,
    None,
}
pub(super) struct Assignment<'a> {
    pub(super) name: Cow<'a, str>,
    pub(super) value: Word<'a>,
}
pub(super) struct Redirect<'a> {
    pub(super) fd: Option<Cow<'a, str>>,
    pub(super) op: RedirectOp,
    pub(super) target: RedirectTarget<'a>,
}
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum RedirectOp {
    In,
    Out,
    Append,
    Here,
    HereTabs,
    DupIn,
    DupOut,
    ReadWrite,
    Clobber,
    Both,
    BothAppend,
    HereString,
}
pub(super) enum RedirectTarget<'a> {
    Word(Word<'a>),
    Heredoc(usize),
}
pub(super) struct Heredoc<'a> {
    pub(super) quoted: bool,
    pub(super) parts: Vec<WordPart<'a>>,
}
pub(super) struct Word<'a> {
    pub(super) parts: Vec<WordPart<'a>>,
}
pub(super) enum WordPart<'a> {
    Lit(Cow<'a, str>),
    SglQuoted(Cow<'a, str>),
    DblQuoted(Vec<WordPart<'a>>),
    AnsiQuoted(Cow<'a, str>),
    LocaleQuoted(Vec<WordPart<'a>>),
    Param(Cow<'a, str>),
    CmdSubst(Script<'a>),
    Backquote(String),
    ProcSubst { dir: Direction, script: Script<'a> },
}
pub(super) enum Direction {
    In,
    Out,
}
impl Word<'_> {
    pub(super) fn literal(&self) -> Option<&str> {
        if self.parts.len() != 1 {
            return None;
        }
        match self.parts.first()? {
            WordPart::Lit(s) => Some(s),
            _ => None,
        }
    }
}
