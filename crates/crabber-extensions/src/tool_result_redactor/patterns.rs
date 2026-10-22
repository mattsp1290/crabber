use regex::Regex;

type Boundary = (fn(u8) -> bool, fn(u8) -> bool);

#[derive(Clone)]
pub(super) struct Rule {
    pub regex: Regex,
    boundary: Option<Boundary>,
}
impl Rule {
    pub fn plain(regex: Regex) -> Self {
        Self {
            regex,
            boundary: None,
        }
    }
    pub fn admits(&self, text: &str, start: usize, end: usize) -> bool {
        self.boundary.is_none_or(|(left, right)| {
            (start == 0 || !left(text.as_bytes()[start - 1]))
                && (end == text.len() || !right(text.as_bytes()[end]))
        })
    }
}

pub(super) fn builtins() -> Vec<Rule> {
    let simple = |s| Rule::plain(Regex::new(s).expect("built-in regex"));
    let bounded = |s, left, right| Rule {
        regex: Regex::new(s).expect("built-in regex"),
        boundary: Some((left, right)),
    };
    vec![
        simple(r"(?ms)^-----BEGIN PRIVATE KEY-----\r?$.*?^-----END PRIVATE KEY-----\r?$"),
        simple(
            r"(?ms)^-----BEGIN ENCRYPTED PRIVATE KEY-----\r?$.*?^-----END ENCRYPTED PRIVATE KEY-----\r?$",
        ),
        bounded(
            r"(?i:Authorization[\t ]*:[\t ]*Bearer) +[A-Za-z0-9\-._~+/]+=*",
            |b| b.is_ascii_alphanumeric() || b"_-".contains(&b),
            |b| b.is_ascii_alphanumeric() || b"-._~+/=".contains(&b),
        ),
        bounded(
            r"ghs_[0-9]+_[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+",
            |b| b.is_ascii_alphanumeric() || b"-._".contains(&b),
            |b| b.is_ascii_alphanumeric() || b"-._".contains(&b),
        ),
        bounded(
            r"(?:ghp_|github_pat_|gho_|ghu_|ghs_|ghr_)[A-Za-z0-9_]{16,}",
            |b| b.is_ascii_alphanumeric() || b == b'_',
            |b| b.is_ascii_alphanumeric() || b == b'_',
        ),
    ]
}
