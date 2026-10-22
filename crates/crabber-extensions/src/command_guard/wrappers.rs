use super::{Dialect, Outcome, parser::Context, variables, walk::Analysis, words::Word};

#[derive(Clone, Copy)]
enum Operand {
    Flag,
    Nonempty,
    Variable,
    Duration,
    Signal,
}
const ENV: &[(&str, Operand)] = &[
    ("-i", Operand::Flag),
    ("--ignore-environment", Operand::Flag),
    ("-u", Operand::Variable),
    ("--unset=", Operand::Variable),
    ("-C", Operand::Nonempty),
    ("--chdir=", Operand::Nonempty),
];
const SUDO: &[(&str, Operand)] = &[
    ("-n", Operand::Flag),
    ("-E", Operand::Flag),
    ("-H", Operand::Flag),
    ("-u", Operand::Nonempty),
    ("--user=", Operand::Nonempty),
    ("-g", Operand::Nonempty),
    ("--group=", Operand::Nonempty),
    ("-D", Operand::Nonempty),
    ("--chdir=", Operand::Nonempty),
];
const TIMEOUT: &[(&str, Operand)] = &[
    ("--foreground", Operand::Flag),
    ("--preserve-status", Operand::Flag),
    ("-v", Operand::Flag),
    ("--verbose", Operand::Flag),
    ("-s", Operand::Signal),
    ("--signal=", Operand::Signal),
    ("-k", Operand::Duration),
    ("--kill-after=", Operand::Duration),
];
fn duration(s: &str) -> bool {
    let s = s.strip_suffix(['s', 'm', 'h', 'd']).unwrap_or(s);
    let mut parts = s.split('.');
    let digits = |s: &str| !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit());
    parts.next().is_some_and(digits) && parts.next().is_none_or(digits) && parts.next().is_none()
}
fn valid(s: &str, kind: Operand) -> bool {
    if s.is_empty() {
        return false;
    }
    match kind {
        Operand::Flag | Operand::Nonempty => true,
        Operand::Variable => variables::variable_name(s),
        Operand::Duration => duration(s),
        Operand::Signal => {
            s.bytes().all(|b| b.is_ascii_digit()) && s.bytes().any(|b| b != b'0')
                || s.bytes().next().is_some_and(|b| b.is_ascii_alphabetic())
                    && s.bytes().all(|b| b.is_ascii_alphanumeric())
        }
    }
}
fn scan_options<'a>(mut args: &'a [Word], spec: &[(&str, Operand)]) -> Result<&'a [Word], Outcome> {
    while let Some((word, rest)) = args.split_first() {
        if !word.known {
            return Err(Outcome::Unanalysable);
        }
        if word.text == "--" {
            return Ok(rest);
        }
        if !word.text.starts_with('-') {
            break;
        }
        let mut consumed = None;
        for (name, kind) in spec {
            if name.ends_with('=') {
                if word
                    .text
                    .strip_prefix(name)
                    .is_some_and(|s| valid(s, *kind))
                {
                    consumed = Some(rest);
                    break;
                }
            } else if word.text == *name {
                if matches!(kind, Operand::Flag) {
                    consumed = Some(rest);
                    break;
                }
                if let Some((operand, remaining)) = rest.split_first()
                    && operand.known
                    && valid(&operand.text, *kind)
                {
                    consumed = Some(remaining);
                    break;
                }
            }
        }
        args = consumed.ok_or(Outcome::Unanalysable)?;
    }
    Ok(args)
}
fn assignments(mut args: &[Word]) -> Result<&[Word], Outcome> {
    while let Some((word, rest)) = args.split_first() {
        if !word.known {
            return Err(Outcome::Unanalysable);
        }
        let Some((key, _)) = word.text.split_once('=') else {
            break;
        };
        if !variables::variable_name(key) || variables::opaque_variable_target(key) {
            return Err(Outcome::Unanalysable);
        }
        args = rest;
    }
    Ok(args)
}
fn arguments<'a>(name: &str, mut args: &'a [Word]) -> Result<&'a [Word], Outcome> {
    match name {
        "command" | "exec" => {
            if name == "command" && args.first().is_some_and(|w| w.known && w.text == "-p") {
                args = args.get(1..).ok_or(Outcome::Unanalysable)?;
            }
            if let Some((word, rest)) = args.split_first() {
                if word.known && word.text == "--" {
                    return Ok(rest);
                }
                if !word.known || word.text.starts_with('-') {
                    return Err(Outcome::Unanalysable);
                }
            }
        }
        "env" => args = assignments(scan_options(args, ENV)?)?,
        "sudo" => {
            args = assignments(scan_options(args, SUDO)?)?;
            if args.first().is_some_and(|w| w.text.starts_with('-')) {
                return Err(Outcome::Unanalysable);
            }
        }
        "timeout" => {
            args = scan_options(args, TIMEOUT)?;
            let (word, rest) = args.split_first().ok_or(Outcome::Unanalysable)?;
            if !word.known || !duration(&word.text) {
                return Err(Outcome::Unanalysable);
            }
            args = rest;
        }
        _ => return Err(Outcome::Unanalysable),
    }
    Ok(args)
}
impl Analysis<'_> {
    pub(super) fn wrapper(
        &mut self,
        name: &str,
        args: &[Word],
        depth: usize,
        wrappers: usize,
    ) -> Outcome {
        if !matches!(
            name,
            "env" | "command" | "exec" | "sudo" | "timeout" | "sh" | "bash"
        ) {
            return Outcome::Abstain;
        }
        if wrappers >= self.policy.limits.max_wrapper_depth {
            return Outcome::AnalysisLimit;
        }
        let next = depth.saturating_add(1);
        if let Err(o) = self.budget.depth(next) {
            return o;
        }
        let wrappers = wrappers.saturating_add(1);
        if matches!(name, "sh" | "bash") {
            return self.shell(name, args, next, wrappers);
        }
        match arguments(name, args) {
            Err(o) => o,
            Ok([]) if name == "env" => Outcome::Abstain,
            Ok([]) => Outcome::Unanalysable,
            Ok(rest) => self.command(rest, next, wrappers),
        }
    }
    fn shell(&mut self, name: &str, mut args: &[Word], depth: usize, wrappers: usize) -> Outcome {
        while args
            .first()
            .is_some_and(|w| w.known && matches!(w.text.as_str(), "-e" | "-u" | "-x"))
        {
            args = args.get(1..).unwrap_or(&[]);
        }
        let Some((option, rest)) = args.split_first() else {
            return Outcome::Unanalysable;
        };
        if !option.known || !matches!(option.text.as_str(), "-c" | "-lc") {
            return Outcome::Unanalysable;
        }
        let Some((script, rest)) = rest.split_first() else {
            return Outcome::Unanalysable;
        };
        if !script.known
            || script.text.starts_with(['-', '+'])
            || rest.first().is_some_and(|w| !w.known)
        {
            return Outcome::Unanalysable;
        }
        let dialect = if name == "bash" {
            Dialect::Bash
        } else {
            Dialect::Posix
        };
        self.script(&script.text, dialect, Context::Top, depth, wrappers)
    }
}
