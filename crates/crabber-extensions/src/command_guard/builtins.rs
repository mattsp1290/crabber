use super::{Outcome, words::Word};

// Structural analysis boundaries from the reference, not a danger catalog.
pub(super) fn check(name: &str, args: &[Word]) -> Result<(), Outcome> {
    if matches!(
        name,
        "eval"
            | "."
            | "source"
            | "alias"
            | "unalias"
            | "builtin"
            | "enable"
            | "trap"
            | "fc"
            | "history"
            | "bind"
            | "complete"
            | "compgen"
            | "read"
            | "unset"
            | "getopts"
            | "mapfile"
            | "readarray"
            | "declare"
            | "typeset"
            | "local"
            | "export"
            | "readonly"
            | "let"
    ) {
        return Err(Outcome::Unanalysable);
    }
    if name == "printf"
        && args
            .first()
            .is_some_and(|w| !w.known || w.text.starts_with('-') && w.text != "--")
    {
        return Err(Outcome::Unanalysable);
    }
    if matches!(name, "test" | "[")
        && (args
            .iter()
            .any(|w| !w.known || matches!(w.text.as_str(), "-v" | "-R"))
            || name == "[" && args.last().is_none_or(|w| w.text != "]"))
    {
        return Err(Outcome::Unanalysable);
    }
    Ok(())
}
