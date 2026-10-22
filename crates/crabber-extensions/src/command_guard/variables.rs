pub(super) fn variable_name(s: &str) -> bool {
    let mut bytes = s.bytes();
    bytes
        .next()
        .is_some_and(|b| b == b'_' || b.is_ascii_alphabetic())
        && bytes.all(|b| b == b'_' || b.is_ascii_alphanumeric())
}
pub(super) fn opaque_variable_target(s: &str) -> bool {
    s.starts_with("BASH_")
        || matches!(
            s,
            "OPTIND"
                | "RANDOM"
                | "SRANDOM"
                | "SECONDS"
                | "HISTCMD"
                | "MAILCHECK"
                | "PS0"
                | "PS1"
                | "PS2"
                | "PS3"
                | "PS4"
                | "PROMPT_COMMAND"
                | "ENV"
        )
}
