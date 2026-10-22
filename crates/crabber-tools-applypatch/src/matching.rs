//! Linear overlapping literal matching with bounded cancellation intervals.
use crabber_tools_core::{ToolError, category};
use tokio_util::sync::CancellationToken;
const CHECK_INTERVAL: usize = 4096;
fn conflict(message: &str) -> ToolError {
    ToolError::new("conflict", message)
}
pub(crate) fn unique_line_match(
    content: &str,
    needle: &str,
    cancel: &CancellationToken,
) -> Result<usize, ToolError> {
    scan(content.as_bytes(), needle.as_bytes(), &mut || {
        if cancel.is_cancelled() {
            Err(ToolError::new(category::UNKNOWN, "cancelled"))
        } else {
            Ok(())
        }
    })
}
fn scan(
    content: &[u8],
    needle: &[u8],
    check: &mut impl FnMut() -> Result<(), ToolError>,
) -> Result<usize, ToolError> {
    check()?;
    if needle.is_empty() {
        return Err(conflict("patch context does not match"));
    }
    let mut border = vec![0usize; needle.len()];
    let mut matched = 0;
    for i in 1..needle.len() {
        if i % CHECK_INTERVAL == 0 {
            check()?;
        }
        while matched > 0 && needle[i] != needle[matched] {
            matched = border[matched - 1];
        }
        if needle[i] == needle[matched] {
            matched += 1;
        }
        border[i] = matched;
    }
    let mut found = None;
    matched = 0;
    for (i, &byte) in content.iter().enumerate() {
        if i % CHECK_INTERVAL == 0 {
            check()?;
        }
        while matched > 0 && byte != needle[matched] {
            matched = border[matched - 1];
        }
        if byte == needle[matched] {
            matched += 1;
        }
        if matched == needle.len() {
            let start = i + 1 - needle.len();
            if (start == 0 || content[start - 1] == b'\n') && found.replace(start).is_some() {
                return Err(conflict("patch context is ambiguous"));
            }
            matched = border[matched - 1];
        }
    }
    check()?;
    found.ok_or_else(|| conflict("patch context does not match"))
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn overlapping_and_unicode_line_anchors() {
        let token = CancellationToken::new();
        assert_eq!(
            unique_line_match("a\na\na\n", "a\na\n", &token)
                .unwrap_err()
                .category,
            "conflict"
        );
        assert_eq!(
            unique_line_match("prefixfoo\n", "foo\n", &token)
                .unwrap_err()
                .category,
            "conflict"
        );
        assert_eq!(
            unique_line_match("prefix\né\nend\n", "é\n", &token).unwrap(),
            7
        );
        assert_eq!(
            unique_line_match("first\nsecond\n", "first\n", &token).unwrap(),
            0
        );
        assert_eq!(
            unique_line_match("", "", &token).unwrap_err().category,
            "conflict"
        );
    }
    #[test]
    fn repetitive_prefix_is_linear_and_cancellable() {
        let content = "a\n".repeat(4 * 1024 * 1024);
        let needle = "a\n".repeat(262144) + "b\n";
        assert_eq!(
            unique_line_match(&content, &needle, &CancellationToken::new())
                .unwrap_err()
                .category,
            "conflict"
        );
        let mut checks = 0;
        let result = scan(content.as_bytes(), needle.as_bytes(), &mut || {
            checks += 1;
            if checks == 5 {
                Err(ToolError::new(category::UNKNOWN, "cancelled"))
            } else {
                Ok(())
            }
        });
        assert_eq!(result.unwrap_err().message, "cancelled");
        assert_eq!(checks, 5);
        let cancel = CancellationToken::new();
        cancel.cancel();
        assert_eq!(
            unique_line_match(&content, &needle, &cancel)
                .unwrap_err()
                .message,
            "cancelled"
        );
    }
    #[test]
    fn short_inputs_agree_with_all_line_boundary_candidates() {
        let token = CancellationToken::new();
        // Exhaustive short binary-line strings include overlapping matches and
        // non-line-anchored occurrences without timing assumptions.
        for source_bits in 0..256 {
            let source: String = (0..8)
                .map(|i| {
                    if source_bits & (1 << i) == 0 {
                        'a'
                    } else {
                        '\n'
                    }
                })
                .collect();
            for needle_bits in 0..16 {
                let needle: String = (0..4)
                    .map(|i| {
                        if needle_bits & (1 << i) == 0 {
                            'a'
                        } else {
                            '\n'
                        }
                    })
                    .collect();
                let expected: Vec<_> = (0..source.len())
                    .filter(|&i| {
                        (i == 0 || source.as_bytes()[i - 1] == b'\n')
                            && source[i..].starts_with(&needle)
                    })
                    .collect();
                let actual = unique_line_match(&source, &needle, &token);
                match expected.as_slice() {
                    [i] => assert_eq!(actual.unwrap(), *i),
                    _ => assert_eq!(actual.unwrap_err().category, "conflict"),
                }
            }
        }
    }
}
