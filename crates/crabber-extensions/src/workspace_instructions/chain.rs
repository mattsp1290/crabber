//! Pure routing arithmetic. Directory strings confer no filesystem authority.
use super::failure;

pub(super) fn segment(text: &str) -> bool {
    !text.is_empty()
        && text != "."
        && text != ".."
        && !text.chars().any(|c| c.is_control() || "\\:".contains(c))
}
pub(super) fn absolute(text: &str) -> bool {
    text.len() <= 4096
        && (text == "/"
            || (text.starts_with('/')
                && text[1..].split('/').all(|part| {
                    !part.is_empty()
                        && part != "."
                        && part != ".."
                        && !part.chars().any(char::is_control)
                })))
}
pub(super) fn derive(
    boundaries: &[String],
    directory: &str,
    max_depth: usize,
) -> Result<Option<Vec<String>>, String> {
    if !absolute(directory) {
        return Err(failure("workspace"));
    }
    let Some(boundary) = boundaries
        .iter()
        .filter(|b| {
            b.as_str() == "/"
                || directory == b.as_str()
                || directory
                    .strip_prefix(b.as_str())
                    .is_some_and(|rest| rest.starts_with('/'))
        })
        .max_by_key(|b| b.len())
    else {
        return Ok(None);
    };
    let rest = directory[boundary.len()..].trim_start_matches('/');
    let mut chain = vec![String::new()];
    if !rest.is_empty() {
        let mut cumulative = String::new();
        for part in rest.split('/') {
            if !segment(part) {
                return Err(failure("workspace"));
            }
            if chain.len() >= max_depth {
                return Err(failure("chain-depth"));
            }
            if !cumulative.is_empty() {
                cumulative.push('/');
            }
            cumulative.push_str(part);
            chain.push(cumulative.clone());
        }
    }
    Ok(Some(chain))
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn longest_matching_boundary_and_root_preserve_chain_order() {
        let b = vec!["/".into(), "/a".into(), "/a/b".into()];
        assert_eq!(
            derive(&b, "/a/b/c/d", 4).unwrap().unwrap(),
            ["", "c", "c/d"]
        );
        assert_eq!(derive(&b, "/ab", 4).unwrap().unwrap(), ["", "ab"]);
        assert_eq!(derive(&b, "/a", 4).unwrap().unwrap(), [""]);
        assert_eq!(derive(&["/a".into()], "/ab", 4).unwrap(), None);
        assert_eq!(derive(&b, "/", 4).unwrap().unwrap(), [""]);
        // Literal POSIX punctuation in an admitted boundary is not a relative path.
        assert_eq!(
            derive(&["/a:b".into()], "/a:b/child", 4).unwrap().unwrap(),
            ["", "child"]
        );
    }
    #[test]
    fn invalid_directory_and_depth_fail_before_resolving() {
        for value in [
            "relative", "/a/../b", "/a//b", "/a/", "/a/.", "/a/\0", "/a/\\b", "/a/c:d", "/a/\u{1}",
        ] {
            assert!(derive(&["/".into()], value, 64).is_err(), "{value:?}");
        }
        assert!(derive(&["/a".into()], "/a/b", 1).is_err());
    }
}
