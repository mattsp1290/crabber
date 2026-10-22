//! Component-aware ordered target overlap checks.
use crabber_tools_core::{ToolError, category};
use std::{
    collections::BTreeSet,
    path::{Component, Path, PathBuf},
};
pub(crate) fn record(seen: &mut BTreeSet<PathBuf>, path: &Path) -> Result<(), ToolError> {
    let path: PathBuf = path
        .components()
        .filter(|part| *part != Component::CurDir)
        .collect();
    let overlap = path.as_os_str().is_empty()
        || path.ancestors().any(|ancestor| seen.contains(ancestor))
        || seen
            .range(path.clone()..)
            .next()
            .is_some_and(|candidate| candidate.starts_with(&path));
    if overlap {
        return Err(ToolError::new(
            category::VALIDATION,
            "duplicate or overlapping patch targets",
        ));
    }
    seen.insert(path);
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn ancestors_descendants_duplicates_and_lexical_siblings() {
        for (prior, next, overlap) in [
            ("a/child", "a", true),
            ("a", "a/child", true),
            ("a", "a", true),
            ("a-", "a", false),
            ("a-", "a/child", false),
            ("ab", "a", false),
            ("./a", "a/child", true),
            ("a", "./a", true),
        ] {
            let mut seen = BTreeSet::new();
            record(&mut seen, Path::new(prior)).unwrap();
            assert_eq!(
                record(&mut seen, Path::new(next)).is_err(),
                overlap,
                "{prior} {next}"
            );
        }
        let mut seen = BTreeSet::new();
        record(&mut seen, Path::new("a-")).unwrap();
        record(&mut seen, Path::new("a/child")).unwrap();
        assert!(record(&mut seen, Path::new("a")).is_err());
        assert!(record(&mut seen, Path::new(".")).is_err());
    }
    #[test]
    fn many_distinct_targets_keep_component_boundaries() {
        let mut seen = BTreeSet::new();
        for i in 0..20000 {
            record(&mut seen, Path::new(&format!("item-{i:05}/leaf"))).unwrap();
        }
        assert_eq!(seen.len(), 20000);
        assert!(record(&mut seen, Path::new("item-10000")).is_err());
        record(&mut seen, Path::new("item-10000-sibling")).unwrap();
    }
}
