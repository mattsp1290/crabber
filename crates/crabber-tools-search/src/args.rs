use crabber_tools_core::{RelPath, ToolError, category};
use serde::Deserialize;
#[derive(Deserialize)]
#[serde(untagged)]
pub(crate) enum Globs {
    One(String),
    Many(Vec<String>),
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Args {
    pub pattern: String,
    #[serde(default)]
    pub path: String,
    #[serde(default)]
    pub timeout_seconds: u64,
    pub glob: Option<Globs>,
    #[serde(default)]
    pub literal: bool,
    #[serde(default)]
    pub ignore_case: bool,
    #[serde(default)]
    pub context: usize,
    #[serde(default)]
    pub limit: usize,
}
impl Args {
    pub fn parse(value: serde_json::Value) -> Result<Self, ToolError> {
        let mut a: Self = serde_json::from_value(value)
            .map_err(|_| ToolError::new(category::VALIDATION, "invalid arguments"))?;
        let bad = || ToolError::new(category::VALIDATION, "invalid pattern, glob, or limits");
        if a.pattern.is_empty()
            || a.pattern.contains('\0')
            || a.timeout_seconds > 600
            || a.context > 20
            || a.limit > 1000
        {
            return Err(bad());
        }
        if let Some(globs) = &a.glob
            && (globs.values().is_empty()
                || globs
                    .values()
                    .iter()
                    .any(|s| s.is_empty() || s.contains('\0')))
        {
            return Err(bad());
        }
        let path = RelPath::parse(&a.path, true)?;
        a.path = path.as_path().to_string_lossy().into_owned();
        if a.timeout_seconds == 0 {
            a.timeout_seconds = 60;
        }
        if a.limit == 0 {
            a.limit = 200;
        }
        Ok(a)
    }
}
impl Globs {
    pub fn values(&self) -> Vec<&str> {
        match self {
            Self::One(s) => vec![s],
            Self::Many(v) => v.iter().map(String::as_str).collect(),
        }
    }
}
