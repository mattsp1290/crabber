use crate::native_files::{Family, File};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{fs, io, path::Path};

#[derive(Deserialize, Serialize)]
pub(crate) struct Artifact {
    pub(crate) package_id: String,
    pub(crate) target: Target,
    pub(crate) profile: Profile,
    pub(crate) features: Vec<String>,
    pub(crate) executable: Option<String>,
    pub(crate) fresh: bool,
}

#[derive(Deserialize, Serialize)]
pub(crate) struct Target {
    pub(crate) name: String,
    pub(crate) kind: Vec<String>,
    pub(crate) src_path: String,
}

#[derive(Deserialize, Serialize)]
pub(crate) struct Profile {
    opt_level: Optimization,
    debuginfo: DebugInfo,
    debug_assertions: bool,
    overflow_checks: bool,
    pub(crate) test: bool,
}

#[derive(Deserialize, Serialize)]
enum Optimization {
    #[serde(rename = "0")]
    Zero,
    #[serde(rename = "1")]
    One,
    #[serde(rename = "2")]
    Two,
    #[serde(rename = "3")]
    Three,
    #[serde(rename = "s")]
    Size,
    #[serde(rename = "z")]
    MinimumSize,
}

#[derive(Deserialize, Serialize)]
#[serde(untagged)]
enum DebugInfo {
    Level(DebugLevel),
    Named(DebugName),
}

#[derive(Serialize)]
struct DebugLevel(u8);
impl<'de> Deserialize<'de> for DebugLevel {
    fn deserialize<D: serde::Deserializer<'de>>(input: D) -> Result<Self, D::Error> {
        let level = u8::deserialize(input)?;
        if level > 2 {
            return Err(serde::de::Error::custom("unsupported debug level"));
        }
        Ok(Self(level))
    }
}

#[derive(Deserialize, Serialize)]
enum DebugName {
    #[serde(rename = "line-directives-only")]
    Directives,
    #[serde(rename = "line-tables-only")]
    Tables,
}

#[derive(Serialize)]
struct Record {
    index: usize,
    artifact: Artifact,
}

#[derive(Serialize)]
pub(crate) struct Input {
    records: Vec<Record>,
    error: Option<ParseFailure>,
    incomplete: bool,
    #[serde(skip)]
    pub(crate) summary: Option<Value>,
}

#[derive(Serialize)]
struct ParseFailure {
    index: usize,
    category: &'static str,
}

impl Input {
    pub(crate) fn parse(bytes: &[u8]) -> Self {
        let mut input = Self {
            records: Vec::new(),
            error: None,
            incomplete: false,
            summary: None,
        };
        for (index, value) in serde_json::Deserializer::from_slice(bytes)
            .into_iter::<Value>()
            .enumerate()
        {
            let Ok(value) = value else {
                input.reject(index, "invalid-json");
                break;
            };
            if value["reason"] == "compiler-artifact" {
                if value.get("executable").is_none() {
                    input.reject(index, "missing-executable-field");
                    break;
                }
                if let Ok(artifact) = serde_json::from_value(value) {
                    input.records.push(Record { index, artifact });
                } else {
                    input.reject(index, "invalid-artifact-fields");
                    break;
                }
            } else if value.get("rust-binaries").is_some() && input.summary.replace(value).is_some()
            {
                input.reject(index, "duplicate-nextest-summary");
                break;
            }
        }
        if input.error.is_none() && input.summary.is_none() {
            input.reject(input.records.len(), "missing-nextest-summary");
        }
        input
    }
    pub(crate) fn child_failed(&mut self) {
        self.incomplete = true;
    }
    fn reject(&mut self, index: usize, category: &'static str) {
        self.error = Some(ParseFailure { index, category });
        self.incomplete = true;
    }
    pub(crate) fn artifacts(&self) -> impl Iterator<Item = &Artifact> {
        self.records.iter().map(|record| &record.artifact)
    }
    pub(crate) fn persist(&self, directory: &Path) -> io::Result<()> {
        fs::write(
            File::CompilerInput.path(directory),
            serde_json::to_vec_pretty(self)?,
        )?;
        if let Some(summary) = &self.summary {
            fs::write(
                File::Binaries(Family::Remaining).path(directory),
                serde_json::to_vec(summary)?,
            )?;
        }
        Ok(())
    }
    pub(crate) fn admit(&self) -> &Value {
        assert!(
            self.error.is_none() && !self.incomplete,
            "incomplete compiler evidence"
        );
        self.summary
            .as_ref()
            .expect("one actual Nextest binary summary")
    }
    pub(crate) fn capture_failed(directory: &Path) {
        let input = Self {
            records: Vec::new(),
            error: Some(ParseFailure {
                index: 0,
                category: "capture-failed",
            }),
            incomplete: true,
            summary: None,
        };
        if let Err(error) = input.persist(directory) {
            eprintln!("cannot persist incomplete compiler evidence: {error}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn compiler_projection_retains_foreign_and_nonexecutables_without_private_fields() {
        let artifact = json!({"reason":"compiler-artifact","package_id":"foreign", "target":{"name":"example","kind":["lib"],"src_path":"/source.rs","private":"PRIVATE_SENTINEL"},"profile":{"opt_level":"0","debuginfo":0,"debug_assertions":true,"overflow_checks":true,"test":false},"features":[],"executable":null,"fresh":true,"private":"PRIVATE_SENTINEL"});
        let mut workspace = artifact.clone();
        workspace["package_id"] = json!("workspace");
        workspace["executable"] = json!("/workspace/test");
        let mut foreign_executable = artifact.clone();
        foreign_executable["executable"] = json!("/foreign/test");
        let bytes = format!(
            "{}\n{}\n{}\n{}\n",
            json!({"reason":"build-script-executed","env":[["PRIVATE","PRIVATE_SENTINEL"]]}),
            artifact,
            workspace,
            foreign_executable
        );
        let input = Input::parse(bytes.as_bytes());
        assert_eq!(input.artifacts().count(), 3);
        let persisted = serde_json::to_string(&input).unwrap();
        assert!(persisted.contains("foreign"));
        assert!(!persisted.contains("PRIVATE_SENTINEL"));
        let malformed = Input::parse(b"{\"reason\":\"compiler-artifact\",\"executable\":null,\"target\":{\"private\":\"PRIVATE_SENTINEL\"}}\n");
        let persisted = serde_json::to_string(&malformed).unwrap();
        assert!(persisted.contains("invalid-artifact-fields"));
        assert!(!persisted.contains("PRIVATE_SENTINEL"));
    }
}
