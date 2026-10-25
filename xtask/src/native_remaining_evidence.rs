use crate::native_files::{Family, File};
use crate::{native_compiler_input::Input, parts::Quarter};
use serde::Serialize;
use serde_json::{Value, json};
use sha2::Digest;
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Component, Path},
};

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize)]
#[serde(rename_all = "lowercase")]
enum Kind {
    Test,
    Bin,
}
impl Kind {
    fn parse(value: &Value) -> Option<Self> {
        match value.as_array()?.as_slice() {
            [name] if name == "test" => Some(Self::Test),
            [name] if name == "bin" => Some(Self::Bin),
            _ => None,
        }
    }
    fn name(&self) -> &'static str {
        match self {
            Self::Test => "test",
            Self::Bin => "bin",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize)]
struct Key {
    member: String,
    kind: Kind,
    name: String,
}
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize)]
struct Selector {
    kind: Kind,
    name: String,
}
impl Selector {
    fn quarter(&self) -> usize {
        let digest = sha2::Sha256::digest(format!("{}\0{}", self.kind.name(), self.name));
        let prefix: [u8; 8] = digest[..8].try_into().unwrap();
        usize::try_from(u64::from_be_bytes(prefix) % 4).unwrap() + 1
    }
}
#[derive(Clone, Serialize)]
struct Declaration {
    key: Key,
    package_id: String,
    source: String,
    required_features: BTreeSet<String>,
    missing_features: BTreeSet<String>,
    eligible: bool,
}
impl Declaration {
    fn selector(&self) -> Selector {
        Selector {
            kind: self.key.kind.clone(),
            name: self.key.name.clone(),
        }
    }
}

#[derive(Serialize)]
pub(crate) struct Plan {
    source: Value,
    metadata_sha256: String,
    quarter: usize,
    declarations: Vec<Declaration>,
    owned: Vec<Declaration>,
    selectors: BTreeSet<Selector>,
    #[serde(skip)]
    normal_bins: Vec<Declaration>,
}

fn strings(value: &Value) -> BTreeSet<String> {
    value
        .as_array()
        .expect("string array")
        .iter()
        .map(|v| v.as_str().expect("string entry").to_owned())
        .collect()
}
fn declaration(
    package: &Value,
    target: &Value,
    kind: Kind,
    workspace: &Path,
    enabled: &BTreeSet<String>,
) -> Declaration {
    let relative = Path::new(package["manifest_path"].as_str().unwrap())
        .strip_prefix(workspace)
        .expect("workspace member manifest");
    assert!(
        relative
            .components()
            .all(|part| matches!(part, Component::Normal(_))),
        "stable member manifest must remain within workspace"
    );
    let required = target
        .get("required-features")
        .map(strings)
        .unwrap_or_default();
    let missing_features: BTreeSet<_> = required.difference(enabled).cloned().collect();
    Declaration {
        key: Key {
            member: relative.to_str().unwrap().to_owned(),
            kind,
            name: target["name"].as_str().unwrap().to_owned(),
        },
        package_id: package["id"].as_str().unwrap().to_owned(),
        source: target["src_path"].as_str().unwrap().to_owned(),
        required_features: required,
        eligible: missing_features.is_empty(),
        missing_features,
    }
}

impl Plan {
    pub(crate) fn from_metadata(
        metadata: &Value,
        workspace: &Path,
        quarter: Quarter,
        bytes: &[u8],
    ) -> Self {
        let members = strings(&metadata["workspace_members"]);
        let nodes: BTreeMap<_, _> = metadata["resolve"]["nodes"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| (v["id"].as_str().unwrap(), strings(&v["features"])))
            .collect();
        let mut declarations = Vec::new();
        let mut normal_bins = Vec::new();
        for package in metadata["packages"].as_array().unwrap() {
            let id = package["id"].as_str().unwrap();
            if !members.contains(id) {
                continue;
            }
            let enabled = nodes.get(id).expect("workspace resolved features");
            for target in package["targets"].as_array().unwrap() {
                let Some(kind) = Kind::parse(&target["kind"]) else {
                    continue;
                };
                let value = declaration(package, target, kind, workspace, enabled);
                if value.key.kind == Kind::Bin && value.eligible {
                    normal_bins.push(value.clone());
                }
                if target["test"].as_bool().expect("declared test flag") {
                    declarations.push(value);
                }
            }
        }
        declarations.sort_by(|a, b| a.key.cmp(&b.key));
        let keys: BTreeSet<_> = declarations.iter().map(|v| &v.key).collect();
        assert_eq!(
            keys.len(),
            declarations.len(),
            "duplicate target declaration"
        );
        let owned: Vec<_> = declarations
            .iter()
            .filter(|v| v.eligible && v.selector().quarter() == quarter.index())
            .cloned()
            .collect();
        assert!(!owned.is_empty(), "empty remaining target group");
        let selectors = owned.iter().map(Declaration::selector).collect();
        Self {
            source: crate::timings::source_identity(),
            metadata_sha256: format!("{:x}", sha2::Sha256::digest(bytes)),
            quarter: quarter.index(),
            declarations,
            owned,
            selectors,
            normal_bins,
        }
    }
    pub(crate) fn append_arguments(&self, command: &mut std::process::Command) {
        assert!(
            !self.selectors.is_empty(),
            "remaining build needs explicit selectors"
        );
        for selector in &self.selectors {
            command
                .arg(if selector.kind == Kind::Test {
                    "--test"
                } else {
                    "--bin"
                })
                .arg(&selector.name);
        }
    }
    pub(crate) fn persist(&self, directory: &Path, command: &std::process::Command) {
        let argv: Vec<_> = std::iter::once(command.get_program())
            .chain(command.get_args())
            .map(|v| v.to_string_lossy().into_owned())
            .collect();
        fs::write(File::TargetPlan.path(directory), serde_json::to_vec_pretty(&json!({"plan":self,"materialized_argv":argv,"argv_authority":"materialized request; output establishes successful spawn"})).unwrap()).unwrap();
    }
    pub(crate) fn admit_build(&self, directory: &Path, metadata: &Value, input: &Input) {
        let binaries = input.admit();
        let owned: BTreeMap<_, _> = self.owned.iter().map(|v| (&v.key, v)).collect();
        let packages: BTreeSet<_> = self.owned.iter().map(|v| v.package_id.as_str()).collect();
        let mut actual = BTreeMap::new();
        let mut normals = BTreeMap::new();
        for artifact in input.artifacts().filter(|v| v.executable.is_some()) {
            let row = serde_json::to_value(artifact).unwrap();
            let package = metadata["packages"]
                .as_array()
                .unwrap()
                .iter()
                .find(|p| p["id"] == artifact.package_id)
                .expect("declared executable package");
            let declared = if artifact.profile.test {
                &self.owned
            } else {
                &self.normal_bins
            };
            let target = declared
                .iter()
                .find(|v| {
                    v.package_id == artifact.package_id
                        && v.key.name == artifact.target.name
                        && artifact.target.kind.first().map(String::as_str)
                            == Some(v.key.kind.name())
                })
                .expect("foreign or unowned executable target");
            assert_eq!(artifact.target.kind, [target.key.kind.name()]);
            assert_eq!(artifact.target.src_path, target.source);
            crate::native_test_evidence::admit_compiler_context(metadata, &row, package);
            if artifact.profile.test {
                assert!(
                    actual.insert(target.key.clone(), row).is_none(),
                    "duplicate test executable"
                );
            } else {
                assert!(
                    packages.contains(artifact.package_id.as_str()),
                    "normal prerequisite outside selected packages"
                );
                assert!(
                    normals.insert(target.key.clone(), row).is_none(),
                    "duplicate normal executable"
                );
            }
        }
        assert_eq!(
            actual.keys().collect::<Vec<_>>(),
            owned.keys().copied().collect::<Vec<_>>(),
            "compiled remaining targets differ from plan"
        );
        let mut listed = BTreeSet::new();
        for (id, binary) in binaries["rust-binaries"].as_object().unwrap() {
            assert_eq!(binary["binary-id"], id.as_str());
            let target = self
                .owned
                .iter()
                .find(|v| {
                    binary["package-id"] == v.package_id
                        && binary["kind"] == v.key.kind.name()
                        && binary["binary-name"] == v.key.name
                })
                .expect("Nextest binary outside owned targets");
            assert_eq!(binary["binary-path"], actual[&target.key]["executable"]);
            assert_eq!(binary["build-platform"], "target");
            assert!(listed.insert(target.key.clone()));
        }
        assert_eq!(listed, actual.keys().cloned().collect());
        admit_normal_metadata(binaries, &normals);
        let journal = crate::normal_outputs::journal(
            directory,
            &File::Compiler(Family::Remaining).path(directory),
        );
        assert_eq!(journal[0]["source"], self.source);
        assert_eq!(journal[0]["metadata_sha256"], self.metadata_sha256);
        fs::write(File::Targets(Family::Remaining).path(directory), serde_json::to_vec_pretty(&json!({"source":self.source,"metadata_sha256":self.metadata_sha256,"owned_targets":self.owned,"test_artifacts":actual.into_values().collect::<Vec<_>>(),"normal_prerequisites":normals.into_values().collect::<Vec<_>>(),"normal_prerequisite_scope":"eligible declared bins of owned test packages","semantic_credit":false})).unwrap()).unwrap();
    }
}

fn admit_normal_metadata(binaries: &Value, actual: &BTreeMap<Key, Value>) {
    let root = Path::new(
        binaries["rust-build-meta"]["target-directory"]
            .as_str()
            .unwrap(),
    );
    let mut listed = BTreeSet::new();
    for (package, entries) in binaries["rust-build-meta"]["non-test-binaries"]
        .as_object()
        .unwrap()
    {
        for entry in entries.as_array().unwrap() {
            assert_eq!(entry["kind"], "bin-exe");
            let (key, artifact) = actual
                .iter()
                .find(|(_, v)| {
                    v["package_id"] == package.as_str() && v["target"]["name"] == entry["name"]
                })
                .expect("unobserved Nextest normal executable");
            assert_eq!(
                root.join(entry["path"].as_str().unwrap()),
                Path::new(artifact["executable"].as_str().unwrap())
            );
            assert!(listed.insert(key.clone()));
        }
    }
    assert_eq!(listed, actual.keys().cloned().collect());
}

pub(crate) fn read(path: impl AsRef<Path>) -> Value {
    serde_json::from_slice(&fs::read(path).unwrap()).unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;
    fn metadata(root: &str) -> Value {
        json!({"workspace_members":["a","b"],"resolve":{"nodes":[{"id":"a","features":["enabled"]},{"id":"b","features":[]}]},"packages":[
            {"id":"a","name":"a","manifest_path":format!("{root}/a/Cargo.toml"),"targets":[{"name":"continuation","kind":["test"],"src_path":format!("{root}/a/test.rs"),"test":true},{"name":"enabled","kind":["test"],"src_path":format!("{root}/a/enabled.rs"),"test":true,"required-features":["enabled"]},{"name":"disabled","kind":["test"],"src_path":format!("{root}/a/live.rs"),"test":true,"required-features":["live"]}]},
            {"id":"b","name":"b","manifest_path":format!("{root}/b/Cargo.toml"),"targets":[{"name":"continuation","kind":["test"],"src_path":format!("{root}/b/test.rs"),"test":true}]}
        ]})
    }
    #[test]
    fn selector_groups_preserve_duplicates_and_resolved_feature_eligibility() {
        let meta = metadata("/repo");
        let q = [Quarter::One, Quarter::Two, Quarter::Three, Quarter::Four]
            .into_iter()
            .find(|q| {
                q.index()
                    == Selector {
                        kind: Kind::Test,
                        name: "continuation".into(),
                    }
                    .quarter()
            })
            .unwrap();
        let plan = Plan::from_metadata(&meta, Path::new("/repo"), q, b"metadata");
        assert_eq!(
            plan.owned
                .iter()
                .filter(|v| v.key.name == "continuation")
                .count(),
            2
        );
        let disabled = plan
            .declarations
            .iter()
            .find(|v| v.key.name == "disabled")
            .unwrap();
        assert!(!disabled.eligible && disabled.missing_features.contains("live"));
        assert!(
            plan.declarations
                .iter()
                .find(|v| v.key.name == "enabled")
                .unwrap()
                .eligible
        );
        let moved = Plan::from_metadata(
            &metadata("/elsewhere"),
            Path::new("/elsewhere"),
            q,
            b"other",
        );
        assert_eq!(
            plan.owned.iter().map(|v| &v.key).collect::<Vec<_>>(),
            moved.owned.iter().map(|v| &v.key).collect::<Vec<_>>()
        );
        let mut command = std::process::Command::new("cargo");
        plan.append_arguments(&mut command);
        assert!(command.get_args().any(|arg| arg == "continuation"));
        let empty = json!({"workspace_members":[],"resolve":{"nodes":[]},"packages":[]});
        assert!(
            std::panic::catch_unwind(|| Plan::from_metadata(
                &empty,
                Path::new("/repo"),
                q,
                b"empty"
            ))
            .is_err()
        );
    }
}
