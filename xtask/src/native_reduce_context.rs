use crate::native_bundle::Roots;
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    path::{Component, Path},
};

pub(crate) struct Context<'a> {
    pub(crate) roots: &'a Roots,
    packages: BTreeMap<String, String>,
}

fn relative(path: &Path, root: &Path) -> String {
    let path = path
        .strip_prefix(root)
        .expect("recorded path outside declared root");
    assert!(
        path.components()
            .all(|part| matches!(part, Component::Normal(_)))
    );
    path.to_str().unwrap().to_owned()
}

impl<'a> Context<'a> {
    pub(crate) fn new(roots: &'a Roots, metadata: &Value) -> Self {
        let members = metadata["workspace_members"].as_array().unwrap();
        let mut packages = BTreeMap::new();
        for package in metadata["packages"].as_array().unwrap() {
            let id = package["id"].as_str().unwrap();
            let identity = if members.contains(&package["id"]) {
                format!(
                    "workspace:{}",
                    relative(
                        Path::new(package["manifest_path"].as_str().unwrap()),
                        Path::new(&roots.workspace)
                    )
                )
            } else {
                id.to_owned()
            };
            assert!(
                packages.insert(id.to_owned(), identity).is_none(),
                "duplicate metadata package"
            );
        }
        let unique: std::collections::BTreeSet<_> = packages.values().collect();
        assert_eq!(unique.len(), packages.len(), "aliased package identity");
        Self { roots, packages }
    }
    pub(crate) fn package(&self, value: &Value) -> Value {
        json!(
            self.packages
                .get(value.as_str().unwrap())
                .expect("unknown package identity")
        )
    }
    pub(crate) fn path(&self, value: &str) -> String {
        let path = Path::new(value);
        for (root, name) in [
            (&self.roots.run, "<run>"),
            (&self.roots.target, "<target>"),
            (&self.roots.workspace, "<workspace>"),
        ] {
            if path.starts_with(root) {
                return format!("{name}/{}", relative(path, Path::new(root)));
            }
        }
        let relative_run = Path::new(&self.roots.run)
            .strip_prefix(&self.roots.workspace)
            .ok();
        if let Some(run) = relative_run
            && path.starts_with(run)
        {
            return format!("<run>/{}", relative(path, run));
        }
        if let Ok(target) = Path::new(&self.roots.target).strip_prefix(&self.roots.workspace)
            && path.starts_with(target)
        {
            return format!("<target>/{}", relative(path, target));
        }
        value.to_owned()
    }
    pub(crate) fn executable(&self, value: &Value) -> Value {
        let path = value.as_str().unwrap();
        assert!(
            Path::new(path).is_absolute() && Path::new(path).starts_with(&self.roots.target),
            "executable outside recorded target root"
        );
        json!(self.path(path))
    }
    pub(crate) fn artifact(&self, row: &Value) -> Value {
        let source = row["target"]["src_path"].as_str().unwrap();
        assert!(Path::new(source).starts_with(&self.roots.workspace));
        json!({"package":self.package(&row["package_id"]),"target":{"name":row["target"]["name"],"kind":row["target"]["kind"],"source":self.path(source)},"profile":row["profile"],"features":row["features"],"executable":self.executable(&row["executable"])})
    }
    pub(crate) fn suites(&self, manifest: &Value) -> Value {
        let mut suites = manifest["rust-suites"].as_object().unwrap().clone();
        for suite in suites.values_mut() {
            suite["package-id"] = self.package(&suite["package-id"]);
            suite["binary-path"] = self.executable(&suite["binary-path"]);
            if let Some(cwd) = suite["cwd"].as_str() {
                suite["cwd"] = json!(self.path(cwd));
            }
        }
        Value::Object(suites)
    }
    pub(crate) fn gate(&self, gate: &Value) -> Value {
        let mut gate = gate.clone();
        for key in [
            "working_directory",
            "execution_evidence",
            "cargo_artifacts",
            "compiler_input",
            "resolved_plan",
        ] {
            if let Some(value) = gate[key].as_str() {
                gate[key] = json!(self.path(value));
            }
        }
        for key in ["argv", "command_template"] {
            if let Some(argv) = gate[key].as_array_mut() {
                for value in argv {
                    *value = json!(self.path(value.as_str().unwrap()));
                }
            }
        }
        gate
    }
    pub(crate) fn metadata(&self, metadata: &Value) -> Value {
        let mut canonical = metadata.clone();
        for field in ["workspace_members", "workspace_default_members"] {
            for value in canonical[field].as_array_mut().unwrap() {
                *value = self.package(value);
            }
        }
        canonical["workspace_root"] = json!("<workspace>");
        canonical["target_directory"] = json!("<target>");
        if let Some(value) = canonical.get_mut("build_directory") {
            *value = json!("<build>");
        }
        for package in canonical["packages"].as_array_mut().unwrap() {
            self.metadata_package(package);
        }
        let resolve = &mut canonical["resolve"];
        if !resolve["root"].is_null() {
            resolve["root"] = self.package(&resolve["root"]);
        }
        for node in resolve["nodes"].as_array_mut().unwrap() {
            node["id"] = self.package(&node["id"]);
            for id in node["dependencies"].as_array_mut().unwrap() {
                *id = self.package(id);
            }
            for dependency in node["deps"].as_array_mut().unwrap() {
                dependency["pkg"] = self.package(&dependency["pkg"]);
            }
        }
        canonical
    }
    fn metadata_package(&self, package: &mut Value) {
        let id = self.package(&package["id"]);
        let manifest = Path::new(package["manifest_path"].as_str().unwrap()).to_owned();
        let package_root = manifest.parent().unwrap();
        let path = |value: &str| {
            if Path::new(value).starts_with(&self.roots.workspace) {
                self.path(value)
            } else {
                format!(
                    "<package:{}>/{}",
                    id.as_str().unwrap(),
                    relative(Path::new(value), package_root)
                )
            }
        };
        package["id"] = id.clone();
        package["manifest_path"] = json!(path(manifest.to_str().unwrap()));
        for field in ["readme", "license_file"] {
            if let Some(value) = package[field].as_str()
                && Path::new(value).is_absolute()
            {
                package[field] = json!(path(value));
            }
        }
        for target in package["targets"].as_array_mut().unwrap() {
            target["src_path"] = json!(path(target["src_path"].as_str().unwrap()));
        }
        for dependency in package["dependencies"].as_array_mut().unwrap() {
            if let Some(value) = dependency["path"].as_str() {
                assert!(
                    Path::new(value).starts_with(&self.roots.workspace),
                    "foreign local dependency path"
                );
                dependency["path"] = json!(self.path(value));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn metadata(root: &str, foreign: &str) -> Value {
        let id = format!("path+file://{root}/member#member@1");
        json!({"workspace_root":root,"target_directory":format!("{root}/target"),"workspace_members":[id],"workspace_default_members":[id],"packages":[
            {"id":id,"name":"member","manifest_path":format!("{root}/member/Cargo.toml"),"targets":[{"name":"member","src_path":format!("{root}/member/src/lib.rs")}],"dependencies":[]},
            {"id":foreign,"name":"external","manifest_path":"/registry/external/Cargo.toml","targets":[{"name":"external","src_path":"/registry/external/src/lib.rs"}],"dependencies":[]}
        ],"resolve":{"root":null,"nodes":[{"id":id,"dependencies":[foreign],"deps":[{"pkg":foreign}]},{"id":foreign,"dependencies":[],"deps":[]}]}})
    }
    fn canonical(root: &str, foreign: &str) -> Value {
        let roots = Roots {
            workspace: root.into(),
            target: format!("{root}/target"),
            run: format!("{root}/target/run"),
        };
        let metadata = metadata(root, foreign);
        let context = Context::new(&roots, &metadata);
        json!({
            "metadata": context.metadata(&metadata),
            "suites": context.suites(&json!({"rust-suites":{"member":{
                "package-id":metadata["workspace_members"][0],
                "binary-path":format!("{root}/target/debug/deps/member"),
                "cwd":format!("{root}/member")
            }}})),
            "gate": context.gate(&json!({
                "working_directory":root,
                "execution_evidence":format!("{root}/target/run/execution"),
                "cargo_artifacts":format!("{root}/target/run/artifacts.jsonl"),
                "compiler_input":format!("{root}/target/run/compiler-input.json"),
                "resolved_plan":format!("{root}/target/run/plan.json"),
                "argv":["cargo","--cargo-metadata",format!("{root}/target/run/metadata.json")],
                "command_template":null
            }))
        })
    }
    #[test]
    fn relocation_preserves_package_graph_and_foreign_source_identity() {
        assert_eq!(
            canonical("/original", "registry+one#external@1"),
            canonical("/download", "registry+one#external@1")
        );
        assert_ne!(
            canonical("/original", "registry+one#external@1"),
            canonical("/download", "registry+other#external@1")
        );
        let roots = Roots {
            workspace: "/original".into(),
            target: "/original/target".into(),
            run: "/original/target/run".into(),
        };
        let metadata = metadata("/original", "registry+one#external@1");
        let context = Context::new(&roots, &metadata);
        assert!(
            std::panic::catch_unwind(|| context.executable(&json!("/original/target/../foreign")))
                .is_err()
        );
        assert!(
            std::panic::catch_unwind(|| context.executable(&json!("/foreign/native"))).is_err()
        );
    }
}
