use crate::{
    native_files::{self, Family, File, Run},
    parts::Quarter,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fs::{self, OpenOptions},
    io::Write,
    path::{Component, Path, PathBuf},
};

type Result<T> = std::result::Result<T, String>;

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Worker {
    family: Family,
    quarter: usize,
}
#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Roots {
    workspace: String,
    target: String,
    run: String,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Binding {
    path: String,
    bytes: u64,
    sha256: String,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Index {
    version: u32,
    worker: Worker,
    source: Value,
    recorded_roots: Roots,
    recorded_phases: String,
    files: BTreeMap<String, Binding>,
    native_credit_awarded: bool,
    complete_suite_qualified: bool,
}

pub(crate) fn command(args: &[String]) -> Result<()> {
    if args == ["--help"] {
        println!(
            "cargo xtask native-bundle --report <check.json> --data-dir <worker-data> --output <new-directory>\nCopies raw evidence and an unsigned integrity index. Awards no Native or Full credit."
        );
        return Ok(());
    }
    let mut options = BTreeMap::new();
    let mut args = args.iter();
    while let Some(flag) = args.next() {
        require(
            matches!(flag.as_str(), "--report" | "--data-dir" | "--output"),
            "unknown option",
        )?;
        let value = args.next().ok_or("missing option value")?;
        require(
            options.insert(flag.as_str(), Path::new(value)).is_none(),
            "duplicate option",
        )?;
    }
    let get = |key| {
        options
            .get(key)
            .copied()
            .ok_or_else(|| format!("missing {key}"))
    };
    export(get("--report")?, get("--data-dir")?, get("--output")?)?;
    Ok(())
}

fn require(condition: bool, reason: &str) -> Result<()> {
    if condition {
        Ok(())
    } else {
        Err(reason.to_owned())
    }
}
fn json(raw: &[u8]) -> Result<Value> {
    serde_json::from_slice(raw).map_err(|e| format!("invalid JSON: {e}"))
}
fn text<'a>(value: &'a Value, key: &str) -> Result<&'a str> {
    value[key]
        .as_str()
        .ok_or_else(|| format!("missing string {key}"))
}
fn quarter(value: usize) -> Result<Quarter> {
    match value {
        1 => Ok(Quarter::One),
        2 => Ok(Quarter::Two),
        3 => Ok(Quarter::Three),
        4 => Ok(Quarter::Four),
        _ => Err("invalid quarter".into()),
    }
}
fn absolute(value: &str) -> Result<&Path> {
    let path = Path::new(value);
    require(
        path.is_absolute()
            && path
                .components()
                .all(|c| matches!(c, Component::RootDir | Component::Normal(_))),
        "invalid recorded absolute path",
    )?;
    Ok(path)
}
fn recorded_reference(value: &str, workspace: &str) -> Result<PathBuf> {
    let path = Path::new(value);
    if path.is_absolute() {
        return Ok(absolute(value)?.to_owned());
    }
    require(
        !value.is_empty() && path.components().all(|c| matches!(c, Component::Normal(_))),
        "unsafe recorded relative path",
    )?;
    Ok(absolute(workspace)?.join(path))
}
fn phase_sibling(report: &Path) -> Result<PathBuf> {
    let stem = report
        .file_stem()
        .and_then(|s| s.to_str())
        .ok_or("invalid report filename")?;
    Ok(report.with_file_name(format!("{stem}-phases.json")))
}
fn digest(raw: &[u8]) -> String {
    format!("{:x}", Sha256::digest(raw))
}

fn regular_bytes(path: &Path) -> Result<Vec<u8>> {
    require(
        !path.components().any(|c| matches!(c, Component::ParentDir)),
        "unsafe local path",
    )?;
    for ancestor in path.ancestors().filter(|p| !p.as_os_str().is_empty()) {
        let metadata =
            fs::symlink_metadata(ancestor).map_err(|e| format!("{}: {e}", ancestor.display()))?;
        require(!metadata.file_type().is_symlink(), "symlink in local path")?;
        if ancestor == path {
            require(metadata.is_file(), "not a regular file")?;
        } else {
            require(metadata.is_dir(), "not a directory")?;
        }
    }
    fs::read(path).map_err(|e| format!("{}: {e}", path.display()))
}

fn envelope(wrapper: &Value, phase: &Value, metadata: &Value) -> Result<(Worker, Roots)> {
    require(
        wrapper["status"] == "passed"
            && wrapper["child_exit_code"] == 0
            && wrapper.get("reason") == Some(&Value::Null)
            && wrapper["budget_breached"] == false
            && wrapper.get("interrupted_signal") == Some(&Value::Null)
            && phase["status"] == "passed"
            && phase["selected_gates_status"] == "passed"
            && phase["exit_code"] == 0,
        "worker is not terminal and passed",
    )?;
    let family = match text(phase, "part_family")? {
        "native-libraries" => Family::Libraries,
        "native-remaining" => Family::Remaining,
        _ => return Err("not a Native worker".into()),
    };
    let key = if family == Family::Libraries {
        "library_quarter"
    } else {
        "target_quarter"
    };
    let q = phase[key]
        .as_u64()
        .and_then(|v| usize::try_from(v).ok())
        .ok_or("invalid quarter")?;
    quarter(q)?;
    require(phase[format!("{key}_count")] == 4, "invalid quarter count")?;
    let name = if family == Family::Libraries {
        "library"
    } else {
        "remaining"
    };
    let scope = format!("native-{name}-{q}");
    require(
        phase["coverage_scope"] == scope && wrapper["coverage_scope"] == scope,
        "scope binding",
    )?;
    let source = &phase["source"];
    for key in ["revision", "tree"] {
        let hash = text(source, key)?;
        require(
            hash.len() == 40 && hash.bytes().all(|c| c.is_ascii_hexdigit()),
            "invalid source identity",
        )?;
    }
    require(
        source["tracked_source_clean"] == true
            && wrapper["source_clean"] == true
            && wrapper["end_clean"] == true
            && wrapper["source_revision"] == source["revision"]
            && wrapper["end_revision"] == source["revision"],
        "source binding",
    )?;
    require(
        wrapper["qualified"] == false
            && phase["qualified"] == false
            && phase["full_native_suite_executed"] == false
            && phase["full_coverage_policy_executed"] == false,
        "qualification binding",
    )?;
    require(
        phase["processes_drained"] == true
            && phase["owned_postgres_run"] == true
            && phase["requires_owned_postgres"] == true
            && phase["postgres_demos"] == serde_json::json!([]),
        "awaiting Native finalization",
    )?;
    let start = wrapper["started_unix_seconds"]
        .as_f64()
        .ok_or("missing wrapper start")?;
    let end = wrapper["ended_unix_seconds"]
        .as_f64()
        .ok_or("missing wrapper end")?;
    let phase_start = phase["started_unix_seconds"]
        .as_f64()
        .ok_or("missing phase start")?;
    require(
        start.is_finite()
            && end.is_finite()
            && phase_start.is_finite()
            && start <= phase_start
            && phase_start <= end,
        "inconsistent start/end",
    )?;
    Ok((
        Worker { family, quarter: q },
        recorded_roots(phase, metadata)?,
    ))
}

fn recorded_roots(phase: &Value, metadata: &Value) -> Result<Roots> {
    let metrics = absolute(text(phase, "fixture_metrics")?)?;
    require(
        metrics.file_name() == Some(std::ffi::OsStr::new(&File::Fixtures.name())),
        "fixture path binding",
    )?;
    let roots = Roots {
        workspace: text(metadata, "workspace_root")?.into(),
        target: text(metadata, "target_directory")?.into(),
        run: metrics
            .parent()
            .ok_or("missing run root")?
            .to_str()
            .ok_or("non UTF-8 root")?
            .into(),
    };
    for root in [&roots.workspace, &roots.target, &roots.run] {
        absolute(root)?;
    }
    Ok(roots)
}

fn layout(worker: &Worker) -> Result<BTreeMap<String, String>> {
    let mut paths = BTreeMap::from([
        ("wrapper".into(), "reports/check.json".into()),
        ("phases".into(), "reports/check-phases.json".into()),
    ]);
    for file in native_files::files(worker.family, quarter(worker.quarter)?) {
        require(
            paths
                .insert(file.role().into(), format!("evidence/{}", file.name()))
                .is_none(),
            "duplicate file role",
        )?;
    }
    Ok(paths)
}

fn associate(
    values: &BTreeMap<String, Vec<u8>>,
    recorded_phases: &str,
) -> Result<(Worker, Roots, Value)> {
    let wrapper = json(&values["wrapper"])?;
    let phase = json(&values["phases"])?;
    let metadata = json(&values["metadata"])?;
    let (worker, roots) = envelope(&wrapper, &phase, &metadata)?;
    require(
        text(&wrapper, "phases_report")? == recorded_phases,
        "phase path binding",
    )?;
    let phase_path = absolute(recorded_phases)?;
    let recorded_wrapper = phase_path.with_file_name(
        phase_path
            .file_name()
            .and_then(|v| v.to_str())
            .and_then(|v| v.strip_suffix("-phases.json"))
            .map(|v| format!("{v}.json"))
            .ok_or("invalid recorded phase sibling")?,
    );
    require(
        phase_sibling(&recorded_wrapper)? == phase_path,
        "phase sibling binding",
    )?;
    let source = &phase["source"];
    let metadata_sha = digest(&values["metadata"]);
    let bind_record = |record: &Value| -> Result<()> {
        require(
            record["source"] == *source && record["metadata_sha256"] == metadata_sha,
            "record source/metadata binding",
        )
    };
    for role in ["compiler-journal", "migration-journal"] {
        let first = values[role]
            .split(|b| *b == b'\n')
            .next()
            .ok_or("empty journal")?;
        let header = json(first)?;
        require(
            header["kind"] == "header"
                && header["version"] == 1
                && header["workspace_root"] == roots.workspace,
            "journal root binding",
        )?;
        bind_record(&header)?;
    }
    for role in ["targets", "migration-owner"] {
        bind_record(&json(&values[role])?)?;
    }
    if worker.family == Family::Remaining {
        let plan = json(&values["target-plan"])?;
        bind_record(&plan["plan"])?;
        require(
            plan["plan"]["quarter"] == worker.quarter,
            "plan quarter binding",
        )?;
    }
    fixture_interval(&values["fixture-ledger"], &wrapper)?;
    let binaries = json(&values["binaries"])?;
    require(
        binaries["rust-build-meta"]["target-directory"] == roots.target,
        "binary target root binding",
    )?;
    let gates = phase["gates"].as_array().ok_or("missing gates")?;
    let runs = match worker.family {
        Family::Libraries if worker.quarter == 1 => vec![Run::LibraryQuarter, Run::LibraryRole],
        Family::Libraries => vec![Run::LibraryQuarter],
        Family::Remaining => vec![Run::Remaining],
    };
    for run in runs {
        let matching: Vec<_> = gates.iter().filter(|g| g["id"] == run.name()).collect();
        require(matching.len() == 1, "execution reference binding")?;
        let expected = run.path(Path::new(&roots.run));
        require(
            recorded_reference(text(matching[0], "execution_evidence")?, &roots.workspace)?
                == expected,
            "execution run root binding",
        )?;
    }
    let expected_metadata = File::Metadata.path(Path::new(&roots.run));
    let mut references = 0;
    for gate in gates {
        if let Some(argv) = gate["argv"].as_array() {
            for pair in argv.windows(2) {
                if pair[0] == "--cargo-metadata" {
                    let value = pair[1].as_str().ok_or("invalid metadata argument")?;
                    require(
                        recorded_reference(value, &roots.workspace)? == expected_metadata,
                        "metadata run root binding",
                    )?;
                    references += 1;
                }
            }
        }
    }
    require(references > 0, "missing metadata run reference")?;
    Ok((worker, roots, source.clone()))
}

fn fixture_interval(raw: &[u8], wrapper: &Value) -> Result<()> {
    let start = wrapper["started_unix_seconds"]
        .as_f64()
        .ok_or("missing start")?;
    let end = wrapper["ended_unix_seconds"]
        .as_f64()
        .ok_or("missing end")?;
    let mut setup = Vec::new();
    let mut cleanup = Vec::new();
    for line in raw.split(|b| *b == b'\n').filter(|line| !line.is_empty()) {
        let event = json(line)?;
        let nanos = event["unix_nanos"]
            .as_u64()
            .ok_or("invalid fixture timestamp")?;
        let seconds = std::time::Duration::from_nanos(nanos).as_secs_f64();
        require(
            start <= seconds && seconds <= end,
            "fixture interval binding",
        )?;
        match event["phase"].as_str() {
            Some("run_setup") => setup.push(text(&event, "identity")?.to_owned()),
            Some("run_cleanup") => cleanup.push(text(&event, "identity")?.to_owned()),
            _ => {}
        }
    }
    require(
        setup.len() == 1 && !setup[0].is_empty() && setup == cleanup,
        "fixture run binding",
    )
}

pub(crate) fn export(report: &Path, data: &Path, destination: &Path) -> Result<()> {
    let phase_path = phase_sibling(report)?;
    let mut values = BTreeMap::from([
        ("wrapper".into(), regular_bytes(report)?),
        ("phases".into(), regular_bytes(&phase_path)?),
        (
            "metadata".into(),
            regular_bytes(&File::Metadata.path(data))?,
        ),
    ]);
    let wrapper = json(&values["wrapper"])?;
    let recorded_phases = text(&wrapper, "phases_report")?.to_owned();
    require(
        absolute(&recorded_phases)?.file_name() == phase_path.file_name(),
        "local phase sibling binding",
    )?;
    let (worker, _) = envelope(
        &wrapper,
        &json(&values["phases"])?,
        &json(&values["metadata"])?,
    )?;
    for file in native_files::files(worker.family, quarter(worker.quarter)?) {
        values.insert(file.role().into(), regular_bytes(&file.path(data))?);
    }
    let (worker, recorded_roots, source) = associate(&values, &recorded_phases)?;
    let expected = layout(&worker)?;
    safe_output_parent(destination)?;
    fs::create_dir(destination)
        .map_err(|e| format!("new destination {}: {e}", destination.display()))?;
    let mut index = Index {
        version: 1,
        worker,
        source,
        recorded_roots,
        recorded_phases,
        files: BTreeMap::new(),
        native_credit_awarded: false,
        complete_suite_qualified: false,
    };
    for (role, relative) in expected {
        let bytes = &values[&role];
        let path = destination.join(&relative);
        fs::create_dir_all(path.parent().ok_or("missing output parent")?)
            .map_err(|e| e.to_string())?;
        write_new(&path, bytes)?;
        require(regular_bytes(&path)? == *bytes, "copy mismatch")?;
        index.files.insert(
            role,
            Binding {
                path: relative,
                bytes: bytes.len() as u64,
                sha256: digest(bytes),
            },
        );
    }
    let temporary = destination.join("bundle.json.incomplete");
    write_new(
        &temporary,
        &serde_json::to_vec_pretty(&index).map_err(|e| e.to_string())?,
    )?;
    fs::rename(&temporary, destination.join("bundle.json")).map_err(|e| e.to_string())?;
    load(destination)?;
    Ok(())
}
fn safe_output_parent(destination: &Path) -> Result<()> {
    require(
        !destination
            .components()
            .any(|c| matches!(c, Component::ParentDir)),
        "unsafe output path",
    )?;
    let parent = destination
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    for ancestor in parent.ancestors().filter(|p| !p.as_os_str().is_empty()) {
        let metadata = fs::symlink_metadata(ancestor).map_err(|e| e.to_string())?;
        require(
            metadata.is_dir() && !metadata.file_type().is_symlink(),
            "unsafe output parent",
        )?;
    }
    Ok(())
}
fn write_new(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|e| format!("{}: {e}", path.display()))?;
    file.write_all(bytes)
        .and_then(|()| file.sync_all())
        .map_err(|e| e.to_string())
}

pub(crate) fn load(directory: &Path) -> Result<Index> {
    let index: Index = serde_json::from_slice(&regular_bytes(&directory.join("bundle.json"))?)
        .map_err(|e| e.to_string())?;
    require(index.version == 1, "unsupported bundle version")?;
    require(
        !index.native_credit_awarded && !index.complete_suite_qualified,
        "false credit",
    )?;
    let expected = layout(&index.worker)?;
    require(index.files.keys().eq(expected.keys()), "file role set")?;
    let mut values = BTreeMap::new();
    for (role, binding) in &index.files {
        require(
            binding.path == expected[role]
                && Path::new(&binding.path)
                    .components()
                    .all(|c| matches!(c, Component::Normal(_))),
            "unsafe bundle path",
        )?;
        let raw = regular_bytes(&directory.join(&binding.path))?;
        require(
            raw.len() as u64 == binding.bytes && digest(&raw) == binding.sha256,
            "file digest mismatch",
        )?;
        values.insert(role.clone(), raw);
    }
    let mut actual = Vec::new();
    collect_files(directory, directory, &mut actual)?;
    actual.sort();
    let mut wanted: Vec<_> = expected
        .into_values()
        .chain(std::iter::once("bundle.json".into()))
        .collect();
    wanted.sort();
    require(actual == wanted, "unexpected bundle file")?;
    let (worker, roots, source) = associate(&values, &index.recorded_phases)?;
    require(
        worker == index.worker && roots == index.recorded_roots && source == index.source,
        "index identity/root binding",
    )?;
    Ok(index)
}
fn collect_files(root: &Path, directory: &Path, output: &mut Vec<String>) -> Result<()> {
    for entry in fs::read_dir(directory).map_err(|e| e.to_string())? {
        let entry = entry.map_err(|e| e.to_string())?;
        let kind = entry.file_type().map_err(|e| e.to_string())?;
        if kind.is_dir() {
            collect_files(root, &entry.path(), output)?;
        } else {
            require(kind.is_file(), "nonregular bundle entry")?;
            output.push(
                entry
                    .path()
                    .strip_prefix(root)
                    .map_err(|e| e.to_string())?
                    .to_str()
                    .ok_or("non UTF-8 bundle path")?
                    .into(),
            );
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    struct Snapshot {
        root: PathBuf,
        report: PathBuf,
        data: PathBuf,
    }
    impl Snapshot {
        fn new(family: Family, quarter: usize) -> Self {
            let root = std::env::temp_dir().join(format!(
                "native-bundle-{}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ));
            fs::create_dir(&root).unwrap();
            let report = root.join("archived.json");
            let data = root.join("downloaded-worker-data");
            fs::create_dir(&data).unwrap();
            let source = json!({"revision":"1111111111111111111111111111111111111111","tree":"2222222222222222222222222222222222222222","tracked_source_clean":true,"cargo":"archived cargo","rustc":"archived rustc"});
            let scope = format!(
                "native-{}-{quarter}",
                if family == Family::Libraries {
                    "library"
                } else {
                    "remaining"
                }
            );
            let mut phase = json!({"status":"passed","selected_gates_status":"passed","exit_code":0,"part_family":family,"coverage_scope":scope,"source":source,"qualified":false,"full_native_suite_executed":false,"full_coverage_policy_executed":false,"processes_drained":true,"owned_postgres_run":true,"requires_owned_postgres":true,"postgres_demos":[],"fixture_metrics":"/old/workspace/target/run/fixtures.jsonl","started_unix_seconds":2.0});
            let key = if family == Family::Libraries {
                "library_quarter"
            } else {
                "target_quarter"
            };
            phase[key] = json!(quarter);
            phase[format!("{key}_count")] = json!(4);
            let runs = if family == Family::Libraries {
                if quarter == 1 {
                    vec![Run::LibraryQuarter, Run::LibraryRole]
                } else {
                    vec![Run::LibraryQuarter]
                }
            } else {
                vec![Run::Remaining]
            };
            phase["gates"] = json!(runs.iter().map(|r| json!({"id":r.name(),"execution_evidence":format!("/old/workspace/target/run/{}",r.name()),"argv":["cargo","nextest","run","--cargo-metadata","target/run/cargo-metadata.json"]})).collect::<Vec<_>>());
            let wrapper = json!({"status":"passed","child_exit_code":0,"reason":null,"budget_breached":false,"interrupted_signal":null,"coverage_scope":scope,"source_revision":source["revision"],"end_revision":source["revision"],"source_clean":true,"end_clean":true,"qualified":false,"started_unix_seconds":1.0,"ended_unix_seconds":3.0,"phases_report":"/old/reports/archived-phases.json"});
            save(&report, &wrapper);
            save(&phase_sibling(&report).unwrap(), &phase);
            for file in native_files::files(family, super::quarter(quarter).unwrap()) {
                fs::write(
                    file.path(&data),
                    b"raw evidence; no semantic admission claimed\n",
                )
                .unwrap();
            }
            let metadata = json!({"workspace_root":"/old/workspace","target_directory":"/old/workspace/target"});
            save(&File::Metadata.path(&data), &metadata);
            let record = json!({"source":source,"metadata_sha256":digest(&fs::read(File::Metadata.path(&data)).unwrap())});
            fs::write(File::Fixtures.path(&data), b"{\"phase\":\"run_setup\",\"identity\":\"run\",\"unix_nanos\":2000000000}\n{\"phase\":\"run_cleanup\",\"identity\":\"run\",\"unix_nanos\":2500000000}\n").unwrap();
            for file in [File::Targets(family), File::Owner(family)] {
                save(&file.path(&data), &record);
            }
            let mut header = record.clone();
            header["kind"] = json!("header");
            header["version"] = json!(1);
            header["workspace_root"] = json!("/old/workspace");
            for file in [File::Compiler(family), File::OwnerJournal(family)] {
                fs::write(
                    file.path(&data),
                    format!("{}\n", serde_json::to_string(&header).unwrap()),
                )
                .unwrap();
            }
            save(
                &File::Binaries(family).path(&data),
                &json!({"rust-build-meta":{"target-directory":"/old/workspace/target"}}),
            );
            if family == Family::Remaining {
                let mut plan = record;
                plan["quarter"] = json!(quarter);
                save(&File::TargetPlan.path(&data), &json!({"plan":plan}));
            }
            Self { root, report, data }
        }
        fn export(&self) -> PathBuf {
            let out = self.root.join("bundle");
            export(&self.report, &self.data, &out).unwrap();
            out
        }
        fn mutate_phase(&self, key: &str, value: Value) {
            let path = phase_sibling(&self.report).unwrap();
            let mut phase = json(&fs::read(&path).unwrap()).unwrap();
            phase[key] = value;
            save(&path, &phase);
        }
    }
    impl Drop for Snapshot {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.root);
        }
    }
    fn save(path: &Path, value: &impl Serialize) {
        fs::write(path, serde_json::to_vec_pretty(value).unwrap()).unwrap();
    }
    fn edit_index(out: &Path, change: impl FnOnce(&mut Index)) {
        let path = out.join("bundle.json");
        let mut index: Index = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        change(&mut index);
        save(&path, &index);
    }

    #[test]
    fn raw_bundles_relocate_without_original_inputs_or_native_credit() {
        for family in [Family::Libraries, Family::Remaining] {
            for q in 1..=4 {
                let snapshot = Snapshot::new(family, q);
                let out = snapshot.export();
                let bytes =
                    fs::read(out.join("evidence").join(File::Compiler(family).name())).unwrap();
                let moved = snapshot.root.join("relocated");
                fs::rename(out, &moved).unwrap();
                fs::remove_dir_all(&snapshot.data).unwrap();
                fs::remove_file(&snapshot.report).unwrap();
                fs::remove_file(phase_sibling(&snapshot.report).unwrap()).unwrap();
                let index = load(&moved).unwrap();
                assert!(!index.native_credit_awarded && !index.complete_suite_qualified);
                assert_eq!(
                    index.source["revision"],
                    "1111111111111111111111111111111111111111"
                );
                assert_eq!(
                    fs::read(moved.join(&index.files["compiler-journal"].path)).unwrap(),
                    bytes
                );
                assert_eq!(
                    index.files.contains_key("role-cases"),
                    family == Family::Libraries && q == 1
                );
            }
        }
    }
    #[test]
    fn transport_rejects_tamper_missing_traversal_and_private_sidecars() {
        for mutation in [
            "tamper",
            "missing",
            "traversal",
            "missing-role",
            "private",
            "missing-index",
        ] {
            let snapshot = Snapshot::new(Family::Libraries, 2);
            let out = snapshot.export();
            match mutation {
                "tamper" => {
                    fs::write(out.join("evidence/cargo-metadata.json"), b"tampered").unwrap();
                }
                "missing" => fs::remove_file(out.join("evidence/cargo-metadata.json")).unwrap(),
                "traversal" => edit_index(&out, |index| {
                    index.files.get_mut("metadata").unwrap().path = "../cargo-metadata.json".into();
                }),
                "missing-role" => edit_index(&out, |index| {
                    index.files.remove("metadata");
                }),
                "private" => {
                    fs::write(out.join("private-compiler.stdout.log"), b"private").unwrap();
                }
                "missing-index" => fs::remove_file(out.join("bundle.json")).unwrap(),
                _ => unreachable!(),
            }
            assert!(load(&out).is_err(), "accepted {mutation}");
        }
    }
    #[test]
    fn terminal_source_and_run_associations_reject_substitution() {
        for (file, replacement, reason) in [
            (File::TargetPlan, json!({"plan":{"quarter":2}}), "quarter"),
            (
                File::Fixtures,
                json!({"phase":"run_setup","identity":"other","unix_nanos":4_000_000_000_u64}),
                "interval",
            ),
            (
                File::Fixtures,
                json!({"phase":"run_setup","identity":"other","unix_nanos":2_000_000_000_u64}),
                "run",
            ),
        ] {
            let snapshot = Snapshot::new(Family::Remaining, 1);
            let path = file.path(&snapshot.data);
            if matches!(file, File::TargetPlan) {
                let mut plan = json(&fs::read(&path).unwrap()).unwrap();
                plan["plan"]["quarter"] = replacement["plan"]["quarter"].clone();
                save(&path, &plan);
            } else {
                fs::write(&path, serde_json::to_vec(&replacement).unwrap()).unwrap();
            }
            let error = export(
                &snapshot.report,
                &snapshot.data,
                &snapshot.root.join("rejected"),
            )
            .err()
            .unwrap();
            assert!(error.contains(reason), "{error}");
        }
        for (key, value) in [
            ("status", json!("failed")),
            ("processes_drained", json!(false)),
            ("part_family", json!("quality-artifacts")),
            ("fixture_metrics", json!("/different/run/fixtures.jsonl")),
            (
                "source",
                json!({"revision":"3333333333333333333333333333333333333333","tree":"2222222222222222222222222222222222222222","tracked_source_clean":true}),
            ),
        ] {
            let snapshot = Snapshot::new(Family::Remaining, 1);
            snapshot.mutate_phase(key, value);
            let out = snapshot.root.join("rejected");
            assert!(
                export(&snapshot.report, &snapshot.data, &out).is_err(),
                "accepted {key}"
            );
            assert!(!out.exists());
        }
        let snapshot = Snapshot::new(Family::Libraries, 1);
        let out = snapshot.export();
        edit_index(&out, |index| {
            index.recorded_roots.workspace = "/substituted".into();
        });
        assert!(load(&out).is_err());
        assert!(export(&snapshot.report, &snapshot.data, &out).is_err());
        assert!(
            out.join("bundle.json").is_file(),
            "existing destination was damaged"
        );
    }
    #[test]
    fn recorded_metadata_arguments_bind_relative_and_absolute_paths_to_one_run() {
        for (reference, passes) in [
            ("target/run/cargo-metadata.json", true),
            ("/old/workspace/target/run/cargo-metadata.json", true),
            ("target/other-run/cargo-metadata.json", false),
            ("target/run/../run/cargo-metadata.json", false),
        ] {
            let snapshot = Snapshot::new(Family::Remaining, 2);
            let path = phase_sibling(&snapshot.report).unwrap();
            let mut phase = json(&fs::read(&path).unwrap()).unwrap();
            phase["gates"][0]["argv"][4] = json!(reference);
            save(&path, &phase);
            assert_eq!(
                export(&snapshot.report, &snapshot.data, &snapshot.root.join("out")).is_ok(),
                passes,
                "{reference}"
            );
        }
    }
    #[cfg(unix)]
    #[test]
    fn symlinked_evidence_and_output_parents_are_rejected() {
        use std::os::unix::fs::symlink;
        let snapshot = Snapshot::new(Family::Libraries, 2);
        let out = snapshot.export();
        let metadata = out.join("evidence/cargo-metadata.json");
        fs::remove_file(&metadata).unwrap();
        symlink(File::Metadata.path(&snapshot.data), &metadata).unwrap();
        assert!(load(&out).is_err());
        let alias = snapshot.root.join("alias");
        symlink(&snapshot.root, &alias).unwrap();
        assert!(export(&snapshot.report, &snapshot.data, &alias.join("escape")).is_err());
        assert!(!snapshot.root.join("escape").exists());
    }
}
