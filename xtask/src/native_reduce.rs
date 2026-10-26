use crate::{
    native_bundle::{LoadedBundle, Roots},
    native_files::Family,
    native_reduce_context::Context,
    native_test_evidence::Cases,
    parts::{Part, Quarter},
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    io::Write,
    path::{Component, Path},
    process::Command,
};

type Result<T> = std::result::Result<T, String>;

struct Worker {
    bundle: LoadedBundle,
    manifest: Value,
    executed: Cases,
    fixtures: usize,
}
impl Worker {
    fn json(&self, role: &str) -> Value {
        serde_json::from_slice(&self.bundle.files[role]).expect("bundle role JSON")
    }
}

pub(crate) fn command(args: &[String]) -> Result<()> {
    if args == ["--help"] {
        println!(
            "cargo xtask native-reduce --source <40-character-revision> --bundles <eight-bundle-directory> --output <new.json>\nValidates derived Native coverage. Does not qualify the complete suite or authenticate unsigned evidence."
        );
        return Ok(());
    }
    let mut options = BTreeMap::new();
    let mut args = args.iter();
    while let Some(flag) = args.next() {
        if !["--source", "--bundles", "--output"].contains(&flag.as_str()) {
            return Err(format!("unknown option {flag}"));
        }
        let value = args.next().ok_or("missing option value")?;
        if options.insert(flag.as_str(), value.as_str()).is_some() {
            return Err("duplicate option".into());
        }
    }
    let get = |key| {
        options
            .get(key)
            .copied()
            .ok_or_else(|| format!("missing {key}"))
    };
    let source = get("--source")?;
    if source.len() != 40 || !source.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err("source must be an exact commit revision".into());
    }
    let bundles = Path::new(get("--bundles")?);
    let output = Path::new(get("--output")?);
    let result = std::panic::catch_unwind(|| reduce(source, bundles)).map_err(|panic| {
        let message = panic
            .downcast_ref::<String>()
            .map(String::as_str)
            .or_else(|| panic.downcast_ref::<&str>().copied())
            .unwrap_or("invalid recorded evidence");
        format!("Native evidence rejected: {message}")
    })??;
    publish(output, &result)
}

fn git(source: &str, path: &str) -> Result<Vec<u8>> {
    let output = Command::new("git")
        .current_dir(crate::workspace())
        .args(["show", &format!("{source}:{path}")])
        .output()
        .map_err(|e| e.to_string())?;
    if !output.status.success() {
        return Err(format!("cannot read {path} at expected source"));
    }
    Ok(output.stdout)
}

fn reduce(source: &str, input: &Path) -> Result<Value> {
    let policy: Value = serde_json::from_slice(&git(source, "docs/test-coverage-policy.json")?)
        .map_err(|e| e.to_string())?;
    for file in [".config/nextest.toml", ".config/test-tools.json"] {
        assert_eq!(
            git(source, file)?,
            fs::read(crate::workspace().join(file)).map_err(|e| e.to_string())?,
            "archive uses an incompatible runner contract"
        );
    }
    let tree = Command::new("git")
        .current_dir(crate::workspace())
        .args(["rev-parse", "--verify", &format!("{source}^{{tree}}")])
        .output()
        .map_err(|e| e.to_string())?;
    if !tree.status.success() {
        return Err("expected source tree unavailable".into());
    }
    let tree = std::str::from_utf8(&tree.stdout)
        .map_err(|e| e.to_string())?
        .trim();
    let mut workers = BTreeMap::new();
    for entry in fs::read_dir(input).map_err(|e| e.to_string())? {
        let entry = entry.map_err(|e| e.to_string())?;
        assert!(
            entry.file_type().map_err(|e| e.to_string())?.is_dir(),
            "bundle input contains a sidecar or symlink"
        );
        let bundle = crate::native_bundle::load(&entry.path())?;
        assert_eq!(bundle.index.source["revision"], source);
        assert_eq!(bundle.index.source["tree"], tree);
        let slot = (bundle.index.worker.family, bundle.index.worker.quarter);
        let worker = admit(bundle);
        assert!(
            workers.insert(slot, worker).is_none(),
            "duplicate worker slot"
        );
    }
    Ok(close(&workers, &policy))
}

fn admit(bundle: LoadedBundle) -> Worker {
    let mut worker = Worker {
        bundle,
        manifest: Value::Null,
        executed: Cases::new(),
        fixtures: 0,
    };
    let metadata = worker.json("metadata");
    let context = Context::new(&worker.bundle.index.recorded_roots, &metadata);
    let phase = worker.json("phases");
    tools(&worker);
    let profile = schedule(&worker, &context, &phase);
    let compiler = crate::normal_outputs::journal_bytes(
        &worker.bundle.files["compiler-journal"],
        &worker.bundle.files["metadata"],
    );
    let migration = crate::normal_outputs::journal_bytes(
        &worker.bundle.files["migration-journal"],
        &worker.bundle.files["metadata"],
    );
    crate::normal_outputs::admit_owner(
        &metadata,
        &migration,
        &worker.json("migration-owner"),
        Path::new(&context.roots.target),
    );
    profile_controls(&compiler[0], &migration[0]);
    let binaries = worker.json("binaries");
    admit_platform(&binaries, &worker.bundle.index.source, &compiler[0]);
    bind_journal_commands(
        &phase,
        &compiler,
        &migration,
        worker.bundle.index.worker.family,
    );
    let targets = worker.json("targets");
    let rows = if worker.bundle.index.worker.family == Family::Libraries {
        library_build(&metadata, &binaries, &targets, &compiler)
    } else {
        remaining_build(&worker, &metadata, &binaries, &targets, &compiler)
    };
    for row in &rows {
        context.artifact(row);
    }
    bind_debug_controls(&rows, &compiler[0], ProducerProfile::Test);
    bind_debug_controls(
        &[worker.json("migration-owner")["artifact"].clone()],
        &migration[0],
        ProducerProfile::Dev,
    );
    let manifest = worker.json("unpartitioned-cases");
    crate::native_test_evidence::admit_manifest(&binaries, &manifest);
    crate::native_test_evidence::admit_packages(&metadata, &manifest);
    groups(&manifest, &worker.json("groups"));
    let selected = worker.json("selected-cases");
    if worker.bundle.index.worker.family == Family::Libraries {
        crate::native_library_evidence::admit_role(&metadata, &manifest);
        crate::native_library_evidence::admit_selection(&manifest, &selected, false);
    } else {
        assert_eq!(selected, manifest, "remaining selected cases changed");
    }
    let mut executed = execution(&worker, "", &selected);
    if worker.bundle.index.worker.family == Family::Libraries
        && worker.bundle.index.worker.quarter == 1
    {
        let role = worker.json("role-cases");
        crate::native_library_evidence::admit_selection(&manifest, &role, true);
        for (case, count) in execution(&worker, "role-", &role) {
            assert!(
                executed.insert(case, count).is_none(),
                "role overlaps quarter"
            );
        }
    }
    let fixtures = fixtures(
        &worker.bundle.files["fixture-ledger"],
        phase["postgres_transport"].as_str().unwrap(),
    );
    assert!(["complete", "complete-ci"].contains(&profile.as_str()));
    worker.manifest = manifest;
    worker.executed = executed;
    worker.fixtures = fixtures;
    worker
}

fn library_build(
    metadata: &Value,
    binaries: &Value,
    targets: &Value,
    journal: &[Value],
) -> Vec<Value> {
    let rows = targets["artifacts"].as_array().unwrap();
    let actual = crate::native_library_evidence::validate_build(metadata, binaries, rows);
    assert_eq!(
        &actual, rows,
        "library target record differs from admitted build"
    );
    assert_eq!(
        targets["declared_target_count"].as_u64().unwrap(),
        u64::try_from(actual.len()).unwrap()
    );
    journal_matches(journal, &actual);
    actual
}

fn remaining_build(
    worker: &Worker,
    metadata: &Value,
    binaries: &Value,
    targets: &Value,
    journal: &[Value],
) -> Vec<Value> {
    let plan = crate::native_remaining_evidence::Plan::recorded(
        metadata,
        Path::new(&worker.bundle.index.recorded_roots.workspace),
        quarter(worker.bundle.index.worker.quarter),
        &worker.bundle.files["metadata"],
        worker.bundle.index.source.clone(),
    );
    let saved = worker.json("target-plan");
    assert_eq!(
        serde_json::to_value(&plan).unwrap(),
        saved["plan"],
        "remaining plan is not metadata-derived"
    );
    let phase = worker.json("phases");
    let gate = phase["gates"]
        .as_array()
        .unwrap()
        .iter()
        .find(|g| g["id"] == "native-remaining-build")
        .unwrap();
    let template = gate["command_template"].as_array().unwrap();
    let mut command = Command::new(template[0].as_str().unwrap());
    for arg in &template[1..] {
        command.arg(arg.as_str().unwrap());
    }
    plan.append_arguments(&mut command);
    let argv: Vec<_> = std::iter::once(command.get_program())
        .chain(command.get_args())
        .map(|s| s.to_str().unwrap().to_owned())
        .collect();
    assert_eq!(json!(argv), saved["materialized_argv"]);
    assert_eq!(saved["materialized_argv"], journal[0]["argv"]);
    let input = crate::native_compiler_input::Input::retained(
        &worker.bundle.files["compiler-input"],
        binaries.clone(),
    );
    assert_eq!(plan.validate_build(metadata, &input, journal), *targets);
    let rows: Vec<_> = input
        .artifacts()
        .filter(|row| row.executable.is_some())
        .map(|row| serde_json::to_value(row).unwrap())
        .collect();
    journal_matches(journal, &rows);
    rows
}

fn journal_matches(journal: &[Value], artifacts: &[Value]) {
    let projection = |row: &Value| json!({"package_id":row["package_id"],"target":{"name":row["target"]["name"],"kind":row["target"]["kind"],"src_path":row["target"]["src_path"]},"profile":{"opt_level":row["profile"]["opt_level"],"debuginfo":row["profile"]["debuginfo"],"debug_assertions":row["profile"]["debug_assertions"],"overflow_checks":row["profile"]["overflow_checks"],"test":row["profile"]["test"]},"features":row["features"],"executable":row["executable"],"fresh":row["fresh"]});
    let saved: BTreeMap<_, _> = journal
        .iter()
        .filter(|row| row["kind"] == "artifact")
        .map(|row| (row["executable"].as_str().unwrap(), projection(row)))
        .collect();
    assert_eq!(
        saved.len(),
        journal.iter().filter(|r| r["kind"] == "artifact").count(),
        "duplicate journal artifact"
    );
    let mut actual = BTreeMap::new();
    for row in artifacts {
        assert!(
            actual
                .insert(row["executable"].as_str().unwrap(), projection(row))
                .is_none(),
            "duplicate typed artifact"
        );
    }
    assert_eq!(
        actual, saved,
        "journal and actual compiler projection differ"
    );
}

fn execution(worker: &Worker, prefix: &str, selection: &Value) -> Cases {
    let evidence = crate::native_test_evidence::execution(
        selection,
        &worker.bundle.files[&format!("{prefix}stdout")],
        &worker.bundle.files[&format!("{prefix}stderr")],
    );
    assert_eq!(
        worker.json(&format!("{prefix}execution")),
        evidence,
        "saved execution differs from actual runner output"
    );
    crate::native_test_evidence::selected(selection)
}

fn groups(manifest: &Value, groups: &Value) {
    crate::nextest::admit_groups(groups);
    assert_eq!(groups["test-count"], manifest["test-count"]);
    let suites = groups["rust-suites"].as_object().unwrap();
    assert_eq!(
        suites.keys().collect::<Vec<_>>(),
        manifest["rust-suites"]
            .as_object()
            .unwrap()
            .keys()
            .collect::<Vec<_>>()
    );
    for (id, suite) in suites {
        for field in [
            "binary-id",
            "binary-name",
            "binary-path",
            "build-platform",
            "kind",
            "package-id",
            "package-name",
            "status",
        ] {
            assert_eq!(suite[field], manifest["rust-suites"][id][field]);
        }
        let cases = suite["testcases"].as_object().unwrap();
        let original = manifest["rust-suites"][id]["testcases"]
            .as_object()
            .unwrap();
        assert_eq!(
            cases.keys().collect::<Vec<_>>(),
            original.keys().collect::<Vec<_>>()
        );
        for (name, case) in cases {
            assert_eq!(case["ignored"], original[name]["ignored"]);
            assert_ne!(
                case["filter-match"]["status"], "matches",
                "resource group violation"
            );
        }
    }
}

fn quarter(value: usize) -> Quarter {
    match value {
        1 => Quarter::One,
        2 => Quarter::Two,
        3 => Quarter::Three,
        4 => Quarter::Four,
        _ => panic!("invalid quarter"),
    }
}

fn publish(path: &Path, value: &Value) -> Result<()> {
    if path
        .components()
        .any(|part| matches!(part, Component::ParentDir))
    {
        return Err("unsafe output path".into());
    }
    for ancestor in path
        .parent()
        .ok_or("missing output parent")?
        .ancestors()
        .filter(|p| !p.as_os_str().is_empty())
    {
        let metadata = fs::symlink_metadata(ancestor).map_err(|e| e.to_string())?;
        if !metadata.is_dir() || metadata.file_type().is_symlink() {
            return Err("unsafe output parent".into());
        }
    }
    let raw = serde_json::to_vec_pretty(value).map_err(|e| e.to_string())?;
    let mut output = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|e| e.to_string())?;
    output
        .write_all(&raw)
        .and_then(|()| output.sync_all())
        .map_err(|e| e.to_string())
}

fn schedule(worker: &Worker, context: &Context<'_>, phase: &Value) -> String {
    let actual = phase["gates"].as_array().unwrap();
    let execution_id = if worker.bundle.index.worker.family == Family::Libraries {
        "native-library-quarter"
    } else {
        "native-remaining"
    };
    let execution = actual.iter().find(|g| g["id"] == execution_id).unwrap();
    let argv = execution["argv"].as_array().unwrap();
    let profiles: Vec<_> = argv
        .windows(2)
        .filter(|pair| pair[0] == "--profile")
        .map(|pair| pair[1].as_str().unwrap())
        .collect();
    assert_eq!(profiles.len(), 1);
    let profile = profiles[0];
    assert!(["complete", "complete-ci"].contains(&profile));
    let part = match worker.bundle.index.worker.family {
        Family::Libraries => Part::NativeLibraries(quarter(worker.bundle.index.worker.quarter)),
        Family::Remaining => Part::NativeRemaining(quarter(worker.bundle.index.worker.quarter)),
    };
    let directory = crate::workspace().join("target/test-performance/complete-reduce-expected");
    let expected = crate::complete::schedule_profile(
        vec![("CRABBER_REQUIRE_POSTGRES".into(), "1".into())],
        &directory,
        None,
        Some(part),
        profile,
    )
    .inventory();
    let roots = Roots {
        workspace: crate::workspace().to_str().unwrap().into(),
        target: current_target().to_str().unwrap().into(),
        run: directory.to_str().unwrap().into(),
    };
    let metadata = worker.json("metadata");
    let expected_context = Context::new(&roots, &json!({"workspace_members":[],"packages":[]}));
    let expected = expected.as_array().unwrap();
    assert_eq!(
        expected.len(),
        actual.len(),
        "selected gate schedule differs"
    );
    for (expected, actual) in expected.iter().zip(actual) {
        let mut expected = expected_context.gate(expected);
        let mut actual = context.gate(actual);
        assert_eq!(actual["status"], "passed");
        assert_eq!(actual["exit_code"], 0);
        assert!(
            actual["wall_seconds"]
                .as_f64()
                .is_some_and(|v| v.is_finite() && v >= 0.0)
        );
        let object = actual.as_object_mut().unwrap();
        for field in ["status", "exit_code", "wall_seconds"] {
            object.remove(field);
        }
        let command = !expected["argv"].as_array().unwrap().is_empty()
            || expected["command_template"]
                .as_array()
                .is_some_and(|args| !args.is_empty());
        let names: BTreeSet<_> = expected["environment_names"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap().to_owned())
            .chain(if command {
                MANAGED.iter().map(|s| (*s).to_owned()).collect::<Vec<_>>()
            } else {
                Vec::new()
            })
            .collect();
        expected["environment_names"] = json!(names);
        assert_eq!(
            expected, actual,
            "worker gate differs from current shared contract"
        );
    }
    assert_eq!(metadata["target_directory"], context.roots.target);
    profile.to_owned()
}

const MANAGED: [&str; 9] = [
    "CARGO_TARGET_DIR",
    "CRABBER_TEST_RUN_POSTGRES_ADMIN_URL",
    "CRABBER_TEST_POSTGRES_RUN",
    "CRABBER_TEST_POSTGRES_METRICS_FILE",
    "CRABBER_TEST_POSTGRES_URL",
    "CRABBER_POSTGRES_URL",
    "CRABBER_TEST_POSTGRES_CHILD_DATABASE",
    "CRABBER_TEST_POSTGRES_CHILD_ROLE",
    "CRABBER_TEST_HISTORY_CHILD",
];

fn current_target() -> std::path::PathBuf {
    let root = std::env::var_os("CARGO_TARGET_DIR").map_or_else(
        || crate::workspace().join("target"),
        std::path::PathBuf::from,
    );
    if root.is_absolute() {
        root
    } else {
        crate::workspace().join(root)
    }
}

fn profile_controls(compiler: &Value, migration: &Value) {
    for key in [
        "CARGO_PROFILE_DEV_DEBUG",
        "CARGO_PROFILE_TEST_DEBUG",
        "CARGO_BUILD_TARGET",
        "CARGO_BUILD_BUILD_DIR",
    ] {
        assert_eq!(
            compiler["profile"][key], migration["profile"][key],
            "owner/compiler profile controls differ"
        );
    }
}

#[derive(Clone, Copy)]
enum ProducerProfile {
    Test,
    Dev,
}

fn bind_debug_controls(rows: &[Value], header: &Value, producer: ProducerProfile) {
    let key = match producer {
        ProducerProfile::Test => "CARGO_PROFILE_TEST_DEBUG",
        ProducerProfile::Dev => "CARGO_PROFILE_DEV_DEBUG",
    };
    for row in rows {
        if let Some(value) = header["profile"][key].as_str() {
            let expected = match value {
                "0" | "none" => json!(0),
                "1" | "limited" => json!(1),
                "2" | "full" => json!(2),
                "line-directives-only" | "line-tables-only" => json!(value),
                _ => panic!("unsupported recorded Cargo profile control"),
            };
            assert_eq!(
                row["profile"]["debuginfo"], expected,
                "compiler artifact differs from its role's profile control"
            );
        }
    }
}

fn owned_lifecycle(
    lifecycle: &BTreeMap<String, Vec<(usize, String)>>,
    transport: &str,
) -> Vec<usize> {
    let mut ordered = Vec::new();
    for phase in [
        "server_setup",
        "run_setup",
        "template_migration",
        "run_cleanup",
        "server_cleanup",
        "gate_passed",
    ] {
        let entries = lifecycle
            .get(phase)
            .expect("missing fixture lifecycle event");
        assert_eq!(entries.len(), 1, "duplicate lifecycle event");
        ordered.push(entries[0].0);
    }
    assert!(
        ordered.windows(2).all(|pair| pair[0] < pair[1]),
        "fixture lifecycle order"
    );
    let transport = &lifecycle[&format!("server_transport_{transport}")];
    assert_eq!(transport.len(), 1);
    assert_eq!(transport[0].1, "runner");
    assert!(ordered[0] < transport[0].0 && transport[0].0 < ordered[1]);
    let run = &lifecycle["run_setup"][0].1;
    assert_eq!(&lifecycle["template_migration"][0].1, run);
    assert_eq!(&lifecycle["run_cleanup"][0].1, run);
    for phase in ["server_setup", "server_cleanup", "gate_passed"] {
        assert_eq!(lifecycle[phase][0].1, "runner");
    }
    ordered
}

fn fixtures(raw: &[u8], transport: &str) -> usize {
    let rows: Vec<Value> = std::str::from_utf8(raw)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    let mut lifecycle = BTreeMap::<String, Vec<(usize, String)>>::new();
    let mut created = BTreeMap::new();
    let mut finished = BTreeMap::new();
    for (index, row) in rows.iter().enumerate() {
        assert!(
            row["seconds"]
                .as_f64()
                .is_some_and(|v| v.is_finite() && v >= 0.0)
        );
        let phase = row["phase"].as_str().unwrap();
        let identity = row["identity"].as_str().unwrap();
        let mode = row["mode"].as_str().unwrap();
        assert_ne!(identity, "");
        match phase {
            "created" => {
                assert!(["current", "blank"].contains(&mode));
                assert!(
                    created.insert(identity.to_owned(), index).is_none(),
                    "duplicate fixture creation"
                );
            }
            "finished" => {
                assert_eq!(mode, "");
                assert!(
                    finished.insert(identity.to_owned(), index).is_none(),
                    "duplicate fixture finish"
                );
            }
            "server_setup" | "run_setup" | "template_migration" | "run_cleanup"
            | "server_cleanup" | "gate_passed" => {
                assert_eq!(mode, "");
                lifecycle
                    .entry(phase.into())
                    .or_default()
                    .push((index, identity.into()));
            }
            value if value == format!("server_transport_{transport}") => {
                assert_eq!(mode, "");
                lifecycle
                    .entry(value.into())
                    .or_default()
                    .push((index, identity.into()));
            }
            _ => panic!("unexpected or failed fixture event {phase}"),
        }
    }
    let ordered = owned_lifecycle(&lifecycle, transport);
    let run = &lifecycle["run_setup"][0].1;
    assert_eq!(
        created.keys().collect::<Vec<_>>(),
        finished.keys().collect::<Vec<_>>(),
        "fixture cleanup incomplete"
    );
    for (id, start) in &created {
        assert!(
            id.starts_with(&format!("ct_{run}_")),
            "fixture belongs to a different run"
        );
        let end = finished[id];
        assert!(
            ordered[2] < *start && *start < end && end < ordered[3],
            "fixture outside migrated owned lifecycle"
        );
        let nanos = |index: usize| rows[index]["unix_nanos"].as_u64().unwrap();
        assert!(
            nanos(ordered[2]) <= nanos(*start)
                && nanos(*start) <= nanos(end)
                && nanos(end) <= nanos(ordered[3]),
            "fixture timestamp outside owned lifecycle"
        );
    }
    created.len()
}

fn close(workers: &BTreeMap<(Family, usize), Worker>, policy: &Value) -> Value {
    let expected: BTreeSet<_> = [Family::Libraries, Family::Remaining]
        .into_iter()
        .flat_map(|family| (1..=4).map(move |q| (family, q)))
        .collect();
    assert_eq!(
        workers.keys().copied().collect::<BTreeSet<_>>(),
        expected,
        "all eight worker slots required"
    );
    let first = &workers[&(Family::Libraries, 1)];
    let metadata = first.json("metadata");
    let context = Context::new(&first.bundle.index.recorded_roots, &metadata);
    let canonical = context.metadata(&metadata);
    let library = context.suites(&first.manifest);
    let mut suites = library.as_object().unwrap().clone();
    let mut actual = Cases::new();
    let mut compilation = BTreeMap::new();
    let mut provenance = Vec::new();
    let controls = compiler_controls(first);
    for ((family, q), worker) in workers {
        let metadata = worker.json("metadata");
        let context = Context::new(&worker.bundle.index.recorded_roots, &metadata);
        assert_eq!(
            worker.bundle.index.source, first.bundle.index.source,
            "source/toolchain/host architecture differs"
        );
        assert_eq!(
            context.metadata(&metadata),
            canonical,
            "resolved metadata graph differs"
        );
        assert_eq!(
            compiler_controls(worker),
            controls,
            "recorded profile controls differ"
        );
        assert_eq!(
            platform_context(&worker.json("binaries")),
            platform_context(&first.json("binaries")),
            "native target architecture differs"
        );
        for role in ["rustc-version", "docker-version", "wasm-tools-version"] {
            assert_eq!(
                worker.bundle.files[role], first.bundle.files[role],
                "tool version evidence differs"
            );
        }
        let manifest = context.suites(&worker.manifest);
        if *family == Family::Libraries {
            assert_eq!(manifest, library, "library inventories differ");
        } else {
            for (id, suite) in manifest.as_object().unwrap() {
                assert!(
                    suites.insert(id.clone(), suite.clone()).is_none(),
                    "Native target domains overlap"
                );
            }
        }
        for (case, count) in &worker.executed {
            assert_eq!(*count, 1);
            assert!(
                actual.insert(case.clone(), *count).is_none(),
                "Native execution domains overlap"
            );
        }
        let targets = worker.json("targets");
        let artifacts = if *family == Family::Libraries {
            &targets["artifacts"]
        } else {
            &targets["test_artifacts"]
        };
        for artifact in artifacts.as_array().unwrap() {
            let stable = context.artifact(artifact);
            let key = serde_json::to_string(&json!([stable["package"], stable["target"]])).unwrap();
            if let Some(previous) = compilation.insert(key, stable.clone()) {
                assert_eq!(previous, stable, "cross-worker compiler context differs");
                assert_eq!(
                    *family,
                    Family::Libraries,
                    "remaining compiled targets overlap"
                );
            }
        }
        provenance.push(json!({"family":family,"quarter":q,"active_cases":worker.executed.len(),"fixtures":worker.fixtures,"metadata_sha256":format!("{:x}",Sha256::digest(&worker.bundle.files["metadata"]))}));
    }
    let count: usize = suites
        .values()
        .map(|suite| suite["testcases"].as_object().unwrap().len())
        .sum();
    let manifest = json!({"test-count":count,"rust-suites":suites});
    assert_eq!(
        actual,
        crate::native_test_evidence::selected(&manifest),
        "Native executions do not close the derived case domain"
    );
    crate::native_coverage::validate(&metadata, &manifest, policy);
    let ignored = count.checked_sub(actual.len()).unwrap();
    json!({"version":1,"kind":"derived-native-validation","source":first.bundle.index.source,"native_coverage_validated":true,"complete_suite_qualified":false,"full_check_executed":false,"manifest":manifest,"actual_cases":actual.into_iter().map(|((binary,case),count)|json!([binary,case,count])).collect::<Vec<_>>(),"ignored_cases":ignored,"compiled_test_targets":compilation.len(),"workers":provenance,"assurance":{"transport":"unsigned integrity and recorded-envelope consistency","test_artifacts":"compiler identities; executable bytes were not hashed","runtime":"source-bound worker finalizer and command-policy attestations; no remote live queries","schedule":"current shared consumer contract; incompatible archived contracts rejected","environment":"names and trusted source command assembly; values not independently observed"}})
}

fn compiler_controls(worker: &Worker) -> Value {
    let rows = crate::normal_outputs::journal_bytes(
        &worker.bundle.files["compiler-journal"],
        &worker.bundle.files["metadata"],
    );
    let mut controls = serde_json::Map::new();
    for key in [
        "CARGO_PROFILE_DEV_DEBUG",
        "CARGO_PROFILE_TEST_DEBUG",
        "CARGO_BUILD_TARGET",
        "CARGO_BUILD_BUILD_DIR",
    ] {
        controls.insert(key.into(), rows[0]["profile"][key].clone());
    }
    Value::Object(controls)
}

fn bind_journal_commands(phase: &Value, compiler: &[Value], migration: &[Value], family: Family) {
    let gates = phase["gates"].as_array().unwrap();
    let prefix = if family == Family::Libraries {
        "native-library"
    } else {
        "native-remaining"
    };
    let owner = gates
        .iter()
        .find(|g| g["id"] == format!("{prefix}-migration-owner"))
        .unwrap();
    assert_eq!(
        owner["argv"], migration[0]["argv"],
        "owner journal command differs from executed gate"
    );
    if family == Family::Libraries {
        let build = gates
            .iter()
            .find(|g| g["id"] == "native-library-build")
            .unwrap();
        assert_eq!(
            build["argv"], compiler[0]["argv"],
            "library compiler journal command differs from executed gate"
        );
    }
}

fn platform_context(binaries: &Value) -> Value {
    let meta = &binaries["rust-build-meta"];
    let host = &meta["platforms"]["host"]["platform"];
    let targets: Vec<_> = meta["platforms"]["targets"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v["platform"].clone())
        .collect();
    json!({"host":host,"targets":targets,"target-platform":meta["target-platform"],"target-platforms":meta["target-platforms"]})
}

fn admit_platform(binaries: &Value, source: &Value, header: &Value) {
    let rustc = source["rustc"].as_str().unwrap();
    let hosts: Vec<_> = rustc
        .lines()
        .filter_map(|line| line.strip_prefix("host: "))
        .collect();
    assert_eq!(hosts.len(), 1, "missing compiler host architecture");
    let context = platform_context(binaries);
    assert_eq!(context["host"]["triple"], hosts[0]);
    let target = header["profile"]["CARGO_BUILD_TARGET"]
        .as_str()
        .unwrap_or(hosts[0]);
    assert!(
        !target.contains('/') && !target.contains('\\'),
        "custom target specs require an additional source binding"
    );
    let targets = context["target-platforms"].as_array().unwrap();
    assert_eq!(targets.len(), 1, "one recorded target architecture");
    assert_eq!(
        targets[0]["triple"], target,
        "compiler and nextest target architecture differ"
    );
}

fn tools(worker: &Worker) {
    let pins: Value = serde_json::from_str(include_str!("../../.config/test-tools.json")).unwrap();
    for (program, pin) in [
        (
            "rustc",
            Some(format!(
                "rustc {}",
                pins["rust"]["version"].as_str().unwrap()
            )),
        ),
        (
            "wasm-tools",
            Some(format!(
                "wasm-tools {}",
                pins["wasm-tools"]["version"].as_str().unwrap()
            )),
        ),
        ("docker", None),
    ] {
        let raw = std::str::from_utf8(&worker.bundle.files[&format!("{program}-version")]).unwrap();
        crate::complete::admit_tool_version(raw, pin.as_deref());
    }
    let source = &worker.bundle.index.source;
    assert!(source["cargo"].as_str().unwrap().starts_with("cargo "));
    assert_eq!(
        std::str::from_utf8(&worker.bundle.files["rustc-version"])
            .unwrap()
            .trim(),
        source["rustc"].as_str().unwrap().lines().next().unwrap(),
        "source and recorded rustc differ"
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    fn ledger() -> Vec<Value> {
        [("server_setup","runner",""),("server_transport_bridge","runner",""),("run_setup","run",""),("template_migration","run",""),("created","ct_run_child","current"),("finished","ct_run_child",""),("run_cleanup","run",""),("server_cleanup","runner",""),("gate_passed","runner","")].into_iter().enumerate().map(|(index,(phase,identity,mode))| json!({"phase":phase,"identity":identity,"mode":mode,"seconds":0.0,"unix_nanos":u64::try_from(index+1).unwrap()*1_000_000_000})).collect()
    }
    fn bytes(rows: &[Value]) -> Vec<u8> {
        rows.iter()
            .map(|row| serde_json::to_string(row).unwrap())
            .collect::<Vec<_>>()
            .join("\n")
            .into_bytes()
    }
    #[test]
    fn fixture_admission_requires_balanced_unique_migrated_owned_lifecycle() {
        let rows = ledger();
        assert_eq!(fixtures(&bytes(&rows), "bridge"), 1);
        let mut missing = rows.clone();
        missing.remove(5);
        assert!(std::panic::catch_unwind(|| fixtures(&bytes(&missing), "bridge")).is_err());
        let mut duplicate = rows.clone();
        duplicate.insert(7, rows[6].clone());
        assert!(std::panic::catch_unwind(|| fixtures(&bytes(&duplicate), "bridge")).is_err());
        let mut foreign = rows;
        foreign[4]["identity"] = json!("ct_foreign_child");
        foreign[5]["identity"] = json!("ct_foreign_child");
        assert!(std::panic::catch_unwind(|| fixtures(&bytes(&foreign), "bridge")).is_err());
    }
    #[test]
    fn debug_controls_preserve_distinct_test_and_normal_owner_roles() {
        let header =
            json!({"profile":{"CARGO_PROFILE_DEV_DEBUG":"1","CARGO_PROFILE_TEST_DEBUG":"0"}});
        let rows = vec![
            json!({"profile":{"test":true,"debuginfo":0}}),
            json!({"profile":{"test":false,"debuginfo":0}}),
        ];
        bind_debug_controls(&rows, &header, ProducerProfile::Test);
        let mut owner = vec![json!({"profile":{"test":false,"debuginfo":1}})];
        bind_debug_controls(&owner, &header, ProducerProfile::Dev);
        for index in 0..rows.len() {
            let mut invalid = rows.clone();
            invalid[index]["profile"]["debuginfo"] = json!(1);
            assert!(
                std::panic::catch_unwind(|| bind_debug_controls(
                    &invalid,
                    &header,
                    ProducerProfile::Test
                ))
                .is_err()
            );
        }
        owner[0]["profile"]["debuginfo"] = json!(0);
        assert!(
            std::panic::catch_unwind(|| bind_debug_controls(&owner, &header, ProducerProfile::Dev))
                .is_err()
        );
    }
}
