//! Safe, run-owned evidence from the existing native discovery build.
use serde_json::{Value, json};
use sha2::Digest;
use std::{
    collections::BTreeSet,
    fs::{self, File},
    io::{self, Read, Write},
    path::Path,
    process::Command,
};

const MAX_LINE: usize = 1024 * 1024;

pub(crate) struct Journal {
    file: File,
    members: BTreeSet<String>,
    pending: Vec<u8>,
    oversized: bool,
    invalid: bool,
}

impl Journal {
    pub(crate) fn create(path: &Path, command: &Command) -> io::Result<Self> {
        let directory = path
            .parent()
            .ok_or_else(|| io::Error::other("journal directory"))?;
        let metadata = fs::read(directory.join(crate::native_files::METADATA))?;
        let value: Value = serde_json::from_slice(&metadata)?;
        let members = value["workspace_members"]
            .as_array()
            .ok_or_else(|| io::Error::other("workspace members"))?
            .iter()
            .filter_map(Value::as_str)
            .map(str::to_owned)
            .collect();
        let mut journal = Self {
            file: File::options().write(true).create_new(true).open(path)?,
            members,
            pending: Vec::new(),
            oversized: false,
            invalid: false,
        };
        let argv: Vec<_> = std::iter::once(command.get_program())
            .chain(command.get_args())
            .map(|s| s.to_string_lossy().into_owned())
            .collect();
        let profile: serde_json::Map<_, _> = [
            "CARGO_BUILD_JOBS",
            "CARGO_INCREMENTAL",
            "CARGO_PROFILE_DEV_DEBUG",
            "CARGO_PROFILE_TEST_DEBUG",
            "CARGO_BUILD_TARGET",
            "CARGO_BUILD_BUILD_DIR",
        ]
        .into_iter()
        .map(|key| (key.to_owned(), json!(std::env::var(key).ok())))
        .collect();
        journal.write(&json!({
            "kind": "header", "version": 1, "workspace_root": crate::workspace(),
            "source": crate::timings::source_identity(), "argv": argv,
            "metadata_sha256": format!("{:x}", sha2::Sha256::digest(&metadata)),
            "profile": profile,
        }))?;
        Ok(journal)
    }

    fn write(&mut self, value: &Value) -> io::Result<()> {
        serde_json::to_writer(&mut self.file, value)?;
        self.file.write_all(b"\n")
    }

    fn line(&mut self) -> io::Result<()> {
        let line = std::mem::take(&mut self.pending);
        if self.oversized {
            self.invalid = true;
            self.oversized = false;
            return Ok(());
        }
        let value: Value = if let Ok(value) = serde_json::from_slice(&line) {
            value
        } else {
            // The final nextest object is pretty JSON; bare braces and
            // indented fields are not Cargo lines. A damaged compact
            // object must never authorize partial cache maintenance.
            if line.starts_with(b"{") && line.len() > 2 {
                self.invalid = true;
            }
            return Ok(());
        };
        if value["reason"] != "compiler-artifact"
            || !value["package_id"]
                .as_str()
                .is_some_and(|id| self.members.contains(id))
            || value["executable"].is_null()
        {
            return Ok(());
        }
        // Do not copy arbitrary nested objects, build-script envs, diagnostics
        // or raw stdout. Every persisted field is explicitly selected here.
        self.write(&json!({
            "kind": "artifact", "package_id": value["package_id"],
            "target": {"name": value["target"]["name"], "kind": value["target"]["kind"],
                       "src_path": value["target"]["src_path"]},
            "profile": {"opt_level": value["profile"]["opt_level"],
                        "debuginfo": value["profile"]["debuginfo"],
                        "debug_assertions": value["profile"]["debug_assertions"],
                        "overflow_checks": value["profile"]["overflow_checks"],
                        "test": value["profile"]["test"]},
            "features": value["features"], "executable": value["executable"],
            "fresh": value["fresh"],
        }))
    }

    pub(crate) fn read(mut self, mut stream: impl Read) -> io::Result<Vec<u8>> {
        let mut output = Vec::new();
        let mut chunk = [0; 8192];
        // Keep draining after a journal error so the child cannot deadlock on
        // a full pipe. The missing footer prevents maintenance using it.
        let mut error = None;
        loop {
            let count = stream.read(&mut chunk)?;
            if count == 0 {
                break;
            }
            output.extend_from_slice(&chunk[..count]);
            if error.is_some() {
                continue;
            }
            for &byte in &chunk[..count] {
                if !self.oversized {
                    self.pending.push(byte);
                    if self.pending.len() > MAX_LINE {
                        self.oversized = true;
                        self.pending.clear();
                    }
                }
                if byte == b'\n'
                    && let Err(failure) = self.line()
                {
                    error = Some(failure);
                    break;
                }
            }
        }
        if let Some(error) = error {
            return Err(error);
        }
        self.write(
            &json!({"kind": "reader-finished", "invalid": self.invalid || self.oversized,
                           "truncated_tail": !self.pending.is_empty()}),
        )?;
        Ok(output)
    }
}

#[cfg(feature = "postgres-check")]
pub(crate) fn validate_debuginfo(value: &Value) {
    assert!(
        value.as_u64().is_some_and(|level| level <= 2)
            || matches!(
                value.as_str(),
                Some("line-directives-only" | "line-tables-only")
            ),
        "unsupported Cargo debuginfo evidence"
    );
}

#[cfg(any(feature = "postgres-check", test))]
pub(crate) fn summary(bytes: &[u8]) -> Value {
    let mut result = None;
    for value in serde_json::Deserializer::from_slice(bytes).into_iter::<Value>() {
        let value = value.expect("valid Cargo/nextest JSON stream");
        assert!(
            result.is_none(),
            "the native summary must be the final object"
        );
        if value.get("rust-binaries").is_some() && value.get("rust-build-meta").is_some() {
            result = Some(value);
        } else {
            assert!(
                value.get("reason").and_then(Value::as_str).is_some(),
                "unexpected native build object"
            );
        }
    }
    result.expect("one complete canonical nextest binary summary")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        io::Cursor,
        path::PathBuf,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::{Duration, Instant, SystemTime, UNIX_EPOCH},
    };

    struct Fixture(PathBuf);
    impl Fixture {
        fn new() -> Self {
            let directory = std::env::temp_dir().join(format!(
                "crabber-native-artifacts-{}-{}",
                std::process::id(),
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ));
            fs::create_dir(&directory).unwrap();
            fs::write(
                directory.join(crate::native_files::METADATA),
                br#"{"workspace_members":["workspace"]}"#,
            )
            .unwrap();
            Self(directory)
        }
        fn path(&self) -> PathBuf {
            self.0.join("native-artifacts.jsonl")
        }
        fn journal(&self) -> Journal {
            Journal::create(&self.path(), &Command::new("fake-nextest")).unwrap()
        }
        fn records(&self) -> Vec<Value> {
            fs::read_to_string(self.path())
                .unwrap()
                .lines()
                .map(|line| serde_json::from_str(line).unwrap())
                .collect()
        }
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            fs::remove_dir_all(&self.0).unwrap();
        }
    }

    fn artifact() -> Value {
        json!({"reason": "compiler-artifact", "package_id": "workspace",
               "target": {"name": "unit", "kind": ["lib"], "src_path": "src/lib.rs"},
               "profile": {"opt_level": "0", "debuginfo": 0, "debug_assertions": true,
                           "overflow_checks": true, "test": true},
               "features": [], "executable": "unit-1111111111111111", "fresh": true})
    }

    #[test]
    fn summary_requires_one_complete_final_inventory() {
        let canonical = json!({"rust-binaries": {}, "rust-build-meta": {}});
        let prefix = format!("{}\n", artifact());
        assert_eq!(
            summary(format!("{prefix}{canonical}\n").as_bytes()),
            canonical
        );
        for invalid in [
            prefix.clone(),
            format!("{canonical}\n{canonical}"),
            format!("{canonical}\n{prefix}"),
            "{broken}\n".into(),
        ] {
            assert!(std::panic::catch_unwind(|| summary(invalid.as_bytes())).is_err());
        }
    }

    #[test]
    fn journal_filters_private_fields_and_never_records_a_truncated_artifact() {
        let fixture = Fixture::new();
        let mut event = artifact();
        event["target"]["private"] = json!("must-not-be-journaled");
        event["profile"]["private"] = json!("must-not-be-journaled");
        let input = format!(
            "{{\"reason\":\"build-script-executed\",\"envs\":[[\"PRIVATE\",\"must-not-be-journaled\"]]}}\n{event}\n{{\"reason\":\"compiler-message\",\"message\":\"must-not-be-journaled\"}}\n{{\"reason\":\"compiler-artifact\""
        );
        assert_eq!(
            fixture.journal().read(input.as_bytes()).unwrap(),
            input.as_bytes()
        );
        let records = fixture.records();
        assert_eq!(records.len(), 3);
        assert_eq!(records[1]["kind"], "artifact");
        assert_eq!(records[2]["truncated_tail"], true);
        assert_eq!(records[2]["invalid"], false);
        #[cfg(feature = "postgres-check")]
        {
            for value in [
                json!(0),
                json!(1),
                json!(2),
                json!("line-directives-only"),
                json!("line-tables-only"),
            ] {
                validate_debuginfo(&value);
            }
            for value in [
                Value::Null,
                json!(-1),
                json!(3),
                json!(0.0),
                json!(false),
                json!("unknown"),
                json!("1"),
                json!({}),
                json!([]),
            ] {
                assert!(std::panic::catch_unwind(|| validate_debuginfo(&value)).is_err());
            }
        }
        assert!(
            !fs::read_to_string(fixture.path())
                .unwrap()
                .contains("must-not-be-journaled")
        );
    }

    #[test]
    fn malformed_or_oversized_lines_invalidate_partial_maintenance() {
        let fixture = Fixture::new();
        let input = format!(
            "{{\"reason\":broken}}\n{}\n{}\n",
            "x".repeat(MAX_LINE + 1),
            artifact()
        );
        fixture.journal().read(input.as_bytes()).unwrap();
        assert_eq!(fixture.records().last().unwrap()["invalid"], true);
    }

    #[cfg(unix)]
    #[test]
    fn cancelled_build_drains_a_complete_artifact_and_reaps_the_child() {
        let fixture = Fixture::new();
        let ready = fixture.0.join("ready");
        let path = fixture.path();
        let signal = Arc::new(AtomicUsize::new(0));
        let mut command = Command::new("python3");
        command.args(["-I", "-S", "-c", "import pathlib,sys,time; print(sys.argv[1],flush=True); sys.stdout.write('{\"reason\":\"compiler-artifact\"'); sys.stdout.flush(); pathlib.Path(sys.argv[2]).touch(); time.sleep(30)", &artifact().to_string()]).arg(&ready);
        let worker_signal = Arc::clone(&signal);
        let start = Instant::now();
        std::thread::scope(|scope| {
            let worker = scope
                .spawn(move || crate::process::native_output(&mut command, &worker_signal, &path));
            while !ready.exists() && start.elapsed() < Duration::from_secs(5) {
                std::thread::sleep(Duration::from_millis(5));
            }
            signal.store(2, Ordering::SeqCst);
            let output = worker.join().unwrap().unwrap();
            assert!(!output.status.success());
            assert!(
                ready.exists(),
                "child reached its published output boundary"
            );
        });
        assert!(start.elapsed() < Duration::from_secs(10));
        let records = fixture.records();
        assert_eq!(records.len(), 3);
        assert_eq!(records[1]["kind"], "artifact");
        assert_eq!(records[2]["truncated_tail"], true);
        assert_eq!(records[2]["invalid"], false);
    }

    #[test]
    fn failed_journal_write_does_not_stop_draining_stdout() {
        struct Counted {
            input: Cursor<Vec<u8>>,
            bytes: Arc<AtomicUsize>,
        }
        impl Read for Counted {
            fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
                let count = self.input.read(buffer)?;
                self.bytes.fetch_add(count, Ordering::SeqCst);
                Ok(count)
            }
        }
        let fixture = Fixture::new();
        fs::write(fixture.path(), "").unwrap();
        let journal = Journal {
            file: File::open(fixture.path()).unwrap(),
            members: ["workspace".into()].into(),
            pending: Vec::new(),
            oversized: false,
            invalid: false,
        };
        let input = format!("{}\n{}", artifact(), "x".repeat(65536)).into_bytes();
        let expected = input.len();
        let bytes = Arc::new(AtomicUsize::new(0));
        assert!(
            journal
                .read(Counted {
                    input: Cursor::new(input),
                    bytes: Arc::clone(&bytes)
                })
                .is_err()
        );
        assert_eq!(bytes.load(Ordering::SeqCst), expected);
        assert!(
            !fs::read_to_string(fixture.path())
                .unwrap()
                .contains("reader-finished")
        );
    }
}
