//! Optional, run-owned timing events. Records contain identities, never URLs.

use super::{DatabaseMode, FixtureError, Result};
use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    time::{Duration, SystemTime},
};

pub const METRICS_FILE: &str = "CRABBER_TEST_POSTGRES_METRICS_FILE";

/// One append-only file shared explicitly with children of a fixture run.
pub struct FixtureMetrics(PathBuf);

impl FixtureMetrics {
    /// Create a new event file. Refuse reuse so failed records stay intact.
    /// # Errors
    /// Fails if the directory/file cannot be created or the file already exists.
    pub fn create(path: &Path) -> Result<Self> {
        if let Some(parent) = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            fs::create_dir_all(parent)
                .map_err(|_| FixtureError("cannot create fixture metrics directory"))?;
        }
        let mut options = OpenOptions::new();
        options.create_new(true).write(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        options
            .open(path)
            .map_err(|_| FixtureError("fixture metrics file must be new and writable"))?;
        Ok(Self(path.to_owned()))
    }

    pub(super) fn inherited() -> Option<Self> {
        std::env::var_os(METRICS_FILE).map(|path| Self(path.into()))
    }

    /// Append one sanitized event under an interprocess file lock.
    /// # Errors
    /// Fails for invalid labels or a failed event write.
    pub fn record(
        &self,
        phase: &str,
        identity: &str,
        mode: Option<DatabaseMode>,
        elapsed: Duration,
    ) -> Result<()> {
        if !phase
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte == b'_')
            || identity.len() > 128
            || !identity
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
        {
            return Err(FixtureError(
                "fixture metrics labels must be opaque identities",
            ));
        }
        let timestamp = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_err(|_| FixtureError("cannot timestamp fixture metrics"))?
            .as_nanos();
        let mode = match mode {
            Some(DatabaseMode::Current) => "current",
            Some(DatabaseMode::Blank) => "blank",
            None => "",
        };
        let record = format!(
            "{{\"phase\":\"{phase}\",\"identity\":\"{identity}\",\"mode\":\"{mode}\",\"unix_nanos\":{timestamp},\"seconds\":{:.9}}}\n",
            elapsed.as_secs_f64()
        );
        let mut file = OpenOptions::new()
            .append(true)
            .open(&self.0)
            .map_err(|_| FixtureError("cannot open fixture metrics file"))?;
        file.lock()
            .map_err(|_| FixtureError("cannot lock fixture metrics file"))?;
        file.write_all(record.as_bytes())
            .map_err(|_| FixtureError("cannot append fixture metrics event"))?;
        file.unlock()
            .map_err(|_| FixtureError("cannot release fixture metrics lock"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn concurrent_events_keep_records_and_reject_reused_output() {
        let directory =
            std::env::temp_dir().join(format!("crabber-metrics-{}", uuid::Uuid::new_v4()));
        let path = directory.join("events.jsonl");
        let metrics = Arc::new(FixtureMetrics::create(&path).unwrap());
        let threads: Vec<_> = (0..4)
            .map(|_| {
                let metrics = metrics.clone();
                std::thread::spawn(move || {
                    for _ in 0..10 {
                        metrics
                            .record(
                                "created",
                                "ct_fixture",
                                Some(DatabaseMode::Current),
                                Duration::from_millis(1),
                            )
                            .unwrap();
                    }
                })
            })
            .collect();
        for thread in threads {
            thread.join().unwrap();
        }
        assert!(FixtureMetrics::create(&path).is_err());
        let contents = fs::read_to_string(&path).unwrap();
        assert_eq!(contents.lines().count(), 40);
        for line in contents.lines() {
            assert!(line.starts_with("{\"phase\":\"created\",\"identity\":\"ct_fixture\""));
            assert!(line.ends_with("\"seconds\":0.001000000}"));
        }
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn invalid_labels_and_lost_event_files_fail_instead_of_silently_losing_metrics() {
        let directory =
            std::env::temp_dir().join(format!("crabber-metrics-{}", uuid::Uuid::new_v4()));
        let path = directory.join("events.jsonl");
        let metrics = FixtureMetrics::create(&path).unwrap();
        assert!(
            metrics
                .record("created", "not/an/identity", None, Duration::ZERO)
                .is_err()
        );
        assert_eq!(fs::metadata(&path).unwrap().len(), 0);
        fs::remove_file(path).unwrap();
        assert!(
            metrics
                .record("created", "ct_fixture", None, Duration::ZERO)
                .is_err()
        );
        fs::remove_dir_all(directory).unwrap();
    }
}
