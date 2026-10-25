//! Keep Session compilation in the combined graph; the parent owns the run.
use crabber_test_support::postgres::{FixtureMetrics, RUN_ID, RunFixture};
use serde_json::Value;
use std::{process::Output, sync::Arc, time::Duration};

const TEMPLATE_URL: &str = "CRABBER_TEST_POSTGRES_TEMPLATE_URL";

pub(crate) struct TemplateMigration {
    url: String,
    identity: String,
    metrics: Option<Arc<FixtureMetrics>>,
}

impl TemplateMigration {
    pub(super) fn new(run: &RunFixture, metrics: Option<Arc<FixtureMetrics>>) -> Self {
        Self {
            url: run.template_url(),
            identity: run.identity().to_owned(),
            metrics,
        }
    }

    pub(crate) fn schedule(gates: &mut crate::checks::Checks, migration: Option<Self>) {
        // normal-bins has produced this enabled executable. current_exe may
        // still refer to the running thin inode which Cargo just replaced.
        let mut command = crate::complete::binary_command("xtask");
        command.arg("postgres-template-migrate");
        if let Some(context) = &migration {
            command
                .env(TEMPLATE_URL, &context.url)
                .env(RUN_ID, &context.identity);
        } else {
            // --list registers the same gate without credentials or effects.
            command.env_remove(TEMPLATE_URL);
        }
        gates.captured(
            crate::workspace(),
            command,
            "run-owned template migration before native execution",
            move |output: &Output| {
                let context = migration.as_ref().expect("migration requires an owned run");
                let elapsed = response(&output.stdout, &context.identity)
                    .expect("invalid sanitized template migration response");
                if let Some(metrics) = &context.metrics {
                    metrics
                        .record("template_migration", &context.identity, None, elapsed)
                        .expect("persist template migration metric");
                }
            },
        );
    }
}

fn response(bytes: &[u8], identity: &str) -> Result<Duration, &'static str> {
    let value: Value = serde_json::from_slice(bytes).map_err(|_| "invalid migration response")?;
    let object = value.as_object().ok_or("invalid migration response")?;
    if object.len() != 3 || value["version"] != 1 || value["run"] != identity {
        return Err("migration response does not identify the owned run");
    }
    let nanos = value["elapsed_nanos"]
        .as_u64()
        .filter(|nanos| *nanos <= 600_000_000_000)
        .ok_or("invalid migration duration")?;
    Ok(Duration::from_nanos(nanos))
}

#[cfg(all(target_os = "linux", not(feature = "session-fixtures")))]
pub(crate) fn delegate(args: &[String]) -> Option<i32> {
    if !needs_owner(args) {
        return None;
    }
    let mut command = std::process::Command::new("cargo");
    command
        .current_dir(crate::workspace())
        .args([
            "run",
            "--quiet",
            "--package",
            "xtask",
            "--features",
            "session-fixtures",
            "--",
        ])
        .args(args);
    // This must happen before main creates signal/budget threads or resources.
    // Cargo replaces itself with the enabled owner, preserving PID and group.
    let _error = crate::process::replace_before_supervision(&mut command);
    eprintln!("cannot start Session-enabled fixture owner");
    Some(1)
}

#[cfg(any(test, all(target_os = "linux", not(feature = "session-fixtures"))))]
fn needs_owner(args: &[String]) -> bool {
    match args.first().map(String::as_str) {
        Some("check-postgres") => {
            super::Options::parse(&args[1..]).is_ok_and(|options| options.cleanup.is_none())
        }
        Some("postgres-fixture-helper") => {
            args.get(1).map(String::as_str) == Some("scoped-history")
        }
        _ => false,
    }
}

#[cfg(feature = "session-fixtures")]
pub(crate) fn migrate() -> i32 {
    let result = (|| -> Result<(), &'static str> {
        let url = std::env::var(TEMPLATE_URL).map_err(|_| "missing parent template endpoint")?;
        let identity = std::env::var(RUN_ID).map_err(|_| "missing parent run identity")?;
        validate_template(&url, &identity)?;
        crate::process::check_cancelled().map_err(|_| "template migration cancelled")?;
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .map_err(|_| "cannot create template migration runtime")?;
        let started = std::time::Instant::now();
        runtime
            .block_on(crabber_session::PostgresStore::migrate(&url))
            .map_err(|_| "cannot migrate run-owned PostgreSQL template")?;
        crate::process::check_cancelled().map_err(|_| "template migration cancelled")?;
        let nanos = u64::try_from(started.elapsed().as_nanos())
            .map_err(|_| "invalid template migration duration")?;
        println!(
            "{}",
            serde_json::json!({"version": 1, "run": identity, "elapsed_nanos": nanos})
        );
        Ok(())
    })();
    if let Err(error) = result {
        // Never format SQLx errors, URLs, the child environment or arguments.
        eprintln!("template migration: {error}");
        return 1;
    }
    0
}

#[cfg(feature = "session-fixtures")]
fn validate_template(url: &str, identity: &str) -> Result<(), &'static str> {
    use std::str::FromStr;
    if identity.len() != 32
        || !identity
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return Err("invalid parent run identity");
    }
    let options = sqlx::postgres::PgConnectOptions::from_str(url)
        .map_err(|_| "invalid parent template endpoint")?;
    if options.get_database() != Some(format!("ct_{identity}_template").as_str()) {
        return Err("template endpoint does not belong to the parent run");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn migration_response_requires_exact_owned_run_and_bounded_schema() {
        let valid = serde_json::json!({"version":1,"run":"owned","elapsed_nanos":123});
        assert_eq!(
            response(&serde_json::to_vec(&valid).unwrap(), "owned"),
            Ok(Duration::from_nanos(123))
        );
        for invalid in [
            serde_json::json!({"version":2,"run":"owned","elapsed_nanos":123}),
            serde_json::json!({"version":1,"run":"foreign","elapsed_nanos":123}),
            serde_json::json!({"version":1,"run":"owned","elapsed_nanos":-1}),
            serde_json::json!({"version":1,"run":"owned","elapsed_nanos":600_000_000_001_u64}),
            serde_json::json!({"version":1,"run":"owned","elapsed_nanos":123,"extra":"private-endpoint"}),
        ] {
            assert!(response(&serde_json::to_vec(&invalid).unwrap(), "owned").is_err());
        }
        assert!(response(b"not json", "owned").is_err());
    }

    #[test]
    fn only_valid_session_fixture_commands_replace_the_thin_process() {
        for (args, expected) in [
            (vec!["check-postgres"], true),
            (vec!["check-postgres", "--fixtures-only"], true),
            (vec!["check-postgres", "--cleanup-run", "owned"], false),
            (vec!["check-postgres", "--unknown"], false),
            (vec!["check"], false),
            (vec!["postgres-fixture-helper", "scoped-history"], true),
            (vec!["postgres-fixture-helper", "hold"], false),
        ] {
            let args: Vec<_> = args.into_iter().map(str::to_owned).collect();
            assert_eq!(needs_owner(&args), expected);
        }
    }

    #[cfg(feature = "session-fixtures")]
    #[test]
    fn template_endpoint_requires_the_exact_parent_template_without_connecting() {
        let identity = "0123456789abcdef0123456789abcdef";
        let url = format!("postgres://localhost/ct_{identity}_template");
        assert_eq!(validate_template(&url, identity), Ok(()));
        assert!(validate_template(&url, "../foreign").is_err());
        assert!(validate_template("postgres://localhost/foreign", identity).is_err());
        assert!(validate_template("invalid-endpoint", identity).is_err());
    }
}
