//! Run-owned PostgreSQL prerequisite and the required CI obligation schedule.

use std::{
    env,
    path::{Path, PathBuf},
    process::Command,
    time::Instant,
};

use crabber_test_support::postgres::{
    DatabaseMode, FixtureMetrics, METRICS_FILE, RunFixture, TestDatabase,
};
use futures_util::FutureExt;

pub(crate) mod child;
mod container;
mod fixtures;
use container::{ADMIN_OVERRIDE, Endpoint};
mod arguments;
mod schedule;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Runner {
    Cargo,
    Nextest,
}

#[derive(Default)]
struct Options {
    package: Option<String>,
    filter: Option<String>,
    target: Option<String>,
    fixtures_only: bool,
    cleanup: Option<String>,
    test_threads: Option<String>,
    runner: Option<Runner>,
    fixture_metrics: Option<PathBuf>,
}
impl Options {
    fn parse(args: &[String]) -> Result<Self, &'static str> {
        let mut options = Self::default();
        let mut args = args.iter();
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--fixture-metrics" if options.fixture_metrics.is_none() => {
                    options.fixture_metrics = Some(PathBuf::from(
                        args.next()
                            .ok_or("--fixture-metrics needs a new event-file path")?,
                    ));
                }
                "--runner" if options.runner.is_none() => {
                    options.runner = Some(match args.next().map(String::as_str) {
                        Some("cargo") => Runner::Cargo,
                        Some("nextest") => Runner::Nextest,
                        _ => return Err("--runner needs cargo or nextest"),
                    });
                }
                "--package" if options.package.is_none() => {
                    options.package = Some(args.next().ok_or("--package needs a name")?.clone());
                }
                "--filter" if options.filter.is_none() => {
                    options.filter = Some(args.next().ok_or("--filter needs a substring")?.clone());
                }
                "--test" if options.target.is_none() => {
                    options.target = Some(args.next().ok_or("--test needs a target")?.clone());
                }
                "--test-threads" if options.test_threads.is_none() => {
                    let threads = args.next().ok_or("--test-threads needs 1, 2 or 4")?;
                    if !matches!(threads.as_str(), "1" | "2" | "4") {
                        return Err("--test-threads needs 1, 2 or 4");
                    }
                    options.test_threads = Some(threads.clone());
                }
                "--cleanup-run" if options.cleanup.is_none() => {
                    options.cleanup =
                        Some(args.next().ok_or("--cleanup-run needs a nonce")?.clone());
                }
                "--fixtures-only" if !options.fixtures_only => options.fixtures_only = true,
                _ => return Err("unexpected check-postgres argument"),
            }
        }
        if (options.cleanup.is_some() || options.fixtures_only)
            && (options.package.is_some() || options.filter.is_some() || options.target.is_some())
        {
            return Err("cleanup/fixture qualification cannot be combined with test selection");
        }
        if options.cleanup.is_some() && options.fixtures_only {
            return Err("select cleanup or fixture qualification");
        }
        if let Some(package) = &options.package
            && !schedule::COMMANDS.iter().any(|command| {
                command
                    .windows(2)
                    .any(|pair| pair[0] == "-p" && pair[1] == package)
            })
        {
            return Err("package has no required PostgreSQL coverage");
        }
        if let Some(target) = &options.target {
            let package = options
                .package
                .as_ref()
                .ok_or("--test requires --package")?;
            if !schedule::COMMANDS.iter().any(|command| {
                command
                    .windows(2)
                    .any(|pair| pair[0] == "-p" && pair[1] == package)
                    && command
                        .windows(2)
                        .any(|pair| pair[0] == "--test" && pair[1] == target)
            }) {
                return Err("test target is not listed as a required PostgreSQL command");
            }
        }
        Ok(options)
    }
}

pub(crate) fn check(args: &[String]) -> i32 {
    let options = match Options::parse(args) {
        Ok(options) => options,
        Err(error) => {
            eprintln!("{error}");
            return 2;
        }
    };
    if !cfg!(target_os = "linux") {
        eprintln!("managed PostgreSQL qualification currently requires Linux process supervision");
        return 1;
    }
    let Ok(runtime) = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
    else {
        eprintln!("cannot create PostgreSQL fixture runtime");
        return 1;
    };
    match runtime.block_on(run(options)) {
        Ok(code) => code,
        Err(error) => {
            eprintln!("PostgreSQL gate: {error}");
            1
        }
    }
}

async fn run(options: Options) -> Result<i32, Box<dyn std::error::Error>> {
    let started = Instant::now();
    let signal = crate::process::cancellation()?;
    if let Some(nonce) = options.cleanup {
        let admin = env::var(ADMIN_OVERRIDE).map_err(|_| "--cleanup-run requires a dedicated administrative endpoint in CRABBER_TEST_POSTGRES_ADMIN_URL")?;
        RunFixture::cleanup_run(&admin, &nonce).await?;
        println!("cleaned inactive PostgreSQL fixture run {nonce}");
        return Ok(0);
    }
    if !options.fixtures_only && options.runner != Some(Runner::Cargo) {
        let mut version = crate::checks::Checks::default();
        crate::nextest::version_gate(crate::workspace(), &mut version);
        let code = version.execute(&crate::checks::Options::default());
        if code != 0 {
            return Ok(code);
        }
    }
    let metrics_path = options.fixture_metrics.as_ref().map(|path| {
        if path.is_absolute() {
            path.clone()
        } else {
            crate::workspace().join(path)
        }
    });
    let metrics = metrics_path
        .as_deref()
        .map(FixtureMetrics::create)
        .transpose()?;
    let endpoint = Endpoint::start().await?;
    let admin = &endpoint.admin;
    let result = async {
        if let Some(metrics) = &metrics {
            metrics.record("server_setup", "runner", None, started.elapsed())?;
        }
        execute_owned_run(
            admin,
            &options,
            metrics.as_ref(),
            metrics_path.as_deref(),
            started,
        )
        .await
    }
    .await;
    let server_cleanup = Instant::now();
    endpoint.finish().await?;
    if let Some(metrics) = &metrics {
        metrics.record("server_cleanup", "runner", None, server_cleanup.elapsed())?;
    }
    println!(
        "PostgreSQL gate wall time {:.3}s",
        started.elapsed().as_secs_f64()
    );
    let interrupted = signal.load(std::sync::atomic::Ordering::SeqCst);
    if let Some(metrics) = &metrics {
        let phase = if matches!(&result, Ok(0)) && interrupted == 0 {
            "gate_passed"
        } else {
            "gate_failed"
        };
        metrics.record(phase, "runner", None, started.elapsed())?;
    }
    if interrupted != 0 {
        Ok(128 + i32::try_from(interrupted).unwrap_or(1))
    } else {
        result
    }
}

async fn execute_owned_run(
    admin: &str,
    options: &Options,
    metrics: Option<&FixtureMetrics>,
    metrics_path: Option<&Path>,
    started: Instant,
) -> Result<i32, Box<dyn std::error::Error>> {
    crate::process::check_cancelled()?;
    let setup = Instant::now();
    let run = RunFixture::start(admin).await?;
    println!(
        "PostgreSQL 14 fixture run {} provisioned in {:.3}s",
        run.identity(),
        started.elapsed().as_secs_f64()
    );
    let outcome = std::panic::AssertUnwindSafe(async {
        if let Some(metrics) = metrics {
            metrics.record("run_setup", run.identity(), None, setup.elapsed())?;
        }
        let migration = Instant::now();
        crabber_session::PostgresStore::migrate(&run.template_url())
            .await
            .map_err(|_| "cannot migrate run-owned PostgreSQL template")?;
        if let Some(metrics) = metrics {
            metrics.record(
                "template_migration",
                run.identity(),
                None,
                migration.elapsed(),
            )?;
        }
        crate::process::check_cancelled()?;
        if options.fixtures_only {
            fixtures::verify(&run, admin).await?;
            return Ok(0);
        }
        execute_schedule(&run, options, metrics, metrics_path).await
    })
    .catch_unwind()
    .await
    .unwrap_or_else(|_| Err("fixture qualification panicked; performing owned cleanup".into()));
    #[cfg(target_os = "linux")]
    crate::process::verify_drained()?;
    let cleanup_started = Instant::now();
    let identity = run.identity().to_owned();
    let cleanup = run.finish().await;
    if cleanup.is_ok()
        && let Some(metrics) = metrics
    {
        metrics.record("run_cleanup", &identity, None, cleanup_started.elapsed())?;
    }
    // Always attempt sweep, but never turn a primary failure into success.
    cleanup?;
    outcome
}

async fn execute_schedule(
    run: &RunFixture,
    options: &Options,
    metrics: Option<&FixtureMetrics>,
    metrics_path: Option<&Path>,
) -> Result<i32, Box<dyn std::error::Error>> {
    for args in schedule::COMMANDS {
        crate::process::check_cancelled()?;
        if options.package.as_ref().is_some_and(|package| {
            !args
                .windows(2)
                .any(|pair| pair[0] == "-p" && pair[1] == package)
        }) {
            continue;
        }
        if options.filter.is_some() && args[0] != "test" {
            continue;
        }
        if options.target.as_ref().is_some_and(|target| {
            !args
                .windows(2)
                .any(|pair| pair[0] == "--test" && pair[1] == target)
        }) {
            continue;
        }
        // Tests own their fixtures. Actual demo executables receive one database
        // per invocation; no ambient application URL is supplied to test binaries.
        let database = if args[0] == "run" {
            let started = Instant::now();
            let database = TestDatabase::for_run(run, DatabaseMode::Current).await?;
            if let Some(metrics) = metrics {
                metrics.record(
                    "demo_created",
                    database.identity(),
                    Some(DatabaseMode::Current),
                    started.elapsed(),
                )?;
            }
            Some(database)
        } else {
            None
        };
        let mut code = 0;
        for arguments in arguments::commands(args, options) {
            let mut command = Command::new("cargo");
            command
                .args(arguments)
                .current_dir(crate::workspace())
                .envs(run.environment())
                .env("CRABBER_REQUIRE_POSTGRES", "1")
                .env_remove("CRABBER_TEST_POSTGRES_URL")
                .env_remove("CRABBER_POSTGRES_URL")
                .env_remove("CRABBER_TEST_POSTGRES_CHILD_DATABASE")
                .env_remove("CRABBER_TEST_HISTORY_CHILD")
                .env_remove(METRICS_FILE);
            crate::checks::remove_live_configuration(&mut command);
            if let Some(path) = metrics_path {
                command.env(METRICS_FILE, path);
            }
            if let Some(database) = &database {
                command
                    .env("CRABBER_TEST_POSTGRES_URL", database.url())
                    .env("CRABBER_POSTGRES_URL", database.url())
                    .env("CRABBER_TEST_POSTGRES_CHILD_DATABASE", database.identity());
            }
            let mut gates = crate::checks::Checks::default();
            gates.command(
                crate::workspace(),
                command,
                "required PostgreSQL CI obligation",
            );
            code = gates.execute(&crate::checks::Options::default());
            if code != 0 {
                break;
            }
        }
        if let Some(database) = database {
            let identity = database.identity().to_owned();
            let started = Instant::now();
            if let Err(error) = database.finish().await {
                if code == 0 {
                    return Err(error.into());
                }
                eprintln!("demo database cleanup also failed: {error}");
            } else if let Some(metrics) = metrics {
                metrics.record("demo_finished", &identity, None, started.elapsed())?;
            }
        }
        if code != 0 {
            return Ok(code);
        }
    }
    Ok(0)
}
