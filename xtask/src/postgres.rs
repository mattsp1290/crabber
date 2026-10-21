//! Run-owned PostgreSQL prerequisite and the required CI obligation schedule.

use std::{env, process::Command, time::Instant};

use crabber_test_support::postgres::{DatabaseMode, RunFixture, TestDatabase};
use futures_util::FutureExt;

pub(crate) mod child;
mod container;
mod fixtures;
use container::{ADMIN_OVERRIDE, Endpoint};
mod schedule;

#[derive(Default)]
struct Options {
    package: Option<String>,
    filter: Option<String>,
    target: Option<String>,
    fixtures_only: bool,
    cleanup: Option<String>,
    test_threads: Option<String>,
}
impl Options {
    fn parse(args: &[String]) -> Result<Self, &'static str> {
        let mut options = Self::default();
        let mut args = args.iter();
        while let Some(arg) = args.next() {
            match arg.as_str() {
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
    let endpoint = Endpoint::start().await?;
    let admin = &endpoint.admin;
    let result = async {
        crate::process::check_cancelled()?;
        let run = RunFixture::start(admin).await?;
        println!(
            "PostgreSQL 14 fixture run {} provisioned in {:.3}s",
            run.identity(),
            started.elapsed().as_secs_f64()
        );
        let outcome = std::panic::AssertUnwindSafe(async {
            crabber_session::PostgresStore::migrate(&run.template_url())
                .await
                .map_err(|_| "cannot migrate run-owned PostgreSQL template")?;
            crate::process::check_cancelled()?;
            if options.fixtures_only {
                fixtures::verify(&run, admin).await?;
                return Ok(0);
            }
            execute_schedule(&run, &options).await
        })
        .catch_unwind()
        .await
        .unwrap_or_else(|_| Err("fixture qualification panicked; performing owned cleanup".into()));
        #[cfg(target_os = "linux")]
        crate::process::verify_drained()?;
        let cleanup = run.finish().await;
        // Always attempt sweep, but never turn a primary failure into success.
        cleanup?;
        outcome
    }
    .await;
    endpoint.finish().await?;
    println!(
        "PostgreSQL gate wall time {:.3}s",
        started.elapsed().as_secs_f64()
    );
    let interrupted = signal.load(std::sync::atomic::Ordering::SeqCst);
    if interrupted != 0 {
        Ok(128 + i32::try_from(interrupted).unwrap_or(1))
    } else {
        result
    }
}

async fn execute_schedule(
    run: &RunFixture,
    options: &Options,
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
            Some(TestDatabase::for_run(run, DatabaseMode::Current).await?)
        } else {
            None
        };
        let mut arguments = args.to_vec();
        if let Some(filter) = &options.filter {
            let position = arguments
                .iter()
                .position(|argument| *argument == "--")
                .unwrap_or(arguments.len());
            arguments.insert(position, filter);
        }
        if args[0] == "test" {
            if !arguments.contains(&"--") {
                arguments.push("--");
            }
            arguments.push("--test-threads");
            arguments.push(options.test_threads.as_deref().unwrap_or("2"));
        }
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
            .env_remove("ANTHROPIC_API_KEY")
            .env_remove("OPENAI_API_KEY")
            .env_remove("OPENCODE_GO_API_KEY")
            .env_remove("CRABBER_TRACE_LIVE")
            .env_remove("CRABBER_OBS_VERIFY_MARKER")
            .env_remove("DD_API_KEY")
            .env_remove("DD_APP_KEY")
            .env_remove("OTEL_EXPORTER_OTLP_ENDPOINT")
            .env_remove("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT")
            .env_remove("OTEL_EXPORTER_OTLP_HEADERS")
            .env_remove("OTEL_EXPORTER_OTLP_TRACES_HEADERS");
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
        let code = gates.execute(&crate::checks::Options::default());
        if let Some(database) = database
            && let Err(error) = database.finish().await
        {
            if code == 0 {
                return Err(error.into());
            }
            eprintln!("demo database cleanup also failed: {error}");
        }
        if code != 0 {
            return Ok(code);
        }
    }
    Ok(0)
}
