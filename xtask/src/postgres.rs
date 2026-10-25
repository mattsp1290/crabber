//! Run-owned PostgreSQL prerequisite and the required CI obligation schedule.

use std::{
    env,
    path::{Path, PathBuf},
    process::Command,
    sync::Arc,
    time::Instant,
};

use crabber_test_support::postgres::{
    DatabaseMode, FixtureMetrics, METRICS_FILE, RunFixture, TestDatabase,
};
use futures_util::FutureExt;

pub(crate) mod child;
mod container;
#[cfg(feature = "session-fixtures")]
mod fixtures;
pub(crate) mod session;
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
    complete: Option<crate::checks::Options>,
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
    execute_options(options)
}

fn execute_options(options: Options) -> i32 {
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

pub(crate) fn complete(mut options: crate::checks::Options) -> i32 {
    let started = Instant::now();
    let owned_started_epoch = options.part.map(|_| {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs_f64()
    });
    let directory = crate::complete::output_directory();
    if options.list {
        return crate::complete::schedule(Vec::new(), &directory, None, options.part)
            .execute(&options);
    }
    if let Err(error) = std::fs::create_dir_all(&directory) {
        eprintln!("cannot prepare complete-check reports: {error}");
        return 1;
    }
    if options.timings.is_none() {
        options.timings = Some(directory.join("phases.json"));
    }
    let timings = options.timings.clone().unwrap();
    let metrics = directory.join("fixtures.jsonl");
    let part = options.part;
    let mut code = execute_options(Options {
        complete: Some(options),
        fixture_metrics: Some(metrics.clone()),
        ..Options::default()
    });
    let mut report: serde_json::Value = std::fs::read(&timings)
        .ok()
        .and_then(|bytes| serde_json::from_slice(&bytes).ok())
        .unwrap_or_else(|| serde_json::json!({}));
    if part.is_some_and(crate::parts::Part::owns_postgres) {
        let drained = owned_partial_process_cleanup(&mut code);
        report["processes_drained"] = serde_json::json!(drained);
        report["selected_gates_started_unix_seconds"] = report["started_unix_seconds"].clone();
        report["started_unix_seconds"] = serde_json::json!(owned_started_epoch.unwrap());
        report["selected_gates_wall_seconds"] = report["wall_seconds"].clone();
        report["wall_seconds"] = serde_json::json!(started.elapsed().as_secs_f64());
        report["qualified"] = serde_json::json!(false);
        report["full_native_suite_executed"] = serde_json::json!(false);
        report["owned_postgres_run"] = serde_json::json!(observed_owned_run(&metrics));
    }
    report["coverage_scope"] = serde_json::json!(part.map_or("complete", crate::parts::Part::name));
    if let Some(crate::parts::Part::NativeLibraries(quarter)) = part {
        report["part_family"] = serde_json::json!("native-libraries");
        report["library_quarter"] = serde_json::json!(quarter.index());
        report["library_quarter_count"] = serde_json::json!(4);
        report["full_coverage_policy_executed"] = serde_json::json!(false);
        report["requires_owned_postgres"] = serde_json::json!(true);
        report["postgres_demos"] = serde_json::json!([]);
    }
    report["status"] = serde_json::json!(if code == 0 { "passed" } else { "failed" });
    report["exit_code"] = serde_json::json!(code);
    report["wall_seconds_including_fixture_lifecycle"] =
        serde_json::json!(started.elapsed().as_secs_f64());
    report["fixture_metrics"] = serde_json::json!(metrics);
    let transport = std::fs::read_to_string(&metrics).ok().and_then(|events| {
        events.lines().find_map(|line| {
            let event: serde_json::Value = serde_json::from_str(line).ok()?;
            event["phase"]
                .as_str()?
                .strip_prefix("server_transport_")
                .map(str::to_owned)
        })
    });
    report["postgres_transport"] = serde_json::json!(transport);
    report["tool_versions"] = serde_json::json!({
        "rustc": std::fs::read_to_string(directory.join("rustc-version.txt")).ok(),
        "wasm-tools": std::fs::read_to_string(directory.join("wasm-tools-version.txt")).ok(),
        "docker-server": std::fs::read_to_string(directory.join("docker-version.txt")).ok(),
        "nextest": serde_json::from_str::<serde_json::Value>(include_str!("../../.config/test-tools.json")).unwrap()["nextest"]["version"],
    });
    if let Some(parent) = timings.parent()
        && let Err(error) = std::fs::create_dir_all(parent)
    {
        eprintln!("cannot prepare complete-check report directory: {error}");
        return 1;
    }
    let temporary = timings.with_extension("complete.tmp");
    if let Err(error) = std::fs::write(&temporary, serde_json::to_vec_pretty(&report).unwrap())
        .and_then(|()| std::fs::rename(temporary, &timings))
    {
        eprintln!("cannot finish complete-check report: {error}");
        return 1;
    }
    code
}

fn observed_owned_run(metrics: &Path) -> bool {
    let events: Vec<serde_json::Value> = std::fs::read_to_string(metrics)
        .unwrap_or_default()
        .lines()
        .filter_map(|line| serde_json::from_str(line).ok())
        .collect();
    ["server_setup", "run_setup"].into_iter().all(|phase| {
        events
            .iter()
            .filter(|event| event["phase"] == phase)
            .count()
            == 1
    })
}

fn owned_partial_process_cleanup(code: &mut i32) -> bool {
    #[cfg(target_os = "linux")]
    {
        match crate::process::verify_drained() {
            Ok(()) => true,
            Err(error) => {
                eprintln!("artifact owner final process cleanup failed: {error}");
                if *code == 0 {
                    *code = 1;
                }
                false
            }
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        if *code == 0 {
            *code = 1;
        }
        false
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
        .transpose()?
        .map(Arc::new);
    let endpoint = Endpoint::start().await?;
    let admin = &endpoint.admin;
    let mut result = async {
        if let Some(metrics) = &metrics {
            metrics.record("server_setup", "runner", None, started.elapsed())?;
            metrics.record(
                &format!("server_transport_{}", endpoint.transport),
                "runner",
                None,
                std::time::Duration::ZERO,
            )?;
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
    let part = options.complete.as_ref().and_then(|complete| complete.part);
    finish_server(endpoint, metrics.as_ref(), part, &mut result).await?;
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
        let recorded = metrics.record(phase, "runner", None, started.elapsed());
        if part.is_some_and(crate::parts::Part::owns_postgres) && !matches!(&result, Ok(0)) {
            if let Err(error) = recorded {
                eprintln!("owned terminal recording also failed: {error}");
            }
        } else {
            recorded?;
        }
    }
    if interrupted != 0 {
        Ok(128 + i32::try_from(interrupted).unwrap_or(1))
    } else {
        result
    }
}

async fn finish_server(
    endpoint: Endpoint,
    metrics: Option<&Arc<FixtureMetrics>>,
    part: Option<crate::parts::Part>,
    result: &mut Result<i32, Box<dyn std::error::Error>>,
) -> Result<(), Box<dyn std::error::Error>> {
    let server_cleanup = Instant::now();
    let cleanup = endpoint.finish().await;
    if part.is_some_and(crate::parts::Part::owns_postgres) {
        match cleanup {
            Ok(()) => {
                if let Some(metrics) = metrics
                    && let Err(error) =
                        metrics.record("server_cleanup", "runner", None, server_cleanup.elapsed())
                {
                    if matches!(&*result, Ok(0)) {
                        *result = Err(error.into());
                    } else {
                        eprintln!("owned server cleanup recording also failed: {error}");
                    }
                }
            }
            Err(error) => {
                if matches!(&*result, Ok(0)) {
                    *result = Err(error);
                } else {
                    eprintln!("owned server cleanup also failed: {error}");
                }
            }
        }
    } else {
        cleanup?;
        if let Some(metrics) = metrics {
            metrics.record("server_cleanup", "runner", None, server_cleanup.elapsed())?;
        }
    }
    Ok(())
}

async fn execute_owned_run(
    admin: &str,
    options: &Options,
    metrics: Option<&Arc<FixtureMetrics>>,
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
        #[cfg(feature = "session-fixtures")]
        if options.complete.is_none() {
            let environment = run
                .environment()
                .into_iter()
                .map(|(key, value)| (key.to_owned(), value))
                .collect();
            let mut migration = crate::checks::Checks::required(environment);
            session::TemplateMigration::schedule(
                &mut migration,
                Some(session::TemplateMigration::new(&run, metrics.cloned())),
            );
            let code = migration.execute(&crate::checks::Options::default());
            if code != 0 {
                return Ok(code);
            }
        }
        crate::process::check_cancelled()?;
        if options.fixtures_only {
            #[cfg(feature = "session-fixtures")]
            {
                fixtures::verify(&run, admin).await?;
                return Ok(0);
            }
            #[cfg(not(feature = "session-fixtures"))]
            return Err("fixture qualification requires the enabled owner".into());
        }
        if let Some(complete) = &options.complete {
            let mut environment: Vec<(String, String)> = run
                .environment()
                .into_iter()
                .map(|(key, value)| (key.to_owned(), value))
                .collect();
            if let Some(path) = metrics_path {
                environment.push((METRICS_FILE.into(), path.to_string_lossy().into_owned()));
            }
            let directory = metrics_path.unwrap().parent().unwrap();
            let migration = session::TemplateMigration::new(&run, metrics.cloned());
            let code =
                crate::complete::schedule(environment, directory, Some(migration), complete.part)
                    .execute(complete);
            if code != 0 {
                return Ok(code);
            }
            if complete
                .part
                .is_some_and(|part| part.resources() == crate::parts::ResourcePolicy::OwnedPostgres)
            {
                return Ok(0);
            }
            return execute_demos(&run, options, metrics.map(Arc::as_ref), metrics_path).await;
        }
        execute_schedule(&run, options, metrics.map(Arc::as_ref), metrics_path).await
    })
    .catch_unwind()
    .await
    .unwrap_or_else(|_| Err("fixture qualification panicked; performing owned cleanup".into()));
    if options
        .complete
        .as_ref()
        .and_then(|complete| complete.part)
        .is_some_and(crate::parts::Part::owns_postgres)
    {
        return finish_owned_partial_run(run, outcome, metrics).await;
    }
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

async fn finish_owned_partial_run(
    run: RunFixture,
    outcome: Result<i32, Box<dyn std::error::Error>>,
    metrics: Option<&Arc<FixtureMetrics>>,
) -> Result<i32, Box<dyn std::error::Error>> {
    #[cfg(target_os = "linux")]
    let drained = crate::process::verify_drained();
    #[cfg(not(target_os = "linux"))]
    let drained: std::io::Result<()> =
        Err(std::io::Error::other("artifact supervision requires Linux"));
    let started = Instant::now();
    let identity = run.identity().to_owned();
    let cleanup = run.finish().await;
    let recorded = if cleanup.is_ok()
        && let Some(metrics) = metrics
    {
        metrics.record("run_cleanup", &identity, None, started.elapsed())
    } else {
        Ok(())
    };
    if !matches!(&outcome, Ok(0)) {
        if let Err(error) = drained {
            eprintln!("artifact stage also failed process cleanup: {error}");
        }
        if let Err(error) = cleanup {
            eprintln!("artifact stage also failed run cleanup: {error}");
        }
        if let Err(error) = recorded {
            eprintln!("artifact stage also failed cleanup recording: {error}");
        }
        return outcome;
    }
    drained?;
    cleanup?;
    recorded?;
    outcome
}

fn demo_command(args: &[&str]) -> Command {
    let package = args.windows(2).find(|pair| pair[0] == "-p").unwrap()[1];
    let separator = args.iter().position(|arg| *arg == "--").unwrap();
    let mut command = crate::complete::binary_command(package);
    command.args(&args[separator + 1..]);
    command
}

pub(crate) fn demo_inventory() -> serde_json::Value {
    serde_json::Value::Array(schedule::COMMANDS.iter().filter(|args| args[0] == "run").map(|args| {
        let command = demo_command(args);
        let argv: Vec<_> = std::iter::once(command.get_program()).chain(command.get_args())
            .map(|arg| arg.to_string_lossy().into_owned()).collect();
        serde_json::json!({"argv": argv, "execution_role": "owned-postgres-demo", "database_mode": "Current"})
    }).collect())
}

async fn execute_demos(
    run: &RunFixture,
    options: &Options,
    metrics: Option<&FixtureMetrics>,
    metrics_path: Option<&Path>,
) -> Result<i32, Box<dyn std::error::Error>> {
    execute_schedule_mode(run, options, metrics, metrics_path, true).await
}

async fn execute_schedule(
    run: &RunFixture,
    options: &Options,
    metrics: Option<&FixtureMetrics>,
    metrics_path: Option<&Path>,
) -> Result<i32, Box<dyn std::error::Error>> {
    execute_schedule_mode(run, options, metrics, metrics_path, false).await
}

async fn execute_schedule_mode(
    run: &RunFixture,
    options: &Options,
    metrics: Option<&FixtureMetrics>,
    metrics_path: Option<&Path>,
    demos_only: bool,
) -> Result<i32, Box<dyn std::error::Error>> {
    for args in schedule::COMMANDS {
        if demos_only && args[0] != "run" {
            continue;
        }
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
            let mut command = if demos_only {
                demo_command(args)
            } else {
                let mut command = Command::new("cargo");
                command.args(arguments);
                command
            };
            command
                .current_dir(crate::workspace())
                .envs(run.environment())
                .env("CRABBER_REQUIRE_POSTGRES", "1")
                .env_remove("CRABBER_TEST_POSTGRES_URL")
                .env_remove("CRABBER_POSTGRES_URL")
                .env_remove("CRABBER_TEST_POSTGRES_CHILD_DATABASE")
                .env_remove("CRABBER_TEST_POSTGRES_CHILD_ROLE")
                .env_remove("CRABBER_TEST_HISTORY_CHILD")
                .env_remove(METRICS_FILE)
                .envs(metrics_path.map(|path| (METRICS_FILE, path)));
            if options.complete.as_ref().and_then(|complete| complete.part)
                == Some(crate::parts::Part::QualityArtifacts)
            {
                crate::standard_tools::require_subprocess_tools(&mut command);
            }
            crate::checks::remove_live_configuration(&mut command);
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
