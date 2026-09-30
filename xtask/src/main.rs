//! Local development commands, invoked with `cargo xtask`.

use std::{env, fs, path::Path, process::Command};

const GLUE_START: &str = "crabber:glue-start";
const GLUE_END: &str = "crabber:glue-end";
const GLUE_LIMIT: usize = 60;

fn main() {
    let mut args = env::args().skip(1);
    match (args.next().as_deref(), args.next()) {
        (Some("check"), None) => check(),
        (Some("verify-datadog"), None) => verify_datadog(),
        _ => {
            eprintln!("usage: cargo xtask <check|verify-datadog>");
            std::process::exit(2);
        }
    }
}

fn check() {
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("xtask belongs directly under the workspace root");

    run(workspace, &["fmt", "--all", "--", "--check"]);
    run(
        workspace,
        &[
            "clippy",
            "--workspace",
            "--all-targets",
            "--",
            "-D",
            "warnings",
        ],
    );
    run(workspace, &["test", "--workspace"]);
    check_glue(workspace);
}

fn run(workspace: &Path, args: &[&str]) {
    println!("$ cargo {}", args.join(" "));
    let status = Command::new("cargo")
        .args(args)
        .current_dir(workspace)
        .status()
        .expect("failed to launch cargo");
    if !status.success() {
        std::process::exit(status.code().unwrap_or(1));
    }
}

fn check_glue(workspace: &Path) {
    let example_dir = workspace.join("examples/minimal-embed");
    let example_file = workspace.join("examples/minimal-embed.rs");
    if !example_dir.exists() && !example_file.exists() {
        return;
    }

    let mut files = Vec::new();
    if example_file.exists() {
        files.push(example_file);
    }
    if example_dir.exists() {
        collect_rust_files(&example_dir, &mut files);
    }

    let mut regions = 0;
    for file in files {
        let source = fs::read_to_string(&file).expect("failed to read minimal embed example");
        let mut start = None;
        for (index, line) in source.lines().enumerate() {
            if line.contains(GLUE_START) {
                assert!(
                    start.replace(index).is_none(),
                    "duplicate glue start in {}",
                    file.display()
                );
            }
            if line.contains(GLUE_END) {
                let first = start
                    .take()
                    .unwrap_or_else(|| panic!("glue end without start in {}", file.display()));
                let count = index - first - 1;
                assert!(
                    count <= GLUE_LIMIT,
                    "{} has {count} glue lines (limit {GLUE_LIMIT})",
                    file.display()
                );
                regions += 1;
            }
        }
        assert!(
            start.is_none(),
            "glue start without end in {}",
            file.display()
        );
    }
    assert_eq!(
        regions, 1,
        "minimal embed example must have exactly one glue marker pair"
    );
}

fn collect_rust_files(dir: &Path, files: &mut Vec<std::path::PathBuf>) {
    for entry in fs::read_dir(dir).expect("failed to read minimal embed example directory") {
        let path = entry.expect("failed to read example entry").path();
        if path.is_dir() {
            collect_rust_files(&path, files);
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            files.push(path);
        }
    }
}

/// Live ingestion gate. Only this command reads `DD_APP_KEY`.
fn verify_datadog() {
    use std::time::{Duration, SystemTime, UNIX_EPOCH};
    let site = env::var("DD_SITE").expect("DD_SITE is required");
    let api_key = env::var("DD_API_KEY").expect("DD_API_KEY is required");
    let application_key = env::var("DD_APP_KEY").expect("DD_APP_KEY is required");
    let marker = format!(
        "crabber-{}-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs(),
        std::process::id()
    );
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
    let status = Command::new("cargo")
        .args(["run", "--quiet", "-p", "datadog-export"])
        .env("CRABBER_OBS_VERIFY_MARKER", &marker)
        .current_dir(workspace)
        .status()
        .expect("run example");
    assert!(status.success(), "example run or LLM Obs intake failed");
    println!("llmobs_intake_status=202 marker={marker}");
    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap();
    let origin = format!("https://api.{site}");
    let mut metrics = false;
    let mut logs = false;
    for _ in 0..12 {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let metric_response = client
            .get(format!("{origin}/api/v1/query"))
            .header("DD-API-KEY", &api_key)
            .header("DD-APPLICATION-KEY", &application_key)
            .query(&[
                ("from", (now - 600).to_string()),
                ("to", now.to_string()),
                ("query", format!("sum:crabber.run.count{{verify:{marker}}}")),
            ])
            .send();
        if let Ok(response) = metric_response
            && response.status().is_success()
            && let Ok(body) = response.json::<serde_json::Value>()
        {
            metrics = body["series"].as_array().is_some_and(|v| !v.is_empty());
        }
        let log_response = client.post(format!("{origin}/api/v2/logs/events/search"))
            .header("DD-API-KEY", &api_key).header("DD-APPLICATION-KEY", &application_key)
            .json(&serde_json::json!({"filter":{"query":format!("@verify_marker:{marker}"),"from":"now-15m","to":"now"},"page":{"limit":10}})).send();
        if let Ok(response) = log_response
            && response.status().is_success()
            && let Ok(body) = response.json::<serde_json::Value>()
        {
            logs = body["data"].as_array().is_some_and(|v| !v.is_empty());
        }
        println!("metrics_found={metrics} logs_found={logs}");
        if metrics && logs {
            break;
        }
        std::thread::sleep(Duration::from_secs(10));
    }
    if !metrics || !logs {
        std::process::exit(1);
    }
}
