//! Preserve each required command's targets while selecting its test runner.

use super::{Options, Runner};

pub(super) fn commands(args: &[&str], options: &Options) -> Vec<Vec<String>> {
    let separator = args
        .iter()
        .position(|arg| *arg == "--")
        .unwrap_or(args.len());
    if args[0] != "test" || options.runner != Some(Runner::Nextest) {
        let mut command: Vec<_> = args.iter().map(|arg| (*arg).to_owned()).collect();
        if let Some(filter) = &options.filter {
            command.insert(separator, filter.clone());
        }
        if args[0] == "test" {
            if !args.contains(&"--") {
                command.push("--".to_owned());
            }
            command.extend([
                "--test-threads".to_owned(),
                options
                    .test_threads
                    .clone()
                    .unwrap_or_else(|| "2".to_owned()),
            ]);
        }
        return vec![command];
    }
    let profile = match options.test_threads.as_deref() {
        Some("1") => "postgres-one",
        Some("4") => "postgres-four",
        _ => "postgres",
    };
    let mut command: Vec<_> = [
        "nextest",
        "run",
        "--profile",
        profile,
        "--retries",
        "0",
        "--no-fail-fast",
        "--success-output",
        "immediate",
        "--failure-output",
        "immediate",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect();
    command.extend(args[1..separator].iter().map(|arg| (*arg).to_owned()));
    if let Some(threads) = &options.test_threads {
        command.extend(["--test-threads".to_owned(), threads.clone()]);
    }
    if let Some(filter) = &options.filter {
        command.extend(["--".to_owned(), filter.clone()]);
    }
    let mut commands = vec![command];
    if !args
        .iter()
        .any(|arg| matches!(*arg, "--test" | "--lib" | "--bin" | "--example" | "--tests"))
    {
        let mut docs: Vec<_> = args[..separator]
            .iter()
            .map(|arg| (*arg).to_owned())
            .collect();
        docs.push("--doc".to_owned());
        if let Some(filter) = &options.filter {
            docs.push(filter.clone());
        }
        commands.push(docs);
    }
    commands
}
