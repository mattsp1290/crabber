//! Preserve each required command's targets while selecting its test runner.

use super::{Options, Runner};

pub(super) fn commands(args: &[&str], options: &Options) -> Vec<Vec<String>> {
    let separator = args
        .iter()
        .position(|arg| *arg == "--")
        .unwrap_or(args.len());
    let cluster_role_case = args[0] == "test"
        && args
            .windows(2)
            .any(|pair| pair == ["-p", "crabber-session"]);
    let role_name = crate::nextest::CLUSTER_ROLE_TEST;
    let include_role_case = cluster_role_case
        && options
            .filter
            .as_ref()
            .is_none_or(|filter| role_name.contains(filter));
    if args[0] != "test" || options.runner == Some(Runner::Cargo) {
        return cargo_commands(args, options, separator, cluster_role_case);
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
    let mut commands = Vec::new();
    if include_role_case {
        let mut isolated = command.clone();
        isolated.extend([
            "--no-tests".to_owned(),
            "fail".to_owned(),
            "-E".to_owned(),
            format!("test(={role_name})"),
        ]);
        commands.push(isolated);
    }
    if cluster_role_case {
        command.extend(["-E".to_owned(), format!("not test(={role_name})")]);
    }
    if include_role_case && options.filter.is_some() {
        command.extend(["--no-tests".to_owned(), "pass".to_owned()]);
    }
    if let Some(filter) = &options.filter {
        command.extend(["--".to_owned(), filter.clone()]);
    }
    commands.push(command);
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

fn cargo_commands(
    args: &[&str],
    options: &Options,
    separator: usize,
    cluster_role_case: bool,
) -> Vec<Vec<String>> {
    let role_name = crate::nextest::CLUSTER_ROLE_TEST;
    let include_role_case = cluster_role_case
        && options
            .filter
            .as_ref()
            .is_none_or(|filter| role_name.contains(filter));
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
    if cluster_role_case {
        command.extend(["--skip".to_owned(), role_name.to_owned()]);
    }
    let mut commands = Vec::new();
    if include_role_case {
        let mut isolated: Vec<_> = args[..separator]
            .iter()
            .map(|arg| (*arg).to_owned())
            .collect();
        isolated.extend(["--lib".to_owned(), role_name.to_owned(), "--".to_owned()]);
        if separator < args.len() {
            isolated.extend(args[separator + 1..].iter().map(|arg| (*arg).to_owned()));
        }
        isolated.extend([
            "--exact".to_owned(),
            "--test-threads".to_owned(),
            "1".to_owned(),
        ]);
        commands.push(isolated);
    }
    commands.push(command);
    commands
}

#[cfg(test)]
mod tests {
    use super::*;

    const SESSION: &[&str] = &["test", "-p", "crabber-session", "--features", "postgres"];

    #[test]
    fn nextest_runs_cluster_role_mutation_before_complementary_bulk_selection() {
        let scheduled = commands(SESSION, &Options::default());
        assert_eq!(scheduled.len(), 3);
        assert!(scheduled[0].windows(2).any(|args| args
            == [
                "-E",
                &format!("test(={})", crate::nextest::CLUSTER_ROLE_TEST)
            ]));
        assert!(scheduled[1].windows(2).any(|args| args
            == [
                "-E",
                &format!("not test(={})", crate::nextest::CLUSTER_ROLE_TEST)
            ]));
        assert!(scheduled[2].iter().any(|arg| arg == "--doc"));
    }

    #[test]
    fn unrelated_filter_omits_cluster_role_gate() {
        let options = Options {
            filter: Some("queue_contract".into()),
            ..Options::default()
        };
        let scheduled = commands(SESSION, &options);
        assert_eq!(scheduled.len(), 2);
        assert!(scheduled[0].iter().any(|arg| arg == "queue_contract"));
    }

    #[test]
    fn role_only_filter_keeps_a_required_gate_and_allows_empty_complement() {
        let options = Options {
            filter: Some(crate::nextest::CLUSTER_ROLE_TEST.into()),
            ..Options::default()
        };
        let scheduled = commands(SESSION, &options);
        assert!(
            scheduled[0]
                .windows(2)
                .any(|args| args == ["--no-tests", "fail"])
        );
        assert!(
            scheduled[1]
                .windows(2)
                .any(|args| args == ["--no-tests", "pass"])
        );
    }

    #[test]
    fn cargo_runs_exact_role_case_then_skips_it_in_bulk() {
        let options = Options {
            runner: Some(Runner::Cargo),
            ..Options::default()
        };
        let scheduled = commands(SESSION, &options);
        assert_eq!(scheduled.len(), 2);
        assert!(scheduled[0].iter().any(|arg| arg == "--exact"));
        assert!(
            scheduled[0]
                .iter()
                .any(|arg| arg == crate::nextest::CLUSTER_ROLE_TEST)
        );
        assert!(
            scheduled[1]
                .windows(2)
                .any(|args| args == ["--skip", crate::nextest::CLUSTER_ROLE_TEST])
        );
    }
}
