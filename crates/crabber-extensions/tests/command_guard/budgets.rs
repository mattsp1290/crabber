use super::*;

pub(super) fn hard_caps() -> Limits {
    Limits {
        max_bindings: MAX_BINDINGS,
        max_rules: MAX_RULES,
        max_rule_bytes: MAX_RULE_BYTES,
        max_prefix_args: MAX_PREFIX_ARGS,
        max_json_depth: MAX_JSON_DEPTH,
        max_json_nodes: MAX_JSON_NODES,
        max_command_bytes: MAX_COMMAND_BYTES,
        max_analysis_bytes: MAX_ANALYSIS_BYTES,
        max_ast_nodes: MAX_AST_NODES,
        max_ast_depth: MAX_AST_DEPTH,
        max_words: MAX_WORDS,
        max_word_bytes: MAX_WORD_BYTES,
        max_wrapper_depth: MAX_WRAPPER_DEPTH,
        max_in_flight: MAX_IN_FLIGHT,
    }
}
#[test]
fn caps_are_documented_values() {
    assert_eq!(
        [
            MAX_BINDINGS,
            MAX_RULES,
            MAX_RULE_BYTES,
            MAX_PREFIX_ARGS,
            MAX_JSON_DEPTH,
            MAX_JSON_NODES,
            MAX_COMMAND_BYTES,
            MAX_ANALYSIS_BYTES,
            MAX_AST_NODES,
            MAX_AST_DEPTH,
            MAX_WORDS,
            MAX_WORD_BYTES,
            MAX_WRAPPER_DEPTH,
            MAX_IN_FLIGHT
        ],
        [
            64, 256, 16384, 64, 64, 65536, 262144, 1048576, 32768, 32, 16384, 262144, 64, 1024
        ]
    );
}
pub(super) fn stack_rows() -> Vec<(String, Dialect)> {
    vec![
        (
            format!("{}blocked{}", "$(".repeat(5000), ")".repeat(5000)),
            Dialect::Posix,
        ),
        (
            format!("{}blocked{}", "\"$(".repeat(5000), ")\"".repeat(5000)),
            Dialect::Posix,
        ),
        (
            format!("{}blocked{}", "( ".repeat(5000), " )".repeat(5000)),
            Dialect::Posix,
        ),
        (
            format!("{}blocked; {}", "{ ".repeat(5000), "}; ".repeat(5000)),
            Dialect::Posix,
        ),
        (
            format!(
                "{}blocked; {}",
                "if true; then ".repeat(2000),
                "fi; ".repeat(2000)
            ),
            Dialect::Posix,
        ),
        (
            format!("echo {}blocked{}", "<(".repeat(5000), ")".repeat(5000)),
            Dialect::Bash,
        ),
        ("'".repeat(4000), Dialect::Posix),
        ("x ".repeat(MAX_WORDS), Dialect::Posix),
        ("x".repeat(MAX_WORD_BYTES), Dialect::Posix),
        (
            format!("case x in {} esac", "x) echo ok;; ".repeat(5000)),
            Dialect::Posix,
        ),
        (format!("{}blocked", "env ".repeat(20000)), Dialect::Posix),
    ]
}
#[test]
fn stack_proof_at_hard_caps() {
    let rows = stack_rows();
    std::thread::Builder::new()
        .stack_size(512 * 1024)
        .spawn(move || {
            let p = Policy::new(Options {
                limits: hard_caps(),
                ..options()
            })
            .unwrap();
            for (i, (script, dialect)) in rows.iter().enumerate() {
                eprintln!("stack generator {i}");
                let _ = p.analyze_script(script, *dialect);
            }
        })
        .unwrap()
        .join()
        .unwrap();
}

fn check_limits(limits: Limits, rows: &[(&str, Dialect, Outcome)]) {
    let p = Policy::new(Options {
        limits,
        ..options()
    })
    .unwrap();
    for (script, dialect, want) in rows {
        assert_eq!(p.analyze_script(script, *dialect), *want, "{script:?}");
    }
}
#[test]
fn command_bytes() {
    let mut l = limits();
    l.max_command_bytes = 5;
    check_limits(
        l,
        &[
            ("echo ", Dialect::Posix, Outcome::Abstain),
            ("echo  ", Dialect::Posix, Outcome::AnalysisLimit),
        ],
    );
}
#[test]
fn analysis_bytes_shared_by_nested_scripts() {
    let mut l = limits();
    l.max_command_bytes = 100;
    l.max_analysis_bytes = 150;
    let small = format!("sh -c '{}'", " ".repeat(60));
    let big = format!("sh -c '{}'", " ".repeat(92));
    check_limits(
        l,
        &[
            (small.as_str(), Dialect::Posix, Outcome::Abstain),
            (big.as_str(), Dialect::Posix, Outcome::AnalysisLimit),
        ],
    );
}
#[test]
fn backquote_bodies_charge_bytes() {
    let mut l = limits();
    l.max_command_bytes = 100;
    l.max_analysis_bytes = 150;
    let small = format!("echo `{}`", " ".repeat(60));
    let big = format!("echo `{}`", " ".repeat(90));
    check_limits(
        l,
        &[
            (small.as_str(), Dialect::Posix, Outcome::Abstain),
            (big.as_str(), Dialect::Posix, Outcome::AnalysisLimit),
        ],
    );
}
#[test]
fn ast_nodes() {
    // Script + ListItem + AndOr + Pipeline + Command + CommandKind + Word + Lit.
    let mut l = limits();
    l.max_ast_nodes = 8;
    check_limits(
        l,
        &[
            ("echo", Dialect::Posix, Outcome::Abstain),
            ("echo''", Dialect::Posix, Outcome::AnalysisLimit),
        ],
    );
}
#[test]
fn ast_depth() {
    let mut l = limits();
    l.max_ast_depth = 4;
    check_limits(
        l,
        &[
            ("( ( ( echo ok ) ) )", Dialect::Posix, Outcome::Abstain),
            (
                "( ( ( ( echo ok ) ) ) )",
                Dialect::Posix,
                Outcome::AnalysisLimit,
            ),
            (
                "echo $(echo $(echo $(echo ok)))",
                Dialect::Posix,
                Outcome::Abstain,
            ),
            (
                "echo $(echo $(echo $(echo $(echo ok))))",
                Dialect::Posix,
                Outcome::AnalysisLimit,
            ),
            (
                "echo <(echo <(echo <(echo ok)))",
                Dialect::Bash,
                Outcome::Abstain,
            ),
            (
                "echo <(echo <(echo <(echo <(echo ok))))",
                Dialect::Bash,
                Outcome::AnalysisLimit,
            ),
            ("env env env echo ok", Dialect::Posix, Outcome::Abstain),
            (
                "env env env env echo ok",
                Dialect::Posix,
                Outcome::AnalysisLimit,
            ),
        ],
    );
}
#[test]
fn words() {
    let mut l = limits();
    l.max_words = 3;
    check_limits(
        l,
        &[
            ("echo a b", Dialect::Posix, Outcome::Abstain),
            ("echo a b c", Dialect::Posix, Outcome::AnalysisLimit),
        ],
    );
}
#[test]
fn word_bytes() {
    let mut o = options();
    o.bindings = vec![Binding {
        tool_name: "shell".into(),
        command_field: "cmd".into(),
        dialect: Dialect::Posix,
    }];
    o.rules = vec![Rule {
        id: "r".into(),
        executable: "x".into(),
        arg_prefix: vec![],
    }];
    o.limits.max_word_bytes = 5;
    let p = Policy::new(o).unwrap();
    for s in [
        "echo abcdef",
        "cat <<abcdef\nx\nabcdef\n",
        "cat <<'abcdef'\nx\nabcdef\n",
        "cat <<\"abcdef\"\nx\nabcdef\n",
        "cat <<ab\\c'def'\nx\nabcdef\n",
        "for abcdef; do :; done",
        "for abcdef in x; do :; done",
        "echo \"ab\"'cd'ef",
        "a=abcdef",
        "echo >abcdef",
        "for a in abcdef; do :; done",
        "case abcdef in a) :;; esac",
    ] {
        assert_eq!(
            p.analyze_script(s, Dialect::Posix),
            Outcome::AnalysisLimit,
            "{s}"
        );
    }
    for s in [
        "echo abcde",
        "cat <<abcde\nx\nabcde\n",
        "cat <<'abcde'\nx\nabcde\n",
        "cat <<\"abcde\"\nx\nabcde\n",
        "cat <<ab\\c'de'\nx\nabcde\n",
        "for abcde; do :; done",
        "for abcde in x; do :; done",
    ] {
        for d in [Dialect::Posix, Dialect::Bash] {
            assert_eq!(p.analyze_script(s, d), Outcome::Abstain, "{s:?}");
        }
    }
}
#[test]
fn wrapper_depth() {
    let mut l = limits();
    l.max_wrapper_depth = 2;
    check_limits(
        l,
        &[
            ("command env echo ok", Dialect::Posix, Outcome::Abstain),
            (
                "command env command echo ok",
                Dialect::Posix,
                Outcome::AnalysisLimit,
            ),
            (
                "command sh -c 'command echo ok'",
                Dialect::Posix,
                Outcome::AnalysisLimit,
            ),
        ],
    );
}
#[test]
fn worst_case_wall_clock() {
    let p = Policy::new(Options {
        limits: hard_caps(),
        rules: (0..256)
            .map(|i| Rule {
                id: format!("r{i:03}"),
                executable: "blocked".into(),
                arg_prefix: vec!["x".into(); 64],
            })
            .collect(),
        ..options()
    })
    .unwrap();
    let s = "blocked; ".repeat(MAX_COMMAND_BYTES / 9);
    let begin = std::time::Instant::now();
    assert_eq!(p.analyze_script(&s, Dialect::Bash), Outcome::AnalysisLimit);
    let elapsed = begin.elapsed();
    eprintln!("worst case: {elapsed:?}");
    assert!(elapsed < std::time::Duration::from_secs(2));
}

#[test]
fn nested_shell_and_backquote_stacks_at_hard_caps() {
    std::thread::Builder::new()
        .stack_size(512 * 1024)
        .spawn(|| {
            let p = Policy::new(Options {
                limits: hard_caps(),
                ..options()
            })
            .unwrap();
            for (s, d) in nested_rows() {
                let _ = p.analyze_script(&s, d);
            }
        })
        .unwrap()
        .join()
        .unwrap();
}
#[test]
fn long_prefix_matching_stays_within_wall_clock_bound() {
    let p = Policy::new(Options {
        limits: hard_caps(),
        rules: (0..256)
            .map(|i| Rule {
                id: format!("r{i:03}"),
                executable: "blocked".into(),
                arg_prefix: vec!["x".into(); 64],
            })
            .collect(),
        ..options()
    })
    .unwrap();
    let command = format!("blocked {}y; ", "x ".repeat(63));
    let s = command.repeat(200);
    let begin = std::time::Instant::now();
    assert_eq!(p.analyze_script(&s, Dialect::Bash), Outcome::Abstain);
    let elapsed = begin.elapsed();
    eprintln!("long prefix matching: {elapsed:?}");
    assert!(elapsed < std::time::Duration::from_secs(2));
}

pub(super) fn nested_rows() -> Vec<(String, Dialect)> {
    let mut rows = Vec::new();
    let mut s = "echo ok".to_owned();
    let mut shells = 0;
    loop {
        let next = format!("sh -c '{}'", s.replace('\'', "'\\''"));
        if next.len() > MAX_COMMAND_BYTES {
            break;
        }
        s = next;
        shells += 1;
    }
    assert!(shells >= 10);
    rows.push((s, Dialect::Posix));
    let mut s = "echo ok".to_owned();
    let mut quotes = 0;
    loop {
        let next = format!("echo `{}`", s.replace('\\', "\\\\").replace('`', "\\`"));
        if next.len() > MAX_COMMAND_BYTES {
            break;
        }
        s = next;
        quotes += 1;
    }
    assert!(quotes >= 10);
    rows.push((s, Dialect::Posix));
    let mut s = "echo ok".to_owned();
    let mut levels = 0;
    loop {
        let next = if levels % 2 == 0 {
            format!("echo $({s})")
        } else {
            format!("echo `{}`", s.replace('\\', "\\\\").replace('`', "\\`"))
        };
        if next.len() > MAX_COMMAND_BYTES {
            break;
        }
        s = next;
        levels += 1;
    }
    assert!(levels >= 20);
    rows.push((s, Dialect::Posix));
    let fragments = generated_corpus(0x0bad_5eed, 2000);
    let mut mixed = String::with_capacity(MAX_COMMAND_BYTES);
    for fragment in fragments.iter().cycle() {
        if mixed.len() + fragment.len() + 1 > MAX_COMMAND_BYTES {
            mixed.push_str(&" ".repeat(MAX_COMMAND_BYTES - mixed.len()));
            break;
        }
        mixed.push_str(fragment);
        mixed.push('\n');
    }
    assert_eq!(mixed.len(), MAX_COMMAND_BYTES);
    for d in [Dialect::Posix, Dialect::Bash] {
        rows.push((mixed.clone(), d));
    }
    rows
}
