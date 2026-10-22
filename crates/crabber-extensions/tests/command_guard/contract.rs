use super::*;
type Mutation = fn(&mut Options);

#[test]
fn canonical_order_and_default_dialect_do_not_change_hash() {
    let mut original = options();
    let p = Policy::new(original.clone()).unwrap();
    original.rules.reverse();
    original.bindings.reverse();
    assert_eq!(
        p.config_hash(),
        Policy::new(original.clone()).unwrap().config_hash()
    );
    original.rules[0].executable = "other".into();
    assert_eq!(p.rules(), rules());
    assert_eq!(Dialect::default(), Dialect::Posix);
}
#[test]
fn accessors_report_canonical_state() {
    let p = policy();
    assert_eq!(p.bindings()[0].tool_name, "background_job_start");
    assert_eq!(p.rules(), rules());
    assert_eq!(p.limits(), &limits());
    assert!(p.binding("shell").is_some());
    assert!(p.binding("Shell").is_none());
    assert!(p.binding("standard.shell").is_none());
}
#[test]
fn policy_debug_omits_rules() {
    let text = format!("{:?}", policy());
    assert!(!text.contains("blocked"));
    assert!(!text.contains("git-push"));
}
#[test]
fn fixed_outcomes() {
    let rows = [
        (Outcome::Abstain, None),
        (Outcome::RuleMatch, Some("rule-match")),
        (Outcome::InvalidCommand, Some("invalid-command")),
        (Outcome::Unanalysable, Some("unanalysable-command")),
        (Outcome::AnalysisLimit, Some("analysis-limit")),
    ];
    for (o, code) in rows {
        assert_eq!(o.code(), code);
        assert_eq!(o.denies(), code.is_some());
    }
}
fn error(o: Options, code: &str) {
    assert_eq!(
        Policy::new(o).unwrap_err().to_string(),
        format!("extension plan failed: extension configuration invalid: command-guard-{code}")
    );
}
#[test]
fn validation_codes() {
    let cases: Vec<(&str, Mutation)> = vec![
        ("count", |o| o.bindings.clear()),
        ("count", |o| o.rules.clear()),
        ("binding-size", |o| {
            o.bindings[0].tool_name = "a".repeat(4097)
        }),
        ("binding-size", |o| {
            o.bindings[0].command_field = "a".repeat(4097)
        }),
        ("binding", |o| o.bindings[0].tool_name = "bad name".into()),
        ("binding", |o| o.bindings[0].command_field = " \t".into()),
        ("binding", |o| o.bindings[0].command_field = "\0".into()),
        ("rule-size", |o| {
            o.rules[0].arg_prefix = vec!["a".into(); 17]
        }),
        ("rule-size", |o| {
            o.rules[0].arg_prefix = vec!["a".repeat(2049)]
        }),
        ("rule-token", |o| o.rules[0].arg_prefix = vec!["\0".into()]),
        ("rule", |o| o.rules[0].id = "bad name".into()),
        ("rule", |o| o.rules[0].executable = "a/b".into()),
        ("rule", |o| o.rules[0].executable = "a\\b".into()),
        ("rule", |o| o.rules[0].executable = ".".into()),
        ("rule", |o| o.rules[0].executable = "..".into()),
        ("rule", |o| o.rules[0].executable = "\0".into()),
        ("duplicate-binding", |o| {
            o.bindings.push(o.bindings[0].clone())
        }),
        ("duplicate-rule", |o| o.rules.push(o.rules[0].clone())),
    ];
    for (code, mutate) in cases {
        let mut o = options();
        mutate(&mut o);
        error(o, code);
    }
    let mut o = options();
    o.rules[0].id = "..".into();
    assert!(Policy::new(o).is_ok()); // Exact documented identifier alphabet permits dots.
    let mut o = options();
    o.limits.max_analysis_bytes = 4095;
    error(o, "limits");
}
macro_rules! limit_cases {
    ($($field:ident => $cap:ident),* $(,)?) => {
        #[test]
        fn every_limit_is_bounded_and_changes_hash() {
            let hash = policy().config_hash().to_owned();
            $(
                let mut o = options(); o.limits.$field = 0; error(o, "limits");
                let mut o = options(); o.limits.$field = $cap + 1; error(o, "limits");
                let mut o = options(); o.limits.$field += 1;
                assert_ne!(Policy::new(o).unwrap().config_hash(), hash, stringify!($field));
            )*
        }
    }
}
limit_cases!(max_bindings => MAX_BINDINGS, max_rules => MAX_RULES,
    max_rule_bytes => MAX_RULE_BYTES, max_prefix_args => MAX_PREFIX_ARGS,
    max_json_depth => MAX_JSON_DEPTH, max_json_nodes => MAX_JSON_NODES,
    max_command_bytes => MAX_COMMAND_BYTES, max_analysis_bytes => MAX_ANALYSIS_BYTES,
    max_ast_nodes => MAX_AST_NODES, max_ast_depth => MAX_AST_DEPTH,
    max_words => MAX_WORDS, max_word_bytes => MAX_WORD_BYTES,
    max_wrapper_depth => MAX_WRAPPER_DEPTH, max_in_flight => MAX_IN_FLIGHT);
#[test]
fn every_binding_and_rule_field_changes_hash() {
    let mutations: Vec<fn(&mut Options)> = vec![
        |o| o.bindings[0].tool_name.push('x'),
        |o| o.bindings[0].command_field.push('x'),
        |o| o.bindings[0].dialect = Dialect::Bash,
        |o| o.rules[0].id.push('x'),
        |o| o.rules[0].executable.push('x'),
        |o| o.rules[1].arg_prefix[0].push('x'),
    ];
    for mutate in mutations {
        let mut o = options();
        mutate(&mut o);
        assert_ne!(
            Policy::new(o).unwrap().config_hash(),
            policy().config_hash()
        );
    }
}

#[test]
fn hash_of_fixed_configuration_is_pinned() {
    assert_eq!(
        policy().config_hash(),
        "bd35b7b0e00cfe90521053621fe73c969aeb84af53e391bfaac1d748dc539495",
        "bump BEHAVIOR and update both pins in the same commit"
    );
}
#[test]
fn corpus_outcome_digest_is_pinned() {
    use sha2::{Digest, Sha256};
    let rows: Vec<serde_json::Value> =
        serde_json::from_str(include_str!("fixtures/corpus-input.json")).unwrap();
    let mut digest = Sha256::new();
    let p = policy();
    for row in rows {
        let d = row["dialect"].as_str().unwrap();
        let script = row["script"].as_str().unwrap();
        let outcome = p.analyze_script(
            script,
            if d == "bash" {
                Dialect::Bash
            } else {
                Dialect::Posix
            },
        );
        digest.update(d);
        digest.update([0]);
        digest.update(script);
        digest.update([0]);
        digest.update(outcome.code().unwrap_or("abstain"));
        digest.update(b"\n");
    }
    assert_eq!(
        format!("{:x}", digest.finalize()),
        "3257844740c0ec38130e6945411feb51003531043fd14898b6437e788b7a6fb1",
        "bump BEHAVIOR and update both pins in the same commit"
    );
}
#[test]
fn basename_index_does_not_change_matching() {
    let mut o = options();
    o.limits.max_rules = 256;
    o.rules = (0..200)
        .map(|i| Rule {
            id: format!("r{i:03}"),
            executable: "git".into(),
            arg_prefix: vec!["push".into()],
        })
        .collect();
    o.rules.extend((0..50).map(|i| Rule {
        id: format!("x{i:03}"),
        executable: format!("other{i}"),
        arg_prefix: vec![],
    }));
    let p = Policy::new(o).unwrap();
    for (s, want) in [
        ("git push", Outcome::RuleMatch),
        ("git status", Outcome::Abstain),
        ("git \"$x\"", Outcome::Unanalysable),
        ("other push", Outcome::Abstain),
    ] {
        assert_eq!(p.analyze_script(s, Dialect::Posix), want);
    }
}
