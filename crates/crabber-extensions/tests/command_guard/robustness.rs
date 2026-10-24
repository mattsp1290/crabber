use super::*;

#[test]
fn never_panics_and_is_deterministic() {
    let p = policy();
    for script in generated_corpus(0x0bad_5eed, 2000) {
        let mut rows = vec![script.as_str()];
        rows.extend(
            (0..script.len())
                .step_by(97)
                .filter(|i| script.is_char_boundary(*i))
                .map(|i| &script[..i]),
        );
        for s in rows {
            for d in [Dialect::Posix, Dialect::Bash] {
                let first = std::panic::catch_unwind(|| p.analyze_script(s, d))
                    .unwrap_or_else(|_| panic!("analysis panicked on {s:?}"));
                assert_eq!(first, p.analyze_script(s, d));
            }
        }
    }
}
#[test]
fn reference_fuzz_seeds() {
    let seeds = vec![
        "echo ok".into(),
        "blocked".into(),
        "git \"$X\"".into(),
        "echo '\u{fffd}'".into(),
        "echo \0".into(),
        "echo \"$(blocked)\"".into(),
        "cat <<EOF\n$(blocked)\nEOF".into(),
        format!("{}echo ok{}", "(".repeat(1000), ")".repeat(1000)),
        format!("{}echo ok", "env ".repeat(50)),
        "sh -c 'bash -c \"blocked\"'".into(),
        "printf -v 'a[$(blocked)]' x".into(),
        "echo ${x@P}".into(),
        "echo $((a[$(blocked)]))".into(),
        "'".repeat(4000),
        "[blo'c'ked]".into(),
        "x".repeat(4097),
    ];
    for s in seeds {
        for d in [Dialect::Posix, Dialect::Bash] {
            let mut o = options();
            o.bindings[0].dialect = d;
            o.bindings[1].dialect = d;
            let p = Policy::new(o).unwrap();
            let value = serde_json::json!({"cmd":s});
            let first = p.analyze("shell", &value);
            assert_eq!(first, p.analyze("shell", &value));
            if s.len() > p.limits().max_command_bytes {
                assert_eq!(first, Outcome::AnalysisLimit);
            }
        }
    }
    for raw in [
        "{\"cmd\":\"echo ok\"}",
        "{\"cmd\":\"blocked\",\"cmd\":\"echo ok\"}",
        "{\"cmd\":null}",
        "{\"cmd\":\"\\u0000\"}",
        "{\"cmd\":\"echo ok\",\"extra\":{\"x\":1,\"x\":2}}",
    ] {
        let value: serde_json::Value = serde_json::from_str(raw).unwrap();
        assert_eq!(
            policy().analyze("shell", &value),
            policy().analyze("shell", &value)
        );
    }
}
#[test]
fn no_input_retained() {
    let p = policy();
    let o = options();
    let hash = p.config_hash().to_owned();
    for s in generated_corpus(0xdead_beef, 1000) {
        let _ = p.analyze_script(&s, Dialect::Bash);
    }
    assert_eq!(p.config_hash(), hash);
    assert_eq!(p.rules(), o.rules);
    assert_eq!(p.limits(), &o.limits);
    assert_eq!(p.bindings(), policy().bindings());
}
#[test]
fn send_sync() {
    fn assert_traits<T: Send + Sync>() {}
    assert_traits::<Policy>();
}
#[test]
fn committed_generated_rows_match_generator() {
    let rows: Vec<serde_json::Value> =
        serde_json::from_str(include_str!("fixtures/corpus-input.json")).unwrap();
    let tail = &rows[rows.len() - 10000..];
    for (script, pair) in generated_corpus(0x5eed_c0de, 5000)
        .iter()
        .zip(tail.as_chunks::<2>().0)
    {
        assert_eq!(pair[0]["script"], *script);
        assert_eq!(pair[1]["script"], *script);
        assert_eq!(pair[0]["dialect"], "posix");
        assert_eq!(pair[1]["dialect"], "bash");
    }
}
