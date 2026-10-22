use super::*;
use serde_json::{Value, json};

#[test]
fn unbound_arguments_are_not_inspected() {
    assert_eq!(policy().analyze("Shell", &json!("\0")), Outcome::Abstain);
}
#[test]
fn invalid_field_shapes() {
    for value in [
        json!({"cmd":null}),
        json!({"cmd":1}),
        json!({"cmd":["x"]}),
        json!({"command":"blocked"}),
        json!({"cmd":"\0"}),
        json!({"cmd":"", "x":"\0"}),
        json!({"cmd":"", "\0":"x"}),
        json!([{"cmd":"blocked"}]),
    ] {
        assert_eq!(
            policy().analyze("shell", &value),
            Outcome::InvalidCommand,
            "{value}"
        );
    }
}
#[test]
fn command_bytes_boundary() {
    assert_eq!(
        policy().analyze("shell", &json!({"cmd":"x".repeat(4097)})),
        Outcome::AnalysisLimit
    );
}
#[test]
fn json_depth_boundary() {
    let mut nested = json!(1);
    for _ in 1..16 {
        nested = json!([nested]);
    }
    let p = policy();
    assert_eq!(
        p.analyze("shell", &json!({"cmd":"", "x":nested.clone()})),
        Outcome::Abstain
    );
    assert_eq!(
        p.analyze("shell", &json!({"cmd":"", "x":[nested]})),
        Outcome::AnalysisLimit
    );
}
#[test]
fn json_nodes_include_keys() {
    let mut o = options();
    o.limits.max_json_nodes = 5;
    let p = Policy::new(o).unwrap();
    assert_eq!(
        p.analyze("shell", &json!({"cmd":"", "x":null})),
        Outcome::Abstain
    );
    assert_eq!(
        p.analyze("shell", &json!({"cmd":"", "x":[null]})),
        Outcome::AnalysisLimit
    );
}
#[test]
fn unknown_siblings_are_walked() {
    assert_eq!(
        policy().analyze("shell", &json!({"cmd":"", "x":{"a":[null, "\0"]}})),
        Outcome::InvalidCommand
    );
    assert_eq!(
        policy().analyze("shell", &Value::Null),
        Outcome::InvalidCommand
    );
}
#[test]
fn bound_scripts_and_literal_keys() {
    for (value, expected) in [
        (json!({"cmd":"echo ok"}), Outcome::Abstain),
        (json!({"cmd":"blocked"}), Outcome::RuleMatch),
        (
            json!({"cmd":"blocked", "$crabber_prepare_error":"x"}),
            Outcome::RuleMatch,
        ),
    ] {
        assert_eq!(policy().analyze("shell", &value), expected);
    }
    let mut o = options();
    o.bindings = vec![Binding {
        tool_name: "custom".into(),
        command_field: "literal.key".into(),
        dialect: Dialect::Bash,
    }];
    assert_eq!(
        Policy::new(o)
            .unwrap()
            .analyze("custom", &json!({"literal.key":"echo <(blocked)"})),
        Outcome::RuleMatch
    );
}

#[test]
fn json_text_bytes_bound_sibling_work() {
    let mut o = options();
    o.limits.max_command_bytes = 10;
    o.limits.max_analysis_bytes = 20;
    let p = Policy::new(o).unwrap();
    assert_eq!(
        p.analyze("shell", &json!({"cmd":"", "x":"a".repeat(16)})),
        Outcome::Abstain
    );
    assert_eq!(
        p.analyze("shell", &json!({"cmd":"", "x":"a".repeat(17)})),
        Outcome::AnalysisLimit
    );
    assert_eq!(
        p.analyze(
            "shell",
            &json!({"cmd":"", "x":["a".repeat(8),"a".repeat(9)]})
        ),
        Outcome::AnalysisLimit
    );
    let mut obj = serde_json::Map::new();
    obj.insert("cmd".into(), json!(""));
    obj.insert("a".repeat(18), json!(null));
    assert_eq!(
        p.analyze("shell", &Value::Object(obj)),
        Outcome::AnalysisLimit
    );
    assert_eq!(
        p.analyze("unbound", &json!({"x":"a".repeat(100)})),
        Outcome::Abstain
    );
}
