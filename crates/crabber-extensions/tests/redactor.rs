use crabber::extension::Extension;
use crabber_extensions::tool_result_redactor::{
    Limits, Options, PLACEHOLDER, Pattern, ToolResultRedactor,
};
use serde_json::{Value, json};

fn options() -> Options {
    Options {
        order: 1_000_000,
        excluded_tools: vec![],
        additional_patterns: vec![],
        limits: Limits {
            max_field_bytes: 4096,
            max_total_bytes: 8192,
            max_depth: 16,
            max_nodes: 100,
            max_matches_per_field: 8,
            max_patterns: 8,
            max_pattern_bytes: 256,
            max_in_flight: 4,
        },
    }
}
#[test]
fn builtin_catalog_boundaries_and_nested_values() {
    let r = ToolResultRedactor::new(options()).unwrap();
    let github = "ghp_0123456789abcdef";
    let stateless = "ghs_123_abc.def.ghi";
    for secret in [
        github,
        stateless,
        "Authorization: Bearer token.value==",
        "-----BEGIN PRIVATE KEY-----\nsynthetic\n-----END PRIVATE KEY-----",
        "-----BEGIN ENCRYPTED PRIVATE KEY-----\nsynthetic\n-----END ENCRYPTED PRIVATE KEY-----",
    ] {
        let output = r.redact("read", json!({"result": [secret, {"safe": 42}]}));
        assert_eq!(output["result"][0], PLACEHOLDER, "{secret}");
        assert_eq!(output["result"][1], json!({"safe":42}));
    }
    assert_eq!(
        r.redact("read", json!(format!("prefix{github}"))),
        json!(format!("prefix{github}"))
    );
    assert_eq!(
        r.redact("read", json!(format!("({github})"))),
        json!(format!("({PLACEHOLDER})"))
    );
    assert_eq!(
        r.redact("read", json!({github: "value"})),
        json!(PLACEHOLDER)
    );
}
#[test]
fn explicit_exclusion_and_custom_overlap() {
    let mut o = options();
    o.excluded_tools = vec!["trusted".into()];
    o.additional_patterns = vec![
        Pattern {
            id: "first".into(),
            expression: "abc".into(),
        },
        Pattern {
            id: "second".into(),
            expression: "bcd".into(),
        },
    ];
    let r = ToolResultRedactor::new(o).unwrap();
    assert_eq!(r.redact("read", json!("xabcdx")), json!("x[REDACTED]x"));
    assert_eq!(r.redact("trusted", json!("abcd")), json!("abcd"));
    assert_eq!(r.redact("trusted-other", json!("abcd")), json!(PLACEHOLDER));
}
#[test]
fn budget_exhaustion_never_passes_through_a_partial_scan() {
    let mut o = options();
    o.limits.max_depth = 2;
    o.limits.max_nodes = 5;
    o.limits.max_field_bytes = 32;
    o.limits.max_total_bytes = 64;
    o.limits.max_matches_per_field = 1;
    o.additional_patterns.push(Pattern {
        id: "test".into(),
        expression: "secret".into(),
    });
    let r = ToolResultRedactor::new(o).unwrap();
    for value in [
        json!([[["secret"]]]),
        json!([1, 2, 3, 4, 5]),
        json!(["x".repeat(32), "y".repeat(32), "z"]),
    ] {
        assert_eq!(r.redact("read", value), json!(PLACEHOLDER));
    }
    assert_eq!(
        r.redact("read", json!(["secret secret", "safe"])),
        json!([PLACEHOLDER, "safe"])
    );
    assert_eq!(
        r.redact("read", json!(["x".repeat(33), "safe"])),
        json!([PLACEHOLDER, "safe"])
    );
    assert_eq!(
        r.redact("read", json!({"bad\0key": "safe"})),
        json!(PLACEHOLDER)
    );
}
#[test]
fn unchanged_json_types_and_idempotence() {
    let r = ToolResultRedactor::new(options()).unwrap();
    let safe = json!({"n":1.25,"b":true,"empty":null,"list":["hi"]});
    assert_eq!(r.redact("read", safe.clone()), safe);
    let protected = r.redact("read", json!("Authorization: Bearer fixture"));
    assert_eq!(r.redact("read", protected.clone()), protected);
    assert_eq!(r.redact("read", Value::Null), Value::Null);
}
#[test]
fn policy_identity_and_sanitized_validation() {
    let mut a = options();
    a.excluded_tools = vec!["b".into(), "a".into(), "a".into()];
    let mut b = options();
    b.excluded_tools = vec!["a".into(), "b".into()];
    assert_eq!(
        ToolResultRedactor::new(a).unwrap().config_hash(),
        ToolResultRedactor::new(b).unwrap().config_hash()
    );
    let mut changed = options();
    changed.order += 1;
    assert_ne!(
        ToolResultRedactor::new(options()).unwrap().config_hash(),
        ToolResultRedactor::new(changed).unwrap().config_hash()
    );
    for expression in ["(", "a*", r"\b", "sensitive-host-pattern("] {
        let mut o = options();
        o.additional_patterns.push(Pattern {
            id: "host-pattern".into(),
            expression: expression.into(),
        });
        let error = ToolResultRedactor::new(o).err().unwrap().to_string();
        assert!(!error.contains(expression));
    }
}

#[test]
fn limits_always_leave_room_for_fixed_fail_closed_placeholder() {
    for field in [true, false] {
        let mut o = options();
        if field {
            o.limits.max_field_bytes = PLACEHOLDER.len() - 1;
        } else {
            o.limits.max_total_bytes = PLACEHOLDER.len() - 1;
        }
        assert!(ToolResultRedactor::new(o).is_err());
    }
}
