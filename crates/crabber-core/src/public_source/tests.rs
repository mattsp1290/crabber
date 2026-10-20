use super::*;
use serde_json::json;
fn operation(value: Value) -> PublicPatchOperation {
    PublicPatchOperation::new(value).unwrap()
}

#[test]
fn all_patch_operations_preserve_extensions_and_failed_patch_preserves_original() {
    let original = json!({"array":[1,2],"nested":{"null":null},"copy":0});
    let values = vec![
        json!({"op":"test","path":"/nested/null","value":null,"comment":"test"}),
        json!({"op":"add","path":"/array/1","value":3,"from":"legal-extra"}),
        json!({"op":"copy","from":"/nested","path":"/copied","comment":null}),
        json!({"op":"move","from":"/copy","path":"/moved","value":"legal-extra"}),
        json!({"op":"replace","path":"/array/0","value":null}),
        json!({"op":"remove","path":"/array/2","value":{"legal":null}}),
    ];
    let patch = values.iter().cloned().map(operation).collect::<Vec<_>>();
    assert_eq!(serde_json::to_value(&patch).unwrap(), Value::Array(values));
    assert_eq!(
        apply_public_patch(&original, &patch).unwrap(),
        json!({"array":[null,3],"nested":{"null":null},"copied":{"null":null},"moved":0})
    );
    let failed = [
        operation(json!({"op":"replace","path":"/array/0","value":8})),
        operation(json!({"op":"test","path":"/copy","value":1})),
    ];
    assert!(apply_public_patch(&original, &failed).is_err());
    assert_eq!(
        original,
        json!({"array":[1,2],"nested":{"null":null},"copy":0})
    );
    assert!(PublicPatchOperation::new(json!({"op":"add","path":"/missing-value"})).is_err());
}

#[test]
fn required_null_and_explicit_activity_retyping_retain_separate_state() {
    let original = PublicStateContents::new(json!({"private-unrelated":null}), vec![]).unwrap();
    let null = PublicSourceChange::StateSnapshot { state: Value::Null }
        .apply(&original)
        .unwrap();
    let encoded =
        serde_json::to_value(PublicSourceChange::StateSnapshot { state: Value::Null }).unwrap();
    assert!(encoded.get("state").unwrap().is_null());
    assert_eq!(null.state(), &Value::Null);
    let initial = PublicSourceChange::ActivitySnapshot {
        activity: PublicActivity::new("same", "progress", json!({"value":0,"null":null})).unwrap(),
    }
    .apply(&null)
    .unwrap();
    let changed = PublicSourceChange::ActivityDelta {
        activity_id: "same".into(),
        activity_type: "progress".into(),
        delta: vec![operation(json!({"op":"replace","path":"/value","value":2}))],
    }
    .apply(&initial)
    .unwrap();
    assert_eq!(
        changed.activities()[0].content(),
        &json!({"value":2,"null":null})
    );
    let wrong = PublicSourceChange::ActivityDelta {
        activity_id: "same".into(),
        activity_type: "other".into(),
        delta: vec![],
    };
    assert!(wrong.apply(&changed).is_err());
    let retyped = PublicSourceChange::ActivitySnapshot {
        activity: PublicActivity::new("same", "finished", json!({"complete":true})).unwrap(),
    }
    .apply(&changed)
    .unwrap();
    assert_eq!(retyped.activities()[0].activity_type(), "finished");
    assert_eq!(retyped.state(), &Value::Null);
}

#[test]
fn patch_preflights_borrowed_state_and_each_copy_without_mutating_original() {
    let oversized = serde_json::Value::String("x".repeat(crate::MAX_PUBLIC_STATE_BYTES));
    assert!(apply_public_patch(&oversized, &[]).is_err());
    let mut deep = serde_json::Value::Null;
    for _ in 0..80 {
        deep = serde_json::json!([deep]);
    }
    assert!(apply_public_patch(&deep, &[]).is_err());
    let original = serde_json::json!({"text":"x".repeat(64*1024)});
    let copy = PublicPatchOperation::new(serde_json::json!({"op":"copy","from":"","path":"/copy"}))
        .unwrap();
    // Four copies remain a finite fixture even in the previous batch engine;
    // the new implementation refuses before performing the third copy.
    assert!(apply_public_patch(&original, &vec![copy; 4]).is_err());
    assert_eq!(original, serde_json::json!({"text":"x".repeat(64*1024)}));
}
