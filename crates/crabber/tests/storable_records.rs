//! Regression coverage for the public storable-record boundary.

use crabber::session::ensure_storable;
use serde_json::Value;

#[test]
fn deeply_nested_and_nul_bearing_records_are_rejected_before_persistence() {
    let mut accepted = false;
    let mut rejected = false;
    for depth in 100..=140 {
        let mut value = Value::Null;
        for _ in 0..depth {
            value = serde_json::json!([value]);
        }
        let text = serde_json::to_string(&value).unwrap();
        let reader_accepts = serde_json::from_str::<Value>(&text).is_ok();
        assert_eq!(ensure_storable(&value).is_ok(), reader_accepts);
        accepted |= reader_accepts;
        rejected |= !reader_accepts;
    }
    assert!(accepted && rejected);
    assert!(ensure_storable(&serde_json::json!({"value": "a\0b"})).is_err());
}
