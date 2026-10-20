use super::*;
use crate::execution_binding_contract::fixture;

#[test]
fn limits_count_encoded_bytes_and_reject_excessive_nesting_before_hashing() {
    let mut bound = fixture(
        &SessionId::new(),
        time::OffsetDateTime::UNIX_EPOCH,
        "backend",
        'b',
    );
    // Under the raw byte budget, over the encoded budget because JSON escapes
    // every quote. The bounded writer must stop without producing a record.
    bound.binding.request.title = "\"".repeat(MAX_BINDING_BYTES / 2);
    assert!(matches!(
        bound.binding.validate(&bound.keyed.request),
        Err(StoreError::Limit(_))
    ));
    bound.binding.request.title = "private-title".into();
    let mut nested = Value::Null;
    for _ in 0..70 {
        nested = serde_json::json!([nested]);
    }
    bound.binding.runtime_semantics[9] = nested;
    assert!(matches!(
        bound.binding.validate(&bound.keyed.request),
        Err(StoreError::Limit(_))
    ));
}
#[test]
fn reserved_components_must_bind_exactly_one_descriptor_and_behavior_version() {
    let mut bound = fixture(
        &SessionId::new(),
        time::OffsetDateTime::UNIX_EPOCH,
        "backend",
        'b',
    );
    let values = bound.binding.runtime_semantics[12].as_array_mut().unwrap();
    values.push(values[0].clone());
    bound.binding.config_hash =
        InputFingerprint::new(admission_config_hash(&bound.binding.runtime_semantics)).unwrap();
    bound.keyed.request.config_hash = bound.binding.config_hash.as_str().into();
    assert_eq!(
        bound.binding.validate(&bound.keyed.request).unwrap_err(),
        StoreError::AdmissionConflict
    );
    let mut bound = fixture(
        &SessionId::new(),
        time::OffsetDateTime::UNIX_EPOCH,
        "backend",
        'b',
    );
    bound.binding.behavior_fingerprint = InputFingerprint::new("e".repeat(64)).unwrap();
    assert_eq!(
        bound.binding.validate(&bound.keyed.request).unwrap_err(),
        StoreError::AdmissionConflict
    );
}
