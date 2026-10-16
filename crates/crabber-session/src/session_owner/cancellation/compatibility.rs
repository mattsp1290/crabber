//! The additive optional field preserves uncancelled data and refuses old stripping.
use super::*;
use crate::session_owner::{audit::RevisionState, record::Record};
use serde::Deserialize;
use serde_json::Value;
// These model the old strict carrier's fields without knowing the new member.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
#[allow(dead_code)]
struct OldRecord {
    version: Value,
    snapshot: OldSnapshot,
    origin: Value,
    preparation: Option<Value>,
    parent: Value,
    sequence: Value,
    owner: Value,
    token: Value,
    lease_until: Value,
    ever_began: Value,
    resolver: Value,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
#[allow(dead_code)]
struct OldSnapshot {
    binding: Value,
    target: Value,
    state: Value,
    termination: Value,
    created_at: Value,
    updated_at: Value,
}
#[derive(Deserialize)]
enum OldOperation {
    Reserve,
    Begin,
    Renew,
    Join,
    MarkUncertain,
    ReclaimUnstarted,
    ResolveJoined,
}
#[test]
fn cancellation_preserves_old_uncancelled_shape_and_fails_closed_in_old_readers() {
    let request = crate::session_owner_contract::reservation(crabber_core::SessionId::new());
    let mut record = Record::reserve(&request, None, time::OffsetDateTime::UNIX_EPOCH).unwrap();
    for version in [1, 2] {
        record.version = version;
        let value = serde_json::to_value(&record).unwrap();
        assert!(value.get("cancellation").is_none());
        assert!(value["snapshot"].get("cancellation").is_none());
        let _: OldRecord = serde_json::from_value(value.clone()).unwrap();
        let decoded: Record = serde_json::from_value(value).unwrap();
        assert!(decoded == record);
        decoded.validate().unwrap();
    }
    let cancel = RequestSessionOwnerCancel {
        id: crabber_core::SessionOwnerOperationId::new(),
        session_id: request.binding.session_id,
        target: record.snapshot.target.clone(),
        expected_revision: None,
        claim: SessionOwnerResolver {
            resolver_id: "host".into(),
            authority_claim: "cancel".into(),
        },
    };
    record
        .request_cancel(
            &cancel,
            RevisionState::Absent(1),
            time::OffsetDateTime::UNIX_EPOCH,
        )
        .unwrap();
    let value = serde_json::to_value(&record).unwrap();
    assert!(serde_json::from_value::<OldRecord>(value.clone()).is_err());
    assert!(serde_json::from_value::<OldSnapshot>(value["snapshot"].clone()).is_err());
    assert!(serde_json::from_value::<OldOperation>(serde_json::json!("RequestCancel")).is_err());
    assert!(serde_json::from_value::<OldOperation>(serde_json::json!("ObserveCancel")).is_err());
    let decoded: Record = serde_json::from_value(value).unwrap();
    assert!(decoded == record);
}
