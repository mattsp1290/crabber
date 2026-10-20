//! Fault-injection conformance for atomic replacements.
use super::{admit, contents};
use crate::{PublicStateError as Error, PublicStateMutation, Store};
use crabber_core::{ControlId, PublicStateContents};
use serde_json::json;
use time::OffsetDateTime;

/// Adapter test seams must fail immediately before COMMIT or withhold a successful
/// COMMIT acknowledgement. This does not authorize faults in a production store.
#[derive(Debug, Clone, Copy)]
#[repr(u8)]
pub enum FaultPoint {
    Off = 0,
    BeforeCommit = 1,
    AfterCommit = 2,
}
/// Verify rollback and unknown committed response reconciliation on the original key.
/// # Panics
/// Panics unless injected failures really occur and the complete contract holds.
pub async fn run_fault_contract(store: &impl Store, inject: impl Fn(FaultPoint)) {
    let admitted = admit(store, OffsetDateTime::now_utc()).await;
    let execution = store.execution(admitted.fence).await.unwrap();
    let before = store.public_state(&admitted.session.id).await.unwrap();
    let first = PublicStateMutation {
        id: ControlId::new(),
        expected_revision: before.revision,
        contents: contents(),
    };
    inject(FaultPoint::BeforeCommit);
    assert_eq!(
        execution
            .mutate_public_state(first.clone())
            .await
            .unwrap_err(),
        Error::UnknownStoreOutcome
    );
    inject(FaultPoint::Off);
    assert_eq!(
        store.public_state(&admitted.session.id).await.unwrap(),
        before
    );
    assert_eq!(
        store
            .lookup_public_state_mutation(&admitted.session.id, &first.id)
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        store
            .list_events(&admitted.session.id, None, 100)
            .await
            .unwrap()
            .len(),
        0
    );
    let committed = execution.mutate_public_state(first).await.unwrap();
    let second = PublicStateMutation {
        id: ControlId::new(),
        expected_revision: committed.revision,
        contents: PublicStateContents::new(json!({"accepted":true}), vec![]).unwrap(),
    };
    inject(FaultPoint::AfterCommit);
    assert_eq!(
        execution
            .mutate_public_state(second.clone())
            .await
            .unwrap_err(),
        Error::UnknownStoreOutcome
    );
    inject(FaultPoint::Off);
    let receipt = store
        .lookup_public_state_mutation(&admitted.session.id, &second.id)
        .await
        .unwrap()
        .unwrap();
    let after = store.public_state(&admitted.session.id).await.unwrap();
    assert_eq!(after.contents, second.contents);
    assert_eq!(after.revision, receipt.revision);
    assert_eq!(
        execution.mutate_public_state(second).await.unwrap(),
        receipt
    );
    assert_eq!(
        store.public_state(&admitted.session.id).await.unwrap(),
        after
    );
    assert_eq!(
        store
            .list_events(&admitted.session.id, None, 100)
            .await
            .unwrap()
            .len(),
        2
    );
}
