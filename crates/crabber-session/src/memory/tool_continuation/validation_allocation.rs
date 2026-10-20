//! Validation borrows an actual native suspended checkpoint. Mandatory canonical
//! fingerprinting still allocates; only the redundant retained-batch copy is absent.
use super::{MemoryStore, OffsetDateTime};
use crate::tool_continuation_contract::fixture;
use crabber_core::{ManualClock, ToolCheckpoint};
use std::sync::Arc;

#[tokio::test]
async fn validation_does_not_clone_the_native_private_checkpoint() {
    let mut overhead = Vec::new();
    for size in [1024, 48 * 1024] {
        let store = MemoryStore::with_clock(Arc::new(ManualClock::new(OffsetDateTime::UNIX_EPOCH)));
        let mut f = fixture::fixture(&store).await;
        f.suspension.phases[0].checkpoint = ToolCheckpoint::new(
            "fixture",
            "1",
            serde_json::json!({"private": "x".repeat(size)}),
        )
        .unwrap();
        f.execution
            .suspend_tool_continuation(f.suspension.clone(), f.authority.clone())
            .await
            .unwrap();
        let state = store.state.lock().unwrap();
        let record = &state.tool_continuations[&f.suspension.continuation.id];
        let fingerprint = allocation_counter::measure(|| {
            record
                .batch
                .fingerprint(&record.session_id, &record.run_id)
                .unwrap();
        });
        let validation = allocation_counter::measure(|| record.validate().unwrap());
        overhead.push(
            validation
                .bytes_total
                .saturating_sub(fingerprint.bytes_total),
        );
        assert_eq!(
            record.batch.phases[0].checkpoint,
            f.suspension.phases[0].checkpoint
        );
    }
    assert!(
        overhead[1] <= overhead[0] + 8192,
        "validation recopied payload beyond mandatory fingerprint: {overhead:?}"
    );
}

#[tokio::test]
async fn borrowed_questions_keep_exact_native_order_and_expiry() {
    let store = MemoryStore::with_clock(Arc::new(ManualClock::new(OffsetDateTime::UNIX_EPOCH)));
    let mut f = fixture::fixture(&store).await;
    f.suspension.phases[0].interactions[0].description = crabber_core::InteractionDescription::new(
        "first",
        crabber_core::InteractionKind::UserInput,
        "Choose",
        serde_json::json!({"type":"string"}),
        crabber_core::AuthorityRequirement::new("operator", "1").unwrap(),
        Some(100),
    )
    .unwrap();
    f.execution
        .suspend_tool_continuation(f.suspension.clone(), f.authority.clone())
        .await
        .unwrap();
    let state = store.state.lock().unwrap();
    let original = &state.tool_continuations[&f.suspension.continuation.id];
    original.validate().unwrap();
    assert_eq!(
        original.questions.len(),
        2,
        "actual fixture retains both questions"
    );
    assert_eq!(
        original.questions[0].view.expires_at,
        Some(OffsetDateTime::UNIX_EPOCH + time::Duration::milliseconds(100))
    );
    for case in 0..6 {
        let mut changed = original.clone();
        match case {
            0 => changed.questions.swap(0, 1),
            1 => {
                changed.questions.pop();
            }
            2 => changed.questions.push(changed.questions[0].clone()),
            3 => changed.questions[0].view.call_id = crabber_core::ToolCallId::new(),
            4 => {
                changed.questions[0].view.description =
                    changed.questions[1].view.description.clone();
            }
            _ => changed.questions[0].view.expires_at = None,
        }
        assert_eq!(
            changed.validate().unwrap_err(),
            crate::ToolContinuationError::UnknownStoreOutcome
        );
    }
    original.validate().unwrap();
}
