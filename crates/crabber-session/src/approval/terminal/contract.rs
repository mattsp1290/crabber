//! Native cancellation/expiry publications, not synthetic terminal Runs.
use super::*;
use crate::{Store, attempt::terminal::TerminalFacts};
use crabber_core::{Clock, ManualClock, Run};
fn facts(run: &Run) -> TerminalFacts<'_> {
    TerminalFacts {
        session: &run.session_id,
        run: &run.id,
        status: run.status,
        error: run.error.as_deref(),
        usage: &run.usage,
    }
}
pub(crate) async fn run(store: &impl Store, clock: &ManualClock) {
    for expire in [false, true] {
        clock.set(OffsetDateTime::UNIX_EPOCH);
        let mut fixture = crate::approval_contract::fixture::staged(store, clock).await;
        fixture.batch.usage.input_tokens = 3;
        fixture.batch.usage.output_tokens = 5;
        fixture
            .execution
            .suspend_for_approval(fixture.batch.clone(), fixture.owner.clone())
            .await
            .unwrap();
        let request = crate::approval_contract::termination::request(store, &fixture, expire).await;
        if expire {
            clock.set(clock.now() + time::Duration::seconds(2));
        }
        let receipt = store.terminate_approval(request.clone()).await.unwrap();
        assert_eq!(
            store.terminate_approval(request.clone()).await.unwrap(),
            receipt
        );
        let current = store.get_run(&fixture.run.id).await.unwrap().unwrap();
        let events = store
            .list_events(&current.session_id, None, 1000)
            .await
            .unwrap();
        let event = events
            .iter()
            .find(|event| event.cursor == Some(receipt.event_cursor))
            .unwrap();
        assert!(candidate(event));
        let captured = settlement(&receipt, event).unwrap();
        let original = events
            .iter()
            .find(|event| event.cursor == captured.cursor)
            .unwrap();
        assert_eq!(captured, *original);
        verify_scope(facts(&current), &receipt, event, original, &fixture.batch).unwrap();
        check_rejection(&current, &receipt, event, original, &fixture.batch);
        let mut legacy = event.clone();
        legacy.payload["version"] = serde_json::json!(1);
        legacy.payload.as_object_mut().unwrap().remove("terminal");
        crate::approval::verify_receipt(&receipt, &legacy).unwrap();
        assert!(!candidate(&legacy));
        assert!(settlement(&receipt, &legacy).is_err());
    }
}
fn check_rejection(
    current: &Run,
    receipt: &ApprovalReceipt,
    event: &EventRecord,
    original: &EventRecord,
    batch: &crate::ApprovalSuspension,
) {
    let mut changed = facts(current);
    changed.error = Some("coherent mutable error forgery");
    assert!(verify_scope(changed, receipt, event, original, batch).is_err());
    let mut usage = current.usage.clone();
    usage.input_tokens += 1;
    changed = facts(current);
    changed.usage = &usage;
    assert!(verify_scope(changed, receipt, event, original, batch).is_err());
    coherent_rewrite(current, receipt, event, original, batch, true);
    coherent_rewrite(current, receipt, event, original, batch, false);
    let mut relinked = original.clone();
    relinked.cursor = Some(crabber_core::EventCursor(original.cursor.unwrap().0 - 1));
    assert!(verify_scope(facts(current), receipt, event, &relinked, batch).is_err());
    let mut changed_source = event.clone();
    changed_source.payload["terminal"]["request"]["expected_revision"] = serde_json::json!(0);
    assert!(settlement(receipt, &changed_source).is_err());
    let mut wrong_frame = event.clone();
    wrong_frame.payload["terminal"]["settlement"]["payload"]["status"] =
        serde_json::json!("completed");
    assert!(crate::approval::verify_receipt(receipt, &wrong_frame).is_err());
    let mut oversized = event.clone();
    oversized.payload["terminal"]["settlement"]["payload"]["padding"] =
        serde_json::json!("x".repeat(32768));
    assert!(settlement(receipt, &oversized).is_err());
}

fn coherent_rewrite(
    current: &Run,
    receipt: &ApprovalReceipt,
    event: &EventRecord,
    original: &EventRecord,
    batch: &crate::ApprovalSuspension,
    usage: bool,
) {
    let mut forged_run = current.clone();
    let mut forged_frame = original.clone();
    if usage {
        forged_run.usage.input_tokens += 1;
        forged_frame.payload["usage"] = serde_json::to_value(&forged_run.usage).unwrap();
        forged_frame.payload["input_tokens"] = serde_json::json!(forged_run.usage.input_tokens);
    } else {
        forged_frame.turn_id = Some(crabber_core::TurnId("forged-original-turn".into()));
    }
    let mut forged_audit = event.clone();
    forged_audit.payload["terminal"]["settlement"] = serde_json::to_value(&forged_frame).unwrap();
    // Neither the unchanged receipt nor frame canonicalization authenticates
    // usage/presentation. The independently frozen batch must reject this.
    crate::approval::verify_receipt(receipt, &forged_audit).unwrap();
    assert!(
        verify_scope(
            facts(&forged_run),
            receipt,
            &forged_audit,
            &forged_frame,
            batch
        )
        .is_err()
    );
}
