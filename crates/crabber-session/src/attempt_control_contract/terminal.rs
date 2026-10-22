//! Actual retirement sources, independent of a mutable final Run label.
use super::{
    AdmitOutcome, AttemptControlOutcome, AttemptError, AttemptReceipt, AttemptRetirementOutcome,
    RunStatus, Store, fresh, retirement, uncertainty,
};
use crabber_core::{EventKind, EventRecord, Run};
pub(crate) struct Fixture {
    pub admitted: AdmitOutcome,
    pub receipt: AttemptReceipt,
    pub audit: EventRecord,
    pub older: EventRecord,
}
pub(crate) async fn fixture(store: &dyn Store, outcome: AttemptRetirementOutcome) -> Fixture {
    let (admitted, execution, _, planned) = crate::phase_contract::fixture(store).await;
    let status = match outcome {
        AttemptRetirementOutcome::Failed => RunStatus::Failed,
        AttemptRetirementOutcome::Interrupted => RunStatus::Interrupted,
    };
    execution
        .append_event(crate::attempt_control::terminal_event_scope(
            &admitted.session.id,
            &admitted.run.id,
            status,
            &admitted.run.usage,
            admitted.run.created_at,
            None,
        ))
        .await
        .unwrap();
    let request = uncertainty(
        &admitted,
        planned.command.target().attempt.clone(),
        store.session_revision(&admitted.session.id).await.unwrap(),
    );
    let marked = fresh(store.mark_attempt_uncertain(request.clone()).await.unwrap());
    let mut request = retirement(&request, marked.revision);
    request.outcome = outcome;
    let receipt = fresh(store.retire_attempt(request.clone()).await.unwrap());
    let events = store
        .list_events(&admitted.session.id, None, 100)
        .await
        .unwrap();
    let audit = events
        .iter()
        .find(|event| event.cursor == Some(receipt.event_cursor))
        .unwrap()
        .clone();
    assert_eq!(audit.payload["version"], 5);
    assert_eq!(
        audit.payload["request"],
        serde_json::json!({"Retire":request})
    );
    let source = crate::attempt::terminal::settlement(&receipt, &audit).unwrap();
    let original = events
        .iter()
        .find(|event| event.cursor == source.cursor)
        .unwrap();
    let current = store.get_run(&admitted.run.id).await.unwrap().unwrap();
    verify(&current, &receipt, &audit, original).unwrap();
    assert_eq!(
        verify_changed(&current, &receipt, &audit, original),
        AttemptError::UnknownStoreOutcome
    );
    assert_eq!(
        store.retire_attempt(request).await.unwrap(),
        AttemptControlOutcome::Replayed(receipt.clone())
    );
    Fixture {
        admitted,
        receipt,
        audit,
        older: events
            .into_iter()
            .find(|event| event.kind == EventKind::RunSettled)
            .unwrap(),
    }
}
fn verify(
    run: &Run,
    receipt: &AttemptReceipt,
    audit: &EventRecord,
    source: &EventRecord,
) -> Result<(), AttemptError> {
    crate::attempt::terminal::verify_scope(
        crate::attempt::terminal::TerminalFacts {
            session: &run.session_id,
            run: &run.id,
            status: run.status,
            error: run.error.as_deref(),
            usage: &run.usage,
        },
        receipt,
        audit,
        source,
    )
}
fn verify_changed(
    run: &Run,
    receipt: &AttemptReceipt,
    audit: &EventRecord,
    source: &EventRecord,
) -> AttemptError {
    crate::attempt::terminal::verify_scope(
        crate::attempt::terminal::TerminalFacts {
            session: &run.session_id,
            run: &run.id,
            status: run.status,
            error: None,
            usage: &run.usage,
        },
        receipt,
        audit,
        source,
    )
    .unwrap_err()
}
pub(super) async fn run(store: &dyn Store) {
    for outcome in [
        AttemptRetirementOutcome::Failed,
        AttemptRetirementOutcome::Interrupted,
    ] {
        let fixture = fixture(store, outcome).await;
        assert_eq!(
            fixture.receipt.attempt.session_id,
            fixture.admitted.session.id
        );
        assert!(
            fixture.older.cursor
                < crate::attempt::terminal::settlement(&fixture.receipt, &fixture.audit)
                    .unwrap()
                    .cursor
        );
    }
}
