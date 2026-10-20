//! Actual generic and tool waiting cancellation, preserving original receipts.
use super::*;
use crate::{Store, attempt::terminal::TerminalFacts};
use crabber_core::{ManualClock, Run};
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
    for (tool, previous) in [(false, false), (true, false), (false, true)] {
        let fixture =
            crate::run_control_contract::waiting_terminal_fixture(store, clock, tool, previous)
                .await;
        let request = fixture.request;
        let receipt = store
            .lookup_run_control(&request.target.session_id, &request.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            store.control_run(request.clone()).await.unwrap(),
            crate::RunControlOutcome::Replayed(receipt.clone())
        );
        let summary = store
            .run_controls(&request.target)
            .await
            .unwrap()
            .controls
            .into_iter()
            .find(|item| item.receipt == receipt)
            .unwrap();
        let current = store
            .get_run(&request.target.run_id)
            .await
            .unwrap()
            .unwrap();
        let events = store
            .list_events(&request.target.session_id, None, 1000)
            .await
            .unwrap();
        let audit = events
            .iter()
            .find(|event| event.cursor == Some(summary.event_cursor))
            .unwrap();
        crate::run_control::verify_event(&summary, audit).unwrap();
        let saved: Source = serde_json::from_value(audit.payload["terminal"].clone()).unwrap();
        assert_eq!(saved.request, request);
        let original = events
            .iter()
            .find(|event| event.cursor == saved.settlement.cursor)
            .unwrap();
        assert_eq!(saved.settlement, *original);
        let usage = fixture.usage;
        let turn = fixture.turn;
        verify_scope(
            facts(&current),
            &summary,
            audit,
            original,
            FrozenFacts {
                usage: &usage,
                turn: turn.as_ref(),
            },
        )
        .unwrap();
        negatives(&current, &summary, audit, original);
        if let Some(earlier) = fixture.earlier {
            other_control(store, &request.target, &earlier, &summary.status, &events).await;
        }

        legacy(&current, &summary, audit, original, &usage, turn.as_ref());
        if tool {
            let old = events
                .iter()
                .find(|event| {
                    event.kind == EventKind::ContinuationChanged
                        && event
                            .payload
                            .get("kind")
                            .and_then(serde_json::Value::as_str)
                            == Some("tool_continuation")
                        && event.payload["outcome"]["operation"] == "terminated"
                })
                .unwrap();
            assert!(old.cursor < original.cursor);
        }
    }
}
fn negatives(
    current: &Run,
    summary: &RunControlSummary,
    audit: &EventRecord,
    original: &EventRecord,
) {
    for turn_change in [false, true] {
        let mut changed = current.clone();
        let mut frame = original.clone();
        if turn_change {
            frame.turn_id = Some(crabber_core::TurnId("changed-frozen-turn".into()));
        } else {
            changed.usage.input_tokens += 1;
            frame.payload["usage"] = serde_json::to_value(&changed.usage).unwrap();
            frame.payload["input_tokens"] = serde_json::json!(changed.usage.input_tokens);
        }
        let mut event = audit.clone();
        event.payload["terminal"]["settlement"] = serde_json::to_value(&frame).unwrap();
        crate::run_control::verify_event(summary, &event).unwrap();
        assert!(
            verify_scope(
                facts(&changed),
                summary,
                &event,
                &frame,
                FrozenFacts {
                    usage: &current.usage,
                    turn: original.turn_id.as_ref()
                }
            )
            .is_err()
        );
    }
    let mut wrong = current.clone();
    wrong.error = Some("coherent mutable error forgery".into());
    assert!(
        verify_scope(
            facts(&wrong),
            summary,
            audit,
            original,
            FrozenFacts {
                usage: &current.usage,
                turn: original.turn_id.as_ref()
            }
        )
        .is_err()
    );
    wrong = current.clone();
    wrong.status = RunStatus::Completed;
    assert!(
        verify_scope(
            facts(&wrong),
            summary,
            audit,
            original,
            FrozenFacts {
                usage: &current.usage,
                turn: original.turn_id.as_ref()
            }
        )
        .is_err()
    );
    let mut relinked = audit.clone();
    relinked.payload["terminal"]["settlement"]["cursor"] =
        serde_json::json!(original.cursor.unwrap().0 - 1);
    assert!(
        verify_scope(
            facts(current),
            summary,
            &relinked,
            original,
            FrozenFacts {
                usage: &current.usage,
                turn: original.turn_id.as_ref()
            }
        )
        .is_err()
    );
    let mut changed = audit.clone();
    changed.payload["terminal_version"] = serde_json::json!(2);
    assert!(crate::run_control::verify_event(summary, &changed).is_err());
    changed = audit.clone();
    changed
        .payload
        .as_object_mut()
        .unwrap()
        .remove("terminal_version");
    assert!(crate::run_control::verify_event(summary, &changed).is_err());
    let mut changed = audit.clone();
    changed.payload["terminal"]["request"]["expected_revision"] = serde_json::json!(0);
    assert!(crate::run_control::verify_event(summary, &changed).is_err());
    changed = audit.clone();
    changed.payload["terminal"]["settlement"]["payload"]["padding"] =
        serde_json::json!("x".repeat(32768));
    assert!(crate::run_control::verify_event(summary, &changed).is_err());
}

fn legacy(
    current: &Run,
    summary: &RunControlSummary,
    audit: &EventRecord,
    original: &EventRecord,
    usage: &Usage,
    turn: Option<&crabber_core::TurnId>,
) {
    let mut legacy = audit.clone();
    legacy
        .payload
        .as_object_mut()
        .unwrap()
        .remove("terminal_version");
    legacy.payload.as_object_mut().unwrap().remove("terminal");
    crate::run_control::verify_event(summary, &legacy).unwrap();
    assert!(
        verify_scope(
            facts(current),
            summary,
            &legacy,
            original,
            FrozenFacts { usage, turn }
        )
        .is_err()
    );
}

async fn other_control(
    store: &impl Store,
    target: &crate::RunControlTarget,
    earlier: &RunControlRequest,
    status: &RunControlStatus,
    events: &[EventRecord],
) {
    let other = store
        .run_controls(target)
        .await
        .unwrap()
        .controls
        .into_iter()
        .find(|item| item.receipt.operation_id == earlier.id)
        .unwrap();
    assert_eq!(&other.status, status);
    let other_audit = events
        .iter()
        .find(|event| event.cursor == Some(other.event_cursor))
        .unwrap();
    crate::run_control::verify_event(&other, other_audit).unwrap();
    assert!(other_audit.payload.get("terminal").is_none());
}
