//! Actual original sources reject coherent current/public terminal rewrites.
use super::super::super::super::events::EventLog;
use super::*;
use crate::MemoryStore;
use crate::approval::terminal::history_contract::{Fixture, fixture};
use crate::deletion::coverage::MAX_PROOF_BYTES;
use crabber_core::{EventRecord, ManualClock};
#[tokio::test]
async fn approval_terminal_history_requires_frozen_origin_and_independent_cursor() {
    let clock = std::sync::Arc::new(ManualClock::new(time::OffsetDateTime::UNIX_EPOCH));
    let store = MemoryStore::with_clock(clock.clone());
    for expire in [false, true] {
        let f = fixture(&store, &clock, expire).await;
        let mut state = store.state.lock().unwrap();
        assert!(matches!(read(&state, &f, MAX_PROOF_BYTES), Ok(true)));
        let mut exact = Proof::bounded(MAX_PROOF_BYTES);
        assert!(verify(&state, &state.runs[&f.current.id], &mut exact).unwrap());
        assert!(matches!(read(&state, &f, exact.used()), Ok(true)));
        assert!(matches!(
            read(&state, &f, exact.used() - 1),
            Err(Failure::Limited(_))
        ));
        assert!(matches!(read(&state, &f, 1), Err(Failure::Limited(_))));
        let original = state.clone();
        for variation in 0..7 {
            *state = original.clone();
            mutate(&mut state, &f, variation);
            let result = read(&state, &f, MAX_PROOF_BYTES);
            match variation {
                4 => assert!(matches!(result, Ok(false))),
                5 => assert!(matches!(result, Err(Failure::Limited(_)))),
                _ => assert!(matches!(result, Err(Failure::Error(_)))),
            }
        }
        *state = original;
    }
}
fn read(state: &State, f: &Fixture, budget: usize) -> Result<bool, Failure> {
    verify(
        state,
        &state.runs[&f.current.id],
        &mut Proof::bounded(budget),
    )
}
fn mutate(state: &mut State, f: &Fixture, variation: usize) {
    match variation {
        0 => state.runs.get_mut(&f.current.id).unwrap().error = Some("forged private error".into()),
        1 => {
            state
                .runs
                .get_mut(&f.current.id)
                .unwrap()
                .usage
                .output_tokens = 13;
            let mut source = f.original.clone();
            source.payload["usage"]["output_tokens"] = serde_json::json!(13);
            source.payload["output_tokens"] = serde_json::json!(13);
            let mut audit = f.audit.clone();
            audit.payload["terminal"]["settlement"] = serde_json::to_value(&source).unwrap();
            rewrite(state, source);
            rewrite(state, audit);
        }
        2 => {
            let mut audit = f.audit.clone();
            audit.payload["terminal"]["settlement"] = serde_json::to_value(&f.older).unwrap();
            rewrite(state, audit);
        }
        3 => {
            state
                .operation_receipts
                .remove(&(f.current.session_id.clone(), f.batch.id.clone()));
        }
        4 => {
            let mut audit = f.audit.clone();
            audit.payload["version"] = serde_json::json!(1);
            audit.payload.as_object_mut().unwrap().remove("terminal");
            rewrite(state, audit);
        }
        5 => {
            state.runs.get_mut(&f.current.id).unwrap().error =
                Some("x".repeat(MAX_PROOF_BYTES + 1));
        }
        6 => {
            let mut source = f.original.clone();
            source.turn_id = Some(crabber_core::TurnId::new());
            let mut audit = f.audit.clone();
            audit.payload["terminal"]["settlement"] = serde_json::to_value(&source).unwrap();
            rewrite(state, source);
            rewrite(state, audit);
        }
        _ => unreachable!(),
    }
}
fn rewrite(state: &mut State, replacement: EventRecord) {
    let cursor = replacement.cursor;
    let mut events = state.events.iter().cloned().collect::<Vec<_>>();
    *events
        .iter_mut()
        .find(|event| event.cursor == cursor)
        .unwrap() = replacement;
    state.events = EventLog::default();
    for event in events {
        state.events.append(event).unwrap();
    }
}
