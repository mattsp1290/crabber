use super::*;
use crabber::core::{EventKind, PublicSourceChange, PublicSourceEvent};

pub(super) fn publications(events: &[crabber::core::EventRecord]) -> Vec<PublicSourceEvent> {
    events
        .iter()
        .filter(|e| {
            matches!(
                e.kind,
                EventKind::ActivitySnapshot | EventKind::ActivityDelta
            )
        })
        .map(|e| serde_json::from_value(e.payload.clone()).unwrap())
        .collect()
}
pub(super) fn status(event: &PublicSourceEvent) -> &str {
    match &event.change {
        PublicSourceChange::ActivitySnapshot { activity } => {
            activity.content()["status"].as_str().unwrap()
        }
        PublicSourceChange::ActivityDelta { delta, .. } => {
            delta[0].members()["value"].as_str().unwrap()
        }
        _ => panic!("activity publication"),
    }
}
#[path = "attribution/publications.rs"]
mod actual;
#[path = "attribution/interleaved.rs"]
mod interleaved;
#[path = "attribution/nested.rs"]
mod nested;
#[path = "attribution/retry.rs"]
mod retry;
