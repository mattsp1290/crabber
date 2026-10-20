use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

use crabber_core::{EventKind, EventRecord, RunId, SessionId, TraceContext};
use crabber_extension::MountCloseTimeout;
use crabber_runtime::{Observer, OperationKind, OperationalObservation, TerminalReason};

use super::AgentObservers;

struct SecondaryPayload(Arc<AtomicUsize>);
impl Drop for SecondaryPayload {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
        panic!("secondary panic payload must not be dropped");
    }
}
struct Payload {
    primary_drops: Arc<AtomicUsize>,
    secondary_drops: Arc<AtomicUsize>,
}
impl Drop for Payload {
    fn drop(&mut self) {
        self.primary_drops.fetch_add(1, Ordering::SeqCst);
        std::panic::panic_any(SecondaryPayload(self.secondary_drops.clone()));
    }
}
struct Panics {
    panic_on_payload_drop: bool,
    calls: Arc<AtomicUsize>,
    primary_drops: Arc<AtomicUsize>,
    secondary_drops: Arc<AtomicUsize>,
}
impl Panics {
    fn fail(&self) {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.panic_on_payload_drop {
            std::panic::panic_any(Payload {
                primary_drops: self.primary_drops.clone(),
                secondary_drops: self.secondary_drops.clone(),
            });
        }
        panic!("local observer failed");
    }
}
impl Observer for Panics {
    fn emit(&self, _: &EventRecord) {
        self.fail();
    }
    fn model_completed(&self, _: &EventRecord) {
        self.fail();
    }
    fn operational_completed(&self, _: &OperationalObservation) {
        self.fail();
    }
    fn mount_close_timed_out(&self, _: &MountCloseTimeout) {
        self.fail();
    }
}

#[derive(Debug, PartialEq)]
struct Seen {
    method: &'static str,
    context: Option<TraceContext>,
    attempt: Option<RunId>,
}
struct Healthy {
    event: EventRecord,
    operation: OperationalObservation,
    timeout: MountCloseTimeout,
    seen: Mutex<Vec<Seen>>,
}
impl Healthy {
    fn record(
        &self,
        method: &'static str,
        context: Option<&TraceContext>,
        attempt: Option<&RunId>,
    ) {
        self.seen.lock().unwrap().push(Seen {
            method,
            context: context.cloned(),
            attempt: attempt.cloned(),
        });
    }
    fn event(
        &self,
        method: &'static str,
        event: &EventRecord,
        context: Option<&TraceContext>,
        attempt: Option<&RunId>,
    ) {
        assert_eq!(event, &self.event);
        self.record(method, context, attempt);
    }
}
impl Observer for Healthy {
    fn emit(&self, event: &EventRecord) {
        self.event("emit", event, None, None);
    }
    fn model_completed(&self, event: &EventRecord) {
        self.event("model", event, None, None);
    }
    fn emit_with_context(&self, event: &EventRecord, context: Option<&TraceContext>) {
        self.event("emit_context", event, context, None);
    }
    fn model_completed_with_context(&self, event: &EventRecord, context: Option<&TraceContext>) {
        self.event("model_context", event, context, None);
    }
    fn emit_in_attempt(
        &self,
        event: &EventRecord,
        context: Option<&TraceContext>,
        attempt: &RunId,
    ) {
        self.event("emit_attempt", event, context, Some(attempt));
    }
    fn model_completed_in_attempt(
        &self,
        event: &EventRecord,
        context: Option<&TraceContext>,
        attempt: &RunId,
    ) {
        self.event("model_attempt", event, context, Some(attempt));
    }
    fn operational_completed(&self, operation: &OperationalObservation) {
        assert_eq!(operation, &self.operation);
        self.record("operation", None, None);
    }
    fn operational_completed_in_attempt(
        &self,
        operation: &OperationalObservation,
        context: Option<&TraceContext>,
        attempt: &RunId,
    ) {
        assert_eq!(operation, &self.operation);
        self.record("operation_attempt", context, Some(attempt));
    }
    fn mount_close_timed_out(&self, timeout: &MountCloseTimeout) {
        assert_eq!(timeout, &self.timeout);
        self.record("mount", None, None);
    }
}

fn healthy() -> Healthy {
    let session = SessionId::new();
    let run = RunId::new();
    Healthy {
        event: EventRecord {
            cursor: None,
            session_id: session.clone(),
            run_id: run.clone(),
            turn_id: None,
            kind: EventKind::RunStarted,
            payload: serde_json::json!({"original":"unchanged"}),
            correlation: None,
            live_only: true,
            created_at: time::OffsetDateTime::UNIX_EPOCH,
        },
        operation: OperationalObservation {
            session_id: session,
            run_id: run,
            kind: OperationKind::Run,
            reason: TerminalReason::Success,
            elapsed: Duration::from_millis(17),
            first_token: None,
        },
        timeout: MountCloseTimeout {
            extension: "original-extension".into(),
            bound: Duration::from_millis(19),
            leases: 2,
            pending_tasks: 3,
        },
        seen: Mutex::new(Vec::new()),
    }
}

fn exercise(panic_on_payload_drop: bool) {
    let healthy = Arc::new(healthy());
    let secondary_drops = Arc::new(AtomicUsize::new(0));
    let primary_drops = Arc::new(AtomicUsize::new(0));
    let calls = Arc::new(AtomicUsize::new(0));
    let fanout = AgentObservers {
        observers: vec![
            Arc::new(Panics {
                panic_on_payload_drop,
                calls: calls.clone(),
                primary_drops: primary_drops.clone(),
                secondary_drops: secondary_drops.clone(),
            }),
            healthy.clone(),
        ],
        #[cfg(feature = "datadog")]
        datadog: None,
    };
    let context = TraceContext::new("1234567890abcdef", "1234567890abcdef").unwrap();
    let attempt = RunId::new();
    fanout.mount_close_timed_out(&healthy.timeout);
    fanout.operational_completed(&healthy.operation);
    fanout.operational_completed_in_attempt(&healthy.operation, Some(&context), &attempt);
    fanout.emit(&healthy.event);
    fanout.model_completed(&healthy.event);
    fanout.emit_with_context(&healthy.event, Some(&context));
    fanout.model_completed_with_context(&healthy.event, Some(&context));
    fanout.emit_in_attempt(&healthy.event, Some(&context), &attempt);
    fanout.model_completed_in_attempt(&healthy.event, Some(&context), &attempt);
    assert_eq!(*healthy.seen.lock().unwrap(), expected(&context, &attempt));
    assert_eq!(calls.load(Ordering::SeqCst), 9);
    assert_eq!(
        primary_drops.load(Ordering::SeqCst),
        if panic_on_payload_drop { 9 } else { 0 }
    );
    assert_eq!(secondary_drops.load(Ordering::SeqCst), 0);
}

fn expected(context: &TraceContext, attempt: &RunId) -> Vec<Seen> {
    [
        ("mount", None, None),
        ("operation", None, None),
        (
            "operation_attempt",
            Some(context.clone()),
            Some(attempt.clone()),
        ),
        ("emit_context", None, None),
        ("model_context", None, None),
        ("emit_context", Some(context.clone()), None),
        ("model_context", Some(context.clone()), None),
        ("emit_attempt", Some(context.clone()), Some(attempt.clone())),
        (
            "model_attempt",
            Some(context.clone()),
            Some(attempt.clone()),
        ),
    ]
    .into_iter()
    .map(|(method, context, attempt)| Seen {
        method,
        context,
        attempt,
    })
    .collect()
}

#[test]
fn every_observer_method_survives_an_earlier_observer_panic() {
    exercise(false);
}
#[test]
fn every_observer_method_contains_panicking_panic_payload_disposal() {
    exercise(true);
}
