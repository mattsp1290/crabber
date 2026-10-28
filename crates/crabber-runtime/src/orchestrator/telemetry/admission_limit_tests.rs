use super::{RuntimeError, StoreError};
use crabber_core::{AdmissionLimit, SessionId};
use crabber_session::session_owner::{SessionOwnerTarget, preparation::PreparationReconciliation};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};
use tracing::{
    Subscriber,
    field::{Field, Visit},
    span::{Id, Record},
};
use tracing_subscriber::{
    Layer,
    layer::{Context, SubscriberExt},
};

#[derive(Clone, Default)]
struct CaptureLayer(Arc<Mutex<BTreeMap<String, String>>>);
struct Fields<'a>(&'a mut BTreeMap<String, String>);
impl Visit for Fields<'_> {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.0.insert(field.name().into(), format!("{value:?}"));
    }
    fn record_str(&mut self, field: &Field, value: &str) {
        self.0.insert(field.name().into(), value.into());
    }
}
impl<S: Subscriber> Layer<S> for CaptureLayer {
    fn on_record(&self, _: &Id, values: &Record<'_>, _: Context<'_, S>) {
        values.record(&mut Fields(&mut self.0.lock().unwrap()));
    }
}
struct CaptureSubscriber {
    _guard: tracing::subscriber::DefaultGuard,
    _dispatch: tracing::Dispatch,
}
impl CaptureSubscriber {
    fn new(layer: &CaptureLayer) -> Self {
        let dispatch = tracing::Dispatch::new(tracing::subscriber::NoSubscriber::default());
        let guard =
            tracing::subscriber::set_default(tracing_subscriber::registry().with(layer.clone()));
        Self {
            _guard: guard,
            _dispatch: dispatch,
        }
    }
}

fn limit(cap: Option<u64>) -> RuntimeError {
    StoreError::AdmissionLimit(AdmissionLimit::proof_bytes(cap)).into()
}

fn descriptor() -> PreparationReconciliation {
    PreparationReconciliation {
        session_id: SessionId::new(),
        workspace_id: "limit-test".into(),
        directory: "/synthetic/limit-test".into(),
        owner: SessionOwnerTarget {
            id: crabber_core::SessionOwnerId::new(),
            generation: crabber_core::SessionOwnerGeneration::INITIAL,
        },
        reserve_id: crabber_core::SessionOwnerOperationId::new(),
        begin_id: crabber_core::SessionOwnerOperationId::new(),
        join_id: crabber_core::SessionOwnerOperationId::new(),
        expected_revision: crabber_core::SessionRevision::INITIAL,
    }
}

fn incomplete(preparation: PreparationReconciliation, source: RuntimeError) -> RuntimeError {
    RuntimeError::PreparationIncomplete {
        preparation: Box::new(preparation),
        source: Box::new(source),
    }
}

fn record(error: &RuntimeError) -> BTreeMap<String, String> {
    let layer = CaptureLayer::default();
    let _subscriber = CaptureSubscriber::new(&layer);
    let span = tracing::info_span!(
        "crabber.admission",
        "admission.limit_kind" = tracing::field::Empty,
        "admission.limit_bytes" = tracing::field::Empty,
        outcome = tracing::field::Empty,
        "error.kind" = tracing::field::Empty,
        "otel.status_code" = tracing::field::Empty,
    );
    super::record_admission_result(&span, Err(error));
    layer.0.lock().unwrap().clone()
}

#[test]
fn direct_and_double_preparation_limits_retain_their_classification_and_correlation() {
    let direct = limit(Some(32 << 20));
    assert_eq!(direct.error_kind(), "store_admission_limit");
    assert!(direct.preparation_reconciliation().is_none());
    let direct_span = record(&direct);
    assert_eq!(direct_span["outcome"], "rejected");
    assert_eq!(direct_span["error.kind"], "store_admission_limit");
    assert_eq!(direct_span["otel.status_code"], "ERROR");

    let outer = descriptor();
    let nested = incomplete(outer.clone(), incomplete(descriptor(), direct));
    let before = nested.preparation_reconciliation().cloned();
    assert_eq!(
        nested.admission_limit(),
        Some(&AdmissionLimit::proof_bytes(Some(32 << 20)))
    );
    assert_eq!(nested.preparation_reconciliation(), before.as_ref());
    assert_eq!(nested.preparation_reconciliation(), Some(&outer));
    assert_eq!(nested.error_kind(), "preparation_incomplete");
    assert_eq!(
        nested.operation_error().error_kind(),
        "store_admission_limit"
    );
    let nested_span = record(&nested);
    assert_eq!(nested_span["outcome"], "unknown");
    assert_eq!(nested_span["error.kind"], "preparation_incomplete");
    assert_eq!(nested_span["otel.status_code"], "OK");
    for span in [direct_span, nested_span] {
        assert_eq!(span["admission.limit_kind"], "proof_bytes");
        assert_eq!(span["admission.limit_bytes"], "33554432");
    }
}

#[test]
fn telemetry_omits_unavailable_zero_and_unrepresentable_caps() {
    let maximum_cap = u64::try_from(i64::MAX).unwrap();
    for cap in [None, Some(0), Some(maximum_cap + 1), Some(u64::MAX)] {
        let error = limit(cap);
        assert_eq!(error.admission_limit().unwrap().limit_bytes(), cap);
        let span = record(&error);
        assert_eq!(span["admission.limit_kind"], "proof_bytes");
        assert!(!span.contains_key("admission.limit_bytes"));
    }
    let span = record(&limit(Some(maximum_cap)));
    assert_eq!(span["admission.limit_bytes"], i64::MAX.to_string());
}

#[test]
fn unrelated_validation_and_cleanup_errors_have_no_limit_metadata() {
    for error in [
        RuntimeError::Store(StoreError::Validation("SYNTHETIC_SOURCE_SECRET".into())),
        incomplete(descriptor(), RuntimeError::PreparationRecoveryRequired),
    ] {
        let before = error.preparation_reconciliation().cloned();
        assert!(error.admission_limit().is_none());
        assert_eq!(error.preparation_reconciliation(), before.as_ref());
        let span = record(&error);
        assert!(!span.contains_key("admission.limit_kind"));
        assert!(!span.contains_key("admission.limit_bytes"));
        assert!(
            span.values()
                .all(|value| !value.contains("SYNTHETIC_SOURCE_SECRET"))
        );
    }
}
