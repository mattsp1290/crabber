#![allow(dead_code)]
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};
use tracing::{
    Subscriber,
    field::{Field, Visit},
    span::{Attributes, Id, Record},
};
use tracing_subscriber::{Layer, layer::Context, registry::LookupSpan};
#[derive(Clone, Debug, Default)]
pub(crate) struct Captured {
    pub(crate) name: String,
    pub(crate) target: String,
    pub(crate) level: String,
    pub(crate) fields: BTreeMap<String, String>,
}
impl Visit for Captured {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.fields
            .insert(field.name().into(), format!("{value:?}"));
    }
    fn record_str(&mut self, field: &Field, value: &str) {
        self.fields.insert(field.name().into(), value.into());
    }
}
#[derive(Clone, Default)]
pub(crate) struct Capture(
    pub(crate) Arc<Mutex<Vec<Captured>>>,
    Arc<tokio::sync::Notify>,
    Arc<Mutex<BTreeMap<u64, String>>>,
);
impl Capture {
    pub(crate) async fn wait_for_closed(&self, name: &str, outcome: &str) {
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                let notification = self.1.notified();
                if self.0.lock().unwrap().iter().any(|r| {
                    r.name == name && r.fields.get("outcome").is_some_and(|v| v == outcome)
                }) {
                    return;
                }
                notification.await;
            }
        })
        .await
        .unwrap_or_else(|_| {
            panic!(
                "missing closed span {name}/{outcome}: closed={:#?}, open={:#?}",
                self.0.lock().unwrap(),
                self.2.lock().unwrap()
            )
        });
    }
}
impl<S: Subscriber + for<'a> LookupSpan<'a>> Layer<S> for Capture {
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        let mut captured = Captured {
            name: attrs.metadata().name().into(),
            target: attrs.metadata().target().into(),
            level: attrs.metadata().level().to_string(),
            ..Captured::default()
        };
        attrs.record(&mut captured);
        self.2
            .lock()
            .unwrap()
            .insert(id.into_u64(), captured.name.clone());
        ctx.span(id).unwrap().extensions_mut().insert(captured);
    }
    fn on_record(&self, id: &Id, values: &Record<'_>, ctx: Context<'_, S>) {
        values.record(
            ctx.span(id)
                .unwrap()
                .extensions_mut()
                .get_mut::<Captured>()
                .unwrap(),
        );
    }
    fn on_close(&self, id: Id, ctx: Context<'_, S>) {
        self.2.lock().unwrap().remove(&id.into_u64());
        self.0.lock().unwrap().push(
            ctx.span(&id)
                .unwrap()
                .extensions()
                .get::<Captured>()
                .unwrap()
                .clone(),
        );
        self.1.notify_one();
    }
    fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
        let mut captured = Captured {
            name: "event".into(),
            target: event.metadata().target().into(),
            level: event.metadata().level().to_string(),
            ..Captured::default()
        };
        event.record(&mut captured);
        self.0.lock().unwrap().push(captured);
    }
}
pub(crate) struct CaptureGuard {
    _subscriber: tracing::subscriber::DefaultGuard,
    _interest_guard: tracing::Dispatch,
}
pub(crate) fn capture() -> (Capture, CaptureGuard) {
    use tracing_subscriber::prelude::*;
    let captured = Capture::default();
    let guard =
        tracing::subscriber::set_default(tracing_subscriber::registry().with(captured.clone()));
    // tracing-core's single-dispatcher fast path registers callsites against
    // the calling thread. A second live dispatcher keeps parallel subscriberless
    // tests from caching Never for callsites this scoped capture needs.
    let interest_guard = tracing::Dispatch::new(tracing::subscriber::NoSubscriber::default());
    (
        captured,
        CaptureGuard {
            _subscriber: guard,
            _interest_guard: interest_guard,
        },
    )
}

#[test]
fn subscriberless_callsite_registration_does_not_disable_scoped_capture() {
    fn same_callsite() {
        let _span = tracing::info_span!(target: crate::targets::EXTENSION, "capture.registration")
            .entered();
    }
    let (captured, _guard) = capture();
    std::thread::spawn(same_callsite).join().unwrap();
    same_callsite();
    assert_eq!(
        captured
            .0
            .lock()
            .unwrap()
            .iter()
            .filter(|r| r.name == "capture.registration")
            .count(),
        1
    );
}
