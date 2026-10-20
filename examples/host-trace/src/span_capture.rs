//! Memory journey verifies runtime facade spans without configuring exporters.
use std::sync::{Arc, Mutex};
use tracing::{
    Subscriber,
    field::{Field, Visit},
    span::{Attributes, Id, Record},
};
use tracing_subscriber::{Layer, layer::Context};
#[derive(Clone, Default)]
pub struct SpanCapture(Arc<Mutex<Vec<(u64, String)>>>);
struct TraceField<'a>(&'a mut String);
impl Visit for TraceField<'_> {
    fn record_debug(&mut self, _: &Field, _: &dyn std::fmt::Debug) {}
    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "host.trace_id" {
            value.clone_into(self.0);
        }
    }
}
impl<S: Subscriber> Layer<S> for SpanCapture {
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, _: Context<'_, S>) {
        if attrs.metadata().name() == "crabber.run" {
            let mut value = String::new();
            attrs.record(&mut TraceField(&mut value));
            self.0.lock().unwrap().push((id.into_u64(), value));
        }
    }
    fn on_record(&self, id: &Id, record: &Record<'_>, _: Context<'_, S>) {
        if let Some((_, trace)) = self
            .0
            .lock()
            .unwrap()
            .iter_mut()
            .rev()
            .find(|(span, _)| *span == id.into_u64())
        {
            record.record(&mut TraceField(trace));
        }
    }
}
impl SpanCapture {
    pub fn assert_trace(&self, expected: &str) {
        assert!(
            self.0
                .lock()
                .unwrap()
                .iter()
                .any(|(_, trace)| trace == expected)
        );
    }
}
