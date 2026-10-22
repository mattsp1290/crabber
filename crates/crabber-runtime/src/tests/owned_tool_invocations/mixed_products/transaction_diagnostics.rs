//! Capture the existing Store outcome before its public error is sanitized.
use std::{cell::RefCell, fmt::Write as _, future::Future, time::Instant};
use tracing::{Metadata, Subscriber, dispatcher, field::Visit, span};
use tracing_subscriber::{Layer, layer::Context, prelude::*};

thread_local! {
    static WORKER_DISPATCH: RefCell<Option<dispatcher::DefaultGuard>> = const { RefCell::new(None) };
}

struct Transactions(Instant);
#[derive(Default)]
struct Fields(String);
impl Visit for Fields {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if matches!(
            field.name(),
            "db.system" | "crabber.store.scope" | "outcome" | "error.kind"
        ) {
            write!(&mut self.0, " {}={value:?}", field.name()).unwrap();
        }
    }
}
impl<S: Subscriber> Layer<S> for Transactions {
    fn enabled(&self, metadata: &Metadata<'_>, _: Context<'_, S>) -> bool {
        // Unscoped owner transactions are DEBUG spans. Do not collect SQL,
        // requests, credentials, or unrelated runtime fields.
        metadata.name() == "crabber.store.transaction"
    }
    fn on_new_span(&self, attributes: &span::Attributes<'_>, id: &span::Id, _: Context<'_, S>) {
        let mut fields = Fields::default();
        attributes.record(&mut fields);
        eprintln!("transaction {:?} {id:?} new{}", self.0.elapsed(), fields.0);
    }
    fn on_record(&self, id: &span::Id, values: &span::Record<'_>, _: Context<'_, S>) {
        let mut fields = Fields::default();
        values.record(&mut fields);
        // Record immediately: a transaction can fail or remain open before its
        // span closes. IDs and elapsed time tie updates to their creation.
        if !fields.0.is_empty() {
            eprintln!(
                "transaction {:?} {id:?} update{}",
                self.0.elapsed(),
                fields.0
            );
        }
    }
    fn on_close(&self, id: span::Id, _: Context<'_, S>) {
        eprintln!("transaction {:?} {id:?} closed", self.0.elapsed());
    }
}

pub(super) fn run(history: impl Future<Output = ()>) {
    let dispatch =
        tracing::Dispatch::new(tracing_subscriber::registry().with(Transactions(Instant::now())));
    let _caller = dispatcher::set_default(&dispatch);
    // Match the original Tokio test's four-worker runtime. Owned admissions
    // run on these workers, so a caller-thread subscriber alone misses them.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .on_thread_start(move || {
            WORKER_DISPATCH.with_borrow_mut(|guard| {
                *guard = Some(dispatcher::set_default(&dispatch));
            });
        })
        .on_thread_stop(|| {
            WORKER_DISPATCH.with_borrow_mut(|guard| {
                guard.take();
            });
        })
        .build()
        .unwrap();
    runtime.block_on(history);
}
