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
pub(crate) struct Capture(pub(crate) Arc<Mutex<Vec<Captured>>>);
impl<S: Subscriber + for<'a> LookupSpan<'a>> Layer<S> for Capture {
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        let mut captured = Captured {
            name: attrs.metadata().name().into(),
            target: attrs.metadata().target().into(),
            level: attrs.metadata().level().to_string(),
            ..Captured::default()
        };
        attrs.record(&mut captured);
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
        self.0.lock().unwrap().push(
            ctx.span(&id)
                .unwrap()
                .extensions()
                .get::<Captured>()
                .unwrap()
                .clone(),
        );
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
#[cfg(feature = "sqlite")]
#[tokio::test(flavor = "current_thread")]
async fn transaction_scope_outcomes_and_redaction() {
    use crate::transaction::{StoreScope, begin, begin_scoped};
    let (captured, _guard) = capture();
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("secret.sqlite");
    crate::SqliteStore::migrate(&path).await.unwrap();
    let store = crate::SqliteStore::connect(&path).await.unwrap();
    drop(store);
    let pool = sqlx::SqlitePool::connect(&format!("sqlite:{}", path.display()))
        .await
        .unwrap();
    begin_scoped(&pool, StoreScope::Admission)
        .await
        .unwrap()
        .commit()
        .await
        .unwrap();
    drop(begin(&pool).await.unwrap());
    let mut tx = begin_scoped(&pool, StoreScope::Run).await.unwrap();
    sqlx::query("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query(
        "CREATE TABLE child (id INTEGER REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED)",
    )
    .execute(&mut *tx)
    .await
    .unwrap();
    sqlx::query("INSERT INTO child VALUES (123)")
        .execute(&mut *tx)
        .await
        .unwrap();
    assert!(tx.commit_in_place().await.is_err());
    tx.rollback().await.unwrap();
    pool.close().await;
    let records = captured.0.lock().unwrap();
    assert!(
        records.iter().any(|r| r.name == "crabber.store.connect"
            && r.fields.get("outcome").is_some_and(|v| v == "ok"))
    );
    for (scope, outcome, level) in [
        ("admission", "committed", "INFO"),
        ("verify", "rolled_back", "INFO"),
        ("unscoped", "abandoned", "DEBUG"),
        ("run", "commit_failed", "INFO"),
    ] {
        let record = records
            .iter()
            .find(|r| {
                r.name == "crabber.store.transaction"
                    && r.fields
                        .get("crabber.store.scope")
                        .is_some_and(|v| v == scope)
                    && r.fields.get("outcome").is_some_and(|v| v == outcome)
            })
            .unwrap();
        assert_eq!(record.level, level);
        assert_eq!(record.target, "crabber::session");
        assert_eq!(record.fields["db.system"], "sqlite");
        assert_eq!(record.fields["otel.kind"], "client");
        if outcome == "commit_failed" {
            assert_eq!(record.fields["error.kind"], "database");
        }
    }
    let allowed = [
        "db.system",
        "crabber.store.scope",
        "outcome",
        "error.kind",
        "otel.kind",
        "otel.status_code",
        "schema.version",
        "message",
    ];
    for record in records.iter().filter(|r| r.target == "crabber::session") {
        for (name, value) in &record.fields {
            assert!(allowed.contains(&name.as_str()), "{name}");
            assert!(
                !value.contains("postgres://")
                    && !value.contains(path.to_str().unwrap())
                    && !value.contains("SELECT")
            );
        }
    }
}

#[cfg(feature = "postgres")]
#[tokio::test(flavor = "current_thread")]
async fn postgres_transaction_spans_record_scope_and_outcome() {
    let Ok(url) = std::env::var("CRABBER_TEST_POSTGRES_URL") else {
        return;
    };
    let (captured, _guard) = capture();
    crate::PostgresStore::migrate(&url).await.unwrap();
    let store = crate::PostgresStore::connect(&url).await.unwrap();
    drop(store);
    let pool = sqlx::PgPool::connect(&url).await.unwrap();
    crate::transaction::begin_scoped(&pool, crate::transaction::StoreScope::Admission)
        .await
        .unwrap()
        .commit()
        .await
        .unwrap();
    pool.close().await;
    let records = captured.0.lock().unwrap();
    let record = records
        .iter()
        .find(|r| {
            r.name == "crabber.store.transaction"
                && r.fields
                    .get("crabber.store.scope")
                    .is_some_and(|v| v == "admission")
        })
        .unwrap();
    assert_eq!(record.target, "crabber::session");
    assert_eq!(record.fields["db.system"], "postgresql");
    assert_eq!(record.fields["outcome"], "committed");
    assert_eq!(record.level, "INFO");
    for record in records
        .iter()
        .filter(|record| record.target == crate::targets::SESSION)
    {
        for (field, value) in &record.fields {
            assert!(
                !value.contains(&url) && !value.contains("SELECT"),
                "store signal {} leaked database content in field {field}",
                record.name
            );
        }
    }
}
