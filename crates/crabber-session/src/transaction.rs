//! Own the connection before transaction I/O so cancellation cannot return ambiguous state.
//! Discarding an ambiguous connection prevents reuse; it does not prove that a
//! database worker or remote transaction has stopped or failed to commit.
use sqlx::{Database, Error, Pool, TransactionManager, pool::MaybePoolConnection};
use std::borrow::Cow;
use std::{
    fmt,
    ops::{Deref, DerefMut},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

#[cfg_attr(not(feature = "postgres"), allow(dead_code))] // Shared scope vocabulary across backends.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum StoreScope {
    Unscoped,
    Session,
    Run,
    Lease,
    Admission,
    Events,
    Snapshot,
    Approval,
    ToolContinuation,
    Queue,
    Verify,
}
impl StoreScope {
    fn as_str(self) -> &'static str {
        match self {
            Self::Unscoped => "unscoped",
            Self::Session => "session",
            Self::Run => "run",
            Self::Lease => "lease",
            Self::Admission => "admission",
            Self::Events => "events",
            Self::Snapshot => "snapshot",
            Self::Approval => "approval",
            Self::ToolContinuation => "tool_continuation",
            Self::Queue => "queue",
            Self::Verify => "verify",
        }
    }
}
pub(crate) fn sqlx_error_kind(error: &Error) -> &'static str {
    match error {
        Error::Database(_) => "database",
        Error::Io(_) => "io",
        Error::PoolTimedOut => "pool_timeout",
        Error::PoolClosed => "pool_closed",
        Error::Protocol(_) => "protocol",
        _ => "other",
    }
}
fn transaction_span<DB: Database>(scope: StoreScope) -> tracing::Span {
    let system = if DB::NAME == "PostgreSQL" {
        "postgresql"
    } else {
        "sqlite"
    };
    macro_rules! span {
        ($level:expr) => {
            tracing::span!(target: crate::targets::SESSION, $level, "crabber.store.transaction",
                db.system = system, crabber.store.scope = scope.as_str(),
                outcome = tracing::field::Empty, error.kind = tracing::field::Empty,
                otel.kind = "client", otel.status_code = tracing::field::Empty)
        };
    }
    if scope == StoreScope::Unscoped {
        span!(tracing::Level::DEBUG)
    } else {
        span!(tracing::Level::INFO)
    }
}

#[derive(Clone, Copy, Debug)]
enum State {
    Opening,
    Open,
    Finishing,
    Closed,
}

struct TransactionTelemetry {
    span: tracing::Span,
    outcome_recorded: bool,
}
impl TransactionTelemetry {
    fn new<DB: Database>(scope: StoreScope) -> Self {
        Self {
            span: transaction_span::<DB>(scope),
            outcome_recorded: false,
        }
    }
    fn finish(&mut self, outcome: &'static str, kind: Option<&'static str>) {
        if self.outcome_recorded {
            return;
        }
        self.outcome_recorded = true;
        self.span.record("outcome", outcome);
        self.span.record(
            "otel.status_code",
            if kind.is_some() { "ERROR" } else { "OK" },
        );
        if let Some(kind) = kind {
            self.span.record("error.kind", kind);
        }
    }
}
impl Drop for TransactionTelemetry {
    fn drop(&mut self) {
        self.finish("abandoned", None);
    }
}

pub(crate) struct Transaction<'c, DB: Database> {
    telemetry: TransactionTelemetry,
    connection: MaybePoolConnection<'c, DB>,
    state: State,
    poisoned: Arc<AtomicBool>,
}

pub(crate) async fn begin<DB: Database>(
    pool: &Pool<DB>,
) -> Result<Transaction<'static, DB>, Error> {
    start_pool(pool, None, StoreScope::Unscoped).await
}
#[cfg(feature = "sqlite")]
pub(crate) async fn begin_with<DB: Database>(
    pool: &Pool<DB>,
    statement: &'static str,
) -> Result<Transaction<'static, DB>, Error> {
    start_pool(pool, Some(Cow::Borrowed(statement)), StoreScope::Unscoped).await
}
pub(crate) async fn begin_scoped<DB: Database>(
    pool: &Pool<DB>,
    scope: StoreScope,
) -> Result<Transaction<'static, DB>, Error> {
    start_pool(pool, None, scope).await
}
#[cfg(feature = "sqlite")]
pub(crate) async fn begin_with_scoped<DB: Database>(
    pool: &Pool<DB>,
    statement: &'static str,
    scope: StoreScope,
) -> Result<Transaction<'static, DB>, Error> {
    start_pool(pool, Some(Cow::Borrowed(statement)), scope).await
}
async fn start_pool<DB: Database>(
    pool: &Pool<DB>,
    statement: Option<Cow<'static, str>>,
    scope: StoreScope,
) -> Result<Transaction<'static, DB>, Error> {
    let mut telemetry = TransactionTelemetry::new::<DB>(scope);
    let connection = match pool.acquire().await {
        Ok(connection) => connection,
        Err(error) => {
            telemetry.finish("begin_failed", Some(sqlx_error_kind(&error)));
            return Err(error);
        }
    };
    Transaction::start(
        connection.into(),
        Arc::new(AtomicBool::new(false)),
        statement,
        telemetry,
    )
    .await
}
#[cfg(feature = "sqlite")]
pub(crate) async fn begin_on<'c, DB: Database>(
    connection: &'c mut DB::Connection,
    statement: Option<&'static str>,
) -> Result<Transaction<'c, DB>, Error> {
    begin_on_scoped::<DB>(connection, statement, StoreScope::Unscoped).await
}
#[cfg(feature = "sqlite")]
pub(crate) async fn begin_on_scoped<'c, DB: Database>(
    connection: &'c mut DB::Connection,
    statement: Option<&'static str>,
    scope: StoreScope,
) -> Result<Transaction<'c, DB>, Error> {
    Transaction::start(
        MaybePoolConnection::Connection(connection),
        Arc::new(AtomicBool::new(false)),
        statement.map(Cow::Borrowed),
        TransactionTelemetry::new::<DB>(scope),
    )
    .await
}

impl<'c, DB: Database> Transaction<'c, DB> {
    async fn start(
        connection: MaybePoolConnection<'c, DB>,
        poisoned: Arc<AtomicBool>,
        statement: Option<Cow<'static, str>>,
        telemetry: TransactionTelemetry,
    ) -> Result<Self, Error> {
        let mut transaction = Self {
            telemetry,
            connection,
            state: State::Opening,
            poisoned,
        };
        if let Err(error) =
            DB::TransactionManager::begin(&mut transaction.connection, statement).await
        {
            transaction.finish("begin_failed", Some(sqlx_error_kind(&error)));
            return Err(error);
        }
        transaction.state = State::Open;
        Ok(transaction)
    }

    #[cfg(test)]
    pub(crate) async fn begin(&mut self) -> Result<Transaction<'_, DB>, Error> {
        self.check()?;
        Transaction::start(
            MaybePoolConnection::<DB>::Connection(&mut *self.connection),
            self.poisoned.clone(),
            None,
            TransactionTelemetry::new::<DB>(StoreScope::Unscoped),
        )
        .await
    }

    pub(crate) async fn commit(mut self) -> Result<(), Error> {
        self.commit_in_place().await
    }
    // Retain ownership through an acknowledged failed COMMIT so SQLite's
    // existing finish boundary can explicitly roll back before returning.
    pub(crate) async fn commit_in_place(&mut self) -> Result<(), Error> {
        if let Err(error) = self.check() {
            self.finish("commit_failed", Some(sqlx_error_kind(&error)));
            return Err(error);
        }
        self.state = State::Finishing;
        if let Err(error) = DB::TransactionManager::commit(&mut self.connection).await {
            self.finish("commit_failed", Some(sqlx_error_kind(&error)));
            return Err(error);
        }
        self.finish("committed", None);
        self.state = State::Closed;
        Ok(())
    }

    pub(crate) async fn rollback(mut self) -> Result<(), Error> {
        self.check()?;
        self.state = State::Finishing;
        DB::TransactionManager::rollback(&mut self.connection).await?;
        self.finish("rolled_back", None);
        self.state = State::Closed;
        Ok(())
    }

    fn finish(&mut self, outcome: &'static str, kind: Option<&'static str>) {
        self.telemetry.finish(outcome, kind);
    }

    fn check(&self) -> Result<(), Error> {
        if self.poisoned.load(Ordering::SeqCst) {
            Err(Error::Protocol(format!(
                "cancelled {} transaction cannot be reused",
                DB::NAME
            )))
        } else {
            Ok(())
        }
    }

    fn close(&mut self) {
        self.poisoned.store(true, Ordering::SeqCst);
        if let MaybePoolConnection::PoolConnection(connection) = &mut self.connection {
            connection.close_on_drop();
        }
    }
}

impl<DB: Database> Drop for Transaction<'_, DB> {
    fn drop(&mut self) {
        if self.poisoned.load(Ordering::SeqCst)
            || matches!(self.state, State::Opening | State::Finishing)
        {
            self.close();
        } else if matches!(self.state, State::Open) {
            DB::TransactionManager::start_rollback(&mut self.connection);
        }
        if !matches!(self.state, State::Closed) {
            self.finish("abandoned", None);
        }
    }
}

impl<DB: Database> Deref for Transaction<'_, DB> {
    type Target = DB::Connection;
    fn deref(&self) -> &Self::Target {
        &self.connection
    }
}

impl<DB: Database> DerefMut for Transaction<'_, DB> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.connection
    }
}

impl<DB: Database> fmt::Debug for Transaction<'_, DB> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Transaction")
            .field("state", &self.state)
            .finish_non_exhaustive()
    }
}
