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

#[derive(Clone, Copy, Debug)]
enum State {
    Opening,
    Open,
    Finishing,
    Closed,
}

pub(crate) struct Transaction<'c, DB: Database> {
    connection: MaybePoolConnection<'c, DB>,
    state: State,
    poisoned: Arc<AtomicBool>,
}

pub(crate) async fn begin<DB: Database>(
    pool: &Pool<DB>,
) -> Result<Transaction<'static, DB>, Error> {
    start_pool(pool, None).await
}
#[cfg(feature = "sqlite")]
pub(crate) async fn begin_with<DB: Database>(
    pool: &Pool<DB>,
    statement: &'static str,
) -> Result<Transaction<'static, DB>, Error> {
    start_pool(pool, Some(Cow::Borrowed(statement))).await
}
async fn start_pool<DB: Database>(
    pool: &Pool<DB>,
    statement: Option<Cow<'static, str>>,
) -> Result<Transaction<'static, DB>, Error> {
    let connection = pool.acquire().await?;
    Transaction::start(
        connection.into(),
        Arc::new(AtomicBool::new(false)),
        statement,
    )
    .await
}
#[cfg(feature = "sqlite")]
pub(crate) async fn begin_on<'c, DB: Database>(
    connection: &'c mut DB::Connection,
    statement: Option<&'static str>,
) -> Result<Transaction<'c, DB>, Error> {
    Transaction::start(
        MaybePoolConnection::Connection(connection),
        Arc::new(AtomicBool::new(false)),
        statement.map(Cow::Borrowed),
    )
    .await
}

impl<'c, DB: Database> Transaction<'c, DB> {
    async fn start(
        connection: MaybePoolConnection<'c, DB>,
        poisoned: Arc<AtomicBool>,
        statement: Option<Cow<'static, str>>,
    ) -> Result<Self, Error> {
        let mut transaction = Self {
            connection,
            state: State::Opening,
            poisoned,
        };
        DB::TransactionManager::begin(&mut transaction.connection, statement).await?;
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
        )
        .await
    }

    pub(crate) async fn commit(mut self) -> Result<(), Error> {
        self.commit_in_place().await
    }
    // Retain ownership through an acknowledged failed COMMIT so SQLite's
    // existing finish boundary can explicitly roll back before returning.
    pub(crate) async fn commit_in_place(&mut self) -> Result<(), Error> {
        self.check()?;
        self.state = State::Finishing;
        DB::TransactionManager::commit(&mut self.connection).await?;
        self.state = State::Closed;
        Ok(())
    }

    pub(crate) async fn rollback(mut self) -> Result<(), Error> {
        self.check()?;
        self.state = State::Finishing;
        DB::TransactionManager::rollback(&mut self.connection).await?;
        self.state = State::Closed;
        Ok(())
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
