//! Own the connection before transaction I/O so cancellation cannot return ambiguous state.
use sqlx::{Database, Error, PgPool, Postgres, TransactionManager, pool::MaybePoolConnection};
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

pub(crate) async fn begin(pool: &PgPool) -> Result<Transaction<'static, Postgres>, Error> {
    let connection = pool.acquire().await?;
    Transaction::start(connection.into(), Arc::new(AtomicBool::new(false))).await
}

impl<'c, DB: Database> Transaction<'c, DB> {
    async fn start(
        connection: MaybePoolConnection<'c, DB>,
        poisoned: Arc<AtomicBool>,
    ) -> Result<Self, Error> {
        let mut transaction = Self {
            connection,
            state: State::Opening,
            poisoned,
        };
        DB::TransactionManager::begin(&mut transaction.connection, None).await?;
        transaction.state = State::Open;
        Ok(transaction)
    }

    #[cfg(test)]
    pub(crate) async fn begin(&mut self) -> Result<Transaction<'_, DB>, Error> {
        self.check()?;
        Transaction::start(
            MaybePoolConnection::<DB>::Connection(&mut *self.connection),
            self.poisoned.clone(),
        )
        .await
    }

    pub(crate) async fn commit(mut self) -> Result<(), Error> {
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
            Err(Error::Protocol(
                "cancelled PostgreSQL transaction cannot be reused".into(),
            ))
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
