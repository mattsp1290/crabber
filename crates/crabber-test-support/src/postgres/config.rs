//! PostgreSQL fixture config implementation.

use super::{DatabaseMode, FixtureError, Result};
use sqlx::{
    PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use std::{
    str::FromStr,
    time::{Duration, Instant},
};
use url::Url;
use uuid::Uuid;

#[derive(Clone)]
pub(super) struct Config {
    pub(super) admin: Url,
    pub(super) nonce: String,
}
impl Config {
    pub(super) fn new(admin: &str, nonce: &str) -> Result<Self> {
        if nonce.len() != 32
            || !nonce
                .bytes()
                .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
        {
            return Err(FixtureError("invalid PostgreSQL fixture run identity"));
        }
        let admin = Url::parse(admin)
            .map_err(|_| FixtureError("invalid administrative PostgreSQL endpoint"))?;
        if !matches!(admin.scheme(), "postgres" | "postgresql") {
            return Err(FixtureError(
                "administrative fixture endpoint must use PostgreSQL",
            ));
        }
        Ok(Self {
            admin,
            nonce: nonce.to_owned(),
        })
    }
    pub(super) fn schema(&self) -> String {
        format!("ct_{}", self.nonce)
    }
    pub(super) fn template(&self) -> String {
        format!("ct_{}_template", self.nonce)
    }
    pub(super) fn database(&self) -> String {
        format!(
            "ct_{}_{}",
            self.nonce,
            &Uuid::new_v4().simple().to_string()[..16]
        )
    }
    pub(super) fn owns(&self, name: &str) -> bool {
        if name == self.template() {
            return true;
        }
        name.strip_prefix(&format!("ct_{}_", self.nonce))
            .is_some_and(|suffix| {
                suffix.len() == 16
                    && suffix
                        .bytes()
                        .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
            })
    }
    pub(super) fn url(&self, database: &str) -> String {
        let mut url = self.admin.clone();
        url.set_path(&format!("/{database}"));
        // A dbname query parameter can override the URL path in libpq clients.
        let query: Vec<_> = url
            .query_pairs()
            .filter(|(key, _)| key != "dbname")
            .map(|(key, value)| (key.into_owned(), value.into_owned()))
            .collect();
        url.set_query(None);
        if !query.is_empty() {
            url.query_pairs_mut().extend_pairs(query);
        }
        url.into()
    }
    pub(super) async fn pool(&self) -> Result<PgPool> {
        let options = PgConnectOptions::from_str(self.admin.as_str())
            .map_err(|_| FixtureError("invalid administrative PostgreSQL options"))?;
        PgPoolOptions::new()
            .max_connections(1)
            .acquire_timeout(Duration::from_secs(10))
            .connect_with(options.application_name("crabber-test-admin"))
            .await
            .map_err(|_| FixtureError("cannot connect to administrative PostgreSQL endpoint"))
    }
    pub(super) async fn create(&self, pool: &PgPool, name: &str, mode: DatabaseMode) -> Result<()> {
        if !self.owns(name) {
            return Err(FixtureError("refusing unowned database creation"));
        }
        let mut connection = pool
            .acquire()
            .await
            .map_err(|_| FixtureError("cannot acquire owned database administration connection"))?;
        // Keep lock ownership on the backend executing CREATE/DROP. Closing the
        // client during CREATE may not cancel the server operation immediately;
        // the sweep must wait until that backend releases this lock.
        connection.close_on_drop();
        sqlx::query("SELECT pg_advisory_lock(hashtextextended($1, 1))")
            .bind(name)
            .execute(&mut *connection)
            .await
            .map_err(|_| FixtureError("cannot acquire owned database provisioning lock"))?;
        sqlx::query(&format!(
            "INSERT INTO {}.databases (name) VALUES ($1)",
            self.schema()
        ))
        .bind(name)
        .execute(&mut *connection)
        .await
        .map_err(|_| FixtureError("cannot register database create intent"))?;
        let template = match mode {
            DatabaseMode::Current => self.template(),
            DatabaseMode::Blank => "template0".to_owned(),
        };
        if let Err(error) = sqlx::query(&format!("CREATE DATABASE {name} TEMPLATE {template}"))
            .execute(&mut *connection)
            .await
        {
            if error
                .as_database_error()
                .is_some_and(|error| error.code().as_deref() == Some("42P04"))
            {
                // A duplicate name is a pre-existing database, never ours to
                // reclaim. Remove intent so the final sweep cannot drop it.
                sqlx::query(&format!(
                    "DELETE FROM {}.databases WHERE name=$1",
                    self.schema()
                ))
                .bind(name)
                .execute(&mut *connection)
                .await
                .map_err(|_| FixtureError("cannot revoke duplicate database intent"))?;
            }
            return Err(FixtureError(
                "cannot create owned PostgreSQL database; check CREATEDB permission and template connections",
            ));
        }
        connection
            .close()
            .await
            .map_err(|_| FixtureError("cannot close owned database administration connection"))?;
        Ok(())
    }
    pub(super) async fn drop_database(&self, pool: &PgPool, name: &str, force: bool) -> Result<()> {
        if !self.owns(name) {
            return Err(FixtureError("refusing unowned database cleanup"));
        }
        let mut connection = pool
            .acquire()
            .await
            .map_err(|_| FixtureError("cannot acquire owned database administration connection"))?;
        // Keep lock ownership on the backend executing CREATE/DROP. Closing the
        // client during CREATE may not cancel the server operation immediately;
        // the sweep must wait until that backend releases this lock.
        connection.close_on_drop();
        sqlx::query("SELECT pg_advisory_lock(hashtextextended($1, 1))")
            .bind(name)
            .execute(&mut *connection)
            .await
            .map_err(|_| FixtureError("cannot acquire owned database provisioning lock"))?;
        let registered: bool = sqlx::query_scalar(&format!(
            "SELECT EXISTS(SELECT 1 FROM {}.databases WHERE name=$1)",
            self.schema()
        ))
        .bind(name)
        .fetch_one(&mut *connection)
        .await
        .map_err(|_| FixtureError("cannot verify database ownership registry"))?;
        if !registered {
            return Err(FixtureError("database is absent from ownership registry"));
        }
        if force {
            sqlx::query("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname=$1 AND pid<>pg_backend_pid()")
                .bind(name).execute(&mut *connection).await.map_err(|_| FixtureError("cannot terminate owned database connections"))?;
        } else {
            let deadline = Instant::now() + Duration::from_secs(5);
            loop {
                let sessions: i64 =
                    sqlx::query_scalar("SELECT count(*) FROM pg_stat_activity WHERE datname=$1")
                        .bind(name)
                        .fetch_one(&mut *connection)
                        .await
                        .map_err(|_| {
                            FixtureError("cannot check owned database connection drain")
                        })?;
                if sessions == 0 {
                    break;
                }
                if Instant::now() >= deadline {
                    let activity: Vec<(i32, Option<String>, String, String)> = sqlx::query_as(
                        "SELECT pid,state,backend_type,application_name FROM pg_stat_activity WHERE datname=$1",
                    )
                    .bind(name)
                    .fetch_all(&mut *connection)
                    .await
                    .map_err(|_| FixtureError("cannot inspect undrained fixture connections"))?;
                    eprintln!(
                        "fixture {name} has undrained sessions (pid,state,backend type): {activity:?}"
                    );
                    return Err(FixtureError(
                        "owned database connections did not drain; drop Stores and close pools before finish",
                    ));
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
        sqlx::query(&format!("DROP DATABASE IF EXISTS {name}"))
            .execute(&mut *connection)
            .await
            .map_err(|_| FixtureError("cannot drop owned PostgreSQL database"))?;
        sqlx::query(&format!(
            "DELETE FROM {}.databases WHERE name=$1",
            self.schema()
        ))
        .bind(name)
        .execute(&mut *connection)
        .await
        .map_err(|_| FixtureError("cannot remove database ownership record"))?;
        connection
            .close()
            .await
            .map_err(|_| FixtureError("cannot close owned database administration connection"))?;
        Ok(())
    }
}
