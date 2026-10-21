//! PostgreSQL databases owned by one quality-gate invocation.
//!
//! Only the runner holds the administrative advisory lock. Tests register create
//! intent before cloning a database, then explicitly finish after their Stores,
//! raw pools and child processes have been drained. Panics leave registry rows
//! for the runner's final sweep. Connection URLs are never included in errors.

use std::fmt;

/// Dedicated administrative endpoint passed only from runner to its children.
pub const ADMIN_URL: &str = "CRABBER_TEST_RUN_POSTGRES_ADMIN_URL";
/// Random run identity, also the administrative advisory-lock identity.
pub const RUN_ID: &str = "CRABBER_TEST_POSTGRES_RUN";

/// Sanitized fixture failure. `SQLx` errors and connection URLs stay private.
#[derive(Debug)]
pub struct FixtureError(&'static str);
impl fmt::Display for FixtureError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0)
    }
}
impl std::error::Error for FixtureError {}

type Result<T> = std::result::Result<T, FixtureError>;

/// Current clones the run's migrated template; Blank really starts at template0.
#[derive(Clone, Copy)]
pub enum DatabaseMode {
    Current,
    Blank,
}

mod child;
mod config;
mod database;
mod history_process;
mod runner;
use config::Config;
pub use database::TestDatabase;
pub use runner::RunFixture;

#[cfg(test)]
mod tests {
    use super::*;
    use url::Url;
    #[test]
    fn ownership_rejects_foreign_names_and_sql_injection() {
        let config = Config::new("postgres://localhost/postgres", &"a".repeat(32)).unwrap();
        assert!(config.owns(&config.template()));
        assert!(config.owns(&config.database()));
        for name in [
            "postgres",
            "ct_other_template",
            "ct_a;DROP DATABASE postgres",
            "ct_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa_template_extra",
        ] {
            assert!(!config.owns(name));
        }
        assert!(Config::new("postgres://localhost/postgres", "abc';DROP SCHEMA public").is_err());
    }
    #[test]
    fn database_url_cannot_retain_an_override_to_the_admin_database() {
        let config = Config::new(
            "postgres://localhost/postgres?dbname=postgres&sslmode=disable",
            &"b".repeat(32),
        )
        .unwrap();
        let url = Url::parse(&config.url(&config.database())).unwrap();
        assert!(url.path().starts_with("/ct_"));
        assert!(!url.query_pairs().any(|(key, _)| key == "dbname"));
    }
}
