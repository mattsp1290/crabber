//! Explicit backend selection for shared durability-test process helpers.

use std::{path::Path, process::Command};

/// A child attaches to its parent's backend; it never allocates a new fixture.
#[derive(Clone, Copy)]
pub enum ChildDatabase<'a> {
    Sqlite(&'a Path),
    #[cfg(feature = "postgres")]
    Postgres(&'a str),
}

impl ChildDatabase<'_> {
    /// Apply the handoff to this Command only, preserving each SQLite helper's
    /// existing environment-key convention.
    /// # Panics
    /// Panics if a PostgreSQL endpoint does not belong to the configured run.
    pub fn configure(self, command: &mut Command, sqlite_key: &str) {
        match self {
            Self::Sqlite(path) => {
                command
                    .env(sqlite_key, path)
                    .env_remove("CRABBER_TEST_POSTGRES_URL")
                    .env_remove("CRABBER_TEST_POSTGRES_CHILD_DATABASE");
            }
            #[cfg(feature = "postgres")]
            Self::Postgres(url) => {
                command.env_remove(sqlite_key).envs(
                    crate::postgres::TestDatabase::child_environment(url)
                        .expect("explicit parent PostgreSQL fixture handoff"),
                );
            }
        }
    }
}
