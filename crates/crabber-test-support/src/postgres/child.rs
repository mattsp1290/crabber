//! Explicit parent-to-child database attachment, without process environment writes.

use super::{ADMIN_URL, Config, FixtureError, RUN_ID, Result, TestDatabase};
use sqlx::{Connection, PgConnection};
use std::env;
use url::Url;

const CHILD_URL: &str = "CRABBER_TEST_POSTGRES_URL";
const CHILD_ID: &str = "CRABBER_TEST_POSTGRES_CHILD_DATABASE";

fn attachment(config: &Config, endpoint: &str) -> Result<String> {
    let endpoint =
        Url::parse(endpoint).map_err(|_| FixtureError("invalid child PostgreSQL endpoint"))?;
    let name = endpoint.path().trim_start_matches('/');
    if !config.owns(name) || name == config.template() || endpoint.as_str() != config.url(name) {
        return Err(FixtureError("child endpoint is not an owned test database"));
    }
    Ok(name.to_owned())
}

fn config() -> Result<Config> {
    Config::new(
        &env::var(ADMIN_URL)
            .map_err(|_| FixtureError("missing child administrative fixture configuration"))?,
        &env::var(RUN_ID).map_err(|_| FixtureError("missing child fixture run identity"))?,
    )
}

impl TestDatabase {
    /// Environment entries for one child Command. The URL must be the parent's
    /// owned fixture URL. Never apply these entries to the process environment.
    /// # Errors
    /// Fails if the endpoint does not belong to this fixture run.
    pub fn child_environment(endpoint: &str) -> Result<[(&'static str, String); 2]> {
        let identity = attachment(&config()?, endpoint)?;
        Ok([(CHILD_URL, endpoint.to_owned()), (CHILD_ID, identity)])
    }

    /// Attach a marked helper to its parent's registered database and verify
    /// the actual server identity using an independent, explicitly closed client.
    /// Only the opaque identity is printed. The helper never provisions or drops.
    /// # Errors
    /// Fails on a missing handoff, foreign identity or unexpected server database.
    pub async fn attached_url() -> Result<String> {
        let config = config()?;
        let endpoint =
            env::var(CHILD_URL).map_err(|_| FixtureError("missing parent database endpoint"))?;
        let identity = attachment(&config, &endpoint)?;
        if env::var(CHILD_ID).as_deref() != Ok(identity.as_str()) {
            return Err(FixtureError(
                "child database identity does not match parent handoff",
            ));
        }
        let admin = config.pool().await?;
        let registered: bool = sqlx::query_scalar(&format!(
            "SELECT EXISTS (SELECT 1 FROM {}.databases WHERE name=$1)",
            config.schema()
        ))
        .bind(&identity)
        .fetch_one(&admin)
        .await
        .map_err(|_| FixtureError("cannot verify registered parent database"))?;
        admin.close().await;
        if !registered {
            return Err(FixtureError(
                "parent database is not registered to this run",
            ));
        }
        let mut connection = PgConnection::connect(&endpoint)
            .await
            .map_err(|_| FixtureError("cannot attach child to parent database"))?;
        let actual: String = sqlx::query_scalar("SELECT current_database()")
            .fetch_one(&mut connection)
            .await
            .map_err(|_| FixtureError("cannot verify attached child database"))?;
        connection
            .close()
            .await
            .map_err(|_| FixtureError("cannot close child identity connection"))?;
        if actual != identity {
            return Err(FixtureError(
                "attached child database differs from parent handoff",
            ));
        }
        eprintln!("attached PostgreSQL child database={identity}");
        Ok(endpoint)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn child_attachment_rejects_foreign_database_server_and_query_overrides() {
        let config = Config::new("postgres://localhost/postgres", &"a".repeat(32)).unwrap();
        let name = config.database();
        let endpoint = config.url(&name);
        assert_eq!(attachment(&config, &endpoint).unwrap(), name);
        for invalid in [
            config.url(&config.template()),
            config.url("postgres"),
            endpoint.replace("localhost", "foreign.invalid"),
            format!("{endpoint}?dbname=postgres"),
            "invalid endpoint".to_owned(),
        ] {
            assert!(attachment(&config, &invalid).is_err());
        }
    }
}
