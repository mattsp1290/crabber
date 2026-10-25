//! Explicit parent-to-child database attachment, without process environment writes.

use super::{ADMIN_URL, Config, FixtureError, RUN_ID, Result, TestDatabase};
use sqlx::{Connection, PgConnection};
use std::env;
use url::Url;

const CHILD_URL: &str = "CRABBER_TEST_POSTGRES_URL";
const CHILD_ID: &str = "CRABBER_TEST_POSTGRES_CHILD_DATABASE";
const CHILD_ROLE: &str = "CRABBER_TEST_POSTGRES_CHILD_ROLE";

fn attachment(config: &Config, endpoint: &str, role: Option<&str>) -> Result<String> {
    let endpoint =
        Url::parse(endpoint).map_err(|_| FixtureError("invalid child PostgreSQL endpoint"))?;
    let name = endpoint.path().trim_start_matches('/');
    if !config.owns(name) || name == config.template() {
        return Err(FixtureError("child endpoint is not an owned test database"));
    }
    let mut expected = Url::parse(&config.url(name))
        .map_err(|_| FixtureError("invalid owned PostgreSQL endpoint"))?;
    let mut actual = endpoint.clone();
    if let Some(role) = role {
        if role.is_empty() {
            return Err(FixtureError("missing child PostgreSQL role"));
        }
        expected
            .set_username(role)
            .map_err(|()| FixtureError("invalid child PostgreSQL role"))?;
        expected
            .set_password(None)
            .map_err(|()| FixtureError("invalid owned PostgreSQL endpoint"))?;
        actual
            .set_password(None)
            .map_err(|()| FixtureError("invalid child PostgreSQL endpoint"))?;
    }
    if actual != expected {
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
    pub fn child_environment(endpoint: &str) -> Result<[(&'static str, String); 3]> {
        let identity = attachment(&config()?, endpoint, None)?;
        Ok([
            (CHILD_URL, endpoint.to_owned()),
            (CHILD_ID, identity),
            (CHILD_ROLE, String::new()),
        ])
    }

    /// Delegate this owned database to a child using an explicit application role.
    /// Administrative provisioning and registry checks retain the runner identity.
    /// # Errors
    /// Fails when the role cannot form a PostgreSQL endpoint.
    pub fn child_environment_as_role(
        &self,
        role: &str,
        password: &str,
    ) -> Result<[(&'static str, String); 3]> {
        if role.is_empty() {
            return Err(FixtureError("missing child PostgreSQL role"));
        }
        let mut endpoint = Url::parse(&self.url())
            .map_err(|_| FixtureError("invalid owned PostgreSQL endpoint"))?;
        endpoint
            .set_username(role)
            .map_err(|()| FixtureError("invalid child PostgreSQL role"))?;
        endpoint
            .set_password(Some(password))
            .map_err(|()| FixtureError("invalid child PostgreSQL credentials"))?;
        Ok([
            (CHILD_URL, endpoint.into()),
            (CHILD_ID, self.identity().to_owned()),
            (CHILD_ROLE, role.to_owned()),
        ])
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
        let role = match env::var(CHILD_ROLE) {
            Ok(role) if role.is_empty() => None,
            Ok(role) => Some(role),
            Err(env::VarError::NotPresent) => None,
            Err(env::VarError::NotUnicode(_)) => {
                return Err(FixtureError("invalid child PostgreSQL role"));
            }
        };
        let identity = attachment(&config, &endpoint, role.as_deref())?;
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
        let (actual, actual_role, login_role): (String, String, String) =
            sqlx::query_as("SELECT current_database(), current_user::text, session_user::text")
                .fetch_one(&mut connection)
                .await
                .map_err(|_| FixtureError("cannot verify attached child database"))?;
        connection
            .close()
            .await
            .map_err(|_| FixtureError("cannot close child identity connection"))?;
        if actual != identity
            || role
                .as_ref()
                .is_some_and(|role| *role != actual_role || *role != login_role)
        {
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
        assert_eq!(attachment(&config, &endpoint, None).unwrap(), name);
        for invalid in [
            config.url(&config.template()),
            config.url("postgres"),
            endpoint.replace("localhost", "foreign.invalid"),
            format!("{endpoint}?dbname=postgres"),
            "invalid endpoint".to_owned(),
        ] {
            assert!(attachment(&config, &invalid, None).is_err());
        }
    }
    #[test]
    fn role_handoff_preserves_owned_server_database_and_exact_role() {
        let config = Config::new("postgres://admin@localhost/postgres", &"b".repeat(32)).unwrap();
        let name = config.database();
        let mut endpoint = Url::parse(&config.url(&name)).unwrap();
        endpoint.set_username("worker").unwrap();
        endpoint
            .set_password(Some("generated-fixture-password"))
            .unwrap();
        assert_eq!(
            attachment(&config, endpoint.as_str(), Some("worker")).unwrap(),
            name
        );
        assert!(attachment(&config, endpoint.as_str(), None).is_err());
        assert!(attachment(&config, endpoint.as_str(), Some("other")).is_err());
        assert!(attachment(&config, endpoint.as_str(), Some("")).is_err());
        for invalid in [
            endpoint.as_str().replace("localhost", "foreign.invalid"),
            endpoint.as_str().replace(&name, "postgres"),
            format!("{endpoint}?dbname=postgres"),
        ] {
            assert!(attachment(&config, &invalid, Some("worker")).is_err());
        }
    }
}
