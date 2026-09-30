# Crabber session stores

The `postgres` feature uses `sqlx` with Tokio, rustls, and a PostgreSQL pool. It targets PostgreSQL 14 or newer and requires a dedicated database. Migrations create the Crabber tables in the default schema, so do not run them in a shared application schema.

The host calls `PostgresStore::migrate(url)` before `PostgresStore::connect(url)`. Connect verifies the schema version without changing it. The default build does not include the PostgreSQL driver.

Set `CRABBER_TEST_POSTGRES_URL` to run the live contract tests. Set `CRABBER_REQUIRE_POSTGRES=1` to fail if that URL is missing.
