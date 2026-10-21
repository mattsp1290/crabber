//! The container owner remains alive until tests, descendants and DB sweep end.

use std::{env, time::SystemTime};
use testcontainers_modules::{
    postgres::Postgres,
    testcontainers::{ContainerAsync, ImageExt, runners::AsyncRunner},
};

pub(super) const ADMIN_OVERRIDE: &str = "CRABBER_TEST_POSTGRES_ADMIN_URL";
// Pin the same manifest used for the initial baseline, across architectures.
const IMAGE_TAG: &str =
    "14@sha256:c2427de38f998489d36de7ca3553db2134872c400f2b08be4b824e5c50e4d619";

pub(super) struct Endpoint {
    pub(super) admin: String,
    handle: Option<ContainerAsync<Postgres>>,
}
impl Endpoint {
    pub(super) async fn start() -> Result<Self, Box<dyn std::error::Error>> {
        if let Ok(admin) = env::var(ADMIN_OVERRIDE) {
            return Ok(Self {
                admin,
                handle: None,
            });
        }
        if env::var("TESTCONTAINERS_COMMAND").as_deref() == Ok("keep") {
            return Err("unset TESTCONTAINERS_COMMAND=keep for required fixture cleanup".into());
        }
        let owner = format!(
            "{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)?
                .as_nanos()
        );
        let instance=Postgres::default().with_fsync_enabled().with_tag(IMAGE_TAG)
            .with_label("crabber.test-runner",&owner)
            .with_cmd(["postgres","-c","fsync=on","-c","synchronous_commit=on","-c","full_page_writes=on"])
            .start().await.map_err(|_|"cannot start PostgreSQL 14 testcontainer; check Docker availability and image access")?;
        println!(
            "managed PostgreSQL container={} owner={owner}",
            instance.id()
        );
        let host = instance
            .get_host()
            .await
            .map_err(|_| "cannot resolve managed PostgreSQL host")?;
        let port = instance
            .get_host_port_ipv4(5432)
            .await
            .map_err(|_| "cannot resolve managed PostgreSQL port")?;
        let admin = format!("postgres://postgres:postgres@{host}:{port}/postgres");
        Ok(Self {
            admin,
            handle: Some(instance),
        })
    }
    pub(super) async fn finish(self) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(container) = self.handle {
            container
                .rm()
                .await
                .map_err(|_| "cannot remove owned PostgreSQL container")?;
        }
        Ok(())
    }
}
