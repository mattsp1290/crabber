//! The container owner remains alive until tests, descendants and DB sweep end.

use sqlx::{Connection, PgConnection, postgres::PgConnectOptions};
use std::{
    env,
    net::{IpAddr, SocketAddr},
    time::{Duration, SystemTime},
};
use testcontainers_modules::{
    postgres::Postgres,
    testcontainers::{ContainerAsync, ImageExt, runners::AsyncRunner},
};

pub(super) const ADMIN_OVERRIDE: &str = "CRABBER_TEST_POSTGRES_ADMIN_URL";
const TRANSPORT: &str = "CRABBER_TEST_POSTGRES_TRANSPORT";
// Pin the same manifest used for the initial baseline, across architectures.
const IMAGE_TAG: &str =
    "14@sha256:c2427de38f998489d36de7ca3553db2134872c400f2b08be4b824e5c50e4d619";

pub(super) struct Endpoint {
    pub(super) admin: String,
    pub(super) transport: &'static str,
    handle: Option<ContainerAsync<Postgres>>,
}
impl Endpoint {
    pub(super) async fn start() -> Result<Self, Box<dyn std::error::Error>> {
        if let Ok(admin) = env::var(ADMIN_OVERRIDE) {
            return Ok(Self {
                admin,
                transport: "external",
                handle: None,
            });
        }
        if env::var("TESTCONTAINERS_COMMAND").as_deref() == Ok("keep") {
            return Err("unset TESTCONTAINERS_COMMAND=keep for required fixture cleanup".into());
        }
        let transport = env::var(TRANSPORT).unwrap_or_else(|_| "auto".into());
        if !matches!(transport.as_str(), "auto" | "bridge" | "published") {
            return Err("CRABBER_TEST_POSTGRES_TRANSPORT must be auto, bridge or published".into());
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
        let bridge = if transport == "published" {
            None
        } else {
            local_bridge(
                &instance,
                &host.to_string(),
                &format!("postgres://postgres:postgres@{host}:{port}/postgres"),
            )
            .await
        };
        if transport == "bridge" && bridge.is_none() {
            instance
                .rm()
                .await
                .map_err(|_| "cannot remove unreachable owned PostgreSQL container")?;
            return Err("owned PostgreSQL bridge could not be verified from this runner".into());
        }
        let (address, mode) = bridge.map_or_else(
            || (format!("{host}:{port}"), "published"),
            |address| (address.to_string(), "bridge"),
        );
        println!("managed PostgreSQL transport={mode}");
        let admin = format!("postgres://postgres:postgres@{address}/postgres");
        Ok(Self {
            admin,
            transport: mode,
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

/// A local Linux runner can reach its owned container without Docker's TCP
/// proxy. Remote/rootless Docker keeps the published endpoint when this bounded
/// reachability check fails. Container creation and durability are unchanged.
async fn local_bridge(
    instance: &ContainerAsync<Postgres>,
    host: &str,
    published: &str,
) -> Option<SocketAddr> {
    // A loopback Docker daemon can itself be reached through a tunnel. Keep
    // non-Unix daemon configurations on their explicitly published route.
    if env::var("DOCKER_HOST").is_ok_and(|value| !value.starts_with("unix://")) {
        return None;
    }
    if host != "localhost"
        && !host
            .trim_matches(['[', ']'])
            .parse::<IpAddr>()
            .is_ok_and(|ip| ip.is_loopback())
    {
        return None;
    }
    let published: PgConnectOptions = published.parse().ok()?;
    let probe = async {
        let address = SocketAddr::new(instance.get_bridge_ip_address().await.ok()?, 5432);
        let direct = published
            .clone()
            .host(&address.ip().to_string())
            .port(address.port());
        let (mut published, mut direct) = futures_util::try_join!(
            PgConnection::connect_with(&published),
            PgConnection::connect_with(&direct),
        )
        .ok()?;
        // Confirm the actual cluster before any fixture administration. This
        // also protects rootless configurations with overlapping private IPs.
        let query = "SELECT system_identifier::text FROM pg_control_system()";
        let expected: String = sqlx::query_scalar(query)
            .fetch_one(&mut published)
            .await
            .ok()?;
        let actual: String = sqlx::query_scalar(query)
            .fetch_one(&mut direct)
            .await
            .ok()?;
        published.close().await.ok()?;
        direct.close().await.ok()?;
        (expected == actual).then_some(address)
    };
    tokio::time::timeout(Duration::from_millis(500), probe)
        .await
        .ok()
        .flatten()
}
