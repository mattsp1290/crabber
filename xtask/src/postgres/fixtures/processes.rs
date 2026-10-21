//! Kill a real database-connected parent with a separately grouped grandchild.

use super::super::child;
use crabber_test_support::postgres::{DatabaseMode, RunFixture, TestDatabase};
use std::{
    fs,
    path::Path,
    process::Command,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

pub(super) async fn verify(run: &RunFixture) -> Result<()> {
    let directory = crate::workspace()
        .join("target/test-performance")
        .join(format!("fixture-processes-{}", run.identity()));
    fs::create_dir_all(&directory)?;
    scoped_history(run, &directory).await?;
    kill_parent(run, &directory).await?;
    panic_child(run, &directory).await?;
    panic_after_create(run, &directory).await?;
    fs::remove_dir_all(&directory)?;
    println!(
        "fixture process cleanup: killed parent, separate-group grandchild, panic and explicit child identities passed"
    );
    Ok(())
}
fn command(database: &TestDatabase, ready: &Path, mode: &str) -> Result<Command> {
    let mut command = Command::new(std::env::current_exe()?);
    command
        .args(["postgres-fixture-helper", mode])
        .current_dir(crate::workspace())
        .env(child::CHILD_URL, database.url())
        .env(child::CHILD_ID, database.identity())
        .env(child::READY, ready);
    Ok(command)
}
fn read_identity(path: &Path, identity: &str) -> Result<serde_json::Value> {
    let record: serde_json::Value = serde_json::from_slice(&fs::read(path)?)?;
    if record["database"] != identity {
        return Err("helper readiness has the wrong database identity".into());
    }
    Ok(record)
}
async fn kill_parent(run: &RunFixture, directory: &Path) -> Result<()> {
    let database = TestDatabase::for_run(run, DatabaseMode::Current).await?;
    let parent = directory.join("parent.json");
    let grandchild = directory.join("grandchild.json");
    let mut command = command(&database, &parent, "parent")?;
    command.env(child::DESCENDANT_READY, &grandchild);
    let signal = Arc::new(AtomicUsize::new(0));
    let cancelled = Arc::clone(&signal);
    let observation = parent.clone();
    let waiter = std::thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(15);
        while !observation.exists() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(20));
        }
        cancelled.store(2, Ordering::SeqCst);
        observation.exists()
    });
    let result = crate::process::status(&mut command, &signal);
    let ready = waiter
        .join()
        .map_err(|_| "fixture readiness waiter panicked")?;
    if !ready {
        return Err("fixture parent/grandchild did not reach the database barrier".into());
    }
    if result?.success() {
        return Err("killed fixture parent unexpectedly succeeded".into());
    }
    for path in [&parent, &grandchild] {
        let record = read_identity(path, database.identity())?;
        let pid = record["pid"].as_u64().ok_or("missing helper PID")?;
        if Path::new(&format!("/proc/{pid}")).exists() {
            return Err("owned fixture helper survived process supervision".into());
        }
    }
    #[cfg(target_os = "linux")]
    crate::process::verify_drained()?;
    database.finish().await?;
    Ok(())
}
async fn panic_child(run: &RunFixture, directory: &Path) -> Result<()> {
    let database = TestDatabase::for_run(run, DatabaseMode::Current).await?;
    let ready = directory.join("panic.json");
    let mut command = command(&database, &ready, "panic")?;
    let result = crate::process::status(&mut command, &AtomicUsize::new(0))?;
    read_identity(&ready, database.identity())?;
    if result.code() != Some(101) {
        return Err("fixture helper did not produce the expected panic exit".into());
    }
    database.finish().await?;
    Ok(())
}

async fn panic_after_create(run: &RunFixture, directory: &Path) -> Result<()> {
    let ready = directory.join("allocated-panic.json");
    let mut command = Command::new(std::env::current_exe()?);
    command
        .args(["postgres-fixture-helper", "allocate-panic"])
        .envs(run.environment())
        .env(child::READY, &ready);
    let status = crate::process::status(&mut command, &AtomicUsize::new(0))?;
    let record: serde_json::Value = serde_json::from_slice(&fs::read(ready)?)?;
    let identity = record["database"]
        .as_str()
        .ok_or("allocated helper has no database identity")?;
    if status.code() != Some(101)
        || !run
            .registered_databases()
            .await?
            .iter()
            .any(|name| name == identity)
    {
        return Err("panic after CREATE lost its failure or ownership record".into());
    }
    Ok(())
}

async fn scoped_history(run: &RunFixture, directory: &Path) -> Result<()> {
    let before = run.registered_databases().await?;
    let ready = directory.join("scoped-history.json");
    let mut command = Command::new(std::env::current_exe()?);
    command
        .args(["postgres-fixture-helper", "scoped-history"])
        .envs(run.environment())
        .env("CRABBER_REQUIRE_POSTGRES", "1")
        .env(child::READY, &ready);
    let status = crate::process::status(&mut command, &AtomicUsize::new(0))?;
    if !status.success() {
        return Err("scoped history helper failed".into());
    }
    let record: serde_json::Value = serde_json::from_slice(&fs::read(ready)?)?;
    let identity = record["database"]
        .as_str()
        .ok_or("scoped history has no fixture identity")?;
    if !identity.starts_with(&format!("ct_{}_", run.identity()))
        || run.registered_databases().await? != before
    {
        return Err("scoped history leaked a database or used another run".into());
    }
    Ok(())
}
