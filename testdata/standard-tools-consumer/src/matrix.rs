//! Credential-free external host qualification, including a reopened SQLite store.
mod durable;
pub(crate) use durable::{done, results, turn, value};
mod lifecycle;
mod policies;
pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    durable::run().await?;
    policies::run().await?;
    lifecycle::run().await?;
    Ok(())
}
