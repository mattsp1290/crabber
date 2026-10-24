//! Credential-free host example; --check also verifies a permission-denied shell call.
mod contexts;
mod probe;
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    probe::run(false, None).await?;
    if std::env::args().any(|arg| arg == "--check") {
        probe::run(true, None).await?;
    }
    contexts::run().await?;
    Ok(())
}
