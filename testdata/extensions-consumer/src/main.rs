//! Standalone public-facade consumer; dependencies resolve only through Git pins.
#[path = "../tests/support/agent.rs"]
mod agent_support;
mod common;
mod composition;
mod custom;
#[allow(dead_code)]
#[path = "../tests/contextual/options.rs"]
mod factory_options;
#[allow(dead_code)]
#[path = "../tests/support/owned_process.rs"]
mod owned_process;
mod policies;
#[cfg(feature = "sqlite")]
mod restart;
#[path = "../tests/support/scripted.rs"]
mod scripted;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut arguments = std::env::args().skip(1);
    match arguments.next().as_deref() {
        Some("journeys") => composition::run().await?,
        #[cfg(feature = "sqlite")]
        Some(mode @ ("restart-phase-1" | "restart-phase-2")) => {
            let database = arguments.next().expect("database path");
            restart::run(mode, std::path::Path::new(&database)).await?;
        }
        _ => panic!("expected journeys or a SQLite restart phase"),
    }
    Ok(())
}
