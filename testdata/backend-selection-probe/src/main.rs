macro_rules! check {
    ($condition:expr $(, $message:expr)? $(,)?) => {
        if !$condition {
            eprintln!("backend-selection assertion at {}:{}", file!(), line!());
            return Err("backend-selection assertion failed".into());
        }
    };
}
#[cfg(any(
    feature = "default",
    feature = "memory",
    feature = "postgres",
    feature = "sqlite"
))]
macro_rules! check_eq {
    ($left:expr, $right:expr $(, $message:expr)? $(,)?) => {
        check!($left == $right);
    };
}
#[cfg(any(feature = "postgres", feature = "sqlite"))]
mod native;
#[cfg(any(feature = "postgres", feature = "sqlite"))]
mod profile;
mod selection;
#[cfg(any(feature = "postgres", feature = "sqlite"))]
#[path = "../../../examples/server-host-probe/src"]
mod audited {
    mod store;
    pub use store::AuditedStore;
}

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
#[cfg(any(
    feature = "default",
    feature = "memory",
    feature = "postgres",
    feature = "sqlite"
))]
const BOUND: std::time::Duration = std::time::Duration::from_secs(30);

#[tokio::main]
async fn main() {
    std::panic::set_hook(Box::new(|info| {
        if let Some(location) = info.location() {
            eprintln!(
                "backend-selection contract assertion at {}:{}",
                location.file(),
                location.line()
            );
        } else {
            eprintln!("backend-selection contract assertion failed");
        }
    }));
    if run().await.is_err() {
        eprintln!("backend-selection runtime failed; inspect the owned sanitized case log");
        std::process::exit(1);
    }
}

async fn run() -> Result<()> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    let (backend, cases) = args.split_first().ok_or("backend required")?;
    if cases.is_empty() {
        return Err("cases required".into());
    }
    for case in cases {
        selection::run(backend, case).await?;
        println!(
            "{}",
            serde_json::json!({"case":case,"backend":backend,"result":"passed"})
        );
    }
    Ok(())
}
