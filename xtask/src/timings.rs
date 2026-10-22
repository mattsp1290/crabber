//! Sanitized, incrementally persisted gate results. Raw child output stays on the
//! caller's stdout/stderr and is never copied into the JSON report.

use std::{
    fs,
    path::Path,
    time::{Instant, SystemTime},
};

use serde_json::{Value, json};

use crate::checks::GateCommand;

pub(crate) struct Report;

impl Report {
    pub(crate) fn run(mut gates: Vec<GateCommand>, path: Option<&Path>) -> i32 {
        let signal = match crate::process::cancellation() {
            Ok(signal) => signal,
            Err(error) => {
                eprintln!("cannot install cancellation handler: {error}");
                return 1;
            }
        };
        let start = Instant::now();
        let mut report = json!({
            "version": 1,
            "source": source_identity(),
            "available_parallelism": std::thread::available_parallelism().map_or(1, std::num::NonZero::get),
            "started_unix_seconds": SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs(),
            "status": "running",
            "gates": gates.iter().map(|gate| {
                let mut record = gate.inventory.clone();
                record["status"] = json!("pending");
                record
            }).collect::<Vec<_>>(),
        });
        if let Err(error) = persist(path, &report) {
            eprintln!("cannot create timing report: {error}");
            return 1;
        }
        let mut exit = 0;
        for (index, gate) in gates.iter_mut().enumerate() {
            let interrupted = signal.load(std::sync::atomic::Ordering::SeqCst);
            if interrupted != 0 {
                exit = 128 + i32::try_from(interrupted).unwrap_or(1);
            }
            if exit != 0 {
                report["gates"][index]["status"] = json!("cancelled");
                continue;
            }
            report["gates"][index]["status"] = json!("running");
            if let Err(error) = persist(path, &report) {
                eprintln!("cannot update timing report: {error}");
                exit = 1;
                report["gates"][index]["status"] = json!("cancelled");
                continue;
            }
            let phase = Instant::now();
            let result =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| gate.execute(&signal)));
            let mut code = result.unwrap_or(Err(1)).err().unwrap_or(0);
            let interrupted = signal.load(std::sync::atomic::Ordering::SeqCst);
            if interrupted != 0 {
                code = 128 + i32::try_from(interrupted).unwrap_or(1);
            }
            report["gates"][index]["wall_seconds"] = json!(phase.elapsed().as_secs_f64());
            report["gates"][index]["exit_code"] = json!(code);
            report["gates"][index]["status"] = json!(if interrupted != 0 {
                "cancelled"
            } else if code == 0 {
                "passed"
            } else {
                "failed"
            });
            exit = code;
            // Persist every completed phase, including a failing child, before
            // considering the next obligation.
            if let Err(error) = persist(path, &report) {
                eprintln!("cannot update timing report: {error}");
                exit = 1;
            }
        }
        report["status"] = json!(if signal.load(std::sync::atomic::Ordering::SeqCst) != 0 {
            "cancelled"
        } else if exit == 0 {
            "passed"
        } else {
            "failed"
        });
        report["exit_code"] = json!(exit);
        report["wall_seconds"] = json!(start.elapsed().as_secs_f64());
        if let Err(error) = persist(path, &report) {
            eprintln!("cannot finish timing report: {error}");
            return 1;
        }
        exit
    }
}

fn persist(path: Option<&Path>, report: &Value) -> std::io::Result<()> {
    if let Some(path) = path {
        if let Some(parent) = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            fs::create_dir_all(parent)?;
        }
        // A reader sees either the previous complete JSON document or the new
        // one. Each invocation must own its requested output path.
        let temporary = path.with_extension(format!("{}.tmp", std::process::id()));
        fs::write(&temporary, serde_json::to_vec_pretty(report)?)?;
        fs::rename(temporary, path)?;
    }
    Ok(())
}

pub(crate) fn source_identity() -> Value {
    let output = |program: &str, args: &[&str]| {
        std::process::Command::new(program)
            .args(args)
            .current_dir(crate::workspace())
            .output()
            .ok()
            .filter(|output| output.status.success())
            .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_owned())
    };
    json!({
        "revision": output("git", &["rev-parse", "HEAD"]),
        "tree": output("git", &["rev-parse", "HEAD^{tree}"]),
        "tracked_source_clean": std::process::Command::new("git").args(["diff", "--quiet", "HEAD"])
            .current_dir(crate::workspace()).status().ok().map(|status| status.success()),
        "rustc": output("rustc", &["-Vv"]),
        "cargo": output("cargo", &["-V"]),
    })
}
