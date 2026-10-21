//! Interruptible child commands. Every command gets its own Unix process group;
//! a handled interrupt stops that group before returning to the gate reporter.

use std::{
    io::{self, Read},
    process::{Child, Command, ExitStatus, Output, Stdio},
    sync::{
        Arc, OnceLock,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

static SIGNAL: OnceLock<Arc<AtomicUsize>> = OnceLock::new();

pub(crate) fn cancellation() -> io::Result<Arc<AtomicUsize>> {
    if let Some(signal) = SIGNAL.get() {
        return Ok(Arc::clone(signal));
    }
    let signal = Arc::new(AtomicUsize::new(0));
    #[cfg(unix)]
    for number in [signal_hook::consts::SIGINT, signal_hook::consts::SIGTERM] {
        signal_hook::flag::register_usize(
            number,
            Arc::clone(&signal),
            usize::try_from(number).unwrap(),
        )?;
    }
    Ok(Arc::clone(SIGNAL.get_or_init(|| signal)))
}

fn spawn(command: &mut Command) -> io::Result<Child> {
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        command.process_group(0);
    }
    command.spawn()
}

fn wait(child: &mut Child, signal: &AtomicUsize) -> io::Result<ExitStatus> {
    loop {
        if signal.load(Ordering::SeqCst) != 0 {
            terminate(child)?;
            return child.wait();
        }
        if let Some(status) = child.try_wait()? {
            return Ok(status);
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

fn terminate(child: &mut Child) -> io::Result<()> {
    #[cfg(unix)]
    {
        use nix::{
            errno::Errno,
            sys::signal::{Signal, killpg},
            unistd::Pid,
        };
        let group = Pid::from_raw(i32::try_from(child.id()).map_err(io::Error::other)?);
        // Reap only after escalation so the group leader's PID cannot be reused
        // while we still intend to address its process group.
        for number in [Signal::SIGTERM, Signal::SIGKILL] {
            match killpg(group, number) {
                Ok(()) | Err(Errno::ESRCH) => {}
                Err(error) => return Err(io::Error::other(error)),
            }
            if number == Signal::SIGTERM {
                std::thread::sleep(Duration::from_millis(200));
            }
        }
        Ok(())
    }
    #[cfg(not(unix))]
    child.kill()
}

pub(crate) fn status(command: &mut Command, signal: &AtomicUsize) -> io::Result<ExitStatus> {
    wait(&mut spawn(command)?, signal)
}

pub(crate) fn output(command: &mut Command, signal: &AtomicUsize) -> io::Result<Output> {
    command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = spawn(command)?;
    // Drain both streams while the process runs, avoiding a full-pipe deadlock.
    let reader = |mut stream: Box<dyn Read + Send>| {
        std::thread::spawn(move || {
            let mut bytes = Vec::new();
            stream.read_to_end(&mut bytes).map(|_| bytes)
        })
    };
    let stdout = reader(Box::new(child.stdout.take().unwrap()));
    let stderr = reader(Box::new(child.stderr.take().unwrap()));
    let status = wait(&mut child, signal)?;
    Ok(Output {
        status,
        stdout: stdout
            .join()
            .map_err(|_| io::Error::other("stdout reader panicked"))??,
        stderr: stderr
            .join()
            .map_err(|_| io::Error::other("stderr reader panicked"))??,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    #[test]
    fn cancellation_terminates_and_reaps_child() {
        let mut command = Command::new("sh");
        command.args(["-c", "exec sleep 30"]);
        let signal = AtomicUsize::new(2);
        let start = std::time::Instant::now();
        assert!(!status(&mut command, &signal).unwrap().success());
        assert!(start.elapsed() < Duration::from_secs(5));
    }

    #[test]
    fn captured_streams_are_drained_concurrently() {
        let mut command = Command::new("sh");
        command.args([
            "-c",
            "head -c 131072 /dev/zero; head -c 131072 /dev/zero >&2",
        ]);
        let output = output(&mut command, &AtomicUsize::new(0)).unwrap();
        assert!(output.status.success());
        assert_eq!(output.stdout.len(), 131_072);
        assert_eq!(output.stderr.len(), 131_072);
    }
}
