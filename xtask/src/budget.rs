//! The outer launcher provides an absolute Linux monotonic work deadline.
//! Setting the existing cancellation flag lets normal descendant/fixture cleanup
//! use the reserve; the launcher retains final authority over the whole process.
use std::{
    io,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

pub(crate) fn watch(signal: Arc<AtomicUsize>) -> io::Result<()> {
    let Some(value) = std::env::var_os("CRABBER_CHECK_WORK_DEADLINE_NS") else {
        return Ok(());
    };
    let deadline = value
        .to_str()
        .and_then(|value| value.parse::<u128>().ok())
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid complete-check work deadline",
            )
        })?;
    std::thread::Builder::new()
        .name("complete-check-budget".into())
        .spawn(move || {
            loop {
                let now = match nix::time::clock_gettime(nix::time::ClockId::CLOCK_MONOTONIC) {
                    Ok(value) => {
                        u128::try_from(value.tv_sec()).unwrap_or(0) * 1_000_000_000
                            + u128::try_from(value.tv_nsec()).unwrap_or(0)
                    }
                    Err(error) => {
                        eprintln!("cannot read complete-check deadline clock: {error}");
                        signal.store(15, Ordering::SeqCst);
                        return;
                    }
                };
                if now >= deadline {
                    eprintln!("complete-check work budget expired; cleaning owned resources");
                    signal.store(15, Ordering::SeqCst);
                    return;
                }
                std::thread::sleep(Duration::from_nanos(
                    u64::try_from((deadline - now).min(50_000_000)).unwrap(),
                ));
            }
        })?;
    Ok(())
}
