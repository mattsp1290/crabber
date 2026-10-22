//! Concurrent fixed-memory process capture with bounded pipe grace.
use crate::{ProcessGroupGuard, WAIT_AFTER_KILL};
use std::io;
use std::process::ExitStatus;
use tokio::{io::AsyncReadExt, time::Instant};
/// Fixed-memory stream capture; excess data is drained rather than buffered.
#[derive(Default)]
pub struct CappedOutput {
    /// Retained prefix, at most the requested cap.
    pub bytes: Vec<u8>,
    /// Whether any bytes were discarded.
    pub truncated: bool,
}
impl CappedOutput {
    /// Append a chunk without retaining bytes beyond the cap.
    pub fn push(&mut self, chunk: &[u8], cap: usize) {
        let count = chunk.len().min(cap.saturating_sub(self.bytes.len()));
        self.bytes.extend_from_slice(&chunk[..count]);
        self.truncated |= count < chunk.len();
    }
    /// Decode lossily while omitting an incomplete trailing UTF-8 codepoint.
    pub fn text(&self) -> String {
        let bytes = if self.truncated {
            match std::str::from_utf8(&self.bytes) {
                Err(e) if e.error_len().is_none() => &self.bytes[..e.valid_up_to()],
                _ => &self.bytes,
            }
        } else {
            &self.bytes
        };
        String::from_utf8_lossy(bytes).into_owned()
    }
}
/// Captured streams and leader status from a bounded invocation.
pub struct Capture {
    /// Retained standard output.
    pub stdout: CappedOutput,
    /// Retained standard error.
    pub stderr: CappedOutput,
    /// Reaped leader status, if available.
    pub status: Option<ExitStatus>,
    /// Whether the command deadline expired.
    pub timed_out: bool,
}
/// Drain both pipes concurrently with the leader wait. Once the leader exits,
/// allow at most five seconds for inherited pipes; preserve partial output.
pub async fn capture(
    guard: &mut ProcessGroupGuard,
    cap: usize,
    deadline: Instant,
) -> io::Result<Capture> {
    let mut stdout = guard
        .take_stdout()
        .ok_or_else(|| io::Error::other("stdout required"))?;
    let mut stderr = guard
        .take_stderr()
        .ok_or_else(|| io::Error::other("stderr required"))?;
    let mut out = Capture {
        stdout: CappedOutput::default(),
        stderr: CappedOutput::default(),
        status: None,
        timed_out: false,
    };
    let (mut out_eof, mut err_eof) = (false, false);
    let mut stop = deadline;
    let (mut ob, mut eb) = ([0; 8192], [0; 8192]);
    loop {
        if out.status.is_some() && out_eof && err_eof {
            guard.finish();
            return Ok(out);
        }
        tokio::select! { biased;
            _ = tokio::time::sleep_until(stop) => {
                out.timed_out = out.status.is_none(); guard.kill_group();
                if out.status.is_none() && let Ok(Ok(s)) = tokio::time::timeout(WAIT_AFTER_KILL, guard.wait()).await { out.status = Some(s); }
                return Ok(out);
            }
            s = guard.wait(), if out.status.is_none() => { out.status = Some(s?); stop = deadline.min(Instant::now()+WAIT_AFTER_KILL); }
            n = stdout.read(&mut ob), if !out_eof => { let n=n?; out_eof=n==0; out.stdout.push(&ob[..n],cap); }
            n = stderr.read(&mut eb), if !err_eof => { let n=n?; err_eof=n==0; out.stderr.push(&eb[..n],cap); }
        }
    }
}
