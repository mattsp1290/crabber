use std::time::Duration;

use serde::{Deserialize, Serialize};
#[cfg(unix)]
use sha2::{Digest, Sha256};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt, BufReader},
    process::{ChildStdin, ChildStdout},
    time::{Instant, sleep, sleep_until},
};
use tokio_util::sync::CancellationToken;

use crate::process::{Reap, RunnerChild, RunnerLaunch, spawn_runner};

pub(super) const RUNNER_PROTOCOL: &str = "python-repl-runner-v1";
pub(super) const RUNNER_SOURCE: &str = include_str!("source.py");
pub(super) const INTERPRETER_FLAGS: [&str; 3] = ["-I", "-u", "-B"];
pub(super) const READY_MAX: u32 = 256;

#[cfg(unix)]
pub(super) fn runner_digest() -> String {
    format!("{:x}", Sha256::digest(RUNNER_SOURCE.as_bytes()))
}

/// A UTF-8 prefix bounded by the configured byte limit.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct BoundedText {
    /// Retained text.
    pub text: String,
    /// Whether text exceeded its bound.
    pub truncated: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Response {
    version: String,
    id: u64,
    pub(super) status: Status,
    pub(super) stdout: BoundedText,
    pub(super) stderr: BoundedText,
    pub(super) result: BoundedText,
    pub(super) exception: BoundedText,
}

/// The two validated execution statuses, serialized in snake case.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    /// Execution completed without a Python exception.
    Completed,
    /// Execution raised a Python exception recorded in the result.
    PythonError,
}

pub(super) fn arguments(source: &str, bounds: Bounds) -> Vec<String> {
    INTERPRETER_FLAGS
        .into_iter()
        .map(str::to_owned)
        .chain([
            "-c".into(),
            source.into(),
            RUNNER_PROTOCOL.into(),
            bounds.request.to_string(),
            bounds.response.to_string(),
            bounds.output.to_string(),
            bounds.output.to_string(),
            bounds.result.to_string(),
            bounds.exception.to_string(),
        ])
        .collect()
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Ready {
    version: String,
    phase: String,
    python: [u32; 2],
}

#[derive(Debug, PartialEq, Eq)]
pub(super) struct Protocol;

pub(super) fn encode_frame(value: &impl Serialize, maximum: u32) -> Result<Vec<u8>, Protocol> {
    let body = serde_json::to_vec(value).map_err(|_| Protocol)?;
    let size = u32::try_from(body.len()).map_err(|_| Protocol)?;
    if size == 0 || size > maximum {
        return Err(Protocol);
    }
    let mut frame = size.to_be_bytes().to_vec();
    frame.extend(body);
    Ok(frame)
}

pub(super) async fn read_frame(
    reader: &mut (impl AsyncRead + Unpin),
    maximum: u32,
) -> Result<Vec<u8>, Protocol> {
    let size = reader.read_u32().await.map_err(|_| Protocol)?;
    if size == 0 || size > maximum {
        return Err(Protocol);
    }
    let mut bytes = vec![0; size as usize];
    reader.read_exact(&mut bytes).await.map_err(|_| Protocol)?;
    Ok(bytes)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum StartFault {
    Spawn,
    Timeout,
    Readiness,
    Bootstrap,
    Cancelled,
    Closing,
}

pub(super) struct StartFailure {
    pub(super) fault: StartFault,
    // Failed cleanup remains owned for quarantine instead of disappearing.
    pub(super) runner: Option<Box<Runner>>,
}

#[derive(Debug)]
pub(super) enum ExecuteOutcome {
    Completed(Response),
    TimedOut { may_have_executed: bool },
    Interrupted { may_have_executed: bool },
    Closing { may_have_executed: bool },
    Failed { may_have_executed: bool },
}

/// Frozen frame and field budgets, shared by startup and response validation.
#[derive(Clone, Copy)]
pub(super) struct Bounds {
    pub(super) request: u32,
    pub(super) response: u32,
    pub(super) output: usize,
    pub(super) result: usize,
    pub(super) exception: usize,
}

pub(super) struct Runner {
    pub(super) child: RunnerChild,
    pipes: Option<(ChildStdin, BufReader<ChildStdout>)>,
    next_id: u64,
    bounds: Bounds,
    #[cfg(test)]
    pub(super) after_request_write: Option<super::manager::Hook>,
    #[cfg(test)]
    pub(super) before_commit: Option<super::manager::Hook>,
}

impl Runner {
    pub(super) async fn start(
        launch: RunnerLaunch<'_>,
        bounds: Bounds,
        cancel: &CancellationToken,
        close: &CancellationToken,
        deadline: Instant,
        grace: Duration,
        kill_wait: Duration,
    ) -> Result<Self, StartFailure> {
        let mut child = spawn_runner(launch).map_err(|_| StartFailure {
            fault: StartFault::Spawn,
            runner: None,
        })?;
        let (stdin, stdout) = child.take_pipes().expect("spawned runner pipes");
        let mut runner = Self {
            child,
            pipes: Some((stdin, BufReader::new(stdout))),
            next_id: 0,
            bounds,
            #[cfg(test)]
            after_request_write: None,
            #[cfg(test)]
            before_commit: None,
        };
        let fault = {
            let (_, stdout) = runner.pipes.as_mut().unwrap();
            tokio::select! { biased;
                () = cancel.cancelled() => Some(StartFault::Cancelled),
                () = close.cancelled() => Some(StartFault::Closing),
                () = sleep_until(deadline) => Some(StartFault::Timeout),
                frame = read_frame(stdout, READY_MAX) => match frame.ok().and_then(|raw| serde_json::from_slice::<Ready>(&raw).ok()) {
                    None => Some(StartFault::Readiness),
                    Some(ready) if ready.phase != "ready" => Some(StartFault::Readiness),
                    Some(ready) if ready.version != RUNNER_PROTOCOL || ready.python[0] != 3 || !(11..=14).contains(&ready.python[1]) => Some(StartFault::Bootstrap),
                    Some(_) => None,
                },
                () = exited(&runner.child) => Some(StartFault::Readiness),
            }
        };
        if let Some(fault) = fault {
            let reaped = runner.terminate(grace, kill_wait).await.reaped;
            return Err(StartFailure {
                fault,
                runner: (!reaped).then(|| Box::new(runner)),
            });
        }
        Ok(runner)
    }

    pub(super) async fn execute(
        &mut self,
        code: &str,
        cancel: &CancellationToken,
        close: &CancellationToken,
        deadline: Instant,
    ) -> ExecuteOutcome {
        let outcome = self.exchange(code, cancel, close, deadline).await;
        if !matches!(outcome, ExecuteOutcome::Completed(_)) {
            self.pipes.take();
        }
        outcome
    }

    async fn exchange(
        &mut self,
        code: &str,
        cancel: &CancellationToken,
        close: &CancellationToken,
        deadline: Instant,
    ) -> ExecuteOutcome {
        if cancel.is_cancelled() {
            return ExecuteOutcome::Interrupted {
                may_have_executed: false,
            };
        }
        if close.is_cancelled() {
            return ExecuteOutcome::Closing {
                may_have_executed: false,
            };
        }
        if self.child.leader_exited() {
            return ExecuteOutcome::Failed {
                may_have_executed: false,
            };
        }
        let Some(id) = self.next_id.checked_add(1) else {
            return ExecuteOutcome::Failed {
                may_have_executed: false,
            };
        };
        self.next_id = id;
        let Ok(frame) = encode_frame(
            &serde_json::json!({"version": RUNNER_PROTOCOL, "id": id, "code": code}),
            self.bounds.request,
        ) else {
            return ExecuteOutcome::Failed {
                may_have_executed: false,
            };
        };
        let Some((stdin, stdout)) = self.pipes.as_mut() else {
            return ExecuteOutcome::Failed {
                may_have_executed: false,
            };
        };
        let write = async {
            let mut written = 0;
            while written < frame.len() {
                let count = stdin.write(&frame[written..]).await.map_err(|_| Protocol)?;
                if count == 0 {
                    return Err(Protocol);
                }
                written += count;
            }
            Ok(())
        };
        tokio::select! { biased;
            () = cancel.cancelled() => return ExecuteOutcome::Interrupted { may_have_executed: false },
            () = close.cancelled() => return ExecuteOutcome::Closing { may_have_executed: false },
            () = sleep_until(deadline) => return ExecuteOutcome::TimedOut { may_have_executed: false },
            () = exited(&self.child) => return ExecuteOutcome::Failed { may_have_executed: false },
            result = write => if result.is_err() { return ExecuteOutcome::Failed { may_have_executed: false }; },
        }
        #[cfg(test)]
        if let Some(hook) = &self.after_request_write {
            hook().await;
        }
        let read = async {
            let frame = read_frame(stdout, self.bounds.response).await;
            #[cfg(test)]
            if let Some(hook) = &self.before_commit {
                hook().await;
            }
            frame
        };
        tokio::select! { biased;
            () = cancel.cancelled() => ExecuteOutcome::Interrupted { may_have_executed: true },
            () = close.cancelled() => ExecuteOutcome::Closing { may_have_executed: true },
            () = sleep_until(deadline) => ExecuteOutcome::TimedOut { may_have_executed: true },
            raw = read => {
                match raw.ok().and_then(|raw| serde_json::from_slice::<Response>(&raw).ok()) {
                    Some(response) if valid_response(&response, id, self.bounds) => ExecuteOutcome::Completed(response),
                    _ => ExecuteOutcome::Failed { may_have_executed: true },
                }
            }
            () = exited(&self.child) => ExecuteOutcome::Failed { may_have_executed: true },
        }
    }

    pub(super) async fn terminate(&mut self, grace: Duration, kill_wait: Duration) -> Reap {
        self.pipes.take();
        self.child.terminate(grace, kill_wait).await
    }
}

async fn exited(child: &RunnerChild) {
    while !child.leader_exited() {
        sleep(Duration::from_millis(25)).await;
    }
}

fn valid_response(response: &Response, id: u64, bounds: Bounds) -> bool {
    let empty = |text: &BoundedText| text.text.is_empty() && !text.truncated;
    response.version == RUNNER_PROTOCOL
        && response.id == id
        && response.stdout.text.len() <= bounds.output
        && response.stderr.text.len() <= bounds.output
        && response.result.text.len() <= bounds.result
        && response.exception.text.len() <= bounds.exception
        && match response.status {
            Status::Completed => empty(&response.exception),
            Status::PythonError => empty(&response.result),
        }
}
