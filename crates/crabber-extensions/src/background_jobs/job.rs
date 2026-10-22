use super::{
    manager::{Owner, Policy},
    time::rfc3339_utc,
};
use crate::process::{Group, GroupSignal, Reap, Tails};
use serde::Serialize;
use serde_json::{Value, json};
use std::{
    sync::{Arc, Mutex},
    time::SystemTime,
};
use tokio::sync::watch;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum State {
    Running,
    Succeeded,
    Failed,
    Killed,
    TimedOut,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Cause {
    Natural,
    Kill,
    Timeout,
    Close,
    Cancelled,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum AttemptResult {
    Reaped,
    Incomplete,
}
#[derive(Clone, Default)]
pub(super) struct Attempt {
    pub(super) requested: u64,
    pub(super) completed: u64,
    pub(super) last: Option<AttemptResult>,
}
#[derive(Clone)]
pub(super) struct Terminal {
    pub(super) state: State,
    pub(super) completed_at: SystemTime,
    pub(super) exit_code: Option<i32>,
    pub(super) output_forced: bool,
}
pub(super) struct Job {
    pub(super) shared_permit: Mutex<Option<tokio::sync::OwnedSemaphorePermit>>,
    pub(super) id: String,
    pub(super) owner: Owner,
    pub(super) started_at: SystemTime,
    pub(super) timeout_seconds: u64,
    pub(super) tails: Tails,
    pub(super) cause: watch::Sender<Option<Cause>>,
    pub(super) attempt: watch::Sender<Attempt>,
    pub(super) terminal: Mutex<Option<Terminal>>,
    pub(super) done: watch::Sender<bool>,
}
impl Job {
    pub(super) fn new(id: String, owner: Owner, timeout_seconds: u64, tails: Tails) -> Self {
        Self {
            shared_permit: Mutex::new(None),
            id,
            owner,
            timeout_seconds,
            tails,
            started_at: SystemTime::now(),
            cause: watch::channel(None).0,
            attempt: watch::channel(Attempt::default()).0,
            terminal: Mutex::new(None),
            done: watch::channel(false).0,
        }
    }
    pub(super) fn set_cause_once(&self, cause: Cause) -> bool {
        self.cause.send_if_modified(|value| {
            if value.is_some() {
                false
            } else {
                *value = Some(cause);
                true
            }
        })
    }
    pub(super) fn request_attempt(&self) -> u64 {
        let mut requested = 0;
        self.attempt.send_modify(|a| {
            a.requested = a.requested.saturating_add(1);
            requested = a.requested;
        });
        requested
    }
    pub(super) fn state(&self) -> State {
        self.terminal
            .lock()
            .unwrap()
            .as_ref()
            .map_or(State::Running, |t| t.state)
    }
    pub(super) fn start_result(&self) -> Value {
        json!({"id": self.id, "state":"running", "started_at":rfc3339_utc(self.started_at), "timeout_seconds":self.timeout_seconds})
    }
    pub(super) fn summary(&self) -> Value {
        let terminal = self.terminal.lock().unwrap();
        let mut value = json!({"id":self.id, "state":terminal.as_ref().map_or(State::Running,|t|t.state), "started_at":rfc3339_utc(self.started_at), "timeout_seconds":self.timeout_seconds});
        if let Some(terminal) = terminal.as_ref() {
            value["completed_at"] = rfc3339_utc(terminal.completed_at).into();
        }
        value
    }
    pub(super) fn status(&self) -> Value {
        let terminal = self.terminal.lock().unwrap().clone();
        let mut value = json!({"id":self.id, "state":terminal.as_ref().map_or(State::Running,|t|t.state), "started_at":rfc3339_utc(self.started_at), "timeout_seconds":self.timeout_seconds});
        let forced = terminal.as_ref().is_some_and(|t| t.output_forced);
        if let Some(terminal) = terminal {
            value["completed_at"] = rfc3339_utc(terminal.completed_at).into();
            if let Some(code) = terminal.exit_code {
                value["exit_code"] = code.into();
            }
        }
        for (name, tail) in [
            ("stdout", &self.tails.stdout),
            ("stderr", &self.tails.stderr),
        ] {
            let (text, truncated) = tail.lock().unwrap().snapshot();
            value[name] = json!({"text":text,"truncated":truncated || forced});
        }
        value
    }
    pub(super) fn kill_result(&self, newly_accepted: bool) -> Value {
        json!({"id":self.id,"state":self.state(),"newly_accepted":newly_accepted})
    }
    pub(super) fn finish(&self, cause: Cause, reap: &Reap) {
        let code = reap.status.and_then(|status| status.code());
        let (state, exit_code) = match cause {
            Cause::Kill | Cause::Close | Cause::Cancelled => (State::Killed, None),
            Cause::Timeout => (State::TimedOut, None),
            Cause::Natural if !reap.output_forced && code.is_some() => (
                if code == Some(0) {
                    State::Succeeded
                } else {
                    State::Failed
                },
                code,
            ),
            Cause::Natural => (State::Failed, None),
        };
        *self.terminal.lock().unwrap() = Some(Terminal {
            state,
            exit_code,
            completed_at: SystemTime::now(),
            output_forced: reap.output_forced,
        });
    }
}

pub(super) async fn coordinate(
    policy: Arc<Policy>,
    job: Arc<Job>,
    mut group: Group,
    gate_failed: bool,
) {
    let mut cause_rx = job.cause.subscribe();
    let mut attempt_rx = job.attempt.subscribe();
    let mut observe_exit = true;
    let mut natural_status = None;
    loop {
        let reap = if let Some(status) = natural_status {
            group
                .sweep_and_reap(status, policy.configuration.limits.kill_wait)
                .await
        } else {
            tokio::select! {
                biased;
                status = group.exited(), if observe_exit => {
                    match status {
                        Ok(status) => {
                            observe_exit = false;
                            natural_status = Some(status);
                            job.set_cause_once(Cause::Natural);
                            group.sweep_and_reap(status, policy.configuration.limits.kill_wait).await
                        },
                        Err(_) => {
                            observe_exit = false;
                            job.set_cause_once(Cause::Close);
                            group.terminate(
                                GroupSignal::Kill,
                                policy.configuration.limits.terminate_grace,
                                policy.configuration.limits.kill_wait,
                            ).await
                        },
                    }
                }
                _ = async {
                    let _ = cause_rx.wait_for(|cause| {
                        matches!(cause, Some(cause) if *cause != Cause::Natural)
                    }).await;
                } => {
                    let cause = (*job.cause.borrow()).expect("termination cause is set");
                    let first = if cause == Cause::Cancelled || gate_failed {
                        GroupSignal::Kill
                    } else {
                        GroupSignal::Terminate
                    };
                    group.terminate(
                        first,
                        policy.configuration.limits.terminate_grace,
                        policy.configuration.limits.kill_wait,
                    ).await
                }
            }
        };
        if reap.reaped {
            let cause = (*job.cause.borrow()).expect("reaped job has a cause");
            job.finish(cause, &reap);
            policy.job_completed(&job);
            job.done.send_replace(true);
        }
        // Every request accepted during this attempt shares its outcome. A
        // completed count increment of one would strand concurrent kill callers.
        job.attempt.send_modify(|attempt| {
            attempt.completed = attempt.requested;
            attempt.last = Some(if reap.reaped {
                AttemptResult::Reaped
            } else {
                AttemptResult::Incomplete
            });
        });
        if reap.reaped {
            return;
        }
        let served = attempt_rx.borrow_and_update().completed;
        tokio::select! {
            _ = async { let _ = attempt_rx.wait_for(|attempt| attempt.requested > served).await; } => {},
            _ = group.exited(), if observe_exit => {},
        }
    }
}
