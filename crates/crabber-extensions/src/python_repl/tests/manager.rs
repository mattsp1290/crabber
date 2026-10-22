use super::support::{group_gone, options};
use crate::python_repl::{
    config,
    manager::{Hook, Manager, OwnerKey},
    *,
};
use crabber::ExtensionError;
use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::{
    sync::Semaphore,
    time::{Instant, sleep, timeout},
};
use tokio_util::sync::CancellationToken;

struct Fixture {
    manager: Arc<Manager>,
    _directory: tempfile::TempDir,
    root: PathBuf,
    temp: PathBuf,
    cleaned: AtomicBool,
}
impl Fixture {
    fn new(change: impl FnOnce(&mut Options)) -> Option<Self> {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("workspace");
        let temp = directory.path().join("temporary");
        std::fs::create_dir(&root).unwrap();
        std::fs::create_dir(&temp).unwrap();
        let mut options = options(&temp)?;
        change(&mut options);
        Some(Self {
            manager: Arc::new(Manager::new(config::validate(options).unwrap().0)),
            _directory: directory,
            root,
            temp,
            cleaned: AtomicBool::new(false),
        })
    }
    async fn execute(&self, key: OwnerKey, code: &str) -> Result<ExecuteResult, ExtensionError> {
        self.manager
            .execute_owner(
                key,
                self.root.clone(),
                CancellationToken::new(),
                code.into(),
                Duration::from_secs(10),
            )
            .await
    }
    async fn close(&self) {
        self.manager.close(Duration::from_secs(5)).await.unwrap();
        self.manager
            .cleanup
            .join(Duration::from_secs(5))
            .await
            .unwrap();
        assert_eq!(self.manager.live_runners(), 0);
        assert_eq!(std::fs::read_dir(&self.temp).unwrap().count(), 0);
        let pgids = self.manager.hooks.lock().unwrap().pgids.clone();
        for id in pgids {
            group_gone(id).await;
        }
        self.cleaned.store(true, Ordering::SeqCst);
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        if self.cleaned.load(Ordering::SeqCst) {
            return;
        }
        for raw in self.manager.hooks.lock().unwrap().pgids.clone() {
            if let Some(pid) = rustix::process::Pid::from_raw(raw) {
                let _ = rustix::process::kill_process_group(pid, rustix::process::Signal::KILL);
            }
        }
    }
}
fn key(id: &str) -> OwnerKey {
    OwnerKey {
        session: id.into(),
        workspace: "workspace".into(),
    }
}
fn code(error: ExtensionError, code: &str) {
    assert!(error.to_string().ends_with(code), "{error}");
}
struct Barrier {
    entered: Arc<Semaphore>,
    release: Arc<Semaphore>,
}
impl Barrier {
    fn new() -> Self {
        Self {
            entered: Arc::new(Semaphore::new(0)),
            release: Arc::new(Semaphore::new(0)),
        }
    }
    fn hook(&self) -> Hook {
        let entered = self.entered.clone();
        let release = self.release.clone();
        Arc::new(move || {
            let entered = entered.clone();
            let release = release.clone();
            Box::pin(async move {
                entered.add_permits(1);
                release.acquire().await.unwrap().forget();
            })
        })
    }
    async fn entered(&self) {
        timeout(Duration::from_secs(10), self.entered.acquire())
            .await
            .unwrap()
            .unwrap()
            .forget();
    }
    fn release(&self) {
        self.release.add_permits(1);
    }
}

#[path = "manager/cancellation.rs"]
mod cancellation;
#[path = "manager/close.rs"]
mod close;
#[path = "manager/queue.rs"]
mod queue;
#[path = "manager/state.rs"]
mod state;
