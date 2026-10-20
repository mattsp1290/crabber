//! Observe the actual cleanup body independently of entry-lock ownership.
use super::super::heartbeat::RetainedTask;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU8, Ordering},
};

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum CleanupOutcome {
    Clean,
    Retained,
}
pub(in crate::orchestrator::preparation) struct CleanupSupervisor {
    task: tokio::sync::Mutex<RetainedTask>,
    status: AtomicU8,
    outcome: Arc<Mutex<Option<CleanupOutcome>>>,
}
impl Default for CleanupSupervisor {
    fn default() -> Self {
        Self {
            task: tokio::sync::Mutex::new(RetainedTask::default()),
            status: AtomicU8::new(0),
            outcome: Arc::new(Mutex::new(None)),
        }
    }
}
impl CleanupSupervisor {
    pub(super) fn running(
        task: tokio::task::JoinHandle<()>,
        outcome: Arc<Mutex<Option<CleanupOutcome>>>,
    ) -> Self {
        Self {
            task: tokio::sync::Mutex::new(RetainedTask::running(task)),
            status: AtomicU8::new(1),
            outcome,
        }
    }
    pub(in crate::orchestrator::preparation) fn is_running(&self) -> bool {
        self.status.load(Ordering::Acquire) == 1
    }
    pub(in crate::orchestrator::preparation) fn is_finished(&self) -> bool {
        self.status.load(Ordering::Acquire) >= 2
    }
    pub(super) fn clean(&self) -> bool {
        *self.outcome.lock().unwrap() == Some(CleanupOutcome::Clean)
    }
    pub(in crate::orchestrator::preparation) async fn join(&self) -> Result<(), ()> {
        if self.status.load(Ordering::Acquire) == 0 {
            return Ok(());
        }
        let mut task = self.task.lock().await;
        let result = task.observe_join().await;
        self.status
            .store(if result.is_ok() { 2 } else { 3 }, Ordering::Release);
        drop(task);
        match result {
            Ok(()) => Ok(()),
            Err(error) => {
                super::super::heartbeat::discard_join_error(error);
                Err(())
            }
        }
    }
}
#[cfg(test)]
mod tests {
    use super::{CleanupOutcome, CleanupSupervisor};
    use std::sync::{Arc, Mutex};
    #[tokio::test]
    async fn clean_output_is_not_a_cleanup_join_before_original_epilogue() {
        let release = Arc::new(tokio::sync::Semaphore::new(0));
        let outcome = Arc::new(Mutex::new(None));
        let output = outcome.clone();
        let gate = release.clone();
        let (entered, held) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            *output.lock().unwrap() = Some(CleanupOutcome::Clean);
            entered.send(()).unwrap();
            gate.acquire().await.unwrap().forget();
        });
        let supervisor = CleanupSupervisor::running(task, outcome);
        held.await.unwrap();
        assert!(supervisor.clean());
        assert!(supervisor.is_running());
        assert!(!supervisor.is_finished());
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(15), supervisor.join())
                .await
                .is_err()
        );
        assert!(supervisor.is_running());
        release.add_permits(1);
        supervisor.join().await.unwrap();
        assert!(supervisor.is_finished());
        assert!(!supervisor.is_running());
    }
    struct JoinPayload {
        original: Arc<Mutex<Option<std::sync::Weak<CleanupSupervisor>>>>,
        disposed_unlocked: Arc<std::sync::atomic::AtomicBool>,
    }
    impl Drop for JoinPayload {
        fn drop(&mut self) {
            let original = self
                .original
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .upgrade()
                .unwrap();
            let unlocked = original.task.try_lock().is_ok();
            self.disposed_unlocked.store(
                unlocked && original.is_finished() && !original.is_running(),
                std::sync::atomic::Ordering::Release,
            );
        }
    }
    #[tokio::test]
    async fn actual_failed_cleanup_join_is_sticky_before_unlocked_payload_disposal() {
        let original = Arc::new(Mutex::new(None));
        let disposed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let payload = JoinPayload {
            original: original.clone(),
            disposed_unlocked: disposed.clone(),
        };
        let (release, ready) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            ready.await.unwrap();
            std::panic::panic_any(payload);
        });
        let supervisor = Arc::new(CleanupSupervisor::running(task, Arc::new(Mutex::new(None))));
        *original.lock().unwrap() = Some(Arc::downgrade(&supervisor));
        release.send(()).unwrap();
        assert!(supervisor.join().await.is_err());
        assert!(disposed.load(std::sync::atomic::Ordering::Acquire));
        assert!(supervisor.join().await.is_err());
        assert!(!supervisor.clean());
    }
}
