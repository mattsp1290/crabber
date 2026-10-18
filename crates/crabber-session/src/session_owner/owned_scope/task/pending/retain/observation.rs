//! A bookkeeping observer awaits the actual resource-owning supervisor handle.
use super::{Arc, Entry, Ordering, Registry};
use tokio::task::JoinHandle;
pub(super) struct Observer {
    registry: Arc<Registry>,
    id: uuid::Uuid,
    entry: Option<Arc<Entry>>,
    supervisor: Option<JoinHandle<()>>,
    finished: bool,
}
impl Observer {
    pub(super) fn new(
        registry: Arc<Registry>,
        id: uuid::Uuid,
        entry: Arc<Entry>,
        supervisor: JoinHandle<()>,
    ) -> Self {
        Self {
            registry,
            id,
            entry: Some(entry),
            supervisor: Some(supervisor),
            finished: false,
        }
    }
    fn fail(&self) {
        if let Some(entry) = &self.entry {
            entry.failed.store(true, Ordering::Release);
        }
        self.registry.changed.send_replace(());
    }
    pub(super) async fn run(mut self) {
        let result = self.supervisor.as_mut().expect("original supervisor").await;
        if let Err(error) = result {
            // Persist failure before untrusted payload disposal, outside locks.
            self.fail();
            if error.is_panic() {
                super::super::super::discard_panic(error);
            }
            self.finished = true;
            return;
        }
        let matched = {
            let mut tasks = self
                .registry
                .tasks
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let matched = tasks
                .get(&self.id)
                .zip(self.entry.as_ref())
                .is_some_and(|(actual, expected)| Arc::ptr_eq(actual, expected));
            if matched {
                // Successful entries contain only library bookkeeping and the
                // private permit; no original payload/handle is dropped here.
                tasks.remove(&self.id);
                self.entry.take();
            }
            matched
        };
        if !matched {
            self.fail();
        }
        self.finished = true;
        self.registry.changed.send_replace(());
    }
}
impl Drop for Observer {
    fn drop(&mut self) {
        if !self.finished {
            self.fail();
            if let Some(entry) = &self.entry {
                // Preserve the SAME actual handle after observer cancellation.
                *entry
                    .unobserved
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = self.supervisor.take();
            }
        }
    }
}
