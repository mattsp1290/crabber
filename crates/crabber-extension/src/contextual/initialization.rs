use super::{Initialization, Slot};
use crate::ExtensionError;
use tokio::task::JoinHandle;

#[derive(Default)]
pub(super) struct Task {
    handle: Option<JoinHandle<()>>,
    joined: Option<Result<(), ExtensionError>>,
}

impl Initialization {
    // Called before publication under the owners/slot locks. The handle remains
    // in this ticket while a waiter awaits it, including across cancellation.
    pub(super) fn retain(&self, handle: JoinHandle<()>) {
        self.task
            .try_lock()
            .expect("unpublished initializer")
            .handle = Some(handle);
    }

    pub(super) async fn join(&self) -> Result<(), ExtensionError> {
        let mut task = self.task.lock().await;
        if let Some(result) = &task.joined {
            return result.clone();
        }
        let handle = task
            .handle
            .as_mut()
            .ok_or(ExtensionError::ContextInitializationIncomplete)?;
        let result = match handle.await {
            Ok(()) => Ok(()),
            Err(error) => {
                if error.is_panic() {
                    crate::dispatch::discard_panic_payload(error.into_panic());
                }
                Err(ExtensionError::ContextInitializationIncomplete)
            }
        };
        task.handle = None;
        task.joined = Some(result.clone());
        result
    }
}

pub(super) async fn join_slot(slot: &Slot) -> Result<(), ExtensionError> {
    let initialization = slot.state.lock().unwrap().initialization.clone();
    if let Some(initialization) = initialization {
        initialization
            .join()
            .await
            .map_err(|_| ExtensionError::ContextCleanupIncomplete)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{sync::Arc, time::Duration};
    use tokio::sync::oneshot;

    #[tokio::test]
    async fn logical_completion_and_cancelled_waiters_do_not_replace_actual_join() {
        let initialization = Arc::new(Initialization::default());
        let (entered, started) = oneshot::channel();
        let (release, held) = oneshot::channel();
        let ticket = initialization.clone();
        initialization.retain(tokio::spawn(async move {
            *ticket.result.lock().unwrap() = Some(Ok(()));
            entered.send(()).unwrap();
            held.await.unwrap();
        }));
        started.await.unwrap();
        let waiter = tokio::spawn({
            let initialization = initialization.clone();
            async move { initialization.join().await }
        });
        tokio::task::yield_now().await;
        waiter.abort();
        assert!(waiter.await.unwrap_err().is_cancelled());
        assert!(
            tokio::time::timeout(Duration::from_millis(10), initialization.join())
                .await
                .is_err()
        );
        release.send(()).unwrap();
        let (first, second) = tokio::join!(initialization.join(), initialization.join());
        first.unwrap();
        second.unwrap();
        initialization.join().await.unwrap();
    }

    #[tokio::test]
    async fn abnormal_task_exit_remains_incomplete_on_repeated_observation() {
        let initialization = Initialization::default();
        let task = tokio::spawn(async { std::future::pending::<()>().await });
        task.abort();
        initialization.retain(task);
        for _ in 0..2 {
            assert_eq!(
                initialization.join().await,
                Err(ExtensionError::ContextInitializationIncomplete)
            );
        }
    }
}
