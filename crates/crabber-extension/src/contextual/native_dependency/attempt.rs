//! A caller may drop its waiter without dropping the original ACK operation.
use crate::ExtensionError;
use tokio::{sync::Mutex, task::JoinHandle};

#[derive(Default)]
pub(super) struct Attempt(Mutex<Task>);
#[derive(Default)]
struct Task {
    handle: Option<JoinHandle<Result<(), ExtensionError>>>,
    joined: Option<Result<(), ExtensionError>>,
}
impl Attempt {
    pub(super) fn retain(&self, handle: JoinHandle<Result<(), ExtensionError>>) {
        self.0.try_lock().expect("unpublished registration").handle = Some(handle);
    }

    pub(super) async fn join(&self) -> Result<(), ExtensionError> {
        let mut task = self.0.lock().await;
        if let Some(result) = &task.joined {
            return result.clone();
        }
        let handle = task
            .handle
            .as_mut()
            .ok_or(ExtensionError::ContextInitializationIncomplete)?;
        let result = match handle.await {
            Ok(result) => result,
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
