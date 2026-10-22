//! Synthetic host for typed-envelope and replay tests. Runtime ownership itself
//! is qualified separately against Agent + MemoryStore, never inferred here.
use crabber::extension::{
    CleanupOwner, SessionInvocationAdmission, SessionInvocationContext, SessionInvocationError,
    SessionInvocationRequest, SessionInvocationSpawner, SessionInvocationTask,
};
use std::{collections::BTreeMap, sync::Mutex};
#[derive(Default)]
pub struct JoinedSpawner {
    seen: Mutex<BTreeMap<String, SessionInvocationRequest>>,
}
#[async_trait::async_trait]
impl SessionInvocationSpawner for JoinedSpawner {
    async fn spawn(
        &self,
        request: SessionInvocationRequest,
        task: SessionInvocationTask,
    ) -> Result<SessionInvocationAdmission, SessionInvocationError> {
        {
            let mut seen = self.seen.lock().unwrap();
            if let Some(original) = seen.get(&request.id.0) {
                return if original == &request {
                    Ok(SessionInvocationAdmission::Replayed)
                } else {
                    Err(SessionInvocationError::Rejected)
                };
            }
            seen.insert(request.id.0.clone(), request);
        }
        let cleanup = CleanupOwner::new();
        task(SessionInvocationContext {
            cancel: tokio_util::sync::CancellationToken::new(),
            cleanup: cleanup.tracker().clone(),
            public_source: Default::default(),
            invocations: Default::default(),
        })
        .await
        .unwrap();
        Ok(SessionInvocationAdmission::Started)
    }
}
