//! Fenced extension state capability available only during an active run.
use async_trait::async_trait;
use crabber_core::SessionId;
use std::{collections::BTreeMap, future::Future, sync::Arc};

#[async_trait]
pub trait StateSink: Send + Sync {
    fn session_id(&self) -> &SessionId;
    async fn snapshot(&self, extension_id: &str) -> Result<BTreeMap<String, String>, String>;
    async fn apply(
        &self,
        extension_id: &str,
        changes: Vec<(String, Option<String>)>,
    ) -> Result<(), String>;
}

tokio::task_local! {
    static CURRENT_STATE_SINK: Arc<dyn StateSink>;
}

#[must_use]
pub fn current_state_sink() -> Option<Arc<dyn StateSink>> {
    CURRENT_STATE_SINK.try_with(Arc::clone).ok()
}

pub async fn with_state_sink<T>(sink: Arc<dyn StateSink>, future: impl Future<Output = T>) -> T {
    CURRENT_STATE_SINK.scope(sink, future).await
}
