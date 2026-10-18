//! Public display data is separate from private extension state and conversation.
use async_trait::async_trait;
use crabber_core::{ControlId, PublicSourceChange, PublicStateContents, SessionRevision};
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum PublicSourceError {
    #[error("public source capability unavailable")]
    Unsupported,
    #[error("public source payload rejected")]
    Rejected,
    #[error("public source revision changed")]
    RevisionConflict,
    #[error("public source owner is stale")]
    StaleOwner,
    #[error("public source acknowledgement unknown; retain original operation identity")]
    UnknownOutcome,
}
#[derive(Debug, Clone, PartialEq)]
pub struct PublicSourceSnapshot {
    pub revision: SessionRevision,
    pub contents: PublicStateContents,
}
/// Host-only implementation boundary. Tools receive the bound wrapper; payloads
/// never provide workspace, session, attempt, call or invocation attribution.
#[async_trait]
pub trait PublicSourceSink: Send + Sync {
    async fn snapshot(&self) -> Result<PublicSourceSnapshot, PublicSourceError>;
    async fn publish(
        &self,
        id: ControlId,
        expected: SessionRevision,
        change: PublicSourceChange,
    ) -> Result<SessionRevision, PublicSourceError>;
}
#[derive(Clone, Default)]
pub struct PublicSourceWriter(Option<Arc<dyn PublicSourceSink>>);
impl PublicSourceWriter {
    #[must_use]
    pub fn from_host(host: Arc<dyn PublicSourceSink>) -> Self {
        Self(Some(host))
    }
    /// Read actual public state; this read supplies no mutation permission.
    /// # Errors
    /// Refuses unavailable sources or unknown storage outcomes.
    pub async fn snapshot(&self) -> Result<PublicSourceSnapshot, PublicSourceError> {
        self.0
            .as_ref()
            .ok_or(PublicSourceError::Unsupported)?
            .snapshot()
            .await
    }
    /// Publish payload-only display data under the retained original credential.
    /// Retain id, expected revision and payload to reconcile an unknown result.
    /// # Errors
    /// Refuses stale ownership, invalid data, conflicts or unknown acknowledgements.
    pub async fn publish(
        &self,
        id: ControlId,
        expected: SessionRevision,
        change: PublicSourceChange,
    ) -> Result<SessionRevision, PublicSourceError> {
        if id.0.is_empty()
            || id.0.len() > 128
            || !id
                .0
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"-_.".contains(&byte))
        {
            return Err(PublicSourceError::Rejected);
        }
        change.validate().map_err(|_| PublicSourceError::Rejected)?;
        self.0
            .as_ref()
            .ok_or(PublicSourceError::Unsupported)?
            .publish(id, expected, change)
            .await
    }
}
impl std::fmt::Debug for PublicSourceWriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PublicSourceWriter([bound])")
    }
}

tokio::task_local! {static CURRENT_PUBLIC_SOURCE:PublicSourceWriter;}
#[must_use]
pub fn current_public_source() -> Option<PublicSourceWriter> {
    CURRENT_PUBLIC_SOURCE.try_with(Clone::clone).ok()
}
/// Scope the trusted original capability across extension callback execution.
pub async fn with_public_source<T>(
    source: PublicSourceWriter,
    future: impl std::future::Future<Output = T>,
) -> T {
    CURRENT_PUBLIC_SOURCE.scope(source, future).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Default)]
    struct Host(AtomicUsize);
    #[async_trait]
    impl PublicSourceSink for Host {
        async fn snapshot(&self) -> Result<PublicSourceSnapshot, PublicSourceError> {
            panic!("publishing never asks the host for a snapshot")
        }
        async fn publish(
            &self,
            _: ControlId,
            expected: SessionRevision,
            _: PublicSourceChange,
        ) -> Result<SessionRevision, PublicSourceError> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(expected)
        }
    }
    #[tokio::test]
    async fn invalid_operation_identity_never_enters_bound_host() {
        let host = Arc::new(Host::default());
        let writer = PublicSourceWriter::from_host(host.clone());
        for id in [
            String::new(),
            "x".repeat(1 << 20),
            "space id".into(),
            "é".into(),
        ] {
            assert_eq!(
                writer
                    .publish(
                        ControlId(id),
                        SessionRevision::INITIAL,
                        PublicSourceChange::StateSnapshot {
                            state: serde_json::Value::Null
                        }
                    )
                    .await,
                Err(PublicSourceError::Rejected)
            );
        }
        assert_eq!(host.0.load(Ordering::SeqCst), 0);
        assert_eq!(
            writer
                .publish(
                    ControlId("valid-_.123".into()),
                    SessionRevision::INITIAL,
                    PublicSourceChange::StateSnapshot {
                        state: serde_json::Value::Null
                    }
                )
                .await,
            Ok(SessionRevision::INITIAL)
        );
        assert_eq!(host.0.load(Ordering::SeqCst), 1);
    }
}
