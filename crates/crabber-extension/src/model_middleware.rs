//! Typed model-middleware capabilities and per-attempt context.

use crate::{CleanupTracker, WorkspaceContext};
use async_trait::async_trait;
use crabber_core::{RunId, SessionId, TurnId};
use serde::{Deserialize, Serialize};
use std::{fmt, sync::Arc};
use tokio_util::sync::CancellationToken;

/// Version of the typed system-prompt middleware registration contract.
pub const SYSTEM_PROMPT_MIDDLEWARE_CONTRACT_VERSION: u32 = 1;
/// Maximum UTF-8 byte length of descriptor `kind` and `version` values.
pub const MAX_MIDDLEWARE_DESCRIPTOR_FIELD_BYTES: usize = 128;

/// Stable metadata describing a typed middleware implementation.
///
/// Native registrations are self-attested: the registry validates this shape
/// but does not recompute `config_hash`. First-party recipes should derive it
/// directly from their validated private configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MiddlewareDescriptor {
    kind: String,
    version: String,
    config_hash: String,
}

impl MiddlewareDescriptor {
    /// Creates validated middleware metadata.
    ///
    /// # Errors
    /// Returns a sanitized validation error when either text field is empty,
    /// oversized, or contains control characters, or when the configuration
    /// hash is not exactly 64 lowercase hexadecimal characters.
    pub fn new(
        kind: impl Into<String>,
        version: impl Into<String>,
        config_hash: impl Into<String>,
    ) -> Result<Self, String> {
        let kind = kind.into();
        let version = version.into();
        let config_hash = config_hash.into();
        if !valid_descriptor_field(&kind) {
            return Err("invalid middleware descriptor kind".into());
        }
        if !valid_descriptor_field(&version) {
            return Err("invalid middleware descriptor version".into());
        }
        if config_hash.len() != 64
            || !config_hash
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err("invalid middleware descriptor config hash".into());
        }
        Ok(Self {
            kind,
            version,
            config_hash,
        })
    }

    #[must_use]
    pub fn kind(&self) -> &str {
        &self.kind
    }
    #[must_use]
    pub fn version(&self) -> &str {
        &self.version
    }
    #[must_use]
    pub fn config_hash(&self) -> &str {
        &self.config_hash
    }
}

fn valid_descriptor_field(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_MIDDLEWARE_DESCRIPTOR_FIELD_BYTES
        && !value.chars().any(char::is_control)
}

/// Object-safe typed callback invoked once for each physical model attempt.
#[async_trait]
pub trait SystemPromptMiddleware: Send + Sync {
    async fn contribute(&self, context: ModelAttemptContext) -> Result<Option<String>, String>;
}

/// Sanitized failure classes for workspace reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum WorkspaceReadErrorKind {
    NotFound,
    TooLarge,
    InvalidPath,
    Denied,
    Io,
}

/// A workspace read failure that intentionally carries no backend detail.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceReadError {
    kind: WorkspaceReadErrorKind,
}

impl WorkspaceReadError {
    #[must_use]
    pub fn new(kind: WorkspaceReadErrorKind) -> Self {
        Self { kind }
    }

    #[must_use]
    pub fn kind(&self) -> WorkspaceReadErrorKind {
        self.kind
    }
}

impl fmt::Display for WorkspaceReadError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "workspace read failed ({:?})", self.kind)
    }
}

impl std::error::Error for WorkspaceReadError {}

/// A host-authorized, read-only capability rooted to one workspace.
#[async_trait]
pub trait WorkspaceReader: Send + Sync {
    /// Returns no more than `max_bytes`, or returns [`WorkspaceReadErrorKind::TooLarge`]
    /// without returning any file bytes.
    async fn read_limited(
        &self,
        relative_path: &str,
        max_bytes: usize,
    ) -> Result<Vec<u8>, WorkspaceReadError>;
}

/// Resolves an admitted workspace to a host-authorized reader.
#[async_trait]
pub trait WorkspaceReaderResolver: Send + Sync {
    /// Treats `workspace` only as routing input. The trusted host must validate
    /// the mapping and decide what read authority, if any, to return.
    async fn resolve(
        &self,
        workspace: &WorkspaceContext,
    ) -> Result<Arc<dyn WorkspaceReader>, WorkspaceReadError>;
}

/// Read-only, runtime-authoritative context for one physical model attempt.
///
/// Unlike [`crate::PromptAttemptContext`], this typed context may carry the
/// narrowly scoped workspace-reader resolver configured by the host.
#[derive(Clone)]
pub struct ModelAttemptContext {
    session_id: SessionId,
    run_id: RunId,
    turn_id: TurnId,
    workspace: WorkspaceContext,
    provider_id: String,
    model_id: String,
    attempt: u32,
    after_compaction: bool,
    cancellation: CancellationToken,
    cleanup: CleanupTracker,
    workspace_reader_resolver: Option<Arc<dyn WorkspaceReaderResolver>>,
}

// Construction is crate-restricted so only runtime-owned collection can make an
// authoritative context. The collector wiring lands separately from this API.
#[allow(dead_code)]
impl ModelAttemptContext {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        session_id: SessionId,
        run_id: RunId,
        turn_id: TurnId,
        workspace: WorkspaceContext,
        provider_id: String,
        model_id: String,
        attempt: u32,
        after_compaction: bool,
        workspace_reader_resolver: Option<Arc<dyn WorkspaceReaderResolver>>,
    ) -> Self {
        Self {
            session_id,
            run_id,
            turn_id,
            workspace,
            provider_id,
            model_id,
            attempt,
            after_compaction,
            cancellation: CancellationToken::new(),
            cleanup: CleanupTracker::detached(),
            workspace_reader_resolver,
        }
    }

    #[must_use]
    pub(crate) fn with_cancellation(mut self, cancellation: CancellationToken) -> Self {
        self.cancellation = cancellation;
        self
    }

    #[must_use]
    pub(crate) fn with_cleanup(mut self, cleanup: CleanupTracker) -> Self {
        self.cleanup = cleanup;
        self
    }

    #[must_use]
    pub fn session_id(&self) -> &SessionId {
        &self.session_id
    }

    #[must_use]
    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }

    #[must_use]
    pub fn turn_id(&self) -> &TurnId {
        &self.turn_id
    }

    #[must_use]
    pub fn workspace(&self) -> &WorkspaceContext {
        &self.workspace
    }

    #[must_use]
    pub fn provider_id(&self) -> &str {
        &self.provider_id
    }

    #[must_use]
    pub fn model_id(&self) -> &str {
        &self.model_id
    }

    /// One-based attempt within this execution of the current turn.
    #[must_use]
    pub fn attempt(&self) -> u32 {
        self.attempt
    }

    #[must_use]
    pub fn after_compaction(&self) -> bool {
        self.after_compaction
    }

    #[must_use]
    pub fn cancellation(&self) -> &CancellationToken {
        &self.cancellation
    }

    #[must_use]
    pub fn cleanup(&self) -> &CleanupTracker {
        &self.cleanup
    }

    #[must_use]
    pub fn workspace_reader_resolver(&self) -> Option<&Arc<dyn WorkspaceReaderResolver>> {
        self.workspace_reader_resolver.as_ref()
    }
}

impl fmt::Debug for ModelAttemptContext {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ModelAttemptContext")
            .field("session_id", &self.session_id)
            .field("run_id", &self.run_id)
            .field("turn_id", &self.turn_id)
            .field("workspace", &self.workspace)
            .field("provider_id", &self.provider_id)
            .field("model_id", &self.model_id)
            .field("attempt", &self.attempt)
            .field("after_compaction", &self.after_compaction)
            .field("cancellation", &"<redacted>")
            .field("cleanup", &"<redacted>")
            .field("workspace_reader_resolver", &"<redacted>")
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CleanupTracker, WorkspaceContext};
    use async_trait::async_trait;
    use crabber_core::{RunId, SessionId, TurnId};
    use std::sync::{Arc, Mutex};
    use tokio_util::{sync::CancellationToken, task::TaskTracker};

    #[test]
    fn workspace_read_errors_are_typed_serializable_and_sanitized() {
        let cases = [
            (
                WorkspaceReadErrorKind::NotFound,
                "workspace read failed (NotFound)",
            ),
            (
                WorkspaceReadErrorKind::TooLarge,
                "workspace read failed (TooLarge)",
            ),
            (
                WorkspaceReadErrorKind::InvalidPath,
                "workspace read failed (InvalidPath)",
            ),
            (
                WorkspaceReadErrorKind::Denied,
                "workspace read failed (Denied)",
            ),
            (WorkspaceReadErrorKind::Io, "workspace read failed (Io)"),
        ];

        for (kind, display) in cases {
            let error = WorkspaceReadError::new(kind);
            assert_eq!(error.kind(), kind);
            assert_eq!(error.to_string(), display);
            let encoded = serde_json::to_string(&kind).unwrap();
            assert_eq!(
                serde_json::from_str::<WorkspaceReadErrorKind>(&encoded).unwrap(),
                kind
            );
        }
    }

    struct RecordingReader {
        calls: Arc<Mutex<Vec<(String, usize)>>>,
    }

    #[async_trait]
    impl WorkspaceReader for RecordingReader {
        async fn read_limited(
            &self,
            relative_path: &str,
            max_bytes: usize,
        ) -> Result<Vec<u8>, WorkspaceReadError> {
            self.calls
                .lock()
                .unwrap()
                .push((relative_path.to_owned(), max_bytes));
            Ok(b"contents".to_vec())
        }
    }

    struct RecordingResolver {
        workspaces: Arc<Mutex<Vec<WorkspaceContext>>>,
        reader: Arc<dyn WorkspaceReader>,
        _secret: &'static str,
    }

    #[async_trait]
    impl WorkspaceReaderResolver for RecordingResolver {
        async fn resolve(
            &self,
            workspace: &WorkspaceContext,
        ) -> Result<Arc<dyn WorkspaceReader>, WorkspaceReadError> {
            self.workspaces.lock().unwrap().push(workspace.clone());
            Ok(self.reader.clone())
        }
    }

    #[tokio::test]
    async fn reader_and_resolver_preserve_the_bound_path_workspace_and_arc() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let reader: Arc<dyn WorkspaceReader> = Arc::new(RecordingReader {
            calls: calls.clone(),
        });
        assert_eq!(
            reader.read_limited("nested/AGENTS.md", 123).await.unwrap(),
            b"contents"
        );
        assert_eq!(&*calls.lock().unwrap(), &[("nested/AGENTS.md".into(), 123)]);

        let workspaces = Arc::new(Mutex::new(Vec::new()));
        let resolver = RecordingResolver {
            workspaces: workspaces.clone(),
            reader: reader.clone(),
            _secret: "resolver capability secret",
        };
        let workspace = WorkspaceContext::from_persisted("workspace", "/routing-only");
        let reader_from_resolver = resolver.resolve(&workspace).await.unwrap();
        assert!(Arc::ptr_eq(&reader_from_resolver, &reader));
        assert_eq!(&*workspaces.lock().unwrap(), &[workspace]);
    }

    fn context(resolver: Option<Arc<dyn WorkspaceReaderResolver>>) -> ModelAttemptContext {
        ModelAttemptContext::new(
            SessionId::from("session"),
            RunId::from("run"),
            TurnId::from("turn"),
            WorkspaceContext::from_persisted("workspace", "/routing-only"),
            "provider".into(),
            "model".into(),
            2,
            true,
            resolver,
        )
    }

    #[test]
    fn model_attempt_context_exposes_authoritative_values_and_optional_resolver() {
        let without = context(None);
        assert_eq!(without.session_id(), &SessionId::from("session"));
        assert_eq!(without.run_id(), &RunId::from("run"));
        assert_eq!(without.turn_id(), &TurnId::from("turn"));
        assert_eq!(without.workspace().workspace_id(), Some("workspace"));
        assert_eq!(without.workspace().directory(), Some("/routing-only"));
        assert_eq!(without.provider_id(), "provider");
        assert_eq!(without.model_id(), "model");
        assert_eq!(without.attempt(), 2);
        assert!(without.after_compaction());
        assert!(without.workspace_reader_resolver().is_none());
        assert!(!without.cancellation().is_cancelled());
        assert!(!without.cleanup().is_closing());

        let calls = Arc::new(Mutex::new(Vec::new()));
        let reader: Arc<dyn WorkspaceReader> = Arc::new(RecordingReader { calls });
        let resolver: Arc<dyn WorkspaceReaderResolver> = Arc::new(RecordingResolver {
            workspaces: Arc::new(Mutex::new(Vec::new())),
            reader,
            _secret: "resolver capability secret",
        });
        let token = CancellationToken::new();
        let close = CancellationToken::new();
        let with = context(Some(resolver.clone()))
            .with_cancellation(token.clone())
            .with_cleanup(CleanupTracker::from_parts(
                TaskTracker::new(),
                close.clone(),
            ));
        assert!(Arc::ptr_eq(
            with.workspace_reader_resolver().unwrap(),
            &resolver
        ));
        token.cancel();
        close.cancel();
        assert!(with.cancellation().is_cancelled());
        assert!(with.cleanup().is_closing());
    }

    #[test]
    fn debug_is_redacted_and_does_not_require_capabilities_to_implement_debug() {
        let reader: Arc<dyn WorkspaceReader> = Arc::new(RecordingReader {
            calls: Arc::new(Mutex::new(Vec::new())),
        });
        let resolver: Arc<dyn WorkspaceReaderResolver> = Arc::new(RecordingResolver {
            workspaces: Arc::new(Mutex::new(Vec::new())),
            reader,
            _secret: "resolver capability secret",
        });

        let debug = format!("{:?}", context(Some(resolver)));
        assert!(debug.starts_with("ModelAttemptContext"));
        assert!(!debug.contains("resolver capability secret"));
        assert!(!debug.contains("RecordingResolver"));
    }
}
