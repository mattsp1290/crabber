use crate::{Session, SessionId};

/// Unstable until the durable-host consumer contract is qualified.
///
/// Host-admitted session routing identity. It carries no filesystem authority.
///
/// Values are compared verbatim, with no path resolution or process defaults.
/// Empty persisted workspace fields stay empty. Construction grants no storage
/// or execution authority; admission still verifies the identity transactionally.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionContext {
    session_id: SessionId,
    workspace_id: String,
    directory: String,
}

impl SessionContext {
    #[must_use]
    pub fn new(
        session_id: SessionId,
        workspace_id: impl Into<String>,
        directory: impl Into<String>,
    ) -> Self {
        Self {
            session_id,
            workspace_id: workspace_id.into(),
            directory: directory.into(),
        }
    }

    #[must_use]
    pub fn session_id(&self) -> &SessionId {
        &self.session_id
    }
    #[must_use]
    pub fn workspace_id(&self) -> &str {
        &self.workspace_id
    }
    #[must_use]
    pub fn directory(&self) -> &str {
        &self.directory
    }
}

impl From<&Session> for SessionContext {
    fn from(session: &Session) -> Self {
        Self::new(
            session.id.clone(),
            session.workspace_id.clone(),
            session.directory.clone(),
        )
    }
}
