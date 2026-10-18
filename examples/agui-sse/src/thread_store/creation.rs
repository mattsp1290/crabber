//! Explicit host policy before native preparation captures the thread revision.
use crabber::{
    Agent, RuntimeError,
    core::{ControlId, SessionId, SessionRevision},
    session::{
        ManageSession, ManagementError, SessionChange, SessionDefaults, StoreError,
        admission_config_hash,
    },
};
use serde_json::json;

pub(crate) const WORKSPACE: &str = "default";
pub(crate) const DIRECTORY: &str = ".";
pub(crate) const TITLE: &str = "Crabber session";

pub(crate) async fn ensure_thread(agent: &Agent, session: &SessionId) -> Result<(), RuntimeError> {
    let summary = match agent.session_summary(session).await {
        Ok(summary) => summary,
        Err(ManagementError::NotFound) => {
            // This exact deterministic management request is recoverable from
            // the same host/thread even if its caller loses the response. It is
            // a host operation key, never a worker or admission permission.
            let id = ControlId::from(admission_config_hash(&json!([
                "agui-sse.managed-thread.v1",
                session,
                WORKSPACE,
                DIRECTORY,
                TITLE,
            ])));
            let request = ManageSession {
                id,
                session_id: session.clone(),
                expected_revision: SessionRevision::INITIAL,
                change: SessionChange::Create {
                    workspace_id: WORKSPACE.into(),
                    directory: DIRECTORY.into(),
                    title: TITLE.into(),
                    defaults: SessionDefaults::default(),
                },
            };
            agent.manage_session(request).await?.summary
        }
        Err(error) => return Err(error.into()),
    };
    if summary.session_id != *session
        || summary.workspace_id != WORKSPACE
        || summary.directory != DIRECTORY
    {
        return Err(StoreError::SessionIdentityMismatch.into());
    }
    Ok(())
}
