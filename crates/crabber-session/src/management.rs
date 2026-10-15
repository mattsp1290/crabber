//! Pure host session controls. These records never carry execution authority.
use crabber_core::{ControlId, InputFingerprint, SessionId, SessionRevision};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ManagementError {
    #[error("session management unsupported")]
    Unsupported,
    #[error("session not found")]
    NotFound,
    #[error("invalid session management request")]
    InvalidRequest,
    #[error("session control semantics conflict")]
    SemanticConflict,
    #[error("session revision changed")]
    RevisionConflict,
    #[error("session revision exhausted")]
    RevisionExhausted,
    #[error("session management outcome unknown")]
    UnknownStoreOutcome,
}
/// Selected identifiers only. Hosts authorize these choices before storing them.
/// Default-consuming Agent methods resolve each missing value from configured
/// host selection and freeze the result in the next admission binding.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionDefaults {
    pub provider_id: Option<String>,
    pub model_id: Option<String>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionSummary {
    pub session_id: SessionId,
    pub workspace_id: String,
    pub directory: String,
    pub title: String,
    pub archived: bool,
    pub defaults: SessionDefaults,
    pub revision: SessionRevision,
    pub created_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum SessionChange {
    Create {
        workspace_id: String,
        directory: String,
        title: String,
        defaults: SessionDefaults,
    },
    Rename {
        title: String,
    },
    SetDefaults {
        defaults: SessionDefaults,
    },
    SetArchived {
        archived: bool,
    },
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManageSession {
    pub id: ControlId,
    pub session_id: SessionId,
    /// Create requires INITIAL; all other changes require the current revision.
    pub expected_revision: SessionRevision,
    pub change: SessionChange,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagementReceipt {
    pub session_id: SessionId,
    pub operation_id: ControlId,
    pub semantic_digest: InputFingerprint,
    pub revision: SessionRevision,
    /// Original committed summary, including on retries after subsequent changes.
    pub summary: SessionSummary,
}
/// A stable ID keyset, bound to its workspace and visibility filter. It is not
/// a coherent multi-page snapshot; concurrent changes may alter later pages.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionListCursor {
    pub workspace_id: String,
    pub include_archived: bool,
    pub after: SessionId,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ListSessions {
    pub workspace_id: String,
    pub include_archived: bool,
    pub limit: u16,
    pub cursor: Option<SessionListCursor>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionListPage {
    pub sessions: Vec<SessionSummary>,
    pub next: Option<SessionListCursor>,
}
impl ManageSession {
    /// Stable canonical digest suitable for a host outbox. No authority is granted.
    /// # Errors
    /// Rejects unbounded identifiers, metadata or invalid create revision.
    pub fn fingerprint(&self) -> Result<InputFingerprint, ManagementError> {
        use sha2::{Digest, Sha256};
        validate_scope(&self.session_id, &self.id)?;
        match &self.change {
            SessionChange::Create {
                workspace_id,
                directory,
                title,
                defaults,
            } => {
                bounded(workspace_id, 512, false)?;
                bounded(directory, 4096, false)?;
                bounded(title, 1024, true)?;
                validate_defaults(defaults)?;
                if self.expected_revision != SessionRevision::INITIAL {
                    return Err(ManagementError::InvalidRequest);
                }
            }
            SessionChange::Rename { title } => bounded(title, 1024, true)?,
            SessionChange::SetDefaults { defaults } => validate_defaults(defaults)?,
            SessionChange::SetArchived { .. } => {}
        }
        let mut value = serde_json::json!([
            "crabber.session-management.v1",
            self.session_id,
            self.expected_revision,
            self.change
        ]);
        value.sort_all_objects();
        let bytes = serde_json::to_vec(&value).map_err(|_| ManagementError::InvalidRequest)?;
        InputFingerprint::new(format!("{:x}", Sha256::digest(bytes)))
            .map_err(|_| ManagementError::InvalidRequest)
    }
}

mod records;
pub(crate) use records::*;
