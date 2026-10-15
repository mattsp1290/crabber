use crate::{
    ListSessions, ManageSession, ManagementError, ManagementReceipt, SessionChange,
    SessionDefaults, SessionListCursor, SessionListPage, SessionSummary,
};
use crabber_core::{ControlId, Session, SessionId, SessionRevision};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Metadata {
    pub archived: bool,
    pub defaults: SessionDefaults,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Audit {
    pub request: ManageSession,
    pub receipt: ManagementReceipt,
    pub created_at: OffsetDateTime,
}
pub(super) fn bounded(value: &str, max: usize, empty: bool) -> Result<(), ManagementError> {
    if (!empty && value.is_empty()) || value.len() > max || value.chars().any(char::is_control) {
        return Err(ManagementError::InvalidRequest);
    }
    Ok(())
}
pub(crate) fn validate_defaults(defaults: &SessionDefaults) -> Result<(), ManagementError> {
    for value in [&defaults.provider_id, &defaults.model_id]
        .into_iter()
        .flatten()
    {
        bounded(value, 256, false)?;
    }
    Ok(())
}
pub(crate) fn validate_scope(session: &SessionId, id: &ControlId) -> Result<(), ManagementError> {
    bounded(&session.0, 512, false)?;
    bounded(&id.0, 128, false)
}
pub(crate) fn summary(
    session: &Session,
    metadata: Metadata,
    revision: SessionRevision,
) -> SessionSummary {
    SessionSummary {
        session_id: session.id.clone(),
        workspace_id: session.workspace_id.clone(),
        directory: session.directory.clone(),
        title: session.title.clone(),
        archived: metadata.archived,
        defaults: metadata.defaults,
        revision,
        created_at: session.created_at,
        updated_at: session.updated_at,
    }
}
pub(crate) fn apply(
    request: &ManageSession,
    existing: Option<(Session, Metadata, SessionRevision)>,
    now: OffsetDateTime,
) -> Result<(Session, Metadata, SessionRevision), ManagementError> {
    let (mut session, mut metadata, revision) = match (&request.change, existing) {
        (
            SessionChange::Create {
                workspace_id,
                directory,
                title,
                defaults,
            },
            None,
        ) => (
            Session {
                id: request.session_id.clone(),
                workspace_id: workspace_id.clone(),
                directory: directory.clone(),
                title: title.clone(),
                created_at: now,
                updated_at: now,
            },
            Metadata {
                archived: false,
                defaults: defaults.clone(),
            },
            SessionRevision::INITIAL,
        ),
        (SessionChange::Create { .. }, Some(_)) => return Err(ManagementError::SemanticConflict),
        (_, None) => return Err(ManagementError::NotFound),
        (_, Some(current)) => current,
    };
    if revision != request.expected_revision {
        return Err(ManagementError::RevisionConflict);
    }
    let revision = revision
        .next()
        .map_err(|_| ManagementError::RevisionExhausted)?;
    match &request.change {
        SessionChange::Create { .. } => {}
        SessionChange::Rename { title } => session.title.clone_from(title),
        SessionChange::SetDefaults { defaults } => metadata.defaults.clone_from(defaults),
        SessionChange::SetArchived { archived } => metadata.archived = *archived,
    }
    session.updated_at = now;
    Ok((session, metadata, revision))
}
pub(crate) fn verify_audit(
    audit: &Audit,
    receipt: &ManagementReceipt,
) -> Result<(), ManagementError> {
    let summary = &receipt.summary;
    bounded(&summary.session_id.0, 512, false).map_err(unknown)?;
    bounded(&summary.workspace_id, 512, false).map_err(unknown)?;
    bounded(&summary.directory, 4096, false).map_err(unknown)?;
    bounded(&summary.title, 1024, true).map_err(unknown)?;
    validate_defaults(&summary.defaults).map_err(unknown)?;
    let matches_change = match &audit.request.change {
        SessionChange::Create {
            workspace_id,
            directory,
            title,
            defaults,
        } => {
            summary.workspace_id == *workspace_id
                && summary.directory == *directory
                && summary.title == *title
                && summary.defaults == *defaults
                && !summary.archived
                && summary.created_at == audit.created_at
        }
        SessionChange::Rename { title } => summary.title == *title,
        SessionChange::SetDefaults { defaults } => summary.defaults == *defaults,
        SessionChange::SetArchived { archived } => summary.archived == *archived,
    };
    if !matches_change || summary.updated_at != audit.created_at {
        return Err(ManagementError::UnknownStoreOutcome);
    }
    if &audit.receipt != receipt
        || audit.request.session_id != receipt.session_id
        || audit.request.id != receipt.operation_id
        || audit.request.fingerprint().map_err(unknown)? != receipt.semantic_digest
        || receipt.summary.session_id != receipt.session_id
        || receipt.summary.revision != receipt.revision
        || audit.request.expected_revision.next().ok() != Some(receipt.revision)
    {
        return Err(ManagementError::UnknownStoreOutcome);
    }
    Ok(())
}
pub(crate) fn unknown(_: impl std::fmt::Debug) -> ManagementError {
    ManagementError::UnknownStoreOutcome
}

pub(crate) fn validate_stored(
    session: &Session,
    metadata: &Metadata,
) -> Result<(), ManagementError> {
    bounded(&session.id.0, 512, false)?;
    bounded(&session.workspace_id, 512, false)?;
    bounded(&session.directory, 4096, false)?;
    bounded(&session.title, 1024, true)?;
    validate_defaults(&metadata.defaults)
}
pub(crate) fn validate_list(request: &ListSessions) -> Result<(), ManagementError> {
    bounded(&request.workspace_id, 512, false)?;
    if !(1..=100).contains(&request.limit) {
        return Err(ManagementError::InvalidRequest);
    }
    if let Some(cursor) = &request.cursor {
        bounded(&cursor.after.0, 512, false)?;
        if cursor.workspace_id != request.workspace_id
            || cursor.include_archived != request.include_archived
        {
            return Err(ManagementError::InvalidRequest);
        }
    }
    Ok(())
}
pub(crate) fn list_page(
    request: &ListSessions,
    mut sessions: Vec<SessionSummary>,
) -> SessionListPage {
    let more = sessions.len() > usize::from(request.limit);
    sessions.truncate(usize::from(request.limit));
    let next = more.then(|| SessionListCursor {
        workspace_id: request.workspace_id.clone(),
        include_archived: request.include_archived,
        after: sessions.last().expect("positive limit").session_id.clone(),
    });
    SessionListPage { sessions, next }
}
