use super::MemoryStore;
use crate::{PublicRunOutcome, PublicRunState, PublicRunSummary, StoreError, public_run};
use crabber_core::{Run, SessionId, SessionRevision};
use serde::Serialize;

#[derive(Serialize)]
struct RunView<'a> {
    run_id: &'a crabber_core::RunId,
    session_id: &'a SessionId,
    status: crabber_core::RunStatus,
    usage: &'a crabber_core::Usage,
    created_at: time::OffsetDateTime,
    updated_at: time::OffsetDateTime,
}
impl<'a> From<&'a Run> for RunView<'a> {
    fn from(run: &'a Run) -> Self {
        Self {
            run_id: &run.id,
            session_id: &run.session_id,
            status: run.status,
            usage: &run.usage,
            created_at: run.created_at,
            updated_at: run.updated_at,
        }
    }
}
#[derive(Serialize)]
struct StateView<'a> {
    session_id: &'a SessionId,
    revision: SessionRevision,
    run: Option<RunView<'a>>,
}
impl MemoryStore {
    pub(super) fn read_public_run(
        &self,
        session: &SessionId,
        max_bytes: usize,
    ) -> Result<PublicRunOutcome, StoreError> {
        public_run::validate_session(session)?;
        let state = self.state.lock().unwrap();
        if !state.sessions.contains_key(session) {
            return Err(StoreError::NotFound);
        }
        let mut latest = None;
        for id in state.run_order.iter().rev() {
            let run = state
                .runs
                .get(id)
                .ok_or_else(|| StoreError::Validation("invalid run index".into()))?;
            if run.session_id != *session {
                continue;
            }
            if run.id != *id
                || run.id.0.is_empty()
                || run.id.0.len() > 128
                || run.id.0.chars().any(char::is_control)
            {
                return Err(StoreError::Validation("invalid public run metadata".into()));
            }
            latest = Some(run);
            break;
        }
        let revision = state
            .public_revisions
            .get(session)
            .copied()
            .unwrap_or(SessionRevision::INITIAL);
        let view = StateView {
            session_id: session,
            revision,
            run: latest.map(RunView::from),
        };
        if crate::snapshot::encoded_bytes(&view, max_bytes.min(public_run::MAX_BYTES)).is_none() {
            return Ok(PublicRunOutcome::Limited);
        }
        Ok(PublicRunOutcome::State(PublicRunState {
            session_id: session.clone(),
            revision,
            run: latest.map(|run| PublicRunSummary {
                run_id: run.id.clone(),
                session_id: run.session_id.clone(),
                status: run.status,
                usage: run.usage.clone(),
                created_at: run.created_at,
                updated_at: run.updated_at,
            }),
        }))
    }
}
