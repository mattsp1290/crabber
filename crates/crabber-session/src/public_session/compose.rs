use super::{
    PublicHeaderOutcome, PublicHeaderRequest, PublicSessionError, PublicSessionLimit,
    PublicSessionOutcome, PublicSessionPage, PublicSessionRequest, SessionRevision,
};
use crate::{PublicHistoryOutcome, PublicRunOutcome, SnapshotRequest, Store};
pub(crate) async fn snapshot<S: Store + ?Sized>(
    store: &S,
    request: PublicSessionRequest,
) -> Result<PublicSessionOutcome, PublicSessionError> {
    let check = PublicHeaderRequest {
        session_id: request.session_id.clone(),
        run_id: None,
        expected_revision: SessionRevision::INITIAL,
        max_bytes: request.limits.header_bytes,
        max_proof_bytes: request.limits.proof_bytes,
        max_interactions: request.limits.interactions,
    };
    check.validate()?;
    let state = match store
        .public_run_state(&request.session_id, request.limits.header_bytes)
        .await?
    {
        PublicRunOutcome::State(state) => state,
        PublicRunOutcome::Limited => {
            return Ok(PublicSessionOutcome::Limited {
                limit: PublicSessionLimit::HeaderBytes,
                continuation: request.continuation,
            });
        }
    };
    let run_bytes = crate::snapshot::encoded_bytes(&state, request.limits.header_bytes)
        .ok_or(PublicSessionError::UnknownStoreOutcome)?;
    let mut header = match store
        .public_control_header(PublicHeaderRequest {
            run_id: state.run.as_ref().map(|run| run.run_id.clone()),
            expected_revision: state.revision,
            max_bytes: request.limits.header_bytes.saturating_sub(run_bytes),
            ..check
        })
        .await?
    {
        PublicHeaderOutcome::Header(header) => header,
        PublicHeaderOutcome::Limited(limit) => {
            return Ok(PublicSessionOutcome::Limited {
                limit,
                continuation: request.continuation,
            });
        }
        PublicHeaderOutcome::Invalidated => return Ok(PublicSessionOutcome::Invalidated),
    };
    let history = match store
        .public_history(SnapshotRequest {
            session_id: request.session_id.clone(),
            limits: request.limits.history,
            continuation: request.continuation,
        })
        .await?
    {
        PublicHistoryOutcome::Page(page) => page,
        PublicHistoryOutcome::Limited {
            limit,
            continuation,
            ..
        } => {
            return Ok(PublicSessionOutcome::Limited {
                limit: PublicSessionLimit::History(limit),
                continuation: Some(continuation),
            });
        }
        PublicHistoryOutcome::Invalidated { .. } => return Ok(PublicSessionOutcome::Invalidated),
    };
    if history.captured_revision != state.revision
        || header.revision != state.revision
        || store.session_revision(&request.session_id).await? != state.revision
    {
        return Ok(PublicSessionOutcome::Invalidated);
    }
    header.read_usage.encoded_bytes += run_bytes;
    Ok(PublicSessionOutcome::Page(Box::new(PublicSessionPage {
        version: 1,
        captured_revision: state.revision,
        run: state.run,
        header: *header,
        history,
    })))
}
