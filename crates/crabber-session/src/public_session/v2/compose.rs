use super::{
    PublicHeaderOutcomeV2, PublicHeaderRequestV2, PublicSessionError, PublicSessionLimitV2,
    PublicSessionOutcomeV2, PublicSessionPageV2, PublicSessionRequestV2, SessionRevision,
};
use crate::{PublicHistoryOutcome, PublicRunOutcome, SnapshotRequest, Store};
pub(crate) async fn snapshot<S: Store + ?Sized>(
    store: &S,
    request: PublicSessionRequestV2,
) -> Result<PublicSessionOutcomeV2, PublicSessionError> {
    request.validate()?;
    let check = PublicHeaderRequestV2 {
        session_id: request.session_id.clone(),
        run_id: None,
        expected_revision: SessionRevision::INITIAL,
        limits: request.limits,
    };
    check.validate()?;
    let state = match store
        .public_run_state(&request.session_id, request.limits.header_bytes)
        .await?
    {
        PublicRunOutcome::State(state) => state,
        PublicRunOutcome::Limited => {
            return Ok(PublicSessionOutcomeV2::Limited {
                limit: PublicSessionLimitV2::HeaderBytes,
                continuation: request.continuation,
            });
        }
    };
    let run_bytes = crate::snapshot::encoded_bytes(&state, request.limits.header_bytes)
        .ok_or(PublicSessionError::UnknownStoreOutcome)?;
    let mut header = match store
        .public_control_header_v2(PublicHeaderRequestV2 {
            run_id: state.run.as_ref().map(|run| run.run_id.clone()),
            expected_revision: state.revision,
            limits: super::PublicSessionLimitsV2 {
                header_bytes: request.limits.header_bytes.saturating_sub(run_bytes),
                ..request.limits
            },
            ..check
        })
        .await?
    {
        PublicHeaderOutcomeV2::Header(header) => header,
        PublicHeaderOutcomeV2::Limited(limit) => {
            return Ok(PublicSessionOutcomeV2::Limited {
                limit,
                continuation: request.continuation,
            });
        }
        PublicHeaderOutcomeV2::Invalidated => return Ok(PublicSessionOutcomeV2::Invalidated),
    };
    let history = match store
        .public_history_v2(SnapshotRequest {
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
            return Ok(PublicSessionOutcomeV2::Limited {
                limit: PublicSessionLimitV2::History(limit),
                continuation: Some(continuation),
            });
        }
        PublicHistoryOutcome::Invalidated { .. } => return Ok(PublicSessionOutcomeV2::Invalidated),
    };
    if history.captured_revision != state.revision
        || header.revision != state.revision
        || store.session_revision(&request.session_id).await? != state.revision
    {
        return Ok(PublicSessionOutcomeV2::Invalidated);
    }
    header.read_usage.encoded_bytes += run_bytes;
    Ok(PublicSessionOutcomeV2::Page(Box::new(
        PublicSessionPageV2 {
            version: 2,
            captured_revision: state.revision,
            run: state.run,
            header: *header,
            history,
        },
    )))
}
