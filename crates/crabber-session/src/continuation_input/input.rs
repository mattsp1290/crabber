use super::{Domain, Error, Kind, Receipt, Scope};
use crabber_core::{ControlId, InputFingerprint, InteractionState};
#[derive(Clone, Copy, serde::Serialize)]
#[serde(tag = "kind", content = "original", rename_all = "snake_case")]
pub(crate) enum Input<'a> {
    ApprovalSuspension(&'a crate::ApprovalSuspension),
    ApprovalAnswer(&'a crate::ApprovalAnswer),
    ToolSuspension(&'a crate::ToolContinuationSuspension),
    ToolAnswer(&'a crate::ToolInteractionAnswer),
}
impl Input<'_> {
    pub(super) fn facts(
        self,
        receipt: Receipt<'_>,
    ) -> Result<(Scope, ControlId, Kind, String, InputFingerprint), Error> {
        let (domain, kind, continuation, subject) = receipt.facts()?;
        let scope = Scope {
            domain,
            session: receipt.session().clone(),
            run: receipt.run().clone(),
            continuation: continuation.clone(),
        };
        let (expected_domain, expected_kind, target, operation, fingerprint, expected_subject) =
            match self {
                Self::ApprovalSuspension(batch) => (
                    Domain::Approval,
                    Kind::Suspension,
                    &batch.continuation,
                    &batch.id,
                    batch
                        .fingerprint(receipt.session(), receipt.run())
                        .map_err(|_| Error)?,
                    None,
                ),
                Self::ToolSuspension(batch) => (
                    Domain::Tool,
                    Kind::Suspension,
                    &batch.continuation,
                    &batch.id,
                    batch
                        .fingerprint(receipt.session(), receipt.run())
                        .map_err(|_| Error)?,
                    None,
                ),
                Self::ApprovalAnswer(answer) => {
                    if answer.session_id != scope.session || answer.run_id != scope.run {
                        return Err(Error);
                    }
                    let state = match answer.reply {
                        crate::ApprovalReply::Grant => InteractionState::Answered,
                        crate::ApprovalReply::Deny => InteractionState::Denied,
                    };
                    (
                        Domain::Approval,
                        Kind::Answer,
                        &answer.continuation,
                        &answer.id,
                        answer.fingerprint().map_err(|_| Error)?,
                        Some((&answer.interaction_id, state)),
                    )
                }
                Self::ToolAnswer(answer) => {
                    if answer.session_id != scope.session || answer.run_id != scope.run {
                        return Err(Error);
                    }
                    (
                        Domain::Tool,
                        Kind::Answer,
                        &answer.continuation,
                        &answer.id,
                        answer.fingerprint().map_err(|_| Error)?,
                        Some((
                            &answer.interaction_id,
                            crate::tool_continuation::reply_state(&answer.reply),
                        )),
                    )
                }
            };
        if domain != expected_domain
            || kind != expected_kind
            || continuation != target
            || subject != expected_subject
        {
            return Err(Error);
        }
        Ok((
            scope,
            operation.clone(),
            kind,
            subject.map_or_else(|| "suspension".into(), |s| s.0.0.clone()),
            fingerprint,
        ))
    }
}

impl<'a> Input<'a> {
    pub(crate) fn answer_identity(
        self,
    ) -> Result<(&'a crabber_core::InteractionId, &'a ControlId), Error> {
        match self {
            Self::ApprovalAnswer(a) => Ok((&a.interaction_id, &a.id)),
            Self::ToolAnswer(a) => Ok((&a.interaction_id, &a.id)),
            _ => Err(Error),
        }
    }
}
