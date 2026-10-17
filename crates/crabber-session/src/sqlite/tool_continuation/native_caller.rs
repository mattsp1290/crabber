//! Actual bounded compound carriers under one enclosing caller Proof.
use super::{Error, unknown};
use crate::native_enrollment::caller::{
    Caller, Fresh,
    tool::{Tool, ToolCompletion, ToolInput},
};
use crate::public_session::v2::proof::Proof;
use sqlx::{Sqlite, Transaction};
pub(super) async fn digest(
    tx: &mut Transaction<'_, Sqlite>,
    input: ToolInput<'_>,
    proof: &mut Proof,
) -> Result<crabber_core::InputFingerprint, Error> {
    let exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM operation_receipts WHERE session_id=$1 AND operation_id=$2)",
    )
    .bind(&input.original().session_id.0)
    .bind(&input.id().0)
    .fetch_one(&mut **tx)
    .await
    .map_err(unknown)?;
    if !exists {
        input.measure(proof).map_err(unknown)?;
    }
    input.fingerprint()
}
pub(super) async fn replay(
    tx: &mut Transaction<'_, Sqlite>,
    input: ToolInput<'_>,
    digest: &crabber_core::InputFingerprint,
    proof: &mut Proof,
) -> Result<Option<crate::ToolContinuationReceipt>, Error> {
    let saved =
        super::read::receipt_with_proof(tx, &input.original().session_id, input.id(), proof)
            .await
            .map_err(|e| e.into_domain_or(Error::UnknownStoreOutcome))?
            .map(|(r, _)| r);
    if let Some(saved) = &saved {
        if saved.run_id != input.original().run_id || saved.semantic_digest != *digest {
            return Err(Error::SemanticConflict);
        }
        input.verify_receipt(saved)?;
    }
    Ok(saved)
}
pub(super) async fn outer_replay(
    store: &super::SqliteStore,
    input: ToolInput<'_>,
    proof: &mut Proof,
) -> Result<Option<crate::ToolContinuationReceipt>, Error> {
    let mut tx = store.readers.begin().await.map_err(unknown)?;
    let result = async {
        let digest = digest(&mut tx, input, proof).await?;
        replay(&mut tx, input, &digest, proof).await
    }
    .await;
    super::finish(tx, result).await
}
pub(super) async fn active(
    tx: &mut Transaction<'_, Sqlite>,
    run: &crabber_core::Run,
    proof: &mut Proof,
) -> Result<Option<crate::tool_continuation::ToolContinuationRecord>, Error> {
    super::scan::scan_with_proof(tx, &run.session_id, Some(&run.id), proof)
        .await
        .map(|r| r.active)
        .map_err(|e| e.into_domain_or(Error::UnknownStoreOutcome))
}
pub(super) async fn observe(
    tx: &mut Transaction<'_, Sqlite>,
    caller: Caller,
    done: ToolCompletion<'_>,
    outcome: &crate::ToolContinuationClaimOutcome,
    proof: &mut Proof,
    #[cfg(test)] probe: &crate::native_enrollment::caller::Probe,
) -> Result<(), Error> {
    let crate::ToolContinuationClaimOutcome::Applied { receipt, grant } = outcome else {
        return Err(Error::UnknownStoreOutcome);
    };
    let event = super::super::read_proof::control_event(
        tx,
        &done.current.session_id,
        receipt.event_cursor,
        proof,
    )
    .await
    .map_err(unknown)?;
    super::super::native_enrollment::record_raw_native(
        tx,
        caller,
        &Fresh::Tool(Tool {
            input: done.input,
            previous: done.previous,
            current: done.current,
            record: done.record,
            previous_parent: done.previous_parent,
            parent: done.parent,
            native_request: done.native_request,
            phases: done.phases,
            publication: done.publication,
            completion: done.completion,
            receipt,
            event: &event,
            grant,
        }),
        proof,
        #[cfg(test)]
        probe,
    )
    .await
    .map_err(unknown)
}

impl crate::SqliteStore {
    pub(in crate::sqlite) async fn tool_with_proof(
        &self,
        input: ToolInput<'_>,
        proof: &mut Proof,
    ) -> Result<crate::ToolContinuationClaimOutcome, Error> {
        match input {
            ToolInput::Claim(r) => Box::pin(self.claim_compound_with_proof(r, None, proof)).await,
            ToolInput::FrontendClaim(r) => {
                Box::pin(self.claim_compound_with_proof(&r.claim, Some(r), proof)).await
            }
            ToolInput::Recovery(_) | ToolInput::FrontendRecovery(_) => {
                Box::pin(self.recover_compound_with_proof(input, proof)).await
            }
        }
    }
}

pub(super) async fn issue(
    tx: &mut Transaction<'_, Sqlite>,
    caller: Caller,
    done: ToolCompletion<'_>,
    parts: crate::native_enrollment::caller::tool::ToolGrantParts,
    preparation: Option<&crate::session_owner::preparation::proof::VerifiedPreparation<'_>>,
    proof: &mut Proof,
    #[cfg(test)] probe: &crate::native_enrollment::caller::Probe,
) -> Result<ToolMutation, Error> {
    let outcome = done.issue(parts, proof)?;
    let outcome = match preparation {
        Some(preparation) => ToolMutation::Prepared(
            crate::sqlite::session_owner::verify_tool(tx, preparation.grant, outcome, proof)
                .await
                .map_err(|_| Error::UnknownStoreOutcome)?,
        ),
        None => ToolMutation::Applied(outcome),
    };
    observe(
        tx,
        caller,
        done,
        outcome.outcome().ok_or(Error::UnknownStoreOutcome)?,
        proof,
        #[cfg(test)]
        probe,
    )
    .await?;
    Ok(outcome)
}

pub(super) async fn finish(
    tx: &mut Transaction<'_, Sqlite>,
    caller: Caller,
    done: ToolCompletion<'_>,
    output: crate::native_enrollment::caller::tool::ToolGrantOutput,
    preparation: Option<&crate::session_owner::preparation::proof::VerifiedPreparation<'_>>,
    proof: &mut Proof,
    #[cfg(test)] probe: &crate::native_enrollment::caller::Probe,
) -> Result<ToolMutation, Error> {
    proof
        .measure(&done.parent.snapshot)
        .map_err(|_| Error::UnknownStoreOutcome)?;
    let outcome = if done.input.target().is_some() {
        done.record
            .recovered_outcome(done.parent.snapshot.clone())?
    } else {
        done.record.claimed_outcome(done.parent.snapshot.clone())
    };
    let receipt = super::write::publish_with_proof(
        tx,
        done.previous,
        done.input.id().clone(),
        output.digest,
        outcome,
        done.current.updated_at,
        proof,
    )
    .await?;
    issue(
        tx,
        caller,
        done,
        crate::native_enrollment::caller::tool::ToolGrantParts {
            fence: output.fence,
            authority: output.authority,
            phases: output.phases,
            frontend: output.frontend,
            receipt,
        },
        preparation,
        proof,
        #[cfg(test)]
        probe,
    )
    .await
}

pub(in crate::sqlite) enum ToolMutation {
    Applied(crate::ToolContinuationClaimOutcome),
    Prepared(crate::sqlite::session_owner::VerifiedTool),
    Replayed(crate::ToolContinuationReceipt),
}
impl ToolMutation {
    pub(in crate::sqlite) fn into_outcome(self) -> crate::ToolContinuationClaimOutcome {
        match self {
            Self::Applied(v) => v,
            Self::Prepared(v) => v.into_outcome(),
            Self::Replayed(r) => crate::ToolContinuationClaimOutcome::Replayed(r),
        }
    }
    fn outcome(&self) -> Option<&crate::ToolContinuationClaimOutcome> {
        match self {
            Self::Applied(v) => Some(v),
            Self::Prepared(v) => Some(v.outcome()),
            Self::Replayed(_) => None,
        }
    }
}
