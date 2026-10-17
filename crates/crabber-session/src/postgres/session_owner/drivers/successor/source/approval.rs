//! Exact original approval sources; neither later continuation nor head is read.
use super::{Error, Proof, RegisterSuccessorExecutionDriver, read, unknown};
use crate::native_parent::RunIssuance;
use crate::postgres::transaction::Transaction;
use crate::session_owner::driver::successor::source::{self, SelectedClaim, claim::Native};
use crate::session_owner::driver::successor::source::{approval::Evidence, input::Input};
use crate::session_owner::{audit::Audit, preparation::PreparationKey};
use sqlx::Postgres;

pub(super) async fn original(
    tx: &mut Transaction<'_, Postgres>,
    request: &RegisterSuccessorExecutionDriver,
    previous: &RunIssuance,
    next: &RunIssuance,
    audits: &[Audit],
    proof: &mut Proof,
    binding: Option<&crate::ExecutionBindingCapsule>,
) -> Result<SelectedClaim, Error> {
    let session = request.context.session_id();
    let run = &request.fence.run_id;
    let origin = audits[0]
        .record
        .preparation
        .as_ref()
        .ok_or(Error::UnknownStoreOutcome)?;
    let PreparationKey::Approval(id) = &origin.key else {
        return Err(Error::SemanticConflict);
    };
    Box::pin(with_native(
        tx,
        super::scope::Scope { session, run },
        id,
        next,
        proof,
        |native| source::selected(request, previous, next, audits, native, binding),
    ))
    .await
}
async fn with_native<T>(
    tx: &mut Transaction<'_, Postgres>,
    scope: super::scope::Scope<'_>,
    id: &crabber_core::ControlId,
    next: &RunIssuance,
    proof: &mut Proof,
    use_native: impl FnOnce(Native<'_>) -> Result<T, Error>,
) -> Result<T, Error> {
    let session = scope.session;
    let run = scope.run;
    let (receipt, event) = control(tx, session, id, proof).await?;
    let work = source::approval::work(&receipt)?;
    let (crate::session_owner::preparation::ExistingWorkTarget::ApprovalClaim(target)
    | crate::session_owner::preparation::ExistingWorkTarget::ApprovalRecovery {
        continuation: target,
        ..
    }) = &work
    else {
        return Err(Error::SemanticConflict);
    };
    let batch =
        crate::postgres::approval::original_batch_with_proof(tx, session, run, target, proof)
            .await
            .map_err(unknown)?;
    let (suspension, suspension_event) = control(tx, session, &batch.id, proof).await?;
    let pause_id = crate::approval::derived_id("suspend-attempt", &batch.id);
    let pause = super::attempt(tx, session, &pause_id, proof).await?;
    let pause_frame =
        crate::postgres::read_proof::event(tx, session, pause.receipt.event_cursor, true, proof)
            .await
            .map_err(unknown)?;
    let (claim, native_id) = source::approval::parent_operation(next, id)?;
    let parent = super::attempt(tx, session, &native_id, proof).await?;
    let original = if let crate::ApprovalOutcome::ReclaimedUnstarted {
        original_claim_id, ..
    } = &receipt.outcome
    {
        Some(control(tx, session, original_claim_id, proof).await?)
    } else {
        None
    };
    let status = if claim {
        Some(
            super::scope::issuer(
                tx,
                scope,
                next.sequence
                    .checked_add(1)
                    .ok_or(Error::UnknownStoreOutcome)?,
                proof,
            )
            .await?,
        )
    } else {
        None
    };
    let evidence = Evidence {
        receipt: &receipt,
        event: &event,
        parent: &parent.receipt,
        parent_event: &parent.event,
        batch: &batch,
        suspension: &suspension,
        suspension_event: &suspension_event,
        pause_parent: &pause.receipt,
        pause_event: &pause.event,
        pause_frame: &pause_frame,
        original: original.as_ref().map(|(r, e)| (r, e)),
        status: status.as_ref(),
    };
    let owned = owned_input(tx, session, run, proof).await?;
    use_native(Native::Approval {
        evidence,
        input: owned.as_input()?,
    })
}

async fn original_admission_for(
    tx: &mut Transaction<'_, Postgres>,
    session: &crabber_core::SessionId,
    run: &crabber_core::RunId,
    proof: &mut Proof,
) -> Result<super::super::super::source::Original, Error> {
    if crate::postgres::native_parent::source_protocol::original(tx, session, run, 1, proof)
        .await
        .map_err(unknown)?
        .ok_or(Error::UnknownStoreOutcome)?
        .ownership_handoff
        .is_none()
    {
        return Err(Error::Unsupported);
    }
    super::super::super::source::original(tx, session, run, proof).await
}
pub(in crate::postgres::session_owner::drivers::successor) async fn verify_input(
    tx: &mut Transaction<'_, Postgres>,
    session: &crabber_core::SessionId,
    run: &crabber_core::RunId,
    binding: &crate::ExecutionBindingCapsule,
    proof: &mut Proof,
) -> Result<(), Error> {
    owned_input(tx, session, run, proof)
        .await?
        .as_input()?
        .verify(session, run, Some(binding))
}

pub(in crate::postgres::session_owner::drivers::successor) struct OwnedInput {
    original: super::super::super::source::Original,
    input: Option<OriginalInput>,
}
impl OwnedInput {
    pub(in crate::postgres::session_owner::drivers::successor) fn as_input(
        &self,
    ) -> Result<Input<'_>, Error> {
        Ok(if let Some(input) = &self.input {
            Input::Keyed {
                init: self
                    .original
                    .init
                    .as_ref()
                    .ok_or(Error::UnknownStoreOutcome)?,
                initial: &self.original.initial,
                capsule: &input.capsule,
                receipt: &input.receipt,
                key: &input.key,
            }
        } else {
            Input::Unkeyed(&self.original.witness)
        })
    }
}
pub(in crate::postgres::session_owner::drivers::successor) async fn owned_input(
    tx: &mut Transaction<'_, Postgres>,
    session: &crabber_core::SessionId,
    run: &crabber_core::RunId,
    proof: &mut Proof,
) -> Result<OwnedInput, Error> {
    let original = original_admission_for(tx, session, run, proof).await?;
    let input = if original.witness.is_unkeyed() {
        None
    } else {
        Some(read::<OriginalInput>(tx, &session.0, &run.0, INPUT_COST, INPUT, proof).await?)
    };
    Ok(OwnedInput { original, input })
}
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct OriginalInput {
    capsule: crate::AdmissionExecutionCapsule,
    receipt: crabber_core::AdmissionReceipt,
    key: crabber_core::AdmissionKey,
}
async fn control(
    tx: &mut Transaction<'_, Postgres>,
    session: &crabber_core::SessionId,
    id: &crabber_core::ControlId,
    proof: &mut Proof,
) -> Result<(crate::ApprovalReceipt, crabber_core::EventRecord), Error> {
    let receipt = crate::postgres::approval::receipt_with_proof(tx, session, id, proof)
        .await
        .map_err(unknown)?
        .ok_or(Error::UnknownStoreOutcome)?;
    let event = crate::postgres::read_proof::event(tx, session, receipt.event_cursor, false, proof)
        .await
        .map_err(unknown)?;
    crate::approval::verify_receipt(&receipt, &event).map_err(unknown)?;
    if receipt.operation_id != *id || receipt.session_id != *session {
        return Err(Error::SemanticConflict);
    }
    Ok((receipt, event))
}
const INPUT_COST: &str = r"WITH native AS (SELECT CASE WHEN capsule_version=1 THEN data::text ELSE data_json END::json AS d FROM admission_executions WHERE run_id=$2 AND CAST($1 AS TEXT) IS NOT NULL), original AS (SELECT json_build_object('capsule',d->'capsule','receipt',d->'receipt','key',d->'key')::text AS data FROM native) SELECT CAST(octet_length(data) AS BIGINT) FROM original";
const INPUT: &str = r"WITH native AS (SELECT CASE WHEN capsule_version=1 THEN data::text ELSE data_json END::json AS d FROM admission_executions WHERE run_id=$2 AND CAST($1 AS TEXT) IS NOT NULL), original AS (SELECT json_build_object('capsule',d->'capsule','receipt',d->'receipt','key',d->'key')::text AS data FROM native) SELECT CASE WHEN octet_length(data)<=16777216 THEN data END FROM original";

pub(in crate::postgres::session_owner::drivers::successor) async fn verify_scope(
    tx: &mut Transaction<'_, Postgres>,
    scope: super::scope::Scope<'_>,
    id: &crabber_core::ControlId,
    binding: &crate::ExecutionBindingCapsule,
    proof: &mut Proof,
) -> Result<source::ContinuationFacts, Error> {
    let pair = super::scope::pair(tx, scope, PreparationKey::Approval(id.clone()), proof).await?;
    Box::pin(with_native(tx, scope, id, &pair.next, proof, |native| {
        if let Native::Approval { input, .. } = &native {
            input.verify(scope.session, scope.run, Some(binding))?;
        }
        let verified =
            source::AuthenticatedClaim::verify(&pair.previous, &pair.next, &pair.audits, native)?;
        verified.verify_binding(binding)?;
        let (continuation, attempt) = verified.approval.ok_or(Error::SemanticConflict)?;
        Ok(source::ContinuationFacts {
            revision: verified.revision,
            head: verified.head,
            head_digest: verified.head_digest,
            continuation,
            attempt,
        })
    }))
    .await
}
