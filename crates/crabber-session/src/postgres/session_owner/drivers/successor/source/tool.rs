//! Exact original compound requests; later recovery selectors are not inputs.
use super::{Error, Proof, RegisterSuccessorExecutionDriver, issuer, unknown};
use crate::native_parent::RunIssuance;
use crate::session_owner::driver::successor::source::{self, SelectedClaim, claim::Native, tool};
use crate::session_owner::{audit::Audit, preparation::PreparationKey};
use crabber_core::{ControlId, RunId, SessionId};
use sqlx::{Postgres, Transaction};

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
    let PreparationKey::ToolContinuation(id) = &origin.key else {
        return Err(Error::SemanticConflict);
    };
    let (receipt, event) = control(tx, session, id, proof).await?;
    let (target, _, original_id) = tool::targets(&receipt)?;
    let scope = crate::continuation_input::Scope {
        domain: crate::continuation_input::Domain::Tool,
        session: session.clone(),
        run: run.clone(),
        continuation: target.clone(),
    };
    if !crate::postgres::continuation_input::protocol(tx, &scope, proof)
        .await
        .map_err(unknown)?
    {
        return Err(Error::Unsupported);
    }
    let batch = crate::postgres::tool_continuation::original_plan::batch_with_proof(
        tx, session, run, target, proof,
    )
    .await
    .map_err(unknown)?;
    let projected = projection(tx, &scope, proof).await?;
    let answers: Vec<_> = projected
        .answers
        .iter()
        .map(crate::continuation_input::Input::ToolAnswer)
        .collect();
    crate::postgres::continuation_input::answers(tx, &scope, &answers, proof)
        .await
        .map_err(unknown)?;
    let (suspension, suspension_event) = control(tx, session, &batch.id, proof).await?;
    let original = if let Some(id) = original_id {
        Some(original_claim(tx, request, id, proof).await?)
    } else {
        None
    };
    let evidence = tool::Evidence {
        receipt: &receipt,
        event: &event,
        batch: &batch,
        suspension: &suspension,
        suspension_event: &suspension_event,
        claim: projected.claim.as_ref().ok_or(Error::UnknownStoreOutcome)?,
        frontend: projected.frontend.as_deref(),
        original: original.as_ref().map(|o| tool::Claim {
            previous: &o.previous,
            next: &o.next,
            audits: &o.audits,
            receipt: &o.receipt,
            event: &o.event,
        }),
    };
    let owned = super::approval::owned_input(tx, session, run, proof).await?;
    source::selected(
        request,
        previous,
        next,
        audits,
        Native::Tool {
            evidence,
            input: owned.as_input()?,
        },
        binding,
    )
}

struct Original {
    previous: RunIssuance,
    next: RunIssuance,
    audits: Vec<Audit>,
    receipt: crate::ToolContinuationReceipt,
    event: crabber_core::EventRecord,
}
async fn original_claim(
    tx: &mut Transaction<'_, Postgres>,
    request: &RegisterSuccessorExecutionDriver,
    id: &ControlId,
    proof: &mut Proof,
) -> Result<Original, Error> {
    let session = request.context.session_id();
    let join = crate::session_owner::preparation::identity::ids(
        &PreparationKey::ToolContinuation(id.clone()),
        session,
    )?
    .join;
    // Only the two bounded scalar candidates transfer. No current-head guess or
    // per-Run history ceiling; database scanning cost can grow with history.
    proof.charge(16).map_err(unknown)?;
    let candidates: Vec<i64> = sqlx::query_scalar(ORIGINAL_SLOT)
        .bind(&session.0)
        .bind(&request.fence.run_id.0)
        .bind(&join.0)
        .fetch_all(&mut **tx)
        .await
        .map_err(unknown)?;
    let [sequence] = candidates.as_slice() else {
        return Err(Error::UnknownStoreOutcome);
    };
    let sequence = u64::try_from(*sequence).map_err(unknown)?;
    let next = issuer(tx, request, sequence, proof).await?;
    let previous = issuer(
        tx,
        request,
        sequence
            .checked_sub(1)
            .filter(|n| *n > 0)
            .ok_or(Error::UnknownStoreOutcome)?,
        proof,
    )
    .await?;
    let joined = super::super::super::proof::audit(tx, session, &join, proof)
        .await?
        .ok_or(Error::UnknownStoreOutcome)?;
    let mut audits = Vec::with_capacity(5);
    for id in source::claim::audit_ids(&joined)? {
        audits.push(
            super::super::super::proof::audit(tx, session, &id, proof)
                .await?
                .ok_or(Error::UnknownStoreOutcome)?,
        );
    }
    let (receipt, event) = control(tx, session, id, proof).await?;
    Ok(Original {
        previous,
        next,
        audits,
        receipt,
        event,
    })
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Projection {
    version: u32,
    session: SessionId,
    run: RunId,
    claim: Option<crate::ToolContinuationClaim>,
    frontend: Option<Box<crate::FrontendContinuationClaim>>,
    answers: Vec<crate::ToolInteractionAnswer>,
}
async fn projection(
    tx: &mut Transaction<'_, Postgres>,
    scope: &crate::continuation_input::Scope,
    proof: &mut Proof,
) -> Result<Projection, Error> {
    let generation = i64::try_from(scope.continuation.generation.get()).map_err(unknown)?;
    let cost = format!(
        "WITH selected AS ({PROJECTION}) SELECT CAST(octet_length(data) AS BIGINT) FROM selected"
    );
    crate::postgres::read_proof::charge(
        tx,
        sqlx::query_scalar(&cost)
            .bind(&scope.session.0)
            .bind(&scope.run.0)
            .bind(&scope.continuation.id.0)
            .bind(generation),
        proof,
    )
    .await
    .map_err(unknown)?;
    let query = format!(
        "WITH selected AS ({PROJECTION}) SELECT CASE WHEN octet_length(data)<=4194304 THEN data END FROM selected"
    );
    let raw: Option<String> = sqlx::query_scalar(&query)
        .bind(&scope.session.0)
        .bind(&scope.run.0)
        .bind(&scope.continuation.id.0)
        .bind(generation)
        .fetch_one(&mut **tx)
        .await
        .map_err(unknown)?;
    let projected: Projection =
        serde_json::from_str(raw.as_deref().ok_or(Error::UnknownStoreOutcome)?).map_err(unknown)?;
    if projected.version != 1 || projected.session != scope.session || projected.run != scope.run {
        return Err(Error::UnknownStoreOutcome);
    }
    Ok(projected)
}

async fn control(
    tx: &mut Transaction<'_, Postgres>,
    session: &SessionId,
    id: &ControlId,
    proof: &mut Proof,
) -> Result<(crate::ToolContinuationReceipt, crabber_core::EventRecord), Error> {
    let (receipt, event) =
        crate::postgres::tool_continuation::read::receipt_with_proof(tx, session, id, proof)
            .await
            .map_err(unknown)?
            .ok_or(Error::UnknownStoreOutcome)?;
    if receipt.operation_id != *id || receipt.session_id != *session {
        return Err(Error::SemanticConflict);
    }
    Ok((receipt, event))
}
const ORIGINAL_SLOT: &str = "SELECT sequence FROM native_run_issuances WHERE session_id=$1 AND run_id=$2 AND data_json::json->'completion_source'->>'root_join_id'=$3 ORDER BY sequence LIMIT 2";
const PROJECTION: &str = "SELECT json_build_object('version',format_version,'session',data_json::json->'session_id','run',data_json::json->'run_id','claim',data_json::json->'claim','frontend',data_json::json->'frontend_claim','answers',(SELECT COALESCE(json_agg(q->'answer'),'[]'::json) FROM json_array_elements(data_json::json->'questions') q WHERE json_typeof(q->'answer')<>'null'))::text AS data FROM tool_continuations WHERE session_id=$1 AND run_id=$2 AND id=$3 AND generation=$4";
