//! Exact original compound requests; later recovery selectors are not inputs.
use super::{Error, Proof, RegisterSuccessorExecutionDriver, unknown};
use crate::native_parent::RunIssuance;
use crate::session_owner::driver::successor::source::{self, SelectedClaim, claim::Native, tool};
use crate::session_owner::{audit::Audit, preparation::PreparationKey};
use crabber_core::{ControlId, RunId, SessionId};
use sqlx::{Sqlite, Transaction};

pub(super) async fn original(
    tx: &mut Transaction<'_, Sqlite>,
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
    Box::pin(with_native(
        tx,
        super::scope::Scope { session, run },
        id,
        proof,
        |native, proof| {
            let Native::Tool { evidence, .. } = &native else {
                return Err(Error::Unsupported);
            };
            let evidence = *evidence;
            let mut selected = source::selected(request, previous, next, audits, native, binding)?;
            selected.tool_payload_digest = Some(
                crate::session_owner::preparation::tool_driver::payload::expected(
                    evidence.batch,
                    evidence.answers,
                    evidence.frontend,
                    proof,
                )?,
            );
            Ok(selected)
        },
    ))
    .await
}
async fn with_native<T>(
    tx: &mut Transaction<'_, Sqlite>,
    selected_scope: super::scope::Scope<'_>,
    id: &ControlId,
    proof: &mut Proof,
    use_native: impl FnOnce(Native<'_>, &mut Proof) -> Result<T, Error>,
) -> Result<T, Error> {
    let session = selected_scope.session;
    let run = selected_scope.run;
    let (receipt, event) = control(tx, session, id, proof).await?;
    let (target, _, original_id) = tool::targets(&receipt)?;
    let scope = crate::continuation_input::Scope {
        domain: crate::continuation_input::Domain::Tool,
        session: session.clone(),
        run: run.clone(),
        continuation: target.clone(),
    };
    if !crate::sqlite::continuation_input::protocol(tx, &scope, proof)
        .await
        .map_err(unknown)?
    {
        return Err(Error::Unsupported);
    }
    let batch = crate::sqlite::tool_continuation::original_plan::batch_with_proof(
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
    crate::sqlite::continuation_input::answers(tx, &scope, &answers, proof)
        .await
        .map_err(unknown)?;
    let (suspension, suspension_event) = control(tx, session, &batch.id, proof).await?;
    let original = if let Some(id) = original_id {
        Some(original_claim(tx, selected_scope, id, proof).await?)
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
        answers: &answers,
        original: original.as_ref().map(|o| tool::Claim {
            previous: &o.previous,
            next: &o.next,
            audits: &o.audits,
            receipt: &o.receipt,
            event: &o.event,
        }),
    };
    let owned = super::approval::owned_input(tx, session, run, proof).await?;
    use_native(
        Native::Tool {
            evidence,
            input: owned.as_input()?,
        },
        proof,
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
    tx: &mut Transaction<'_, Sqlite>,
    scope: super::scope::Scope<'_>,
    id: &ControlId,
    proof: &mut Proof,
) -> Result<Original, Error> {
    let pair = super::scope::pair(
        tx,
        scope,
        PreparationKey::ToolContinuation(id.clone()),
        proof,
    )
    .await?;
    let (receipt, event) = control(tx, scope.session, id, proof).await?;
    Ok(Original {
        previous: pair.previous,
        next: pair.next,
        audits: pair.audits,
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
    tx: &mut Transaction<'_, Sqlite>,
    scope: &crate::continuation_input::Scope,
    proof: &mut Proof,
) -> Result<Projection, Error> {
    let generation = i64::try_from(scope.continuation.generation.get()).map_err(unknown)?;
    let cost = format!(
        "WITH selected AS ({PROJECTION}) SELECT CAST(length(CAST(data AS BLOB)) AS BIGINT) FROM selected"
    );
    crate::sqlite::read_proof::charge(
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
        "WITH selected AS ({PROJECTION}) SELECT CASE WHEN length(CAST(data AS BLOB))<=4194304 THEN data END FROM selected"
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
    tx: &mut Transaction<'_, Sqlite>,
    session: &SessionId,
    id: &ControlId,
    proof: &mut Proof,
) -> Result<(crate::ToolContinuationReceipt, crabber_core::EventRecord), Error> {
    let (receipt, event) =
        crate::sqlite::tool_continuation::read::receipt_with_proof(tx, session, id, proof)
            .await
            .map_err(unknown)?
            .ok_or(Error::UnknownStoreOutcome)?;
    if receipt.operation_id != *id || receipt.session_id != *session {
        return Err(Error::SemanticConflict);
    }
    Ok((receipt, event))
}
const PROJECTION: &str = "SELECT json_object('version',format_version,'session',json_extract(data_json,'$.session_id'),'run',json_extract(data_json,'$.run_id'),'claim',json_extract(data_json,'$.claim'),'frontend',json_extract(data_json,'$.frontend_claim'),'answers',json((SELECT json_group_array(json_extract(q.value,'$.answer')) FROM json_each(data_json,'$.questions') q WHERE json_type(q.value,'$.answer')<>'null'))) AS data FROM tool_continuations WHERE session_id=$1 AND run_id=$2 AND id=$3 AND generation=$4";

pub(in crate::sqlite::session_owner::drivers::successor) async fn verify_scope(
    tx: &mut Transaction<'_, Sqlite>,
    scope: super::scope::Scope<'_>,
    id: &ControlId,
    binding: &crate::ExecutionBindingCapsule,
    proof: &mut Proof,
) -> Result<source::ContinuationFacts, Error> {
    let pair = super::scope::pair(
        tx,
        scope,
        PreparationKey::ToolContinuation(id.clone()),
        proof,
    )
    .await?;
    Box::pin(with_native(tx, scope, id, proof, |native, _proof| {
        if let Native::Tool { input, .. } = &native {
            input.verify(scope.session, scope.run, Some(binding))?;
        }
        let verified =
            source::AuthenticatedClaim::verify(&pair.previous, &pair.next, &pair.audits, native)?;
        verified.verify_binding(binding)?;
        let (continuation, attempt) = verified.tool.ok_or(Error::SemanticConflict)?;
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
