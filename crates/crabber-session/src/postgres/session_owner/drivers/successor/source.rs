//! Original selected native closure. Every transferred carrier shares one budget.
#[path = "source/approval.rs"]
pub(super) mod approval;
#[path = "source/tool.rs"]
mod tool;
use super::super::proof;
use super::{Error, Proof, RegisterSuccessorExecutionDriver, unknown};
use crate::native_parent::{CompletionOperation, RunIssuance};
use crate::session_owner::{
    audit::Audit,
    driver::successor::source::{self, SelectedClaim, claim::Native},
    preparation::{ExpiredRunOrigin, expired::replay::Native as Expired},
};
use sqlx::{Postgres, Transaction};
pub(super) async fn original(
    tx: &mut Transaction<'_, Postgres>,
    request: &RegisterSuccessorExecutionDriver,
    budget: &mut Proof,
    binding: Option<&crate::ExecutionBindingCapsule>,
) -> Result<SelectedClaim, Error> {
    let session = request.context.session_id();
    let run = &request.fence.run_id;
    if !crate::postgres::native_parent::source_protocol::protocol(tx, session, run, budget)
        .await
        .map_err(unknown)?
    {
        return Err(Error::Unsupported);
    }
    let next = issuer(tx, request, request.claim.issuance, budget).await?;
    let previous = issuer(tx, request, request.claim.issuance - 1, budget).await?;
    if next
        .completion_source
        .as_ref()
        .is_none_or(|source| source.root_join_id != request.claim.root_join_id)
    {
        return Err(Error::SemanticConflict);
    }
    let join = proof::audit(tx, session, &request.claim.root_join_id, budget)
        .await?
        .ok_or(Error::UnknownStoreOutcome)?;
    let ids = source::claim::audit_ids(&join)?;
    let mut audits = Vec::with_capacity(5);
    for id in ids {
        audits.push(
            proof::audit(tx, session, &id, budget)
                .await?
                .ok_or(Error::UnknownStoreOutcome)?,
        );
    }
    match next
        .completion_source
        .as_ref()
        .ok_or(Error::Unsupported)?
        .native_operation
    {
        CompletionOperation::AdmissionRecovery => {
            let initial = issuer(tx, request, 1, budget).await?;
            let init = init(tx, request, budget).await?;
            let receipt = read(
                tx,
                &session.0,
                init.driver_key().as_str(),
                RECEIPT_COST,
                RECEIPT,
                budget,
            )
            .await?;
            source::selected(
                request,
                &previous,
                &next,
                &audits,
                Native::Admission {
                    init: &init,
                    initial: &initial,
                    receipt: &receipt,
                },
                binding,
            )
        }
        CompletionOperation::ExpiredRunResume => {
            expired(tx, request, &previous, &next, &audits, budget, binding).await
        }
        CompletionOperation::ApprovalClaim | CompletionOperation::ApprovalRecovery => {
            Box::pin(approval::original(
                tx, request, &previous, &next, &audits, budget, binding,
            ))
            .await
        }
        CompletionOperation::ToolClaim
        | CompletionOperation::ToolRecovery
        | CompletionOperation::FrontendClaim
        | CompletionOperation::FrontendRecovery => {
            Box::pin(tool::original(
                tx, request, &previous, &next, &audits, budget, binding,
            ))
            .await
        }
    }
}
async fn expired(
    tx: &mut Transaction<'_, Postgres>,
    request: &RegisterSuccessorExecutionDriver,
    previous: &RunIssuance,
    next: &RunIssuance,
    audits: &[Audit],
    budget: &mut Proof,
    binding: Option<&crate::ExecutionBindingCapsule>,
) -> Result<SelectedClaim, Error> {
    let session = request.context.session_id();
    let run = &request.fence.run_id;
    let target = crate::session_owner::preparation::expired::history::target(audits)?;
    let parent = if let ExpiredRunOrigin::InitialUnstarted { reservation, .. } = &target.origin {
        Some(attempt(tx, session, reservation, budget).await?)
    } else {
        None
    };
    let origin = if let ExpiredRunOrigin::InitialUnstarted { admission, .. } = &target.origin {
        crate::session_owner::preparation::expired::unstarted::admission(admission)
    } else {
        target.origin.clone()
    };
    let verify = |native| {
        source::selected(
            request,
            previous,
            next,
            audits,
            Native::Expired(if let Some(original) = &parent {
                Expired::Unstarted {
                    initial: Box::new(native),
                    original,
                }
            } else {
                native
            }),
            binding,
        )
    };
    match origin {
        ExpiredRunOrigin::InitialUnstarted { .. } => Err(Error::UnknownStoreOutcome),
        ExpiredRunOrigin::Suspended(id) => {
            let original = attempt(tx, session, &id, budget).await?;
            verify(Expired::Suspended(&original.receipt, &original.event))
        }
        ExpiredRunOrigin::UnkeyedAdmission(_) => {
            let selected = super::super::source::original(tx, session, run, budget).await?;
            if !selected.witness.is_unkeyed() {
                return Err(Error::SemanticConflict);
            }
            let original = crate::session_owner::preparation::expired::unkeyed::Original {
                session: selected.session,
                witness: selected.witness,
                initial: selected.initial,
                audits: selected.audits,
            };
            verify(Expired::Unkeyed(&original))
        }
        ExpiredRunOrigin::KeyedInitial(key) => {
            let initial = issuer(tx, request, 1, budget).await?;
            let init = init(tx, request, budget).await?;
            let begin_source: crate::admission_journal::Source =
                read(tx, &session.0, &run.0, BEGIN_COST, BEGIN, budget).await?;
            let crate::admission_journal::Source::Begin(begin) = begin_source else {
                return Err(Error::UnknownStoreOutcome);
            };
            let activated_issuer = issuer(tx, request, begin.issuance.sequence, budget).await?;
            let receipt = read(tx, &session.0, key.as_str(), RECEIPT_COST, RECEIPT, budget).await?;
            verify(Expired::Initial {
                init: &init,
                begin: &begin,
                receipt: &receipt,
                initial: &initial,
                begun: &activated_issuer,
            })
        }
    }
}
async fn init(
    tx: &mut Transaction<'_, Postgres>,
    request: &RegisterSuccessorExecutionDriver,
    budget: &mut Proof,
) -> Result<crate::admission_journal::Init, Error> {
    budget.charge(4).map_err(unknown)?;
    let protocol:Option<i32>=sqlx::query_scalar("SELECT CAST(source_protocol AS INTEGER) FROM admission_executions WHERE session_id=$1 AND run_id=$2").bind(&request.context.session_id().0).bind(&request.fence.run_id.0).fetch_optional(&mut **tx).await.map_err(unknown)?;
    if protocol != Some(1) {
        return Err(Error::UnknownStoreOutcome);
    }
    let native: crate::admission_journal::Source = read(
        tx,
        &request.context.session_id().0,
        &request.fence.run_id.0,
        INIT_COST,
        INIT,
        budget,
    )
    .await?;
    let crate::admission_journal::Source::Init(init) = native else {
        return Err(Error::UnknownStoreOutcome);
    };
    Ok(init)
}
async fn attempt(
    tx: &mut Transaction<'_, Postgres>,
    session: &crabber_core::SessionId,
    id: &crabber_core::ControlId,
    budget: &mut Proof,
) -> Result<crate::session_owner::preparation::expired::unstarted::Original, Error> {
    let receipt = crate::postgres::attempt::records::receipt_with_proof(tx, session, id, budget)
        .await
        .map_err(unknown)?
        .ok_or(Error::UnknownStoreOutcome)?;
    let seq = receipt.event_cursor.0.to_string();
    let value: serde_json::Value = read(tx, &session.0, &seq, EVENT_COST, EVENT, budget).await?;
    let mut event = crate::public_state::decode_event(sqlx::types::Json(value)).map_err(unknown)?;
    if event
        .cursor
        .is_some_and(|cursor| cursor != receipt.event_cursor)
    {
        return Err(Error::UnknownStoreOutcome);
    }
    event.cursor = Some(receipt.event_cursor);
    Ok(crate::session_owner::preparation::expired::unstarted::Original { receipt, event })
}
async fn issuer(
    tx: &mut Transaction<'_, Postgres>,
    request: &RegisterSuccessorExecutionDriver,
    sequence: u64,
    budget: &mut Proof,
) -> Result<RunIssuance, Error> {
    crate::postgres::native_parent::source_protocol::original(
        tx,
        request.context.session_id(),
        &request.fence.run_id,
        sequence,
        budget,
    )
    .await
    .map_err(unknown)?
    .ok_or(Error::UnknownStoreOutcome)
}
async fn read<T: serde::de::DeserializeOwned>(
    tx: &mut Transaction<'_, Postgres>,
    session: &str,
    id: &str,
    cost: &str,
    sql: &str,
    budget: &mut Proof,
) -> Result<T, Error> {
    proof::charge(tx, cost, session, id, budget).await?;
    let text: Option<String> = sqlx::query_scalar(sql)
        .bind(session)
        .bind(id)
        .fetch_optional(&mut **tx)
        .await
        .map_err(unknown)?
        .flatten();
    serde_json::from_str(&text.ok_or(Error::UnknownStoreOutcome)?).map_err(unknown)
}
const INIT_COST: &str = "SELECT CAST(octet_length(data_json) AS BIGINT) FROM admission_execution_sources WHERE session_id=$1 AND run_id=$2 AND kind='init'";
const INIT: &str = "SELECT CASE WHEN octet_length(data_json)<=8192 THEN data_json END FROM admission_execution_sources WHERE session_id=$1 AND run_id=$2 AND kind='init'";
const BEGIN_COST: &str = "SELECT CAST(octet_length(data_json) AS BIGINT) FROM admission_execution_sources WHERE session_id=$1 AND run_id=$2 AND kind='begin'";
const BEGIN: &str = "SELECT CASE WHEN octet_length(data_json)<=8192 THEN data_json END FROM admission_execution_sources WHERE session_id=$1 AND run_id=$2 AND kind='begin'";
const RECEIPT_COST: &str = "SELECT CAST(octet_length(data::text) AS BIGINT) FROM admission_receipts WHERE session_id=$1 AND admission_key=$2";
const RECEIPT: &str = "SELECT CASE WHEN octet_length(data::text)<=8192 THEN data::text END FROM admission_receipts WHERE session_id=$1 AND admission_key=$2";
const EVENT_COST: &str = "SELECT CAST(octet_length(data::text) AS BIGINT) FROM events WHERE session_id=$1 AND seq=CAST($2 AS BIGINT)";
const EVENT: &str = "SELECT CASE WHEN octet_length(data::text)<=65536 THEN data::text END FROM events WHERE session_id=$1 AND seq=CAST($2 AS BIGINT)";
