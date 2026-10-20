//! Exact noninitial native claims own distinct Reserve/Begin identities.
#[path = "successor/approval.rs"]
mod approval;
pub(super) mod fresh;
#[path = "successor/inspection.rs"]
mod inspection;
#[path = "successor/pending_approval.rs"]
mod pending_approval;
mod scoped;
mod source;
#[path = "successor/tool.rs"]
mod tool;
#[path = "successor/tool_inspection.rs"]
mod tool_inspection;
use super::super::{
    Audit, Error, Record, SessionOwnerOperation, before, fault, finish, publish_revision, reads,
    unknown,
};
use super::proof;
use crate::postgres::PostgresStore;
use crate::postgres::transaction::Transaction;
use crate::public_session::v2::proof::Proof;
use crate::session_owner::driver::{
    ExecutionDriverReceipt, RegisterSuccessorExecutionDriver, SuccessorDriverLink,
    SuccessorExecutionDriverGrant, SuccessorExecutionDriverOutcome,
    SuccessorExecutionDriverReceipt,
};
use sqlx::Postgres;
impl PostgresStore {
    pub(in crate::postgres) async fn register_successor_driver(
        &self,
        request: RegisterSuccessorExecutionDriver,
    ) -> Result<SuccessorExecutionDriverOutcome, Error> {
        request.fingerprint()?;
        let mut tx = self
            .session_transaction(request.context.session_id())
            .await
            .map_err(unknown)?;
        let result = Box::pin(async {
            crate::postgres::deletion::ensure_live(&mut tx, request.context.session_id())
                .await
                .map_err(crate::deletion::owner_error)?;
            let mut budget = Proof::bounded(crate::deletion::coverage::MAX_PROOF_BYTES);
            if let Some(receipt) = Box::pin(metadata(&mut tx, &request, &mut budget)).await? {
                return Ok(SuccessorExecutionDriverOutcome::Replayed(receipt));
            }
            let link = Box::pin(fresh::fresh(
                &mut tx,
                &request,
                None,
                self.clock.now(),
                &mut budget,
            ))
            .await?;
            let result = publish(&mut tx, &request, link, self.clock.now(), &mut budget).await?;
            before(self)?;
            #[cfg(test)]
            crate::session_owner_contract::process::pause("successor-before");
            Ok(result)
        })
        .await;
        let result = finish(tx, result).await?;
        #[cfg(test)]
        if matches!(result, SuccessorExecutionDriverOutcome::Applied { .. }) {
            crate::session_owner_contract::process::pause("successor-after");
        }
        if matches!(result, SuccessorExecutionDriverOutcome::Applied { .. }) && fault(self) == 2 {
            Err(Error::UnknownStoreOutcome)
        } else {
            Ok(result)
        }
    }
}
async fn metadata(
    tx: &mut Transaction<'_, Postgres>,
    request: &RegisterSuccessorExecutionDriver,
    budget: &mut Proof,
) -> Result<Option<SuccessorExecutionDriverReceipt>, Error> {
    let ids = request.identities()?;
    let session = request.context.session_id();
    let Some(reserve) = proof::audit(tx, session, &ids.reserve, budget).await? else {
        return Ok(None);
    };
    let actual = reserve
        .record
        .successor_driver
        .as_deref()
        .ok_or(Error::SemanticConflict)?;
    actual.matches_request(request)?;
    let begin = proof::audit(tx, session, &ids.begin, budget)
        .await?
        .ok_or(Error::UnknownStoreOutcome)?;
    crate::session_owner::audit::verify_current(&begin.record, &reserve, &begin)?;
    let original = Box::pin(source::original(tx, request, budget, None)).await?;
    if actual != &original.link {
        return Err(Error::SemanticConflict);
    }
    let worker = ExecutionDriverReceipt {
        session_id: session.clone(),
        run_id: request.fence.run_id.clone(),
        request_digest: request.fingerprint()?,
        reserve: reserve.receipt,
        begin: begin.receipt,
    };
    crate::session_owner::owned_scope::OwnedScopeSource::from_receipts(
        &worker.reserve,
        &worker.begin,
    )?;
    Ok(Some(SuccessorExecutionDriverReceipt {
        claim: request.claim.clone(),
        worker,
    }))
}
async fn publish(
    tx: &mut Transaction<'_, Postgres>,
    request: &RegisterSuccessorExecutionDriver,
    link: SuccessorDriverLink,
    now: time::OffsetDateTime,
    budget: &mut Proof,
) -> Result<SuccessorExecutionDriverOutcome, Error> {
    let reservation = link.reservation(request)?;
    let session = request.context.session_id();
    budget.charge(9).map_err(unknown)?;
    reads::capacity(tx, session).await?;
    let exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM session_owner_current WHERE session_id=$1 AND owner_id=$2)",
    )
    .bind(&session.0)
    .bind(&reservation.binding.id.0)
    .fetch_one(&mut **tx)
    .await
    .map_err(unknown)?;
    if exists {
        return Err(Error::SemanticConflict);
    }
    let mut record = Record::reserve(&reservation, None, now)?;
    record.successor_driver = Some(Box::new(link.clone()));
    let authority = record.grant()?;
    let reserve = record.receipt(
        reservation.id.clone(),
        link.fingerprint(&reservation)?,
        SessionOwnerOperation::Reserve,
    );
    let publication = publish_revision(tx, session, true).await?;
    reads::publish_with_proof(
        tx,
        &Audit {
            version: 2,
            receipt: reserve.clone(),
            record: record.clone(),
            publication: Some(publication),
        },
        true,
        budget,
    )
    .await?;
    let command = crate::session_owner::MutateSessionOwner {
        id: request.identities()?.begin,
        session_id: session.clone(),
        target: authority.target().clone(),
        command: crate::session_owner::SessionOwnerCommand::Begin,
    };
    record.mutate(&command, Some(&authority), now)?;
    let begin = record.receipt(
        command.id.clone(),
        command.fingerprint()?,
        SessionOwnerOperation::Begin,
    );
    let publication = publish_revision(tx, session, true).await?;
    reads::publish_with_proof(
        tx,
        &Audit {
            version: 2,
            receipt: begin.clone(),
            record,
            publication: Some(publication),
        },
        false,
        budget,
    )
    .await?;
    let receipt = SuccessorExecutionDriverReceipt {
        claim: request.claim.clone(),
        worker: ExecutionDriverReceipt {
            session_id: session.clone(),
            run_id: request.fence.run_id.clone(),
            request_digest: request.fingerprint()?,
            reserve,
            begin,
        },
    };
    let grant = SuccessorExecutionDriverGrant::from_fresh_begin(
        receipt.clone(),
        authority,
        request.clone(),
        link.identity()?,
    )?;
    Ok(SuccessorExecutionDriverOutcome::Applied {
        receipt,
        grant: Box::new(grant),
    })
}

#[cfg(test)]
#[path = "successor/approval_tests.rs"]
pub(super) mod approval_tests;

#[cfg(test)]
#[path = "successor/tool_source_tests.rs"]
mod tool_source_tests;

#[cfg(test)]
#[path = "successor/tool_guards_tests.rs"]
mod tool_guards_tests;

#[cfg(test)]
#[path = "successor/tool_fault_tests.rs"]
mod tool_fault_tests;

#[path = "successor/pending_tool.rs"]
mod pending_tool;
