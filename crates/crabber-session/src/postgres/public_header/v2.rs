//! Coherent fixed-profile metadata; every private reader shares one proof context.
use super::super::{PostgresStore, approval, queue, run_control, session_owner, tool_continuation};
use crate::public_session::budget::{self, Budget};
use crate::public_session::v2::{
    projection,
    proof::{Failure, Proof, unknown},
};
use crate::{
    ApprovalContinuationState, AttemptSnapshot, PublicApprovalBarrier, PublicApprovalSummary,
    PublicContinuationBarrierV2, PublicControlHeaderV2, PublicHeaderOutcomeV2, PublicHeaderRequest,
    PublicHeaderRequestV2, PublicHeaderUsage, PublicSessionError, PublicSessionLimit,
};
use crabber_core::{RunStatus, SessionRevision};
use sqlx::{Postgres, Row, Transaction};
impl PostgresStore {
    pub(in crate::postgres) async fn read_public_header_v2(
        &self,
        request: PublicHeaderRequestV2,
    ) -> Result<PublicHeaderOutcomeV2, PublicSessionError> {
        request.validate()?;
        let result=async {
            let mut tx=self.pool.begin().await.map_err(unknown)?; sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY").execute(&mut *tx).await.map_err(unknown)?;
            let row=sqlx::query("SELECT s.public_revision,CAST(octet_length(s.data::text)+COALESCE(octet_length(m.data_json),0)+COALESCE(octet_length(m.workspace_id),0) AS BIGINT) AS bytes FROM sessions s LEFT JOIN session_management m ON m.session_id=s.id WHERE s.id=$1").bind(&request.session_id.0).fetch_optional(&mut *tx).await.map_err(unknown)?.ok_or(Failure::Error(PublicSessionError::NotFound))?;
            let revision=SessionRevision::new(u64::try_from(row.try_get::<i64,_>("public_revision").map_err(unknown)?).map_err(unknown)?).map_err(unknown)?;
            if revision!=request.expected_revision {tx.commit().await.map_err(unknown)?;return Ok(None)}
            let mut safe=Budget::new(request.limits.header_bytes,PublicSessionLimit::HeaderBytes);
            safe.charge(usize::try_from(row.try_get::<i64,_>("bytes").map_err(unknown)?).map_err(unknown)?)?;
            let mut proof=Proof::bounded(request.limits.proof_bytes);
            let header=read(&mut tx,&request,revision,&mut safe,&mut proof).await?;
            let header=projection::finish(header,safe,&proof)?;
            tx.commit().await.map_err(unknown)?;Ok(Some(header))
        }.await;
        match result {
            Ok(Some(header)) => Ok(PublicHeaderOutcomeV2::Header(Box::new(header))),
            Ok(None) => Ok(PublicHeaderOutcomeV2::Invalidated),
            Err(Failure::Limited(limit)) => Ok(PublicHeaderOutcomeV2::Limited(limit)),
            Err(Failure::Error(error)) => Err(error),
        }
    }
}
async fn read(
    tx: &mut Transaction<'_, Postgres>,
    request: &PublicHeaderRequestV2,
    revision: SessionRevision,
    safe: &mut Budget,
    proof: &mut Proof,
) -> Result<PublicControlHeaderV2, Failure> {
    let legacy = PublicHeaderRequest {
        session_id: request.session_id.clone(),
        run_id: request.run_id.clone(),
        expected_revision: revision,
        max_bytes: request.limits.header_bytes,
        max_proof_bytes: request.limits.proof_bytes,
        max_interactions: request.limits.approval_interactions,
    };
    let (base, status) = super::common(tx, &legacy, revision, safe).await?;
    if let Some(run) = &request.run_id {
        let facts = super::super::run_facts::read(tx, &request.session_id, run, proof)
            .await
            .map_err(crate::public_session::v2::proof::ReadError::public)?;
        if Some(facts.status) != status {
            return Err(unknown("current run status"));
        }
    }
    let approval = approval(tx, request, status, base.attempt.as_ref(), safe, proof).await?;
    let (tool, tool_barrier) =
        tools(tx, request, status, base.attempt.as_ref(), safe, proof).await?;
    if approval.1.is_some() && tool_barrier.is_some() {
        return Err(unknown("two active barriers"));
    }
    let barrier = approval
        .1
        .map(PublicContinuationBarrierV2::Approval)
        .or(tool_barrier);
    safe.measure(&barrier)?;
    let queue = queue::public::pending(
        tx,
        &request.session_id,
        request.limits.queue_entries,
        safe,
        proof,
    )
    .await?;
    let owners = session_owner::public::registered(
        tx,
        &request.session_id,
        request.limits.registered_owners,
        safe,
        proof,
    )
    .await?;
    let controls = if let Some(run) = &request.run_id {
        run_control::public::controls(
            tx,
            &request.session_id,
            run,
            status.ok_or_else(|| unknown("control run"))?,
            request.limits.run_controls,
            safe,
            proof,
        )
        .await?
    } else {
        Vec::new()
    };
    Ok(PublicControlHeaderV2 {
        version: 2,
        revision,
        session: base.session,
        attempt: base.attempt,
        approval: approval.0,
        tool,
        barrier,
        queue,
        owners,
        controls,
        contents: base.contents,
        read_usage: PublicHeaderUsage::default(),
    })
}
async fn tools(
    tx: &mut Transaction<'_, Postgres>,
    request: &PublicHeaderRequestV2,
    status: Option<RunStatus>,
    attempt: Option<&AttemptSnapshot>,
    safe: &mut Budget,
    proof: &mut Proof,
) -> Result<
    (
        Option<crate::PublicToolSummary>,
        Option<PublicContinuationBarrierV2>,
    ),
    Failure,
> {
    let records = tool_continuation::scan::scan_with_proof(tx, &request.session_id, None, proof)
        .await
        .map_err(crate::public_session::v2::proof::ReadError::public)?;
    if records
        .active
        .as_ref()
        .is_some_and(|r| Some(&r.run_id) != request.run_id.as_ref())
    {
        return Err(unknown("nonlatest active tool"));
    }
    let tool_record = if let Some(run) = &request.run_id {
        tool_continuation::read::load_with_proof(tx, &request.session_id, run, None, proof)
            .await
            .map_err(crate::public_session::v2::proof::ReadError::public)?
    } else {
        None
    };
    let tool = if let Some(record) = &tool_record {
        projection::validate_tool_state(
            record,
            records
                .active
                .as_ref()
                .is_some_and(|r| r.batch.continuation == record.batch.continuation),
            status.ok_or_else(|| unknown("tool run"))?,
            attempt,
        )?;
        Some(projection::tool(
            record,
            request.limits.tool_requirements,
            safe,
        )?)
    } else {
        None
    };
    let tool_barrier = records
        .active
        .as_ref()
        .map(|r| PublicContinuationBarrierV2::Tool {
            run_id: r.run_id.clone(),
            continuation: r.batch.continuation.clone(),
        });
    Ok((tool, tool_barrier))
}
async fn approval(
    tx: &mut Transaction<'_, Postgres>,
    request: &PublicHeaderRequestV2,
    status: Option<RunStatus>,
    attempt: Option<&AttemptSnapshot>,
    safe: &mut Budget,
    proof: &mut Proof,
) -> Result<(Option<PublicApprovalSummary>, Option<PublicApprovalBarrier>), Failure> {
    // Session membership is independent of the mutable barrier/current state.
    approval::current::session_with_proof(tx, &request.session_id, proof)
        .await
        .map_err(crate::public_session::v2::proof::ReadError::public)?;
    let Some(run) = &request.run_id else {
        return Ok((None, None));
    };
    let Some((record, _)) = approval::load_with_proof(tx, &request.session_id, run, proof)
        .await
        .map_err(crate::public_session::v2::proof::ReadError::public)?
    else {
        return Ok((None, None));
    };
    let active = record.state != ApprovalContinuationState::Released;
    budget::validate_approval_state(
        &record,
        active,
        status.ok_or_else(|| unknown("approval run"))?,
        attempt,
    )?;
    let summary = budget::approval(&record, request.limits.approval_interactions, safe)?;
    let barrier = active.then(|| PublicApprovalBarrier {
        run_id: run.clone(),
        continuation: record.batch.continuation.clone(),
    });
    Ok((Some(summary), barrier))
}

#[cfg(test)]
mod tests;

#[cfg(test)]
#[tokio::test]
async fn aggregate_v2_real_mixed_metadata_and_paging() {
    let Some(url) = crate::postgres::tests::test_url() else {
        return;
    };
    let _guard = crate::postgres::tests::TEST_LOCK.lock().await;
    crate::PostgresStore::migrate(&url).await.unwrap();
    let store = crate::PostgresStore::connect(&url).await.unwrap();
    crate::public_session::v2::tests::mixed(&store).await;
}
#[cfg(test)]
#[tokio::test]
async fn aggregate_v2_real_approval_and_native() {
    let Some(url) = crate::postgres::tests::test_url() else {
        return;
    };
    let _guard = crate::postgres::tests::TEST_LOCK.lock().await;
    crate::PostgresStore::migrate(&url).await.unwrap();
    let store = crate::PostgresStore::connect(&url).await.unwrap();
    let clock = std::sync::Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ));
    let store = store.with_clock(clock.clone());
    crate::public_session::v2::tests::approvals_and_native(&store, &clock).await;
}

#[cfg(test)]
#[tokio::test]
async fn aggregate_v2_constituent_races_and_faults() {
    let Some(url) = crate::postgres::tests::test_url() else {
        return;
    };
    let _guard = crate::postgres::tests::TEST_LOCK.lock().await;
    crate::PostgresStore::migrate(&url).await.unwrap();
    let store = crate::PostgresStore::connect(&url).await.unwrap();
    crate::public_session::races::run_v2(&store).await;
}

#[cfg(test)]
#[tokio::test]
async fn aggregate_v2_large_queue_private_input_stays_on_server() {
    let Some(url) = crate::postgres::tests::test_url() else {
        return;
    };
    let _guard = crate::postgres::tests::TEST_LOCK.lock().await;
    crate::PostgresStore::migrate(&url).await.unwrap();
    let store = crate::PostgresStore::connect(&url).await.unwrap();
    crate::public_session::v2::tests::large_queue_input_is_not_materialized(&store).await;
}
