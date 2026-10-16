//! Memory reads use the same borrowed carrier context as SQL materialization.
use super::super::{MemoryStore, State, approval, queue, run_control, tool_continuation};
#[cfg(test)]
use crate::Store;
use crate::public_session::{
    budget::{self, Budget},
    v2::{
        projection,
        proof::{Failure, Proof, unknown},
    },
};
use crate::{
    ApprovalContinuationState, PublicApprovalBarrier, PublicContinuationBarrierV2,
    PublicControlHeaderV2, PublicHeaderOutcomeV2, PublicHeaderRequest, PublicHeaderRequestV2,
    PublicHeaderUsage, PublicSessionError, PublicSessionLimit, ToolContinuationState,
};
use crabber_core::SessionRevision;
impl MemoryStore {
    pub(in crate::memory) fn read_public_header_v2(
        &self,
        request: &PublicHeaderRequestV2,
    ) -> Result<PublicHeaderOutcomeV2, PublicSessionError> {
        crate::public_session::v2::proof::validate_request(request)?;
        let state = self.state.lock().unwrap();
        if !state.sessions.contains_key(&request.session_id) {
            return Err(PublicSessionError::NotFound);
        }
        let revision = state
            .public_revisions
            .get(&request.session_id)
            .copied()
            .unwrap_or(SessionRevision::INITIAL);
        if revision != request.expected_revision {
            return Ok(PublicHeaderOutcomeV2::Invalidated);
        }
        match read(&state, request, revision) {
            Ok(header) => Ok(PublicHeaderOutcomeV2::Header(Box::new(header))),
            Err(Failure::Limited(limit)) => Ok(PublicHeaderOutcomeV2::Limited(limit)),
            Err(Failure::Error(error)) => Err(error),
        }
    }
}
fn read(
    state: &State,
    request: &PublicHeaderRequestV2,
    revision: SessionRevision,
) -> Result<PublicControlHeaderV2, Failure> {
    let mut safe = Budget::new(request.limits.header_bytes, PublicSessionLimit::HeaderBytes);
    let mut proof = Proof::bounded(request.limits.proof_bytes);
    let legacy = PublicHeaderRequest {
        session_id: request.session_id.clone(),
        run_id: request.run_id.clone(),
        expected_revision: revision,
        max_bytes: request.limits.header_bytes,
        max_proof_bytes: request.limits.proof_bytes,
        max_interactions: request.limits.approval_interactions,
    };
    let (base, status) = super::common(state, &legacy, revision, &mut safe)?;
    let (approval, approval_barrier) = approval_section(
        state,
        request,
        status,
        base.attempt.as_ref(),
        &mut safe,
        &mut proof,
    )?;
    let (tool, tool_barrier) = tool_section(
        state,
        request,
        status,
        base.attempt.as_ref(),
        &mut safe,
        &mut proof,
    )?;
    if approval_barrier.is_some() && tool_barrier.is_some() {
        return Err(unknown("two barriers"));
    }
    let barrier = approval_barrier.or(tool_barrier);
    safe.measure(&barrier)?;
    let queue = queue::public_pending(
        state,
        &request.session_id,
        request.limits.queue_entries,
        &mut safe,
        &mut proof,
    )?;
    let owners = state.session_owners.public_registered(
        &request.session_id,
        request.limits.registered_owners,
        &mut safe,
        &mut proof,
    )?;
    let controls = if let Some(run) = &request.run_id {
        run_control::public_controls(
            state,
            state.runs.get(run).ok_or_else(|| unknown("control run"))?,
            request.limits.run_controls,
            &mut safe,
            &mut proof,
        )?
    } else {
        Vec::new()
    };
    projection::finish(
        PublicControlHeaderV2 {
            version: 2,
            revision,
            session: base.session,
            attempt: base.attempt,
            approval,
            tool,
            barrier,
            queue,
            owners,
            controls,
            contents: base.contents,
            read_usage: PublicHeaderUsage::default(),
        },
        safe,
        &proof,
    )
}

fn approval_section(
    state: &State,
    request: &PublicHeaderRequestV2,
    status: Option<crabber_core::RunStatus>,
    attempt: Option<&crate::AttemptSnapshot>,
    safe: &mut Budget,
    proof: &mut Proof,
) -> Result<
    (
        Option<crate::PublicApprovalSummary>,
        Option<PublicContinuationBarrierV2>,
    ),
    Failure,
> {
    approval::current::session_with_proof(state, &request.session_id, proof)
        .map_err(crate::public_session::v2::proof::ReadError::public)?;
    let approval = request.run_id.as_ref().and_then(|run| {
        state
            .approvals
            .values()
            .filter(|r| r.session_id == request.session_id && r.run_id == *run)
            .max_by_key(|r| r.batch.continuation.generation)
    });
    let (approval, approval_barrier) = if let Some(record) = approval {
        let active = record.state != ApprovalContinuationState::Released;
        budget::validate_approval_state(
            record,
            active,
            status.ok_or_else(|| unknown("approval run"))?,
            attempt,
        )?;
        (
            Some(budget::approval(
                record,
                request.limits.approval_interactions,
                safe,
            )?),
            active.then(|| {
                PublicContinuationBarrierV2::Approval(PublicApprovalBarrier {
                    run_id: record.run_id.clone(),
                    continuation: record.batch.continuation.clone(),
                })
            }),
        )
    } else {
        (None, None)
    };
    Ok((approval, approval_barrier))
}
fn tool_section(
    state: &State,
    request: &PublicHeaderRequestV2,
    status: Option<crabber_core::RunStatus>,
    attempt: Option<&crate::AttemptSnapshot>,
    safe: &mut Budget,
    proof: &mut Proof,
) -> Result<
    (
        Option<crate::PublicToolSummary>,
        Option<PublicContinuationBarrierV2>,
    ),
    Failure,
> {
    let mut count = 0_usize;
    let mut latest = None;
    let mut active = None;
    for (id, borrowed) in state
        .tool_continuations
        .iter()
        .filter(|(_, r)| r.session_id == request.session_id)
    {
        if *id != borrowed.batch.continuation.id {
            return Err(unknown("tool index"));
        }
        let record = tool_continuation::checked_record_with_proof(state, id, proof)
            .map_err(crate::public_session::v2::proof::ReadError::public)?;
        if record.state != ToolContinuationState::Released {
            if active.is_some() || Some(&record.run_id) != request.run_id.as_ref() {
                return Err(unknown("active tool scope"));
            }
            active = Some(record.batch.continuation.clone());
        }
        if Some(&record.run_id) == request.run_id.as_ref()
            && latest
                .as_ref()
                .is_none_or(|r: &crate::tool_continuation::ToolContinuationRecord| {
                    r.batch.continuation.generation < record.batch.continuation.generation
                })
        {
            latest = Some(record);
        }
        count = count.checked_add(1).ok_or_else(|| unknown("tool count"))?;
    }
    let origins = state
        .events
        .iter()
        .filter(|e| e.session_id == request.session_id && tool_continuation::scan::origin(e))
        .count();
    if origins != count
        || state.tool_barriers.get(&request.session_id) != active.as_ref().map(|t| &t.id)
    {
        return Err(unknown("tool membership"));
    }
    let tool = if let Some(record) = latest {
        projection::validate_tool_state(
            &record,
            active.as_ref() == Some(&record.batch.continuation),
            status.ok_or_else(|| unknown("tool run"))?,
            attempt,
        )?;
        Some(projection::tool(
            &record,
            request.limits.tool_requirements,
            safe,
        )?)
    } else {
        None
    };
    let tool_barrier = active.map(|continuation| PublicContinuationBarrierV2::Tool {
        run_id: request.run_id.clone().expect("validated active run"),
        continuation,
    });
    Ok((tool, tool_barrier))
}

#[cfg(test)]
mod tests {
    use crate::*;
    fn limits() -> PublicSessionLimitsV2 {
        PublicSessionLimitsV2 {
            header_bytes: 2 * 1024 * 1024,
            proof_bytes: 32 * 1024 * 1024,
            approval_interactions: 64,
            tool_requirements: 64,
            queue_entries: 64,
            registered_owners: 1024,
            run_controls: 64,
            history: SnapshotLimits {
                messages: 64,
                tool_calls: 64,
                parts: 128,
                text_bytes: 65536,
                encoded_bytes: 131_072,
            },
        }
    }
    #[tokio::test]
    async fn aggregate_v2_frontend_header_is_complete_and_safe() {
        let store = crate::MemoryStore::default();
        let (f, batch, _) = crate::frontend_contract::fixture::paused(&store).await;
        let request = PublicHeaderRequestV2 {
            session_id: f.request.session_id.clone(),
            run_id: Some(f.request.run_id.clone()),
            expected_revision: store.session_revision(&f.request.session_id).await.unwrap(),
            limits: limits(),
        };
        let PublicHeaderOutcomeV2::Header(header) = store
            .public_control_header_v2(request.clone())
            .await
            .unwrap()
        else {
            panic!("header")
        };
        assert_eq!(header.version, 2);
        assert!(matches!(
            header.barrier,
            Some(PublicContinuationBarrierV2::Tool { .. })
        ));
        let tool = header.tool.as_ref().unwrap();
        assert_eq!(tool.continuation, batch.continuation);
        assert_eq!(tool.frontend.len(), 1);
        assert_eq!(tool.frontend[0].name, "frontend");
        assert!(!tool.frontend[0].consumed);
        assert!(header.queue.is_empty() && header.owners.is_empty() && header.controls.is_empty());
        assert!(header.read_usage.proof_bytes > 8192);
        let wire = serde_json::to_string(&header).unwrap();
        for private in [
            "External only",
            "result_contract",
            "\"descriptor\"",
            "checkpoint",
            "resolver",
            "lease",
            "authority",
            "location",
            "arguments",
        ] {
            assert!(!wire.contains(private), "{private}");
        }
        let mut limited = request.clone();
        limited.limits.tool_requirements = 0;
        assert!(matches!(
            store.public_control_header_v2(limited).await.unwrap(),
            PublicHeaderOutcomeV2::Limited(PublicSessionLimitV2::ToolRequirements)
        ));
        let mut short = request.clone();
        short.limits.proof_bytes = header.read_usage.proof_bytes - 1;
        assert!(matches!(
            store.public_control_header_v2(short).await.unwrap(),
            PublicHeaderOutcomeV2::Limited(PublicSessionLimitV2::ProofBytes)
        ));
        let mut exact = request;
        exact.limits.proof_bytes = header.read_usage.proof_bytes;
        assert!(matches!(
            store.public_control_header_v2(exact).await.unwrap(),
            PublicHeaderOutcomeV2::Header(_)
        ));
    }
}

#[cfg(test)]
#[tokio::test]
async fn aggregate_v2_real_mixed_metadata_and_paging() {
    let store = crate::MemoryStore::default();
    crate::public_session::v2::tests::mixed(&store).await;
}
#[cfg(test)]
#[tokio::test]
async fn aggregate_v2_real_approval_and_native() {
    let clock = std::sync::Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ));
    let store = crate::MemoryStore::with_clock(clock.clone());
    crate::public_session::v2::tests::approvals_and_native(&store, &clock).await;
}

#[cfg(test)]
#[tokio::test]
async fn aggregate_v2_constituent_races_and_faults() {
    let store = crate::MemoryStore::default();
    crate::public_session::races::run_v2(&store).await;
}

#[cfg(test)]
#[tokio::test]
async fn aggregate_v2_large_queue_private_input_stays_on_server() {
    let store = crate::MemoryStore::default();
    crate::public_session::v2::tests::large_queue_input_is_not_materialized(&store).await;
}

#[cfg(test)]
#[test]
fn aggregate_v2_private_tool_proof_is_rejected_before_clone() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let store = crate::MemoryStore::default();
    let (f, batch, _) = runtime.block_on(crate::frontend_contract::fixture::paused(&store));
    store
        .state
        .lock()
        .unwrap()
        .tool_continuations
        .get_mut(&batch.continuation.id)
        .unwrap()
        .batch
        .frontend[0]
        .descriptor
        .description = "private proof".repeat(200_000);
    let mut request = runtime.block_on(crate::public_session::v2::tests::request(
        &store,
        &f.request.session_id,
        Some(&f.request.run_id),
    ));
    request.limits.proof_bytes = 1;
    let measured = allocation_counter::measure(|| {
        assert!(matches!(
            runtime
                .block_on(store.public_control_header_v2(request))
                .unwrap(),
            crate::PublicHeaderOutcomeV2::Limited(crate::PublicSessionLimitV2::ProofBytes)
        ));
    });
    assert!(
        measured.bytes_max < 131_072,
        "private proof cloned before budget gate: {measured:?}"
    );
}
