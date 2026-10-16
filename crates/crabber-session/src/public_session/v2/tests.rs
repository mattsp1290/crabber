//! Shared real-store aggregate journeys; no provider/tool dispatcher is involved.
use crate::*;
use crabber_core::{ControlId, InputEnvelope, RunStatus};
pub(crate) fn limits() -> PublicSessionLimitsV2 {
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
            parts: 1024,
            text_bytes: 1024 * 1024,
            encoded_bytes: 2 * 1024 * 1024,
        },
    }
}
pub(crate) async fn request(
    store: &impl Store,
    session: &crabber_core::SessionId,
    run: Option<&crabber_core::RunId>,
) -> PublicHeaderRequestV2 {
    PublicHeaderRequestV2 {
        session_id: session.clone(),
        run_id: run.cloned(),
        expected_revision: store.session_revision(session).await.unwrap(),
        limits: limits(),
    }
}
async fn header(store: &impl Store, request: PublicHeaderRequestV2) -> Box<PublicControlHeaderV2> {
    let PublicHeaderOutcomeV2::Header(header) =
        store.public_control_header_v2(request).await.unwrap()
    else {
        panic!("complete header")
    };
    header
}
pub(crate) async fn mixed(store: &impl Store) {
    let (f, _, _) = crate::frontend_contract::fixture::paused(store).await;
    let session = &f.request.session_id;
    let run = &f.request.run_id;
    let saved = store.get_session(session).await.unwrap().unwrap();
    let mut owner = crate::session_owner_contract::reservation(session.clone());
    owner.binding.workspace_id = saved.workspace_id.clone();
    owner.binding.directory = saved.directory.clone();
    store
        .reserve_session_owner(owner.clone(), None)
        .await
        .unwrap();
    for index in 0..2 {
        let mut queued = crate::queue_contract::entry(store, session.clone()).await;
        let QueueAction::Enqueue { input, .. } = &mut queued.enqueue.action else {
            unreachable!()
        };
        input.input = private_input(
            &format!("PRIVATE_QUEUE_INPUT_{index}"),
            queued.enqueue.expected_revision,
        );
        store.mutate_queue(queued.enqueue).await.unwrap();
    }
    let binding = store.run_control_binding(session, run).await.unwrap();
    store
        .control_run(RunControlRequest {
            id: ControlId::new(),
            target: binding.target,
            expected_revision: binding.revision,
            action: RunControlAction::Steer {
                input: InputEnvelope::text("PRIVATE_STEER_INPUT", binding.revision).unwrap(),
            },
        })
        .await
        .unwrap();
    let req = request(store, session, Some(run)).await;
    let h = header(store, req.clone()).await;
    assert_eq!(h.queue.len(), 2);
    assert!(h.queue[0].sequence < h.queue[1].sequence);
    assert_eq!(h.owners.len(), 1);
    assert_eq!(h.owners[0].target.id, owner.binding.id);
    assert_eq!(h.controls.len(), 1);
    assert_eq!(h.controls[0].status, RunControlStatus::Accepted);
    assert_eq!(h.tool.as_ref().unwrap().frontend.len(), 1);
    let json = serde_json::to_string(&h).unwrap();
    for private in [
        "PRIVATE_QUEUE_INPUT",
        "PRIVATE_STEER_INPUT",
        "owner\"",
        "lease_until",
        "authority_claim",
        "arguments",
        "checkpoint",
    ] {
        assert!(!json.contains(private), "{private}");
    }
    for (domain, limit) in [
        (0, PublicSessionLimitV2::QueueEntries),
        (1, PublicSessionLimitV2::RegisteredOwners),
        (2, PublicSessionLimitV2::RunControls),
    ] {
        let mut bounded = req.clone();
        match domain {
            0 => bounded.limits.queue_entries = 1,
            1 => bounded.limits.registered_owners = 0,
            _ => bounded.limits.run_controls = 0,
        }
        assert!(
            matches!(store.public_control_header_v2(bounded).await.unwrap(),PublicHeaderOutcomeV2::Limited(actual) if actual==limit)
        );
    }
    let mut bounded = req.clone();
    bounded.limits.proof_bytes = h.read_usage.proof_bytes - 1;
    assert!(matches!(
        store.public_control_header_v2(bounded).await.unwrap(),
        PublicHeaderOutcomeV2::Limited(PublicSessionLimitV2::ProofBytes)
    ));
    let mut exact = req;
    exact.limits.proof_bytes = h.read_usage.proof_bytes;
    header(store, exact).await;
    paging(store, session, h.revision).await;
    let revision = store.session_revision(session).await.unwrap();
    assert_eq!(revision, h.revision);
    let PublicRunOutcome::State(run_state) = store.public_run_state(session, 65536).await.unwrap()
    else {
        panic!("run")
    };
    assert_eq!(run_state.run.unwrap().status, RunStatus::Paused);
}
pub(crate) async fn approvals_and_native(store: &impl Store, clock: &crabber_core::ManualClock) {
    let f = crate::approval_contract::fixture::staged(store, clock).await;
    f.execution
        .suspend_for_approval(f.batch.clone(), f.owner.clone())
        .await
        .unwrap();
    let req = request(store, &f.run.session_id, Some(&f.run.id)).await;
    let h = header(store, req.clone()).await;
    assert!(matches!(
        h.barrier,
        Some(PublicContinuationBarrierV2::Approval(_))
    ));
    assert!(h.tool.is_none());
    assert_eq!(h.approval.as_ref().unwrap().interactions.len(), 2);
    let mut low = req;
    low.limits.approval_interactions = 0;
    assert!(matches!(
        store.public_control_header_v2(low).await.unwrap(),
        PublicHeaderOutcomeV2::Limited(PublicSessionLimitV2::ApprovalInteractions)
    ));
    let f = crate::tool_continuation_contract::fixture::fixture(store).await;
    f.execution
        .suspend_tool_continuation(f.suspension.clone(), f.authority.clone())
        .await
        .unwrap();
    let req = request(store, &f.admitted.session.id, Some(&f.admitted.run.id)).await;
    let h = header(store, req.clone()).await;
    assert!(matches!(
        h.barrier,
        Some(PublicContinuationBarrierV2::Tool { .. })
    ));
    assert_eq!(h.tool.unwrap().requirements.len(), 2);
    let mut low = req;
    low.limits.tool_requirements = 1;
    assert!(matches!(
        store.public_control_header_v2(low).await.unwrap(),
        PublicHeaderOutcomeV2::Limited(PublicSessionLimitV2::ToolRequirements)
    ));
    terminated_native(
        store,
        &f.admitted.session.id,
        &f.admitted.run.id,
        &f.suspension.attempt,
    )
    .await;
}

fn private_input(text: &str, revision: crabber_core::SessionRevision) -> InputEnvelope {
    let mut wire = serde_json::to_value(InputEnvelope::text(text, revision).unwrap()).unwrap();
    wire["context"] = serde_json::json!([{"description":"private context","value":{"nul\u{0}":"PRIVATE_CONTEXT\u{0}"}}]);
    serde_json::from_value(wire).unwrap()
}

async fn paging(
    store: &impl Store,
    session: &crabber_core::SessionId,
    revision: crabber_core::SessionRevision,
) {
    let mut page_limits = limits();
    page_limits.history.messages = 1;
    let mut poll = PublicSessionSubscriptionV2::new(session.clone(), page_limits);
    let mut page_count = 0;
    loop {
        let PublicSessionPollV2::Page { page, reset } = poll.poll(store).await.unwrap() else {
            panic!("page")
        };
        assert_eq!(reset, page_count == 0);
        assert_eq!(page.captured_revision, revision);
        assert_eq!(page.header.queue.len(), 2);
        page_count += 1;
        if let Some(token) = page.history.continuation.clone() {
            assert!(
                store
                    .public_history(SnapshotRequest {
                        session_id: session.clone(),
                        limits: page_limits.history,
                        continuation: Some(token)
                    })
                    .await
                    .is_err()
            );
        } else {
            break;
        }
        assert!(page_count < 8);
    }
    assert!(page_count >= 2);
    assert!(matches!(
        poll.poll(store).await.unwrap(),
        PublicSessionPollV2::Unchanged { .. }
    ));
    poll.overflow();
    assert!(matches!(
        poll.poll(store).await.unwrap(),
        PublicSessionPollV2::Resync {
            reason: PublicResyncReason::Overflow
        }
    ));
}

pub(crate) async fn large_queue_input_is_not_materialized(store: &impl Store) {
    let mut f = crate::queue_contract::fixture(store).await;
    let QueueAction::Enqueue { input, .. } = &mut f.enqueue.action else {
        unreachable!()
    };
    let mut wire = serde_json::to_value(&input.input).unwrap();
    wire["context"] = serde_json::json!([{"description":"private","value":{"nul\u{0}":"PRIVATE_LARGE_QUEUE".repeat(150_000)}}]);
    input.input = serde_json::from_value(wire).unwrap();
    store.mutate_queue(f.enqueue).await.unwrap();
    let mut request = request(store, &f.session, None).await;
    request.limits.proof_bytes = 65536;
    let h = header(store, request).await;
    assert_eq!(h.queue.len(), 1);
    assert!(h.read_usage.proof_bytes < 65536);
    assert!(
        !serde_json::to_string(&h)
            .unwrap()
            .contains("PRIVATE_LARGE_QUEUE")
    );
}

async fn terminated_native(
    store: &impl Store,
    session: &crabber_core::SessionId,
    run: &crabber_core::RunId,
    attempt: &AttemptTarget,
) {
    let binding = store.run_control_binding(session, run).await.unwrap();
    let id = ControlId::new();
    store
        .control_run(RunControlRequest {
            id: id.clone(),
            target: binding.target,
            expected_revision: binding.revision,
            action: RunControlAction::Cancel {
                resolver: ApprovalResolverClaim {
                    resolver_id: "aggregate-controller".into(),
                    authority: crabber_core::AuthorityRequirement::new("host-cancel", "1").unwrap(),
                    authority_claim: "explicit authenticated cancellation".into(),
                },
            },
        })
        .await
        .unwrap();
    let h = header(store, request(store, session, Some(run)).await).await;
    assert!(h.barrier.is_none());
    let tool = h.tool.unwrap();
    assert_eq!(tool.state, ToolContinuationState::Released);
    assert_eq!(tool.original_cancel_id, Some(id));
    assert!(
        tool.requirements
            .iter()
            .all(|q| q.consumed && q.state == crabber_core::InteractionState::Cancelled)
    );
    let preserved = h.attempt.unwrap();
    assert_eq!(preserved.target, *attempt);
    assert_eq!(preserved.state, AttemptState::Suspended);
    assert_eq!(preserved.cleanup, AttemptCleanup::Complete);
    assert!(matches!(
        h.controls[0].status,
        RunControlStatus::TerminalCommitted {
            status: RunStatus::Interrupted
        }
    ));
}
