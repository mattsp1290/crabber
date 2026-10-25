//! Host lifecycle controls cannot create results, worker authority, or readiness.
use super::{applied, fixture, mutation, resolver};
#[path = "controls/identity.rs"]
mod identity;
#[path = "controls/lifecycle.rs"]
mod lifecycle;
use crate::{
    PhaseCleanup, PhaseCommand, PhaseControlOutcome, PhaseError, PhasePageRequest,
    PhaseQuiescenceResult, PhaseReceipt, PhaseUncertainty, PhaseUncertaintyCause,
    PhaseWriteAuthority, Store,
};
use crabber_core::{ControlId, ManualClock, SessionRevision};
use time::OffsetDateTime;
fn fresh(outcome: PhaseControlOutcome) -> PhaseReceipt {
    let PhaseControlOutcome::Applied(receipt) = outcome else {
        panic!("fresh host control")
    };
    receipt
}
fn uncertainty(
    session: crabber_core::SessionId,
    run: crabber_core::RunId,
    receipt: &PhaseReceipt,
) -> PhaseUncertainty {
    PhaseUncertainty {
        session_id: session,
        run_id: run,
        id: ControlId::new(),
        expected_revision: receipt.revision,
        target: receipt.phase.target.clone(),
        cause: PhaseUncertaintyCause::ExpiredOrRevokedOwner,
    }
}
fn cleanup(
    request: &PhaseUncertainty,
    revision: SessionRevision,
    result: PhaseQuiescenceResult,
) -> PhaseCleanup {
    PhaseCleanup {
        session_id: request.session_id.clone(),
        run_id: request.run_id.clone(),
        id: ControlId::new(),
        expected_revision: revision,
        target: request.target.clone(),
        result,
        claim: resolver(),
    }
}
async fn evidence(store: &dyn Store, request: &PhaseUncertainty) -> serde_json::Value {
    serde_json::json!({"run":store.get_run(&request.run_id).await.unwrap(),"attempt":store.latest_execution_attempt(&request.run_id).await.unwrap().unwrap().attempt,"calls":super::settlement::read_calls(store,&request.session_id).await.unwrap(),"messages":store.list_messages(&request.session_id,None).await.unwrap(),"approval":store.approval_snapshot(&request.session_id,&request.run_id).await.unwrap()})
}
pub(super) async fn run(store: &dyn Store, clock: &ManualClock) {
    for started in [false, true] {
        for resolve_first in [false, true] {
            lifecycle::run(store, clock, started, resolve_first).await;
        }
    }
    clock.set(OffsetDateTime::UNIX_EPOCH);
    host_stopped_and_guards(store).await;
    let _ = pagination(store).await;
    identity::run(store).await;
}
async fn host_stopped_and_guards(store: &dyn Store) {
    let (admitted, execution, owner, claim) = fixture(store).await;
    let (receipt, worker) = applied(
        execution
            .mutate_phase(claim, Some(PhaseWriteAuthority::Attempt(owner)))
            .await
            .unwrap(),
    );
    let mut request = uncertainty(admitted.session.id, admitted.run.id, &receipt);
    request.cause = PhaseUncertaintyCause::HostStopped { claim: resolver() };
    for field in 0..5 {
        let mut changed = request.clone();
        match field {
            0 => changed.expected_revision = changed.expected_revision.next().unwrap(),
            1 => changed.session_id = crabber_core::SessionId::new(),
            2 => changed.target.call_id = crabber_core::ToolCallId::new(),
            3 => changed.target.id = crabber_core::PhaseId::new(),
            _ => changed.target.generation = changed.target.generation.next().unwrap(),
        }
        assert!(store.mark_phase_uncertain(changed).await.is_err());
        assert_eq!(
            store
                .tool_phase(&request.run_id, &request.target.id)
                .await
                .unwrap()
                .phase,
            receipt.phase
        );
    }
    let mut metadata_key = request.clone();
    metadata_key.id = receipt.operation_id.clone();
    assert_eq!(
        store.mark_phase_uncertain(metadata_key).await.unwrap_err(),
        PhaseError::SemanticConflict
    );
    let (a, b) = futures::join!(
        store.mark_phase_uncertain(request.clone()),
        store.mark_phase_uncertain(request.clone())
    );
    let a = a.unwrap();
    let b = b.unwrap();
    assert!(matches!(
        (&a, &b),
        (
            PhaseControlOutcome::Applied(_),
            PhaseControlOutcome::Replayed(_)
        ) | (
            PhaseControlOutcome::Replayed(_),
            PhaseControlOutcome::Applied(_)
        )
    ));
    let receipt = store
        .lookup_phase_mutation(&request.session_id, &request.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        execution
            .mutate_phase(
                mutation(
                    receipt.revision,
                    PhaseCommand::Begin {
                        target: request.target.clone()
                    }
                ),
                Some(PhaseWriteAuthority::Phase(worker.unwrap()))
            )
            .await
            .unwrap_err(),
        PhaseError::StaleOwner
    );
    let one = cleanup(&request, receipt.revision, PhaseQuiescenceResult::Complete);
    let mut conflicting = one.clone();
    conflicting.id = request.id.clone();
    assert_eq!(
        store.record_phase_cleanup(conflicting).await.unwrap_err(),
        PhaseError::SemanticConflict
    );
    let mut invalid = one.clone();
    invalid.claim.authority_claim.clear();
    assert_eq!(
        store.record_phase_cleanup(invalid).await.unwrap_err(),
        PhaseError::InvalidRequest
    );
    let mut two = one.clone();
    two.id = ControlId::new();
    let (a, b) = futures::join!(
        store.record_phase_cleanup(one),
        store.record_phase_cleanup(two)
    );
    assert!(matches!(
        (&a, &b),
        (Ok(_), Err(PhaseError::RevisionConflict)) | (Err(PhaseError::RevisionConflict), Ok(_))
    ));
}
async fn page_fixture(store: &dyn Store) -> (PhasePageRequest, crate::PhaseTarget) {
    let (admitted, execution, owner, claim) = fixture(store).await;
    let target = claim.command.target().clone();
    applied(
        execution
            .mutate_phase(claim, Some(PhaseWriteAuthority::Attempt(owner.clone())))
            .await
            .unwrap(),
    );
    for index in (0..4).rev() {
        let call = crabber_core::ToolCallRecord {
            id: crabber_core::ToolCallId::new(),
            run_id: admitted.run.id.clone(),
            name: "page".into(),
            arguments: serde_json::json!({}),
            status: crabber_core::ToolCallStatus::Pending,
            retry_safe: true,
            result: None,
        };
        execution
            .create_tool_call(
                call.clone(),
                crabber_core::EventRecord {
                    cursor: None,
                    session_id: admitted.session.id.clone(),
                    run_id: admitted.run.id.clone(),
                    turn_id: None,
                    kind: crabber_core::EventKind::ToolCallPending,
                    payload: serde_json::json!({}),
                    correlation: None,
                    live_only: false,
                    created_at: OffsetDateTime::UNIX_EPOCH,
                },
            )
            .await
            .unwrap();
        let mut next = target.clone();
        next.id = crabber_core::PhaseId(format!("phase-page-{}-{index}", admitted.run.id));
        next.call_id = call.id;
        applied(
            execution
                .mutate_phase(
                    mutation(
                        store.session_revision(&admitted.session.id).await.unwrap(),
                        PhaseCommand::Claim { target: next },
                    ),
                    Some(PhaseWriteAuthority::Attempt(owner.clone())),
                )
                .await
                .unwrap(),
        );
    }
    let request = PhasePageRequest {
        session_id: admitted.session.id.clone(),
        run_id: admitted.run.id.clone(),
        limit: 2,
        after: None,
        expected_revision: None,
    };
    (request, target)
}
pub(crate) async fn pagination(store: &dyn Store) -> (PhasePageRequest, crabber_core::PhaseId) {
    let (request, target) = page_fixture(store).await;
    let first = store.list_tool_phases(request.clone()).await.unwrap();
    assert_eq!(first.phases.len(), 2);
    assert!(first.next_after.is_some());
    let mut next = request.clone();
    next.after = first.next_after.clone();
    next.expected_revision = Some(first.revision);
    let second = store.list_tool_phases(next.clone()).await.unwrap();
    assert_eq!(second.phases.len(), 2);
    next.after = second.next_after.clone();
    let third = store.list_tool_phases(next).await.unwrap();
    assert_eq!(third.phases.len(), 1);
    assert!(third.next_after.is_none());
    let all = [first.phases, second.phases, third.phases].concat();
    assert!(
        all.windows(2)
            .all(|pair| pair[0].target.id.0.as_bytes() < pair[1].target.id.0.as_bytes())
    );
    let json = serde_json::to_string(&all).unwrap();
    assert!(!json.contains("token"));
    assert!(!json.contains("output"));
    for field in 0..5 {
        let mut bad = request.clone();
        match field {
            0 => bad.limit = 0,
            1 => bad.limit = 65,
            2 => bad.after = Some(target.id.clone()),
            3 => bad.expected_revision = Some(first.revision.next().unwrap()),
            _ => bad.session_id = crabber_core::SessionId::new(),
        }
        assert!(store.list_tool_phases(bad).await.is_err());
    }
    let captured = PhasePageRequest {
        after: Some(all[0].target.id.clone()),
        expected_revision: Some(first.revision),
        ..request.clone()
    };
    fresh(
        store
            .mark_phase_uncertain(PhaseUncertainty {
                session_id: request.session_id.clone(),
                run_id: request.run_id.clone(),
                id: ControlId::new(),
                expected_revision: first.revision,
                target: all[0].target.clone(),
                cause: PhaseUncertaintyCause::HostStopped { claim: resolver() },
            })
            .await
            .unwrap(),
    );
    assert_eq!(
        store.list_tool_phases(captured).await.unwrap_err(),
        PhaseError::RevisionConflict
    );
    (request, all[1].target.id.clone())
}

pub(super) async fn faults(store: &dyn Store, set_fault: &impl Fn(super::FaultPoint)) {
    for cleaning in [false, true] {
        for point in [
            super::FaultPoint::BeforeCommit,
            super::FaultPoint::AfterCommit,
        ] {
            let (admitted, execution, owner, claim) = fixture(store).await;
            let (mut receipt, _) = applied(
                execution
                    .mutate_phase(claim, Some(PhaseWriteAuthority::Attempt(owner)))
                    .await
                    .unwrap(),
            );
            let mut request = uncertainty(admitted.session.id, admitted.run.id, &receipt);
            request.cause = PhaseUncertaintyCause::HostStopped { claim: resolver() };
            if cleaning {
                receipt = fresh(store.mark_phase_uncertain(request.clone()).await.unwrap());
            }
            let cleanup = cleanup(&request, receipt.revision, PhaseQuiescenceResult::Complete);
            let before = store
                .tool_phase(&request.run_id, &request.target.id)
                .await
                .unwrap();
            let key = if cleaning { &cleanup.id } else { &request.id };
            set_fault(point);
            let outcome = if cleaning {
                store.record_phase_cleanup(cleanup.clone()).await
            } else {
                store.mark_phase_uncertain(request.clone()).await
            };
            set_fault(super::FaultPoint::Disabled);
            assert_eq!(outcome.unwrap_err(), PhaseError::UnknownStoreOutcome);
            let saved = store
                .lookup_phase_mutation(&request.session_id, key)
                .await
                .unwrap();
            match point {
                super::FaultPoint::BeforeCommit => {
                    assert!(saved.is_none());
                    assert_eq!(
                        store
                            .tool_phase(&request.run_id, &request.target.id)
                            .await
                            .unwrap(),
                        before
                    );
                }
                super::FaultPoint::AfterCommit => assert!(saved.is_some()),
                super::FaultPoint::Disabled => unreachable!(),
            }
            let retry = if cleaning {
                store.record_phase_cleanup(cleanup).await
            } else {
                store.mark_phase_uncertain(request).await
            }
            .unwrap();
            if saved.is_some() {
                assert!(matches!(retry, PhaseControlOutcome::Replayed(_)));
            } else {
                assert!(matches!(retry, PhaseControlOutcome::Applied(_)));
            }
        }
    }
}
