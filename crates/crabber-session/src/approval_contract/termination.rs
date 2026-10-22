//! Waiting approvals terminate without a permission grant or tool execution.
use super::fixture::{Fixture, answer, claim, staged};
use crate::{
    ApprovalError as Error, ApprovalReply, ApprovalTermination, ApprovalTerminationKind, Store,
};
use crabber_core::{
    Clock, ControlId, EventKind, InteractionState, ManualClock, RunStatus, ToolResultStatus,
};

pub(crate) async fn request(store: &dyn Store, f: &Fixture, expire: bool) -> ApprovalTermination {
    let mut resolver = answer(store, f, 0, ApprovalReply::Grant).await.resolver;
    resolver.authority =
        crabber_core::AuthorityRequirement::new("host-administration", "v1").unwrap();
    ApprovalTermination {
        id: ControlId::new(),
        session_id: f.run.session_id.clone(),
        run_id: f.run.id.clone(),
        continuation: f.batch.continuation.clone(),
        expected_revision: store.session_revision(&f.run.session_id).await.unwrap(),
        resolver,
        kind: if expire {
            let crate::CallApproval::Ask { id, .. } = &f.batch.calls[1].decision else {
                panic!("question")
            };
            ApprovalTerminationKind::Expire {
                interaction_id: id.clone(),
            }
        } else {
            ApprovalTerminationKind::Cancel
        },
    }
}
pub(super) async fn run(store: &dyn Store, clock: &ManualClock) {
    for answered in 0..=2 {
        for expire in [false, true] {
            clock.set(time::OffsetDateTime::UNIX_EPOCH);
            let mut f = staged(store, clock).await;
            super::presentation::suspend(store, clock, &mut f).await;
            for index in 0..answered {
                store
                    .answer_approval(answer(store, &f, index, ApprovalReply::Grant).await)
                    .await
                    .unwrap();
            }
            let request = request(store, &f, expire).await;
            let before = store.session_revision(&f.run.session_id).await.unwrap();
            if expire {
                assert_eq!(
                    store.terminate_approval(request.clone()).await.unwrap_err(),
                    if answered == 2 {
                        Error::InvalidTransition
                    } else {
                        Error::NotExpired
                    }
                );
                assert_eq!(
                    store.session_revision(&f.run.session_id).await.unwrap(),
                    before
                );
                if answered == 2 {
                    continue;
                }
                clock.set(clock.now() + time::Duration::seconds(2));
            }
            let old = store.get_run(&f.run.id).await.unwrap().unwrap();
            let receipt = store.terminate_approval(request.clone()).await.unwrap();
            request.verify_receipt(&receipt).unwrap();
            verify(store, &f, &request, answered).await;
            let run = store.get_run(&f.run.id).await.unwrap().unwrap();
            assert_eq!(run.usage, old.usage);
            assert_ne!(run.claim_token, old.claim_token);
            assert!(run.lease_until <= clock.now());
            assert!(
                f.execution
                    .renew_lease(clock.now() + time::Duration::minutes(1))
                    .await
                    .is_err()
            );
            let events = store
                .list_events(&f.run.session_id, None, 1000)
                .await
                .unwrap();
            assert_eq!(
                store.terminate_approval(request.clone()).await.unwrap(),
                receipt
            );
            assert_eq!(
                store
                    .list_events(&f.run.session_id, None, 1000)
                    .await
                    .unwrap(),
                events
            );
            let mut changed = request.clone();
            changed.resolver.authority_claim.push_str("changed");
            assert_eq!(
                store.terminate_approval(changed).await.unwrap_err(),
                Error::SemanticConflict
            );
            let mut late = answer(store, &f, 0, ApprovalReply::Deny).await;
            late.expected_revision = receipt.revision;
            assert!(store.answer_approval(late).await.is_err());
            assert!(
                store
                    .claim_approval_continuation(claim(store, &f).await)
                    .await
                    .is_err()
            );
            // Ordinary admission proves the terminal batch released the session gate.
            let mut prepared = crate::execution_binding_contract::fixture(
                &f.run.session_id,
                clock.now(),
                "after-termination",
                'b',
            );
            prepared.keyed.request.session_id = Some(f.run.session_id.clone());
            prepared.keyed.request.expected_revision = Some(receipt.revision);
            store
                .admit_bound_run(crate::BoundAdmitRequest {
                    request: prepared.keyed.request,
                    binding: prepared.binding,
                })
                .await
                .unwrap();
            assert_eq!(store.terminate_approval(request).await.unwrap(), receipt);
        }
    }
    guards(store, clock).await;
}
async fn verify(store: &dyn Store, f: &Fixture, request: &ApprovalTermination, answered: usize) {
    assert_eq!(
        store.get_run(&f.run.id).await.unwrap().unwrap().status,
        request.run_status()
    );
    let snapshot = store
        .approval_snapshot(&f.run.session_id, &f.run.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(snapshot.termination, Some(request.kind.clone()));
    assert_eq!(snapshot.state, crate::ApprovalContinuationState::Released);
    assert!(snapshot.interactions.iter().all(|q| !q.consumed));
    for (index, q) in snapshot.interactions.iter().enumerate() {
        assert_eq!(
            q.state,
            if index < answered {
                InteractionState::Answered
            } else if matches!(request.kind, ApprovalTerminationKind::Expire { .. }) {
                InteractionState::Expired
            } else {
                InteractionState::Cancelled
            }
        );
    }
    for call in calls(store, &f.run.session_id).await {
        let result = call.result.unwrap();
        assert_eq!(
            result.status,
            if request.run_status() == RunStatus::Interrupted {
                ToolResultStatus::Interrupted
            } else {
                ToolResultStatus::Failed
            }
        );
        assert_eq!(
            result.content,
            crabber_core::ToolOutput::new(vec![], true)
                .unwrap()
                .to_content_blocks()
        );
    }
    let events = store
        .list_events(&f.run.session_id, None, 1000)
        .await
        .unwrap();
    for kind in [EventKind::RunSettled, EventKind::TurnCompleted] {
        let selected: Vec<_> = events.iter().filter(|e| e.kind == kind).collect();
        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].turn_id, f.batch.source_turn_id);
    }
    assert_eq!(
        store
            .latest_execution_attempt(&f.run.id)
            .await
            .unwrap()
            .unwrap()
            .attempt
            .state,
        crate::AttemptState::Suspended
    );
}
async fn guards(store: &dyn Store, clock: &ManualClock) {
    clock.set(time::OffsetDateTime::UNIX_EPOCH);
    let f = staged(store, clock).await;
    f.execution
        .suspend_for_approval(f.batch.clone(), f.owner.clone())
        .await
        .unwrap();
    let request = request(store, &f, false).await;
    let mut stale = request.clone();
    stale.expected_revision = crabber_core::SessionRevision::INITIAL;
    assert_eq!(
        store.terminate_approval(stale).await.unwrap_err(),
        Error::RevisionConflict
    );
    let mut wrong = request.clone();
    wrong.continuation.generation = wrong.continuation.generation.next().unwrap();
    assert_eq!(
        store.terminate_approval(wrong).await.unwrap_err(),
        Error::GenerationConflict
    );
    for index in 0..2 {
        store
            .answer_approval(answer(store, &f, index, ApprovalReply::Grant).await)
            .await
            .unwrap();
    }
    let claim = claim(store, &f).await;
    store.claim_approval_continuation(claim).await.unwrap();
    let mut request = request;
    request.expected_revision = store.session_revision(&f.run.session_id).await.unwrap();
    assert!(store.terminate_approval(request).await.is_err());
}

pub(crate) async fn calls(
    store: &dyn Store,
    session: &crabber_core::SessionId,
) -> Vec<crabber_core::ToolCallRecord> {
    let crate::SnapshotOutcome::Page(page) = store
        .snapshot(crate::SnapshotRequest {
            session_id: session.clone(),
            continuation: None,
            limits: crate::SnapshotLimits {
                messages: 100,
                tool_calls: 100,
                parts: 100,
                text_bytes: 100_000,
                encoded_bytes: 1_000_000,
            },
        })
        .await
        .unwrap()
    else {
        panic!("bounded fixture snapshot")
    };
    assert!(page.continuation.is_none());
    page.tool_calls
}
