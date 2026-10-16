use super::{Store, ToolContinuationError as Error, fixture};
use crate::tool_continuation::ToolContinuationRecord;

pub(crate) async fn suspended(store: &impl Store) -> fixture::Fixture {
    let f = fixture::fixture(store).await;
    f.execution
        .suspend_tool_continuation(f.suspension.clone(), f.authority.clone())
        .await
        .unwrap();
    store
        .answer_tool_interaction(fixture::answer(store, &f, 0).await)
        .await
        .unwrap();
    f
}
pub(crate) async fn setup(
    store: &impl Store,
    clock: &crabber_core::ManualClock,
    kind: u8,
) -> fixture::Fixture {
    use crabber_core::Clock;
    if kind == 7 {
        return super::lifecycle(store).await;
    }
    let f = suspended(store).await;
    if kind >= 5 {
        store
            .answer_tool_interaction(fixture::answer(store, &f, 1).await)
            .await
            .unwrap();
        let mut claim = fixture::claim(store, &f).await;
        claim.lease_ms = 1;
        let crate::ToolContinuationClaimOutcome::Applied { receipt, .. } =
            store.claim_tool_continuation(claim.clone()).await.unwrap()
        else {
            panic!("fresh claim")
        };
        if kind == 6 {
            clock.set(clock.now() + time::Duration::milliseconds(2));
            store
                .recover_tool_continuation(crate::ToolContinuationRecovery {
                    id: crabber_core::ControlId::new(),
                    expected_revision: receipt.revision,
                    original: claim,
                    attempt: f.suspension.next_attempt.clone(),
                    owner: "coherence-recovery".into(),
                    lease_ms: 30000,
                })
                .await
                .unwrap();
        }
    }
    if kind >= 8 {
        let queue = crate::queue_contract::entry(store, f.admitted.session.id.clone()).await;
        store.mutate_queue(queue.enqueue).await.unwrap();
    }
    f
}
pub(crate) fn replace(record: &mut ToolContinuationRecord, kind: u8) {
    match kind {
        0 => {
            record.batch.phases[0].checkpoint = crabber_core::ToolCheckpoint::new(
                "fixture",
                "1",
                serde_json::json!({"coherently":"replaced checkpoint"}),
            )
            .unwrap();
        }
        1 => record.created_at += time::Duration::milliseconds(1),
        2 => {
            record.questions[0].answer.as_mut().unwrap().reply =
                crate::ToolInteractionReply::Answer {
                    value: serde_json::json!("coherently replaced answer"),
                }
        }
        3 => record.batch.source_turn_id = Some(crabber_core::TurnId::new()),
        4 => {
            record.questions[0].answer = None;
            record.questions[0].view.state = crabber_core::InteractionState::Pending;
        }
        5 => {
            record.state = crate::ToolContinuationState::Waiting;
            record.claim = None;
            record.recovery = None;
            for question in &mut record.questions {
                question.view.consumed = false;
            }
        }
        6 => record.recovery = None,
        7 => record.state = crate::ToolContinuationState::Claimed,
        8 => record.state = crate::ToolContinuationState::Released,
        9 => (),
        _ => panic!("unknown corruption"),
    }
    // These changes retain internal coherence. Original receipts must anchor them.
    record.validate().unwrap();
}
pub(crate) async fn refused(store: &impl Store, f: &fixture::Fixture, kind: u8) {
    let session = &f.admitted.session.id;
    let run = &f.admitted.run.id;
    let before = store.session_revision(session).await.unwrap();
    let events = store.list_events(session, None, 100).await.unwrap();
    assert_eq!(
        store
            .tool_continuation_snapshot(session, run)
            .await
            .unwrap_err(),
        Error::UnknownStoreOutcome
    );
    assert!(matches!(
        store
            .claim_tool_continuation(fixture::claim(store, f).await)
            .await
            .unwrap_err(),
        Error::UnknownStoreOutcome | Error::InvalidTransition
    ));
    assert!(matches!(
        store
            .answer_tool_interaction(fixture::answer(store, f, 1).await)
            .await
            .unwrap_err(),
        Error::UnknownStoreOutcome | Error::InvalidTransition
    ));
    if kind >= 8 {
        entrypoints_refuse(store, f, before).await;
    }
    assert_eq!(store.session_revision(session).await.unwrap(), before);
    assert_eq!(store.list_events(session, None, 100).await.unwrap(), events);
    assert!(
        store
            .lookup_tool_continuation(session, &f.suspension.id)
            .await
            .unwrap()
            .is_some()
    );
}

async fn entrypoints_refuse(
    store: &impl Store,
    f: &fixture::Fixture,
    revision: crabber_core::SessionRevision,
) {
    let session = &f.admitted.session.id;
    let prepared = crate::typed_admission_contract::prepare(
        crate::execution_binding_contract::fixture(
            session,
            time::OffsetDateTime::UNIX_EPOCH,
            "corrupt-entry",
            'b',
        ),
        crabber_core::InputEnvelope::text("fresh input", revision).unwrap(),
        time::OffsetDateTime::UNIX_EPOCH,
    );
    let error = store
        .admit_bound_run(crate::BoundAdmitRequest {
            request: prepared.keyed.request,
            binding: prepared.binding,
        })
        .await
        .unwrap_err();
    assert_eq!(
        error,
        crate::StoreError::Validation("invalid tool continuation evidence".into())
    );
    let error = store
        .mutate_queue(crate::QueueMutation {
            session_id: session.clone(),
            id: crabber_core::ControlId::new(),
            expected_revision: revision,
            action: crate::QueueAction::Claim {
                owner: "cannot-bypass".into(),
                lease: std::time::Duration::from_secs(30),
            },
        })
        .await
        .unwrap_err();
    assert_eq!(
        error,
        crate::StoreError::Validation("invalid tool continuation evidence".into())
    );
    let error = store
        .mark_attempt_uncertain(crate::AttemptUncertainty {
            session_id: session.clone(),
            run_id: f.admitted.run.id.clone(),
            id: crabber_core::ControlId::new(),
            expected_revision: revision,
            target: f.suspension.next_attempt.clone(),
            cause: crate::AttemptUncertaintyCause::HostStopped {
                claim: crate::AttemptResolverClaim {
                    resolver_id: "host".into(),
                    authority_claim: "worker stopped".into(),
                },
            },
        })
        .await
        .unwrap_err();
    assert_eq!(error, crate::AttemptError::UnknownStoreOutcome);
}
