//! Two genuine compound approval origins, never a holder or Main classification.
#[path = "approval/integrity.rs"]
pub(crate) mod integrity;
#[path = "approval/pending.rs"]
pub(crate) mod pending;
use super::*;
use crate::session_owner::{SessionOwnerState, SessionOwnerTermination};
use crabber_core::{Clock, SessionContext};
pub(crate) struct Fixture {
    pub request: RegisterSuccessorExecutionDriver,
    pub original: crate::ApprovalContinuationClaim,
    pub native: Box<crate::ApprovalGrant>,
    pub receipt: crate::ApprovalReceipt,
    pub joined: JoinedPreparation,
    pub batch: crate::ApprovalSuspension,
}
pub(crate) async fn fixture(store: &impl Store, clock: &ManualClock, recovery: bool) -> Fixture {
    fixture_input(store, clock, recovery, false).await
}
pub(crate) async fn fixture_input(
    store: &impl Store,
    clock: &ManualClock,
    recovery: bool,
    unkeyed: bool,
) -> Fixture {
    fixture_kind(store, clock, recovery, unkeyed, false).await
}
async fn fixture_kind(
    store: &impl Store,
    clock: &ManualClock,
    recovery: bool,
    unkeyed: bool,
    bare: bool,
) -> Fixture {
    let (context, f) = Box::pin(staged(store, clock, unkeyed, bare)).await;
    let mut original = crate::approval_contract::fixture::claim(store, &f).await;
    if recovery {
        original.lease_ms = 1;
    }
    let joined = prepare(
        store,
        &f.run,
        PreparationSource::ApprovalClaim(original.clone()),
        PreparedOperation::ApprovalClaim(original.clone()),
        PreparationKey::Approval(original.id.clone()),
        ExistingWorkTarget::ApprovalClaim(original.continuation.clone()),
        original.fingerprint().unwrap(),
    )
    .await;
    let PreparationCompletion::Approval(crate::ApprovalClaimOutcome::Applied {
        grant: native,
        receipt,
    }) = store.complete_preparation(joined.clone()).await.unwrap()
    else {
        panic!("actual claim");
    };
    if !recovery {
        return Fixture {
            request: RegisterSuccessorExecutionDriver {
                context,
                fence: native.fence.clone(),
                claim: NativeClaimLocator {
                    issuance: 3,
                    root_join_id: joined.observed_grant().reconciliation().unwrap().join_id,
                },
            },
            native,
            receipt,
            original,
            joined,
            batch: f.batch,
        };
    }
    clock.set(clock.now() + time::Duration::milliseconds(2));
    let request = crate::ApprovalContinuationRecovery {
        id: crabber_core::ControlId::new(),
        expected_revision: store.session_revision(&f.run.session_id).await.unwrap(),
        original: original.clone(),
        attempt: f.batch.next_attempt.clone(),
        owner: "approval-successor".into(),
        lease_ms: 30000,
    };
    let joined = prepare(
        store,
        &f.run,
        PreparationSource::ApprovalRecovery(request.clone()),
        PreparedOperation::ApprovalRecovery(request.clone()),
        PreparationKey::Approval(request.id.clone()),
        ExistingWorkTarget::ApprovalRecovery {
            continuation: original.continuation.clone(),
            attempt: request.attempt.clone(),
        },
        request.fingerprint().unwrap(),
    )
    .await;
    let PreparationCompletion::Approval(crate::ApprovalClaimOutcome::Applied {
        grant: native,
        receipt,
    }) = store.complete_preparation(joined.clone()).await.unwrap()
    else {
        panic!("actual recovery");
    };
    Fixture {
        request: RegisterSuccessorExecutionDriver {
            context,
            fence: native.fence.clone(),
            claim: NativeClaimLocator {
                issuance: 5,
                root_join_id: joined.observed_grant().reconciliation().unwrap().join_id,
            },
        },
        native,
        receipt,
        original,
        joined,
        batch: f.batch,
    }
}
async fn prepare(
    store: &impl Store,
    run: &crabber_core::Run,
    source: PreparationSource,
    operation: PreparedOperation,
    key: PreparationKey,
    work: ExistingWorkTarget,
    intent: crabber_core::InputFingerprint,
) -> JoinedPreparation {
    let mut request = crate::session_owner::preparation_contract::continuations::request(
        store, run, source, key, work, intent,
    )
    .await;
    let binding = store
        .execution_binding(&run.session_id, &run.id)
        .await
        .unwrap()
        .unwrap();
    request.resource_contract =
        crate::session_owner::preparation::resource_contract(&binding.capsule.runtime_semantics)
            .unwrap();
    request.owner.binding.component_id = OWNED_PREPARATION_COMPONENT.into();
    request.owner.lease_ms = 30000;
    let root = crate::session_owner::preparation_contract::begin(store, &request).await;
    crate::run_control_contract::expired::owned::observe(store, root, operation).await
}
pub(crate) async fn run(store: &impl Store, clock: &ManualClock) {
    use SuccessorExecutionDriverOrigin as Origin;
    for recovery in [false, true] {
        let f = Box::pin(fixture_kind(store, clock, recovery, false, true)).await;
        let revision = store
            .session_revision(f.request.context.session_id())
            .await
            .unwrap();
        assert!(matches!(
            store
                .register_successor_execution_driver(f.request.clone())
                .await,
            Err(crate::session_owner::SessionOwnerError::Unsupported)
        ));
        assert_eq!(
            store
                .session_revision(f.request.context.session_id())
                .await
                .unwrap(),
            revision
        );
    }
    for origin in [
        Origin::AdmissionRecovery,
        Origin::ExpiredRun,
        Origin::ApprovalClaim,
        Origin::ApprovalRecovery,
    ] {
        assert!(store.supports_successor_execution_driver_origin(origin));
    }
    for origin in [
        Origin::ToolClaim,
        Origin::ToolRecovery,
        Origin::FrontendClaim,
        Origin::FrontendRecovery,
    ] {
        assert!(!store.supports_successor_execution_driver_origin(origin));
    }
    for (recovery, unkeyed) in [(false, false), (true, false), (false, true), (true, true)] {
        let f = fixture_input(store, clock, recovery, unkeyed).await;
        assert_eq!(f.native.fence, f.request.fence);
        assert_eq!(f.original.continuation, f.batch.continuation);
        assert_eq!(f.receipt.run_id, f.request.fence.run_id);
        assert_eq!(
            f.joined.observed_grant().reconciliation().unwrap().join_id,
            f.request.claim.root_join_id
        );
        let SuccessorExecutionDriverOutcome::Applied { receipt, grant } = store
            .register_successor_execution_driver(f.request.clone())
            .await
            .unwrap()
        else {
            panic!("fresh approval successor");
        };
        let mut task =
            OwnedTaskRegistration::register(grant.scope_source().unwrap(), || async {}).unwrap();
        let OwnedScopeActivationOutcome::Applied { permit, .. } = store
            .activate_owned_scope(
                task.activation().clone(),
                grant.authority().clone(),
                Some(OwnedScopeParentAuthority::SuccessorExecutionDriver(
                    grant.clone(),
                )),
            )
            .await
            .unwrap()
        else {
            panic!("fresh gate");
        };
        task.dispatch(*permit).unwrap();
        let mut cleanup = OwnedCleanupTask::from_host_task(
            task.activation().clone(),
            tokio::spawn(async { Ok(()) }),
        );
        let proof = task
            .join()
            .await
            .unwrap()
            .observe_resources(&mut cleanup)
            .await
            .unwrap();
        let outcome = store
            .join_owned_scope(proof, grant.authority().clone())
            .await
            .unwrap();
        let owner = match outcome {
            crate::session_owner::SessionOwnerOutcome::Applied { receipt, .. }
            | crate::session_owner::SessionOwnerOutcome::Replayed(receipt) => receipt.owner,
        };
        assert_eq!(owner.state, SessionOwnerState::Joined);
        assert_eq!(owner.termination, Some(SessionOwnerTermination::Returned));
        assert!(
            matches!(store.register_successor_execution_driver(f.request).await.unwrap(), SuccessorExecutionDriverOutcome::Replayed(saved) if saved == receipt)
        );
    }
}

/// Register/Activate/Join before/after ACK loss never grants replay authority.
pub(crate) async fn faults<S: Store + 'static>(
    store: Arc<S>,
    clock: &ManualClock,
    fault: impl Fn(u8),
) {
    for recovery in [false, true] {
        for boundary in [1, 2] {
            fault(0);
            let f = fixture(store.as_ref(), clock, recovery).await;
            fault(boundary);
            assert!(matches!(
                store
                    .register_successor_execution_driver(f.request.clone())
                    .await,
                Err(crate::session_owner::SessionOwnerError::UnknownStoreOutcome)
            ));
            fault(0);
            let retry = store
                .register_successor_execution_driver(f.request)
                .await
                .unwrap();
            assert_eq!(
                matches!(retry, SuccessorExecutionDriverOutcome::Replayed(_)),
                boundary == 2
            );
            if let SuccessorExecutionDriverOutcome::Applied { grant, .. } = retry {
                super::faults::exercise(store.as_ref(), grant, boundary, &fault).await;
            }
            let f = fixture(store.as_ref(), clock, recovery).await;
            let (left, right) = futures::join!(
                store.register_successor_execution_driver(f.request.clone()),
                store.register_successor_execution_driver(f.request)
            );
            let outcomes = [left.unwrap(), right.unwrap()];
            assert_eq!(
                outcomes
                    .iter()
                    .filter(|o| matches!(o, SuccessorExecutionDriverOutcome::Applied { .. }))
                    .count(),
                1
            );
            for outcome in outcomes {
                if let SuccessorExecutionDriverOutcome::Applied { grant, .. } = outcome {
                    super::faults::exercise(store.as_ref(), grant, boundary, &fault).await;
                }
            }
        }
    }
}

async fn staged(
    store: &impl Store,
    clock: &ManualClock,
    unkeyed: bool,
    bare: bool,
) -> (SessionContext, crate::approval_contract::fixture::Fixture) {
    let admitted = if bare {
        bare_admission(store).await
    } else if unkeyed {
        crate::session_owner::preparation_contract::resources::initial_unkeyed_driver(store).await
    } else {
        crate::session_owner::preparation_contract::resources::initial_driver(store).await
    };
    let context = SessionContext::new(
        admitted.session.id.clone(),
        admitted.session.workspace_id.clone(),
        admitted.session.directory.clone(),
    );
    let f = crate::approval_contract::fixture::staged_on(store, clock, admitted).await;
    f.execution
        .suspend_for_approval(f.batch.clone(), f.owner.clone())
        .await
        .unwrap();
    for index in 0..2 {
        let answer = crate::approval_contract::fixture::answer(
            store,
            &f,
            index,
            crate::ApprovalReply::Grant,
        )
        .await;
        store.answer_approval(answer).await.unwrap();
    }
    (context, f)
}

async fn bare_admission(store: &impl Store) -> crate::AdmitOutcome {
    let mut bound = crate::session_owner::preparation_contract::fixture(
        &crabber_core::SessionId::new(),
        crabber_core::SessionRevision::INITIAL,
    );
    crate::session_owner::preparation_contract::resources::declaration::bind(
        &mut bound,
        &crabber_core::InputFingerprint::new("d".repeat(64)).unwrap(),
    );
    let crate::KeyedAdmitOutcome::Started { admitted, .. } =
        store.admit_bound_keyed_run(bound).await.unwrap()
    else {
        panic!("genuine bare bound admission");
    };
    *admitted
}
