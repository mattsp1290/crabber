//! Before/after commit ambiguity never reopens an unacknowledged gate.
use super::*;
use crate::session_owner::{SessionOwnerError as Error, SessionOwnerOutcome, SessionOwnerState};
pub(crate) async fn run<S: Store + 'static>(
    store: Arc<S>,
    clock: &ManualClock,
    fault: impl Fn(u8),
) {
    for admission in [true, false] {
        for boundary in [1, 2] {
            fault(0);
            let request = fixture::request(store.as_ref(), clock, admission).await;
            fault(boundary);
            assert!(matches!(
                store
                    .register_successor_execution_driver(request.clone())
                    .await,
                Err(Error::UnknownStoreOutcome)
            ));
            fault(0);
            let retry = store
                .register_successor_execution_driver(request.clone())
                .await
                .unwrap();
            assert_eq!(
                matches!(retry, SuccessorExecutionDriverOutcome::Replayed(_)),
                boundary == 2
            );
            if let SuccessorExecutionDriverOutcome::Applied { grant, .. } = retry {
                exercise(store.as_ref(), grant, boundary, &fault).await;
            }
            let request = fixture::request(store.as_ref(), clock, admission).await;
            let (left, right) = futures::join!(
                store.register_successor_execution_driver(request.clone()),
                store.register_successor_execution_driver(request.clone())
            );
            let outcomes = [left.unwrap(), right.unwrap()];
            assert_eq!(
                outcomes
                    .iter()
                    .filter(|outcome| matches!(
                        outcome,
                        SuccessorExecutionDriverOutcome::Applied { .. }
                    ))
                    .count(),
                1
            );
            for outcome in outcomes {
                if let SuccessorExecutionDriverOutcome::Applied { grant, .. } = outcome {
                    exercise(store.as_ref(), grant, boundary, &fault).await;
                }
            }
        }
    }
}
async fn exercise(
    store: &impl Store,
    grant: Box<SuccessorExecutionDriverGrant>,
    boundary: u8,
    fault: &impl Fn(u8),
) {
    let effects = Arc::new(AtomicUsize::new(0));
    let count = effects.clone();
    let mut task =
        OwnedTaskRegistration::register(grant.scope_source().unwrap(), move || async move {
            count.fetch_add(1, Ordering::SeqCst);
        })
        .unwrap();
    fault(boundary);
    assert!(
        store
            .activate_owned_scope(
                task.activation().clone(),
                grant.authority().clone(),
                Some(OwnedScopeParentAuthority::SuccessorExecutionDriver(
                    grant.clone()
                ))
            )
            .await
            .is_err()
    );
    fault(0);
    let retry = store
        .activate_owned_scope(
            task.activation().clone(),
            grant.authority().clone(),
            Some(OwnedScopeParentAuthority::SuccessorExecutionDriver(
                grant.clone(),
            )),
        )
        .await
        .unwrap();
    assert_eq!(
        matches!(retry, OwnedScopeActivationOutcome::Replayed(_)),
        boundary == 2
    );
    if let OwnedScopeActivationOutcome::Applied { permit, .. } = retry {
        task.dispatch(*permit).unwrap();
    }
    let joined = task.join().await.unwrap();
    assert_eq!(effects.load(Ordering::SeqCst), usize::from(boundary == 1));
    let mut cleanup = OwnedCleanupTask::from_host_task(
        joined.activation().clone(),
        tokio::spawn(async { Ok(()) }),
    );
    let proof = joined.observe_resources(&mut cleanup).await.unwrap();
    fault(boundary);
    assert!(
        store
            .join_owned_scope(proof.clone(), grant.authority().clone())
            .await
            .is_err()
    );
    fault(0);
    let retry = store
        .join_owned_scope(proof, grant.authority().clone())
        .await
        .unwrap();
    assert_eq!(
        matches!(retry, SessionOwnerOutcome::Replayed(_)),
        boundary == 2
    );
    assert_eq!(
        store
            .get_session_owner(
                &grant.receipt().worker.session_id,
                &grant.receipt().worker.begin.owner.target.id
            )
            .await
            .unwrap()
            .state,
        SessionOwnerState::Joined
    );
}
