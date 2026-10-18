//! Same public controller, actual factory and actual returned-output ownership.
use super::*;
use crate::deletion::history::Fact;
use crate::session_owner::{SessionOwnerState, owned_scope::FixedInitialTestHooks};

pub(super) fn native_closed(
    host: &FixedMemoryInitialHost,
    session: &SessionId,
    test: &FixedInitialTestHooks,
) {
    let targets = test.owner().test_targets();
    for target in [targets.0, targets.1].into_iter().flatten() {
        assert_eq!(
            host.store()
                .fixed_preparation_fixture_owner_state(session, &target)
                .unwrap(),
            SessionOwnerState::Joined
        );
    }
}
async fn entered(notify: &tokio::sync::Notify) {
    tokio::time::timeout(Duration::from_secs(2), notify.notified())
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn actual_partial_factory_and_both_handoff_stop_windows_retire_originals() {
    for window in 0..3 {
        for drop_caller in [false, true] {
            let host = FixedMemoryInitialHost::new();
            let (context, revision) = managed(&host).await;
            let session = context.session_id().clone();
            let hooks = FixedInitialTestHooks::new();
            match window {
                0 => hooks.hold_factory(),
                1 => hooks.hold_returned_loan(),
                _ => hooks.hold_registered_inner(),
            }
            let mut task = host
                .register_scripted_test(values(context, revision, false), source(), hooks.clone())
                .unwrap();
            task.start().unwrap();
            match window {
                0 => entered(&hooks.factory_entered).await,
                1 => entered(&hooks.loan_returned).await,
                _ => entered(&hooks.inner_registered).await,
            }
            let actual_owner = hooks.owner();
            assert!(host.control.tasks().available_capacity() < 64);
            let control = task.control();
            control.request_stop();
            let mut task = if drop_caller {
                drop(task);
                None
            } else {
                Some(task)
            };
            let waiting = host.wait_for_supervision();
            tokio::pin!(waiting);
            assert!(futures::poll!(&mut waiting).is_pending());
            match window {
                0 => hooks.factory_release.notify_one(),
                1 => hooks.loan_release.notify_one(),
                _ => hooks.inner_release.notify_one(),
            }
            if let Some(task) = &mut task {
                let observed = tokio::time::timeout(Duration::from_secs(5), task.join())
                    .await
                    .unwrap()
                    .unwrap();
                assert_ne!(
                    observed.status(),
                    Some(&PendingInitialProducerStatus::ClosedAssignment)
                );
            }
            drop(task);
            tokio::time::timeout(Duration::from_secs(5), &mut waiting)
                .await
                .unwrap();
            assert_eq!(host.control.tasks().available_capacity(), 64);
            assert!(!actual_owner.resources_pending());
            native_closed(&host, &session, &hooks);
            assert_eq!(
                host.store().list_unfinished_runs().await.unwrap(),
                [] as [crabber_core::Run; 0]
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn actual_fixed_output_drop_holds_originals_capacity_and_policy_until_disposed() {
    output_disposal(false).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn actual_fixed_output_drop_panic_never_publishes_policy_terminal() {
    output_disposal(true).await;
}
async fn output_disposal(panic: bool) {
    let host = FixedMemoryInitialHost::new();
    let (context, revision) = managed(&host).await;
    let session = context.session_id().clone();
    let hooks = FixedInitialTestHooks::new();
    let gate = hooks.output_gate(panic);
    // Also release both gates if the pre-disposal checkpoint fails. The
    // independent physical-Drop watchdog starts only at that real boundary.
    let _safety = gate.release_on_drop();
    let mut task = host
        .register_scripted_test(values(context, revision, false), source(), hooks.clone())
        .unwrap();
    task.start().unwrap();
    tokio::time::timeout(Duration::from_secs(15), gate.before_drain.notified())
        .await
        .unwrap();
    let mut release = gate.watchdog();
    gate.drain_release.notify_one();
    entered(&gate.entered).await;
    let actual_owner = hooks.owner();
    let joined = actual_owner.test_joined_disposition();
    let pending = actual_owner.resources_pending();
    let capacity = host.control.tasks().available_capacity();
    let report = host.store().fixed_initial_fixture_report(&session).unwrap();
    let observed_pending = {
        let joining = task.join();
        tokio::pin!(joining);
        futures::poll!(&mut joining).is_pending()
    };
    let watchdog_expired = release.release();
    assert!(
        !watchdog_expired,
        "independent output Drop watchdog released prematurely"
    );
    assert_eq!(joined, Some(OwnedMainDisposition::Returned));
    assert!(pending);
    assert!(capacity < 64);
    assert!(observed_pending);
    assert_eq!(report.resource_policy, Fact::Unproven);
    let observed = tokio::time::timeout(Duration::from_secs(5), task.join())
        .await
        .unwrap()
        .unwrap();
    if panic {
        assert_eq!(
            observed.status(),
            Some(&PendingInitialProducerStatus::Unresolved)
        );
        assert!(observed.observation_failed());
        assert!(observed.take_retry().is_some());
        assert_eq!(
            host.store()
                .fixed_initial_fixture_report(&session)
                .unwrap()
                .resource_policy,
            Fact::Unproven
        );
    } else {
        assert_eq!(
            observed.status(),
            Some(&PendingInitialProducerStatus::ClosedAssignment)
        );
        assert!(!observed.observation_failed());
        assert!(observed.error().is_none());
        assert_eq!(
            host.store()
                .fixed_initial_fixture_report(&session)
                .unwrap()
                .resource_policy,
            Fact::Authenticated
        );
        native_closed(&host, &session, &hooks);
    }
    drop(task);
    tokio::time::timeout(Duration::from_secs(5), host.wait_for_supervision())
        .await
        .unwrap();
    assert_eq!(host.control.tasks().available_capacity(), 64);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn actual_literal_factory_panic_is_joined_and_cannot_open_a_source() {
    let host = FixedMemoryInitialHost::new();
    let (context, revision) = managed(&host).await;
    let session = context.session_id().clone();
    let hooks = FixedInitialTestHooks::new();
    hooks.panic_factory();
    let mut task = host
        .register_scripted_test(values(context, revision, false), source(), hooks.clone())
        .unwrap();
    task.start().unwrap();
    let observed = tokio::time::timeout(Duration::from_secs(5), task.join())
        .await
        .unwrap()
        .unwrap();
    assert!(observed.error().is_some());
    assert!(observed.status().is_none());
    assert_eq!(
        hooks.owner().test_joined_disposition(),
        Some(OwnedMainDisposition::Panicked)
    );
    native_closed(&host, &session, &hooks);
    drop(task);
    tokio::time::timeout(Duration::from_secs(5), host.wait_for_supervision())
        .await
        .unwrap();
    assert_eq!(
        host.store().list_unfinished_runs().await.unwrap(),
        [] as [crabber_core::Run; 0]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn actual_child_activation_commits_before_lost_ack_and_retires_no_dispatch() {
    let host = FixedMemoryInitialHost::new();
    let (context, revision) = managed(&host).await;
    let session = context.session_id().clone();
    let hooks = FixedInitialTestHooks::new();
    hooks.lose_activation_ack();
    let mut task = host
        .register_scripted_test(values(context, revision, false), source(), hooks.clone())
        .unwrap();
    task.start().unwrap();
    let observed = tokio::time::timeout(Duration::from_secs(5), task.join())
        .await
        .unwrap()
        .unwrap();
    assert!(observed.error().is_some());
    assert!(observed.status().is_none());
    assert_eq!(
        hooks.owner().test_joined_disposition(),
        Some(OwnedMainDisposition::NoDispatch)
    );
    native_closed(&host, &session, &hooks);
    let child = hooks.owner().test_targets().1.unwrap();
    let activation =
        crate::session_owner::owned_scope::source::activation_key(&session, &child).unwrap();
    assert_eq!(
        host.store()
            .fixed_preparation_fixture_activation(&session, &activation)
            .unwrap(),
        Some(crate::session_owner::SessionOwnerOperation::ActivateScope)
    );
    drop(task);
    tokio::time::timeout(Duration::from_secs(5), host.wait_for_supervision())
        .await
        .unwrap();
    assert_eq!(
        host.store().list_unfinished_runs().await.unwrap(),
        [] as [crabber_core::Run; 0]
    );
}
