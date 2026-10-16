use super::*;
#[tokio::test]
async fn shared_registration_rejects_both_seals_and_capacity_before_poll() {
    for close_first in [true, false] {
        let first = ToolResourceOwner::new();
        let second = ToolResourceOwner::new();
        if close_first {
            first.close();
        } else {
            second.close();
        }
        let polls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let observed = polls.clone();
        assert_eq!(
            first
                .resources()
                .spawn_shared(&second.resources(), async move {
                    observed.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    Ok(())
                }),
            Err(ResourceRegistrationError::Sealed)
        );
        tokio::task::yield_now().await;
        assert_eq!(polls.load(std::sync::atomic::Ordering::SeqCst), 0);
        assert_eq!(first.snapshot().pending + second.snapshot().pending, 0);
    }
    let first = ToolResourceOwner::new();
    let second = ToolResourceOwner::new();
    let release = CancellationToken::new();
    for _ in 0..MAX_TOOL_RESOURCE_TASKS {
        let release = release.clone();
        second
            .resources()
            .spawn(async move {
                release.cancelled().await;
                Ok(())
            })
            .unwrap();
    }
    assert_eq!(
        first
            .resources()
            .spawn_shared(&second.resources(), async { panic!("must not poll") }),
        Err(ResourceRegistrationError::Capacity)
    );
    assert_eq!(first.snapshot().pending, 0);
    first.close();
    assert_eq!(
        second.resources().spawn_shared(&first.resources(), async {
            panic!("sealed capacity must not poll")
        }),
        Err(ResourceRegistrationError::Sealed)
    );
    release.cancel();
    assert_eq!(
        second.drain(Duration::from_secs(2)).await.joined,
        MAX_TOOL_RESOURCE_TASKS
    );
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reversed_shared_registration_joins_each_task_once_under_both_owners() {
    let first = ToolResourceOwner::new();
    let second = ToolResourceOwner::new();
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut registrations = Vec::new();
    for reversed in [true, false] {
        let a = first.resources();
        let b = second.resources();
        let calls = calls.clone();
        registrations.push(tokio::spawn(async move {
            let (a, b) = if reversed { (b, a) } else { (a, b) };
            a.spawn_shared(&b, async move {
                calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Ok(())
            })
            .unwrap();
        }));
    }
    for registration in registrations {
        registration.await.unwrap();
    }
    let (a, b) = tokio::join!(
        first.drain(Duration::from_secs(2)),
        second.drain(Duration::from_secs(2))
    );
    assert_eq!((a.joined, b.joined), (2, 2));
    assert!(a.failures.is_empty() && b.failures.is_empty());
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 2);
}

#[tokio::test]
async fn both_owners_observe_the_same_sticky_failure_after_actual_join() {
    for panic in [false, true] {
        let first = ToolResourceOwner::new();
        let second = ToolResourceOwner::new();
        first
            .resources()
            .spawn_shared(&second.resources(), async move {
                assert!(!panic, "shared resource panic");
                Err(ResourceFailure::CleanupFailed)
            })
            .unwrap();
        let a = first.drain(Duration::from_secs(2)).await;
        let b = second.drain(Duration::from_secs(2)).await;
        assert_eq!((a.joined, b.joined), (1, 1));
        assert_eq!(a.failures, b.failures);
        assert_eq!(
            a.failures,
            vec![if panic {
                ResourceFailure::Panicked
            } else {
                ResourceFailure::CleanupFailed
            }]
        );
    }
}
