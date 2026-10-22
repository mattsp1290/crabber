use super::*;

fn policy(max_tracked: usize) -> Arc<Policy> {
    super::super::super::DelegateTask::new(Options {
        runner_identity: "commit-boundary-test".into(),
        runner: Arc::new(|_| Box::pin(async { unreachable!("no runner dispatch") })),
        limits: Limits {
            max_task_bytes: 128,
            max_profile_bytes: 32,
            max_result_bytes: 128,
            max_in_flight: 64,
            max_wait: Duration::from_secs(1),
            shutdown_grace: Duration::from_secs(1),
            max_tracked,
            max_background_duration: Duration::from_secs(1),
            max_wait_ids: 1,
            max_wait_reply_bytes: 128,
        },
    })
    .unwrap()
    .policy
}
fn reserve(policy: &Arc<Policy>) -> ReservedRecord {
    lock(&policy.tasks)
        .admit(
            Owner {
                session: SessionId::from("session"),
                workspace: "workspace".into(),
            },
            policy.limits.max_tracked,
            SessionInvocationRequest::new(
                COMPONENT_ID.into(),
                InputFingerprint::new(&policy.hash).unwrap(),
            ),
        )
        .unwrap()
        .unwrap()
}
#[test]
fn acknowledged_rows_are_not_prunable_until_all_reservation_pins_are_disposed() {
    // At one slot, recursive successful acknowledgements must never chain old
    // outputs. At 64, every reconciliation slot can hold one saved victim.
    for count in [1, MAX_RECONCILIATIONS] {
        let policy = policy(count);
        let mut all_records = Vec::new();
        for _ in 0..count {
            let ReservedRecord { record, evicted } = reserve(&policy);
            assert!(evicted.is_none());
            *lock(&record.state) = State::Finished(Response::Completed("output".into()));
            record.acknowledged.store(true, Ordering::Release);
            all_records.push(Arc::downgrade(&record));
        }
        for _round in 0..4 {
            let mut pending = Vec::new();
            for _ in 0..count {
                let slot = policy.reconciliation.clone().try_acquire_owned().unwrap();
                let ReservedRecord { record, evicted } = reserve(&policy);
                let old = Arc::downgrade(evicted.as_ref().unwrap());
                let new = Arc::downgrade(&record);
                all_records.push(new.clone());
                *lock(&record.state) = State::Finished(Response::Completed("output".into()));
                pending.push((
                    Registration {
                        policy: policy.clone(),
                        record,
                        evicted,
                        dispatch: Arc::new(Mutex::new(Dispatch::Pending)),
                        slot: Some(slot),
                    },
                    old,
                    new,
                ));
            }
            assert_eq!(
                all_records.iter().filter(|r| r.strong_count() != 0).count(),
                count * 2
            );
            assert_eq!(
                policy.reconciliation.available_permits(),
                MAX_RECONCILIATIONS - count
            );
            for (index, (registration, old, new)) in pending.into_iter().enumerate() {
                registration.commit_started_observed(|| {
                    assert_eq!(
                        old.strong_count(),
                        0,
                        "saved output must precede slot release"
                    );
                    assert_eq!(
                        new.strong_count(),
                        1,
                        "only the registry may retain the newly prunable row"
                    );
                    assert_eq!(
                        policy.reconciliation.available_permits(),
                        MAX_RECONCILIATIONS - count + index + 1
                    );
                    assert_eq!(
                        all_records.iter().filter(|r| r.strong_count() != 0).count(),
                        count * 2 - index - 1
                    );
                    let competing = policy.clone();
                    assert!(
                        std::thread::spawn(move || competing.tasks.try_lock().is_err())
                            .join()
                            .unwrap(),
                        "concurrent admission must wait until full reservation disposal"
                    );
                });
            }
            assert_eq!(
                all_records.iter().filter(|r| r.strong_count() != 0).count(),
                count
            );
        }
    }
}
