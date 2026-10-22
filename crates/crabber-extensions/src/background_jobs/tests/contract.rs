use super::*;

#[test]
fn ids_are_53_bytes_and_monotonic_within_a_manager() {
    let p = policy(options());
    let first = p.reserve_start().unwrap();
    let id = first.id.clone();
    drop(first);
    let second = p.reserve_start().unwrap();
    assert_eq!(id.len(), 53);
    assert!(id < second.id);
    assert_eq!(input::id(&json!({"id":id})).unwrap(), id);
}
#[test]
fn two_managers_have_different_epochs() {
    let a = policy(options());
    let b = policy(options());
    assert_ne!(a.reserve_start().unwrap().id, b.reserve_start().unwrap().id);
}
#[test]
fn reservation_drop_releases_starting() {
    let p = policy(options());
    let r = p.reserve_start().unwrap();
    assert_eq!(p.live_jobs(), 1);
    drop(r);
    assert_eq!(p.live_jobs(), 0);
    assert_eq!(p.registry.lock().unwrap().starting, 0);
}
#[test]
fn cause_is_set_once_and_natural_loses_to_an_earlier_kill() {
    let job = Job::new("id".into(), owner(), 0, tails());
    assert!(job.set_cause_once(Cause::Kill));
    assert!(!job.set_cause_once(Cause::Natural));
    assert_eq!(*job.cause.borrow(), Some(Cause::Kill));
}
#[test]
fn finish_maps_every_cause_and_status_shape() {
    for cause in [
        Cause::Natural,
        Cause::Kill,
        Cause::Timeout,
        Cause::Close,
        Cause::Cancelled,
    ] {
        for status in [
            ExitStatus::from_raw(0),
            ExitStatus::from_raw(7 << 8),
            ExitStatus::from_raw(9),
        ] {
            for forced in [false, true] {
                let job = Job::new("id".into(), owner(), 0, tails());
                let reap = Reap {
                    reaped: true,
                    status: Some(status),
                    output_forced: forced,
                };
                job.finish(cause, &reap);
                let t = job.terminal.lock().unwrap().clone().unwrap();
                let expected = match cause {
                    Cause::Natural if !forced && status.code() == Some(0) => State::Succeeded,
                    Cause::Natural => State::Failed,
                    Cause::Timeout => State::TimedOut,
                    _ => State::Killed,
                };
                assert_eq!(t.state, expected);
                assert_eq!(
                    t.exit_code,
                    if cause == Cause::Natural && !forced {
                        status.code()
                    } else {
                        None
                    }
                );
            }
        }
    }
}
#[test]
fn prune_evicts_oldest_finished_only_when_tracked_is_full() {
    let mut config = options();
    config.limits.max_tracked = 2;
    let p = policy(config);
    let old = Arc::new(Job::new("old".into(), owner(), 0, tails()));
    let new = Arc::new(Job::new("new".into(), owner(), 0, tails()));
    for (job, time) in [
        (&old, UNIX_EPOCH),
        (&new, UNIX_EPOCH + Duration::from_secs(1)),
    ] {
        job.finish(
            Cause::Natural,
            &Reap {
                reaped: true,
                status: Some(ExitStatus::from_raw(0)),
                output_forced: false,
            },
        );
        job.terminal.lock().unwrap().as_mut().unwrap().completed_at = time;
    }
    p.registry.lock().unwrap().jobs.insert("old".into(), old);
    let r = p.reserve_start().unwrap();
    assert!(p.registry.lock().unwrap().jobs.contains_key("old"));
    drop(r);
    p.registry.lock().unwrap().jobs.insert("new".into(), new);
    let r = p.reserve_start().unwrap();
    assert!(!p.registry.lock().unwrap().jobs.contains_key("old"));
    assert!(p.registry.lock().unwrap().jobs.contains_key("new"));
    drop(r);
    let running = Arc::new(Job::new("running".into(), owner(), 0, tails()));
    p.registry
        .lock()
        .unwrap()
        .jobs
        .insert("running".into(), running);
    let _r = p.reserve_start().unwrap();
    assert!(p.registry.lock().unwrap().jobs.contains_key("running"));
}
