use super::*;

#[test]
fn supervisor_protocol_and_digest_are_frozen() {
    assert_eq!(SUPERVISOR_PROTOCOL, "posix-anchor-supervisor-v1");
    assert_eq!(
        supervisor_digest(),
        "09ab3cf71c48b1be62f3e08759218b271bf15c1b31b5a8bb21307faec9e75f5e"
    );
}

#[test]
fn signal_on_a_missing_group_is_gone() {
    assert_eq!(
        signal_group(Pid::from_raw(i32::MAX).unwrap(), GroupSignal::Kill),
        Err(SignalFault::Gone)
    );
}
