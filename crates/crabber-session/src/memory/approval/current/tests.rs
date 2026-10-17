use super::*;
#[tokio::test]
async fn tool_suspension_events_do_not_select_or_hide_approval_sources() {
    let clock = std::sync::Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ));
    let store = super::super::MemoryStore::with_clock(clock.clone());
    crate::approval_contract::current::retired(&store, &clock).await;
    let mut state = store.state.lock().unwrap().clone();
    let record = state.approvals.values().next().unwrap().clone();
    let run = state.runs[&record.run_id].clone();
    let mut event = selected(&state, &record, false).unwrap()[0].clone();
    event.kind = EventKind::ContinuationChanged;
    event.payload["kind"] = "tool_continuation".into();
    event.payload["outcome"]["operation"] = "suspended".into();
    event.cursor = Some(state.events.next_cursor().unwrap());
    state.events.append(event.clone()).unwrap();
    session(&state, &run.session_id).unwrap();
    verify(&state, &run, &record, false).unwrap();
    let mut tool_only = State::default();
    event.cursor = Some(tool_only.events.next_cursor().unwrap());
    tool_only.events.append(event).unwrap();
    assert!(!has_source(&tool_only, &run));
    session(&tool_only, &run.session_id).unwrap();
}
#[test]
fn oversized_current_sources_are_rejected_before_copying() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let clock = std::sync::Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ));
    let store = super::super::MemoryStore::with_clock(clock.clone());
    runtime.block_on(crate::approval_contract::current::retired(&store, &clock));
    let saved = store.state.lock().unwrap().clone();
    let record = saved.approvals.values().next().unwrap().clone();
    let run = saved.runs[&record.run_id].clone();
    for case in 0..3 {
        let mut state = saved.clone();
        if case == 2 {
            let latest = state
                .attempts
                .values_mut()
                .filter(|a| a.snapshot.run_id == run.id)
                .max_by_key(|a| a.snapshot.target.generation)
                .unwrap();
            latest.snapshot.target.id.0 = "x".repeat(1024 * 1024);
        } else {
            let mut event = if case == 0 {
                native(&state, &record).unwrap().clone()
            } else {
                selected(&state, &record, false).unwrap()[0].clone()
            };
            if case == 0 {
                event.correlation = Some("x".repeat(1024 * 1024));
            } else {
                event.kind = EventKind::ContinuationChanged;
                event.payload["outcome"]["operation"] = "suspended".into();
                event.payload["outcome"]["continuation"]["id"] = "x".repeat(1024 * 1024).into();
                event.payload["outcome"]["continuation"]["generation"] = record
                    .batch
                    .continuation
                    .generation
                    .next()
                    .unwrap()
                    .get()
                    .into();
            }
            event.cursor = Some(state.events.next_cursor().unwrap());
            state.events.append(event).unwrap();
        }
        let measured = allocation_counter::measure(|| {
            assert!(verify(&state, &run, &record, false).is_err());
            let mut budget = crate::public_session::budget::Budget::new(
                65536,
                crate::PublicSessionLimit::ProofBytes,
            );
            assert!(preflight(&state, &run, &record, &mut budget).is_err());
        });
        assert!(measured.bytes_max < 131_072, "case {case}: {measured:?}");
    }
}
