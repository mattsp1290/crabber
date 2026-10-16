use super::*;
fn bounded_rejection(kind: u8) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let store = Arc::new(MemoryStore::new());
    let provider = FakeProvider::scripted(vec![frontend_calls()]);
    let protected = Arc::new(AtomicUsize::new(0));
    let dispatches = Arc::new(AtomicUsize::new(0));
    let agent = make(
        store.clone(),
        provider.clone(),
        protected.clone(),
        dispatches.clone(),
    );
    let session = SessionId::new();
    let run = runtime.block_on(start(&agent, &session));
    let view = runtime
        .block_on(agent.tool_continuation_snapshot(&session, &run))
        .unwrap()
        .unwrap();
    let revision = view.revision;
    let mut request = response(&view);
    if kind < 2 {
        let count = if kind == 0 { 65 } else { 64 };
        request.results = (0..count)
            .map(|i| {
                FrontendToolResult::new(
                    ToolCallId::from(format!("large-{i}")),
                    FrontendResultStatus::Completed,
                    ToolOutput::new(
                        vec![crabber::core::MessageContent::Text {
                            text: "x".repeat(70_000),
                        }],
                        false,
                    )
                    .unwrap(),
                )
                .unwrap()
            })
            .collect();
    } else {
        request.resolver.authority_claim = "x".repeat(1024 * 1024);
    }
    runtime.block_on(agent.close_extensions()).unwrap();
    let mut result = None;
    let allocated = allocation_counter::measure(|| {
        result = Some(runtime.block_on(agent.continue_frontend(request)));
    });
    assert!(matches!(
        result.unwrap(),
        Err(RuntimeError::ToolContinuation(
            crabber::session::ToolContinuationError::InvalidRequest
        ))
    ));
    assert!(
        allocated.bytes_total < 262_144,
        "oversized results copied before rejection: {allocated:?}"
    );
    assert_eq!(
        runtime.block_on(store.session_revision(&session)).unwrap(),
        revision
    );
    assert_eq!(provider.requests().len(), 1);
    assert_eq!(protected.load(Ordering::SeqCst), 0);
    assert_eq!(dispatches.load(Ordering::SeqCst), 0);
}
#[test]
fn frontend_allocation_rejects_excess_count_before_copy() {
    bounded_rejection(0);
}
#[test]
fn frontend_allocation_rejects_aggregate_before_copy() {
    bounded_rejection(1);
}
#[test]
fn frontend_allocation_rejects_resolver_before_copy() {
    bounded_rejection(2);
}
