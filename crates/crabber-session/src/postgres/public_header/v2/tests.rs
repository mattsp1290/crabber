use crate::*;
fn limits() -> PublicSessionLimitsV2 {
    PublicSessionLimitsV2 {
        header_bytes: 2 * 1024 * 1024,
        proof_bytes: 32 * 1024 * 1024,
        approval_interactions: 64,
        tool_requirements: 64,
        queue_entries: 64,
        registered_owners: 1024,
        run_controls: 64,
        history: SnapshotLimits {
            messages: 64,
            tool_calls: 64,
            parts: 128,
            text_bytes: 65536,
            encoded_bytes: 131_072,
        },
    }
}
#[tokio::test]
async fn aggregate_v2_frontend_header_is_complete_and_safe() {
    let Some(url) = crate::postgres::tests::test_url() else {
        return;
    };
    let _guard = crate::postgres::tests::TEST_LOCK.lock().await;
    crate::PostgresStore::migrate(&url).await.unwrap();
    let store = crate::PostgresStore::connect(&url).await.unwrap();
    let (f, batch, _) = crate::frontend_contract::fixture::paused(&store).await;
    let request = PublicHeaderRequestV2 {
        session_id: f.request.session_id.clone(),
        run_id: Some(f.request.run_id.clone()),
        expected_revision: store.session_revision(&f.request.session_id).await.unwrap(),
        limits: limits(),
    };
    let PublicHeaderOutcomeV2::Header(header) = store
        .public_control_header_v2(request.clone())
        .await
        .unwrap()
    else {
        panic!("header")
    };
    assert_eq!(header.version, 2);
    assert!(matches!(
        header.barrier,
        Some(PublicContinuationBarrierV2::Tool { .. })
    ));
    let tool = header.tool.as_ref().unwrap();
    assert_eq!(tool.continuation, batch.continuation);
    assert_eq!(tool.frontend.len(), 1);
    assert_eq!(tool.frontend[0].name, "frontend");
    assert!(!tool.frontend[0].consumed);
    assert!(header.queue.is_empty() && header.owners.is_empty() && header.controls.is_empty());
    assert!(header.read_usage.proof_bytes > 8192);
    let wire = serde_json::to_string(&header).unwrap();
    for private in [
        "External only",
        "result_contract",
        "\"descriptor\"",
        "checkpoint",
        "resolver",
        "lease",
        "authority",
        "location",
        "arguments",
    ] {
        assert!(!wire.contains(private), "{private}");
    }
    let mut limited = request.clone();
    limited.limits.tool_requirements = 0;
    assert!(matches!(
        store.public_control_header_v2(limited).await.unwrap(),
        PublicHeaderOutcomeV2::Limited(PublicSessionLimitV2::ToolRequirements)
    ));
    let mut short = request.clone();
    short.limits.proof_bytes = header.read_usage.proof_bytes - 1;
    assert!(matches!(
        store.public_control_header_v2(short).await.unwrap(),
        PublicHeaderOutcomeV2::Limited(PublicSessionLimitV2::ProofBytes)
    ));
    let mut exact = request;
    exact.limits.proof_bytes = header.read_usage.proof_bytes;
    assert!(matches!(
        store.public_control_header_v2(exact).await.unwrap(),
        PublicHeaderOutcomeV2::Header(_)
    ));
}
