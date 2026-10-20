use super::*;
use std::sync::Arc;
#[tokio::test]
async fn actual_grant_payload_budget_precedes_materialization() {
    for frontend in [false, true] {
        let clock = Arc::new(crabber_core::ManualClock::new(
            time::OffsetDateTime::UNIX_EPOCH,
        ));
        let store = crate::MemoryStore::with_clock(clock.clone());
        let mut fixture = Box::pin(crate::successor_driver_contract::tools::fixture(
            &store, &clock, frontend, false, false,
        ))
        .await;
        let mut proof = Proof::bounded(32 << 20);
        let expected = actual(&fixture.native, &mut proof).unwrap();
        assert_eq!(
            actual(&fixture.native, &mut Proof::bounded(proof.used())).unwrap(),
            expected
        );
        assert!(actual(&fixture.native, &mut Proof::bounded(proof.used() - 1)).is_err());
        let measured = allocation_counter::measure(|| {
            assert!(actual(&fixture.native, &mut Proof::bounded(0)).is_err());
        });
        assert!(
            measured.bytes_total < 1024,
            "exhausted caller proof allocated payload: {measured:?}"
        );
        if frontend {
            fixture.native.frontend[0].frozen.arguments =
                serde_json::json!({"oversized":"x".repeat(5 << 20)});
        } else {
            fixture.native.phases[0].responses[0].value =
                Some(serde_json::json!("x".repeat(5 << 20)));
        }
        let measured = allocation_counter::measure(|| {
            assert!(fixture.native.payload_fingerprint().is_err());
        });
        assert!(
            measured.bytes_total < 8192,
            "oversized ACK materialized before refusal: {measured:?}"
        );
        let mut deep = serde_json::json!("untrusted");
        for _ in 0..256 {
            deep = serde_json::Value::Array(vec![deep]);
        }
        if frontend {
            fixture.native.frontend[0].frozen.arguments = deep;
        } else {
            fixture.native.phases[0].responses[0].value = Some(deep);
        }
        let measured = allocation_counter::measure(|| {
            assert!(fixture.native.payload_fingerprint().is_err());
        });
        assert!(
            measured.bytes_total < 8192,
            "deep ACK materialized before refusal: {measured:?}"
        );
    }
}
