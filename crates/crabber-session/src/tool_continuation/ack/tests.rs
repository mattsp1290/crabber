use super::*;
use crate::Store;
use std::sync::Arc;
#[tokio::test]
async fn first_seal_accounts_actual_complete_output_before_hashing() {
    let clock = Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ));
    let store = crate::MemoryStore::with_clock(clock.clone());
    let mut fixture = Box::pin(crate::successor_driver_contract::tools::fixture(
        &store, &clock, false, false, false,
    ))
    .await;
    let (parent, _) = store.tool_ack_test_records(&fixture.native.phases[0].target);
    let bytes = crate::snapshot::encoded_bytes(
        &view::ack(&fixture.native, &fixture.receipt, true).unwrap(),
        MAX_ACK_BYTES,
    )
    .unwrap();
    fixture.native.authority.tool_ack_commitment = None;
    let mut proof = Proof::bounded(bytes);
    reseal(&mut fixture, &parent, &mut proof).unwrap();
    assert_eq!(proof.used(), bytes, "first seal omitted output growth");
    fixture.native.verify_native_ack(&fixture.receipt).unwrap();
    fixture.native.authority.tool_ack_commitment = None;
    let measured = allocation_counter::measure(|| {
        assert!(reseal(&mut fixture, &parent, &mut Proof::bounded(bytes - 1)).is_err());
    });
    assert!(fixture.native.authority.tool_ack_commitment.is_none());
    assert!(
        measured.bytes_total < 1024,
        "first seal materialized ACK before proof refusal: {measured:?}"
    );
}

#[tokio::test]
async fn native_ack_metering_and_same_parent_phase_substitution() {
    let clock = Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ));
    let store = crate::MemoryStore::with_clock(clock.clone());
    let mut fixture = Box::pin(crate::successor_driver_contract::tools::fixture_mixed(
        &store, &clock, false, false,
    ))
    .await;
    fixture.native.verify_native_ack(&fixture.receipt).unwrap();
    let (parent, phase) = store.tool_ack_test_records(&fixture.native.phases[0].target);
    let original = fixture.native.authority.tool_ack_commitment.clone();
    let actual = fixture.native.phases[0].authority.clone();
    fixture.native.phases[0].authority = fixture.native.frontend[0].authority.clone();
    assert!(
        fixture.native.verify_native_ack(&fixture.receipt).is_err(),
        "genuine same-parent foreign phase accepted"
    );
    fixture.native.phases[0].authority = actual;
    fixture.native.verify_native_ack(&fixture.receipt).unwrap();
    // Rebuild the same actual phase identity with a legitimately sealed parent.
    // Nested seals are excluded from the hash, while full metering charges them.
    let before = crate::snapshot::encoded_bytes(
        &view::ack(&fixture.native, &fixture.receipt, true).unwrap(),
        MAX_ACK_BYTES,
    )
    .unwrap();
    fixture.native.phases[0].authority = crate::PhaseAuthority::from_store(
        phase.snapshot,
        phase.token,
        fixture.native.authority.clone(),
    )
    .unwrap();
    fixture.native.verify_native_ack(&fixture.receipt).unwrap();
    let mut proof = Proof::bounded(32 << 20);
    reseal(&mut fixture, &parent, &mut proof).unwrap();
    assert!(
        proof.used() > before,
        "generic whole-authority metering omitted nested seal"
    );
    assert_eq!(
        fixture.native.authority.tool_ack_commitment, original,
        "seal recursively hashed itself"
    );
    let used = proof.used();
    reseal(&mut fixture, &parent, &mut Proof::bounded(used)).unwrap();
    let measured = allocation_counter::measure(|| {
        assert!(reseal(&mut fixture, &parent, &mut Proof::bounded(used - 1)).is_err());
    });
    assert!(
        measured.bytes_total < 1024,
        "exhausted proof materialized ACK before refusal: {measured:?}"
    );
    fixture.native.phases[0].responses[0].value = Some(serde_json::json!("changed"));
    assert!(
        fixture
            .native
            .seal_ack_from_store(
                &fixture.receipt,
                &parent.snapshot,
                "unknown private credential",
                &parent.run_token
            )
            .is_err()
    );
    assert!(fixture.native.verify_native_ack(&fixture.receipt).is_err());
}
#[tokio::test]
async fn original_bare_native_legacy_claim_can_emit_sealed_ack() {
    let clock = Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ));
    let store = crate::MemoryStore::with_clock(clock);
    let f = crate::tool_continuation_contract::fixture::fixture(&store).await;
    f.execution
        .suspend_tool_continuation(f.suspension.clone(), f.authority.clone())
        .await
        .unwrap();
    for index in 0..2 {
        store
            .answer_tool_interaction(
                crate::tool_continuation_contract::fixture::answer(&store, &f, index).await,
            )
            .await
            .unwrap();
    }
    // Genuine old-format absence is not promoted into protected provenance.
    store.tool_ack_test_forget_inputs();
    let claim = crate::tool_continuation_contract::fixture::claim(&store, &f).await;
    let crate::ToolContinuationClaimOutcome::Applied { receipt, grant } =
        store.claim_tool_continuation(claim.clone()).await.unwrap()
    else {
        panic!("fresh raw native grant");
    };
    grant.verify_native_ack(&receipt).unwrap();
    assert!(
        matches!(store.claim_tool_continuation(claim).await.unwrap(),crate::ToolContinuationClaimOutcome::Replayed(saved) if saved==receipt)
    );
}

fn reseal(
    fixture: &mut crate::successor_driver_contract::tools::Fixture,
    parent: &crate::attempt::AttemptRecord,
    proof: &mut Proof,
) -> Result<(), Error> {
    issue(
        &mut fixture.native,
        Issuance {
            receipt: &fixture.receipt,
            token: &parent.token,
            run_token: &parent.run_token,
            proof,
        },
    )
}
#[tokio::test]
async fn native_ack_binds_complete_receipt_and_custom_store_constructor() {
    let clock = Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ));
    let store = crate::MemoryStore::with_clock(clock.clone());
    let mut fixture = Box::pin(crate::successor_driver_contract::tools::fixture(
        &store, &clock, false, false, false,
    ))
    .await;
    let (parent, _) = store.tool_ack_test_records(&fixture.native.phases[0].target);
    for mutation in 0..5 {
        let mut receipt = fixture.receipt.clone();
        match mutation {
            0 => receipt.operation_id = crabber_core::ControlId::new(),
            1 => receipt.semantic_digest = InputFingerprint::new("f".repeat(64)).unwrap(),
            2 => receipt.revision = receipt.revision.next().unwrap(),
            3 => receipt.event_cursor = crabber_core::EventCursor(receipt.event_cursor.0 + 1),
            _ => {
                let crate::ToolContinuationOutcome::Claimed {
                    continuation,
                    attempt,
                } = receipt.outcome
                else {
                    panic!("claim");
                };
                receipt.outcome = crate::ToolContinuationOutcome::FrontendClaimed {
                    continuation,
                    attempt,
                };
            }
        }
        assert!(fixture.native.verify_native_ack(&receipt).is_err());
    }
    fixture.native.authority = crate::AttemptAuthority::from_store(
        parent.snapshot.clone(),
        parent.token.clone(),
        parent.run_token.clone(),
    )
    .unwrap();
    assert_eq!(
        fixture
            .native
            .verify_native_ack(&fixture.receipt)
            .unwrap_err(),
        Error::Unsupported
    );
    fixture
        .native
        .seal_ack_from_store(
            &fixture.receipt,
            &parent.snapshot,
            &parent.token,
            &parent.run_token,
        )
        .unwrap();
    fixture.native.verify_native_ack(&fixture.receipt).unwrap();
}
