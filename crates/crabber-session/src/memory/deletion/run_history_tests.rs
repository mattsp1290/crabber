//! Actual admissions and terminal transactions keep resource policy unknown.
use super::*;
use crate::deletion::coverage::MAX_PROOF_BYTES;
use crate::{MemoryStore, Store};
fn bounded() -> Proof {
    Proof::bounded(MAX_PROOF_BYTES)
}
#[tokio::test]
async fn original_input_and_owned_sources_remain_independent_of_current_rows() {
    let store = MemoryStore::new();
    let receipt =
        crate::session_owner::preparation_contract::resources::successful(&store, false).await;
    {
        let mut state = store.state.lock().unwrap();
        super::super::owned::verify(&state, &receipt.session_id, &mut bounded()).unwrap();
        verify(&state, &receipt.session_id, &mut bounded()).unwrap();
        let run = state.runs.get(&receipt.run_id).unwrap();
        let report = report(&state, run, &mut bounded()).unwrap();
        assert_eq!(
            report.initial_source,
            crate::deletion::history::Fact::Authenticated
        );
        assert_eq!(
            report.every_noninitial_claim,
            crate::deletion::history::Fact::Authenticated
        );
        assert_eq!(
            report.terminal_source,
            crate::deletion::history::Fact::Unproven
        );
        assert_eq!(report.main_join, crate::deletion::history::Fact::Unproven);
        assert_eq!(
            report.resource_policy,
            crate::deletion::history::Fact::Unproven
        );
        assert!(!report.complete());

        assert!(matches!(
            verify(&state, &receipt.session_id, &mut Proof::bounded(1)),
            Err(Failure::Limited(_))
        ));
        let message = state
            .messages
            .iter_mut()
            .find(|message| message.id == receipt.user_message_id)
            .unwrap();
        let saved = message.clone();
        message.parts[0].content = crabber_core::ContentBlock::Text {
            text: "changed original input".into(),
        };
        assert!(matches!(
            verify(&state, &receipt.session_id, &mut bounded()),
            Err(Failure::Error(_))
        ));
        *state
            .messages
            .iter_mut()
            .find(|message| message.id == receipt.user_message_id)
            .unwrap() = saved;
    }
    let run = store.get_run(&receipt.run_id).await.unwrap().unwrap();
    store
        .abandon_run(crabber_core::AbandonRequest {
            expected: crabber_core::RunFence {
                run_id: run.id,
                claim_token: run.claim_token,
            },
            expected_owner: run.owner,
            authority: crabber_core::AbandonAuthority::HostStoppedOwner,
        })
        .await
        .unwrap();
    let mut state = store.state.lock().unwrap();
    let crate::deletion::coverage::CoverageRead::Verified(coverage) =
        super::super::coverage(&state, &receipt.session_id).unwrap()
    else {
        panic!("actual owned admission origin")
    };
    assert!(matches!(
        super::super::readiness::read(&state, &coverage, &mut bounded()).unwrap(),
        Some(crate::DeletionBlocker::OwnershipUnknown)
    ));
    state
        .runs
        .get_mut(&receipt.run_id)
        .unwrap()
        .usage
        .output_tokens = 77;
    assert!(matches!(
        verify(&state, &receipt.session_id, &mut bounded()),
        Err(Failure::Error(_))
    ));
}
#[tokio::test]
async fn terminal_source_authenticates_actual_private_result_before_ownership_unknown() {
    let clock = std::sync::Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ));
    let store = MemoryStore::with_clock(clock.clone());
    let (admitted, execution, owner, conclusion) = crate::attempt_contract::conclusion::started(
        &store,
        &clock,
        crate::attempt_contract::conclusion::terminal(crabber_core::RunStatus::Completed),
    )
    .await;
    execution
        .conclude_attempt(conclusion, Some(owner))
        .await
        .unwrap();
    let mut state = store.state.lock().unwrap();
    verify(&state, &admitted.session.id, &mut bounded()).unwrap();
    state.runs.get_mut(&admitted.run.id).unwrap().error = Some("forged private result".into());
    assert!(matches!(
        verify(&state, &admitted.session.id, &mut bounded()),
        Err(Failure::Error(_))
    ));
}

#[tokio::test]
async fn run_history_report_keeps_legacy_noninitial_claim_unproven() {
    use crate::deletion::history::Fact;
    let store = MemoryStore::new();
    let receipt =
        crate::session_owner::preparation_contract::resources::successful(&store, false).await;
    let mut state = store.state.lock().unwrap();
    state.native_session_protocols.remove(&receipt.session_id);
    // This fixture imports a predecessor cohort, including absence of the later
    // enrollment birth. Partial erasure is covered by the corruption fixtures.
    state
        .execution_enrollment_protocols
        .retain(|(session, _)| session != &receipt.session_id);
    state
        .execution_enrollment_origins
        .retain(|(session, _), _| session != &receipt.session_id);
    state
        .execution_enrollment_unclassified
        .retain(|(session, _), _| session != &receipt.session_id);
    state
        .session_owners
        .legacy_native_session_for_test(&receipt.session_id);
    state
        .native_run_protocols
        .remove(&(receipt.session_id.clone(), receipt.run_id.clone()));
    state
        .native_run_sources
        .retain(|(scope, _, _), _| scope != &receipt.session_id);
    state
        .native_run_source_heads
        .remove(&(receipt.session_id.clone(), receipt.run_id.clone()));
    let key = (receipt.session_id.clone(), receipt.run_id.clone(), 1);
    let mut issuer = state.native_run_issuances.get(&key).unwrap().clone();
    issuer.version = 1;
    issuer.sequence = 2;
    issuer.ownership_handoff = None;
    // Import a coherent legacy current projection, using an actual token and
    // its canonical hash. The original first issuer/Witness stay unchanged;
    // this later legacy claim still has no protected completion source.
    let current = state.runs.get_mut(&receipt.run_id).unwrap();
    current.claim_token = "e".repeat(64);
    issuer.state = crate::native_parent::RunState::from_run(current).unwrap();
    issuer.validate().unwrap();
    state.native_run_issuances.insert(
        (receipt.session_id.clone(), receipt.run_id.clone(), 2),
        issuer,
    );
    let run = state.runs.get(&receipt.run_id).unwrap();
    let observed = report(&state, run, &mut bounded()).unwrap();
    assert_eq!(observed.initial_source, Fact::Authenticated);
    assert_eq!(observed.every_noninitial_claim, Fact::Unproven);
    assert_eq!(observed.retained_noninitial_sources, Fact::Unproven);
    assert_eq!(
        observed.initial_consumption,
        crate::native_enrollment::initial_consumer::InitialConsumption::Unproven
    );
    assert_eq!(observed.main_join, Fact::Unproven);
    assert_eq!(observed.resource_policy, Fact::Unproven);
    assert!(!observed.complete());
    // The original witness cannot silently relink to the remaining issuer.
    state.native_run_issuances.remove(&key);
    assert!(matches!(
        super::super::owned::verify(&state, &receipt.session_id, &mut bounded()),
        Err(Failure::Error(_))
    ));
}

#[path = "run_history/unkeyed_tests.rs"]
mod unkeyed;

#[tokio::test]
async fn joined_driver_does_not_cover_later_same_fence_begin() {
    let clock = std::sync::Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ));
    let store = std::sync::Arc::new(MemoryStore::with_clock(clock));
    let fixture =
        crate::session_owner::driver::history_fixture::begin_after_join(store.clone()).await;
    let state = store.state.lock().unwrap();
    let run = state.runs.get(&fixture.request.fence.run_id).unwrap();
    let record = &state
        .session_owners
        .source_audit(fixture.request.context.session_id(), &fixture.join)
        .unwrap()
        .record;
    assert!(
        crate::memory::session_owner::classify_driver_join(&state, record, &mut bounded()).unwrap()
    );
    let report = report(&state, run, &mut bounded()).unwrap();
    assert_eq!(
        report.initial_source,
        crate::deletion::history::Fact::Authenticated
    );
    assert_eq!(
        report.every_noninitial_claim,
        crate::deletion::history::Fact::Authenticated
    );
    assert_eq!(report.main_join, crate::deletion::history::Fact::Unproven);
    assert!(!report.complete());
    let current = crate::memory::native_parent::source_protocol::current(
        &state,
        &run.session_id,
        &run.id,
        &mut bounded(),
    )
    .unwrap()
    .unwrap();
    assert_eq!(
        current.sequence, 1,
        "new native work did not rotate the Run fence"
    );
}

#[tokio::test]
async fn joined_driver_does_not_cover_external_same_fence_begin_before_join() {
    let clock = std::sync::Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ));
    let store = std::sync::Arc::new(MemoryStore::with_clock(clock));
    let fixture =
        crate::session_owner::driver::history_fixture::begin_before_join(store.clone()).await;
    let state = store.state.lock().unwrap();
    let run = state.runs.get(&fixture.request.fence.run_id).unwrap();
    let record = &state
        .session_owners
        .source_audit(fixture.request.context.session_id(), &fixture.join)
        .unwrap()
        .record;
    assert!(
        crate::memory::session_owner::classify_driver_join(&state, record, &mut bounded()).unwrap()
    );
    let report = report(&state, run, &mut bounded()).unwrap();
    assert_eq!(
        report.initial_source,
        crate::deletion::history::Fact::Authenticated
    );
    assert_eq!(
        report.every_noninitial_claim,
        crate::deletion::history::Fact::Authenticated
    );
    assert_eq!(report.main_join, crate::deletion::history::Fact::Unproven);
    assert!(!report.complete());
    let current = crate::memory::native_parent::source_protocol::current(
        &state,
        &run.session_id,
        &run.id,
        &mut bounded(),
    )
    .unwrap()
    .unwrap();
    assert_eq!(
        current.sequence, 1,
        "new native work did not rotate the Run fence"
    );
}
