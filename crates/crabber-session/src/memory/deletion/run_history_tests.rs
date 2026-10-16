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
