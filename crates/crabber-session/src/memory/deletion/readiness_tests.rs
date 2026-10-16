use super::*;
use crate::{MemoryStore, Store};

#[tokio::test]
async fn empty_managed_scope_is_native_ready_but_bare_scope_is_not_coverage() {
    let store = MemoryStore::new();
    let created = store
        .manage_session(crate::management_contract::create())
        .await
        .unwrap();
    let state = store.state.lock().unwrap();
    let crate::deletion::coverage::CoverageRead::Verified(coverage) =
        super::super::coverage(&state, &created.session_id).unwrap()
    else {
        panic!("managed origin missing")
    };
    assert!(read(&state, &coverage).unwrap().is_none());
    assert!(matches!(
        read_with_limit(&state, &coverage, coverage.proof_bytes() - 1).unwrap(),
        Some(Blocker::ProofLimit)
    ));
}

#[tokio::test]
async fn runtime_has_priority_and_created_origin_never_covers_bare_admission() {
    let store = MemoryStore::new();
    let created = store
        .manage_session(crate::management_contract::create())
        .await
        .unwrap();
    let mut request = crate::deletion::contract::admission(&created.session_id);
    let session = store
        .get_session(&created.session_id)
        .await
        .unwrap()
        .unwrap();
    request.workspace_id = session.workspace_id;
    request.directory = session.directory;
    let admitted = store.admit_run(request).await.unwrap();
    let mut state = store.state.lock().unwrap();
    let crate::deletion::coverage::CoverageRead::Verified(coverage) =
        super::super::coverage(&state, &created.session_id).unwrap()
    else {
        panic!("managed origin missing")
    };
    assert!(matches!(
        read(&state, &coverage).unwrap(),
        Some(Blocker::Runtime)
    ));
    let previous = state.runs.get(&admitted.fence.run_id).unwrap().clone();
    let mut closed = previous.clone();
    closed.status = crabber_core::RunStatus::Interrupted;
    super::super::super::native_parent::record_run(&mut state, Some(&previous), &closed).unwrap();
    state.runs.insert(closed.id.clone(), closed);
    assert!(matches!(
        read(&state, &coverage).unwrap(),
        Some(Blocker::OwnershipUnknown)
    ));
}

#[tokio::test]
async fn released_native_checkpoint_authenticates_all_historical_phases() {
    let clock = std::sync::Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ));
    let store = MemoryStore::with_clock(clock);
    let f = crate::tool_continuation_contract::lifecycle(&store).await;
    let session = &f.admitted.session.id;
    let coverage = VerifiedDeletionCoverage::fixture(
        session.clone(),
        store.session_revision(session).await.unwrap(),
    );
    let mut state = store.state.lock().unwrap();
    assert!(matches!(
        read(&state, &coverage).unwrap(),
        Some(Blocker::OwnershipUnknown)
    ));
    // This earlier generation is no longer current for the logical call. A
    // latest-only scan would miss removal of its native checkpoint ancestry.
    let historical = &f.suspension.phases[0].previous.id;
    state.phases.remove(historical);
    assert!(matches!(
        read(&state, &coverage),
        Err(Error::UnknownStoreOutcome)
    ));
}

fn read(state: &State, coverage: &VerifiedDeletionCoverage) -> Result<Option<Blocker>, Error> {
    read_with_limit(state, coverage, MAX_PROOF_BYTES)
}
fn read_with_limit(
    state: &State,
    coverage: &VerifiedDeletionCoverage,
    cap: usize,
) -> Result<Option<Blocker>, Error> {
    super::read(state, coverage, &mut Proof::bounded(cap))
}

#[tokio::test]
async fn retained_owner_must_actually_join_and_joined_origin_cannot_be_erased() {
    use crate::session_owner::{
        MutateSessionOwner, SessionOwnerCommand, SessionOwnerOutcome, SessionOwnerTermination,
    };
    let store = MemoryStore::new();
    let created = store
        .manage_session(crate::management_contract::create())
        .await
        .unwrap();
    let mut request = crate::session_owner_contract::reservation(created.session_id.clone());
    request.binding.workspace_id = created.summary.workspace_id;
    request.binding.directory = created.summary.directory;
    let SessionOwnerOutcome::Applied {
        receipt,
        authority: Some(authority),
    } = store
        .reserve_session_owner(request.clone(), None)
        .await
        .unwrap()
    else {
        panic!("real authority")
    };
    {
        let state = store.state.lock().unwrap();
        let crate::deletion::coverage::CoverageRead::Verified(coverage) =
            super::super::coverage(&state, &created.session_id).unwrap()
        else {
            panic!("origin")
        };
        assert_eq!(
            read(&state, &coverage).unwrap(),
            Some(Blocker::SessionOwner)
        );
    }
    store
        .mutate_session_owner(
            MutateSessionOwner {
                id: crabber_core::SessionOwnerOperationId::new(),
                session_id: created.session_id.clone(),
                target: receipt.owner.target,
                command: SessionOwnerCommand::Join {
                    termination: SessionOwnerTermination::Returned,
                },
            },
            Some(authority),
        )
        .await
        .unwrap();
    let mut state = store.state.lock().unwrap();
    let crate::deletion::coverage::CoverageRead::Verified(coverage) =
        super::super::coverage(&state, &created.session_id).unwrap()
    else {
        panic!("origin")
    };
    assert_eq!(
        read(&state, &coverage).unwrap(),
        Some(Blocker::OwnershipUnknown)
    );
    state
        .session_owners
        .erase_historical_audit_for_test(&created.session_id, &request.id);
    assert!(matches!(
        read(&state, &coverage),
        Err(Error::UnknownStoreOutcome)
    ));
}

#[tokio::test]
async fn oversized_private_phase_stops_at_shared_proof_limit_before_validation() {
    let store = MemoryStore::with_clock(std::sync::Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    )));
    let f = crate::tool_continuation_contract::lifecycle(&store).await;
    let session = &f.admitted.session.id;
    let coverage = VerifiedDeletionCoverage::fixture(
        session.clone(),
        store.session_revision(session).await.unwrap(),
    );
    let mut state = store.state.lock().unwrap();
    let completed = state
        .phases
        .values_mut()
        .find(|phase| {
            phase.snapshot.run_id == f.admitted.run.id
                && phase.snapshot.state == crate::PhaseState::Completed
        })
        .unwrap();
    completed.output = Some(
        crabber_core::ToolOutput::from_json(
            &serde_json::json!({"oversized": "x".repeat(2 * 1024 * 1024)}),
        )
        .unwrap(),
    );
    assert_eq!(
        read_with_limit(&state, &coverage, 128 * 1024).unwrap(),
        Some(Blocker::ProofLimit)
    );
}

#[tokio::test]
async fn readiness_and_physical_purge_share_one_remaining_allowance() {
    let store = MemoryStore::new();
    let created = store
        .manage_session(crate::management_contract::create())
        .await
        .unwrap();
    let mut state = store.state.lock().unwrap();
    let crate::deletion::coverage::CoverageRead::Verified(coverage) =
        super::super::coverage(&state, &created.session_id).unwrap()
    else {
        panic!("origin")
    };
    let mut probe = Proof::bounded(MAX_PROOF_BYTES);
    assert!(
        super::read(&state, &coverage, &mut probe)
            .unwrap()
            .is_none()
    );
    let used = probe.used();
    let mut shared = Proof::bounded(used + 1);
    assert!(
        super::read(&state, &coverage, &mut shared)
            .unwrap()
            .is_none()
    );
    assert!(matches!(
        super::super::purge::purge(&mut state, &created.session_id, &mut shared),
        Err(Failure::Limited(_))
    ));
    assert!(state.sessions.contains_key(&created.session_id));
    assert!(shared.used() >= used);
}
