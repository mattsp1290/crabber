//! Genuine fixed-source readiness and guarded erasure, using the public host.
use super::*;
use crate::session_owner::owned_scope::{
    FixedMemoryInitialHost, FixedMemoryInitialInput, PendingInitialProducerStatus,
};
use crate::{ManageSession, SessionChange, SessionDefaults, Store};
use crabber_core::{AdmissionKey, ControlId, InputEnvelope, SessionContext};

pub(in crate::memory::deletion) async fn completed(
    keyed: bool,
    turns: usize,
) -> (FixedMemoryInitialHost, SessionId) {
    let host = FixedMemoryInitialHost::new();
    let session = SessionId::new();
    host.store()
        .manage_session(ManageSession {
            id: ControlId::new(),
            session_id: session.clone(),
            expected_revision: SessionRevision::INITIAL,
            change: SessionChange::Create {
                workspace_id: "workspace".into(),
                directory: "/workspace".into(),
                title: "fixed readiness".into(),
                defaults: SessionDefaults::default(),
            },
        })
        .await
        .unwrap();
    for turn in 0..turns {
        let revision = host.store().session_revision(&session).await.unwrap();
        let source = crabber_extension::LibraryScriptedSource::new(
            crabber::Selection {
                provider_id: "library".into(),
                model_id: "text".into(),
            },
            vec!["actual closed source".into()],
        )
        .unwrap();
        let mut task = host
            .register_scripted(
                FixedMemoryInitialInput {
                    context: SessionContext::new(session.clone(), "workspace", "/workspace"),
                    title: "fixed readiness".into(),
                    input: InputEnvelope::text("genuine value input", revision).unwrap(),
                    key: keyed.then(|| AdmissionKey::new(format!("readiness-{turn}")).unwrap()),
                },
                source,
            )
            .unwrap();
        task.start().unwrap();
        // The whole journey has 11/12 sequential native calls, each retaining its
        // independent 5s bound; use the existing 30s preparation lease for the journey.
        let observed = tokio::time::timeout(std::time::Duration::from_secs(30), task.join())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            observed.status(),
            Some(&PendingInitialProducerStatus::ClosedAssignment)
        );
        assert!(!observed.observation_failed());
        assert!(observed.error().is_none(), "{:?}", observed.error());
        drop(task);
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            host.wait_for_supervision(),
        )
        .await
        .unwrap();
    }
    (host, session)
}
pub(in crate::memory::deletion) fn ready(
    state: &State,
    session: &SessionId,
    proof: &mut Proof,
) -> Result<Option<Blocker>, Error> {
    let crate::deletion::coverage::CoverageRead::Verified(coverage) =
        super::super::coverage(state, session)?
    else {
        panic!("actual managed origin");
    };
    super::read(state, &coverage, proof)
}
#[tokio::test]
async fn completed_fixed_sources_are_ready_and_public_delete_is_permanent() {
    for keyed in [false, true] {
        let (host, session) = completed(keyed, 1).await;
        {
            let state = host.store().state.lock().unwrap();
            assert_eq!(
                state
                    .runs
                    .values()
                    .filter(|r| r.session_id == session)
                    .count(),
                1
            );
            assert_eq!(
                ready(&state, &session, &mut Proof::bounded(MAX_PROOF_BYTES)).unwrap(),
                None
            );
        }
        let revision = host.store().session_revision(&session).await.unwrap();
        let request = crate::DeleteSession {
            id: ControlId::new(),
            session_id: session.clone(),
            expected_revision: revision,
        };
        let outcome = host.store().delete_session(request.clone()).await.unwrap();
        let crate::DeletionOutcome::Applied(receipt) = outcome else {
            panic!("complete fixed inventory must delete");
        };
        assert_eq!(receipt.final_revision, revision.next().unwrap());
        assert_eq!(
            host.store().delete_session(request).await.unwrap(),
            crate::DeletionOutcome::Replayed(receipt)
        );
        assert!(host.store().get_session(&session).await.unwrap().is_none());
        let state = host.store().state.lock().unwrap();
        assert!(state.tombstones.contains_key(&session));
        assert!(!state.pending_initial.contains_scope(&session));
        assert!(!state.initial_consumer.contains_scope(&session));
        assert!(!state.session_owners.contains_scope(&session));
        assert!(state.runs.values().all(|run| run.session_id != session));
    }
}
fn retained_inventory(state: &State, session: &SessionId) -> crabber_core::InputFingerprint {
    use crate::native_enrollment::initial_consumer::digest;
    let mut sources = Vec::new();
    for (_, epoch) in state.pending_initial.epoch_entries() {
        let expected = &epoch.correlation;
        if &expected.key.session_id != session {
            continue;
        }
        // Independent test observations of each original inventory, outside the
        // public deletion's unchanged single whole-operation allowance.
        let mut proof = Proof::bounded(MAX_PROOF_BYTES);
        let birth = state
            .pending_initial
            .selected(&expected.key, &expected.identity, &mut proof)
            .unwrap();
        let consumer =
            crate::memory::initial_consumer::selected(state, expected, &mut proof).unwrap();
        let birth_digest = digest(&(
            birth.slot,
            birth.epoch,
            birth.registered,
            birth.assignment,
            birth.close,
            birth.publications,
            birth.head,
        ))
        .unwrap();
        let consumer_digest = digest(&(
            consumer.origin,
            consumer.cohort,
            consumer.assignment,
            &consumer.calls,
            &consumer.results,
            &consumer.roster,
            &consumer.reverse,
            consumer.publications,
            consumer.head,
            consumer.close,
        ))
        .unwrap();
        sources.push((birth_digest, consumer_digest));
    }
    let cohorts: Vec<_> = state
        .pending_initial_protocols
        .iter()
        .filter(|(key, _)| &key.session_id == session)
        .collect();
    digest(&(
        state.session_owners.raw_initial_snapshot(session),
        cohorts,
        sources,
    ))
    .unwrap()
}

#[tokio::test]
async fn two_fixed_sources_are_ready_but_whole_delete_limit_keeps_inventory() {
    for keyed in [false, true] {
        let (host, session) = completed(keyed, 2).await;
        let store = host.store();
        let before_session = store.get_session(&session).await.unwrap();
        let before_messages = store.list_all_messages(&session).await.unwrap();
        let revision = store.session_revision(&session).await.unwrap();
        let before_inventory = {
            let state = store.state.lock().unwrap();
            assert_eq!(
                state
                    .runs
                    .values()
                    .filter(|run| run.session_id == session)
                    .count(),
                2
            );
            assert_eq!(
                ready(&state, &session, &mut Proof::bounded(MAX_PROOF_BYTES)).unwrap(),
                None
            );
            retained_inventory(&state, &session)
        };
        let request = crate::DeleteSession {
            id: ControlId::new(),
            session_id: session.clone(),
            expected_revision: revision,
        };
        // Readiness fits, but deletion shares that same allowance with purge.
        // An exact repeated request must remain blocked without a receipt.
        for _ in 0..2 {
            assert_eq!(
                store.delete_session(request.clone()).await.unwrap(),
                crate::DeletionOutcome::Blocked(Blocker::ProofLimit)
            );
            assert_eq!(store.session_revision(&session).await.unwrap(), revision);
            assert_eq!(store.get_session(&session).await.unwrap(), before_session);
            assert_eq!(
                store.list_all_messages(&session).await.unwrap(),
                before_messages
            );
            let state = store.state.lock().unwrap();
            assert!(!state.tombstones.contains_key(&session));
            assert!(
                !state
                    .operation_receipts
                    .contains_key(&(session.clone(), request.id.clone()))
            );
            assert!(state.pending_initial.contains_scope(&session));
            assert!(state.initial_consumer.contains_scope(&session));
            assert_eq!(retained_inventory(&state, &session), before_inventory);
            assert_eq!(
                ready(&state, &session, &mut Proof::bounded(MAX_PROOF_BYTES)).unwrap(),
                None
            );
        }
    }
}

#[tokio::test]
async fn exact_whole_readiness_allowance_includes_origin_and_membership() {
    let (host, session) = completed(true, 1).await;
    let state = host.store().state.lock().unwrap();
    let mut baseline = Proof::bounded(MAX_PROOF_BYTES);
    assert_eq!(ready(&state, &session, &mut baseline).unwrap(), None);
    let n = baseline.used();
    assert_eq!(
        ready(&state, &session, &mut Proof::bounded(n)).unwrap(),
        None
    );
    assert_eq!(
        ready(&state, &session, &mut Proof::bounded(n - 1)).unwrap(),
        Some(Blocker::ProofLimit)
    );
}
#[tokio::test]
async fn a_genuine_running_raw_run_keeps_runtime_precedence() {
    let (host, session) = completed(false, 1).await;
    let mut request = crate::deletion::contract::admission(&session);
    request.session_id = Some(session.clone());
    request.workspace_id = "workspace".into();
    request.directory = "/workspace".into();
    host.store().admit_run(request).await.unwrap();
    let state = host.store().state.lock().unwrap();
    assert_eq!(
        ready(&state, &session, &mut Proof::bounded(MAX_PROOF_BYTES)).unwrap(),
        Some(Blocker::Runtime)
    );
}

fn pending_uncovered(state: &State, session: &SessionId) -> bool {
    let mut proof = Proof::bounded(MAX_PROOF_BYTES);
    let checked = super::super::run_history::checked(state, session, &mut proof).unwrap();
    super::fixed::pending_uncovered(state, session, &checked, &mut proof).unwrap()
}
#[path = "fixed_initial_readiness/cases.rs"]
mod cases;
