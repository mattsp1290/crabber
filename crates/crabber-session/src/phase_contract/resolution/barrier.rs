//! Resolving one approved phase must not release its continuation barrier.
use super::{descriptor, fresh, mixed_output};
use crate::{
    ApprovalClaimOutcome, ApprovalContinuationClaim, ApprovalReply, AttemptCommand,
    AttemptMutation, AttemptMutationOutcome, PhaseCommand, PhaseQuiescence, PhaseWriteAuthority,
    Store, UncertainPhaseChoice,
};
use crabber_core::{ControlId, ManualClock, RunStatus};

pub(super) async fn run(store: &dyn Store, clock: &ManualClock) {
    let (scope, uncertain) = prepare(store, clock).await;
    let before = store
        .approval_snapshot(&scope.session_id, &scope.id)
        .await
        .unwrap()
        .unwrap();
    let parent = store
        .latest_execution_attempt(&scope.id)
        .await
        .unwrap()
        .map(|versioned| versioned.attempt);
    let run = store.get_run(&scope.id).await.unwrap().unwrap();
    assert_eq!(run.status, RunStatus::Running);
    let request = descriptor(
        scope.session_id.clone(),
        scope.id.clone(),
        crate::phase_contract::mutation(
            uncertain.revision,
            PhaseCommand::ResolveUncertain {
                target: uncertain.phase.target.clone(),
                claim: crate::phase_contract::resolver(),
                choice: UncertainPhaseChoice::Success {
                    output: mixed_output(),
                },
            },
        ),
    );
    let resolved = fresh(store.resolve_uncertain_phase(request).await.unwrap());
    assert_eq!(resolved.phase.quiescence, PhaseQuiescence::Pending);
    let after = store
        .approval_snapshot(&scope.session_id, &scope.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(after.revision, resolved.revision);
    let mut original = before;
    original.revision = after.revision;
    assert_eq!(after, original);
    assert_eq!(store.get_run(&scope.id).await.unwrap(), Some(run));
    assert_eq!(
        store
            .latest_execution_attempt(&scope.id)
            .await
            .unwrap()
            .map(|versioned| versioned.attempt),
        parent
    );
    let session = store.get_session(&scope.session_id).await.unwrap().unwrap();
    let next = admission(session, resolved.revision);
    let outcome = store.admit_run(next).await;
    assert!(
        matches!(outcome, Err(crate::StoreError::Busy)),
        "native approval barrier must reject an otherwise valid admission: {outcome:?}"
    );
}

async fn prepare(
    store: &dyn Store,
    clock: &ManualClock,
) -> (crabber_core::Run, crate::PhaseReceipt) {
    let f = crate::approval_contract::fixture::staged(store, clock).await;
    f.execution
        .suspend_for_approval(f.batch.clone(), f.owner.clone())
        .await
        .unwrap();
    for index in [0, 1] {
        let answer =
            crate::approval_contract::fixture::answer(store, &f, index, ApprovalReply::Grant).await;
        store.answer_approval(answer).await.unwrap();
    }
    let claim = ApprovalContinuationClaim {
        id: ControlId::new(),
        session_id: f.run.session_id.clone(),
        run_id: f.run.id.clone(),
        expected_revision: store.session_revision(&f.run.session_id).await.unwrap(),
        continuation: f.batch.continuation.clone(),
        expected_plan: f.batch.expected_plan,
        owner: "phase-reconciliation-fixture".into(),
        lease_ms: 30_000,
    };
    let ApprovalClaimOutcome::Applied { grant, .. } =
        store.claim_approval_continuation(claim).await.unwrap()
    else {
        panic!("fresh continuation")
    };
    let execution = store.execution(grant.fence).await.unwrap();
    let target = grant.calls[0].target.clone();
    let begin = AttemptMutation {
        id: ControlId::new(),
        expected_revision: store.session_revision(&f.run.session_id).await.unwrap(),
        command: AttemptCommand::Begin {
            target: target.attempt.clone(),
        },
    };
    let AttemptMutationOutcome::Applied { receipt, .. } = execution
        .mutate_attempt(begin, Some(grant.authority.clone()))
        .await
        .unwrap()
    else {
        panic!("fresh attempt begin")
    };
    let (claimed, worker) = crate::phase_contract::applied(
        execution
            .mutate_phase(
                crate::phase_contract::mutation(
                    receipt.revision,
                    PhaseCommand::Claim {
                        target: target.clone(),
                    },
                ),
                Some(PhaseWriteAuthority::Attempt(grant.authority)),
            )
            .await
            .unwrap(),
    );
    let (started, _) = crate::phase_contract::applied(
        execution
            .mutate_phase(
                crate::phase_contract::mutation(
                    claimed.revision,
                    PhaseCommand::Begin {
                        target: target.clone(),
                    },
                ),
                Some(PhaseWriteAuthority::Phase(worker.unwrap())),
            )
            .await
            .unwrap(),
    );
    let (uncertain, _) = crate::phase_contract::applied(
        execution
            .mutate_phase(
                crate::phase_contract::mutation(
                    started.revision,
                    PhaseCommand::MarkStoppedUncertain {
                        target: target.clone(),
                        claim: crate::phase_contract::resolver(),
                    },
                ),
                None,
            )
            .await
            .unwrap(),
    );
    (f.run, uncertain)
}

fn admission(
    session: crabber_core::Session,
    revision: crabber_core::SessionRevision,
) -> crate::AdmitRequest {
    // Use a fresh ordinary request at the captured revision. Reusing the typed
    // fixture's INITIAL revision would reject before reaching the barrier.
    crate::AdmitRequest {
        input_envelope: None,
        additional_messages: Vec::new(),
        expected_revision: Some(revision),
        session_id: Some(session.id.clone()),
        workspace_id: session.workspace_id,
        directory: session.directory,
        title: "blocked next admission".into(),
        config_hash: "next-config".into(),
        plan_fingerprint: "next-plan".into(),
        owner: "next-owner".into(),
        lease: std::time::Duration::from_secs(30),
        user_message: crabber_core::Message {
            id: crabber_core::MessageId::new(),
            session_id: session.id,
            run_id: None,
            role: crabber_core::Role::User,
            parent_id: None,
            parts: Vec::new(),
            created_at: session.created_at,
        },
    }
}
