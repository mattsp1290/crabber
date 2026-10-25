use super::{Prepared, descriptor, evidence, fresh, mixed_output, prepare};
use crate::{
    PhaseCommand, PhaseError, PhaseQuiescence, PhaseResolutionOutcome, Store, UncertainPhaseChoice,
};
use crabber_core::{ControlId, PhaseId, RunId, SessionId, ToolCallId};

pub(super) async fn run(store: &dyn Store) {
    let f = prepare(
        store,
        UncertainPhaseChoice::Success {
            output: mixed_output(),
        },
        PhaseQuiescence::Pending,
        true,
    )
    .await;
    wrong_scope(store, &f).await;
    let receipt = fresh(
        store
            .resolve_uncertain_phase(f.request.clone())
            .await
            .unwrap(),
    );
    let committed = evidence(store, &f.request).await;
    for variant in 0..3 {
        let mut changed = f.request.clone();
        let PhaseCommand::ResolveUncertain { claim, choice, .. } = &mut changed.resolution.command
        else {
            unreachable!()
        };
        match variant {
            0 => *choice = UncertainPhaseChoice::Failure { output: None },
            1 => claim.authority_claim = "changed authority assertion".into(),
            _ => {
                *choice = UncertainPhaseChoice::Success {
                    output: crabber_core::ToolOutput::from_json(&serde_json::json!(
                        "changed output"
                    ))
                    .unwrap(),
                };
            }
        }
        assert_eq!(
            store.resolve_uncertain_phase(changed).await.unwrap_err(),
            PhaseError::SemanticConflict
        );
        assert_eq!(evidence(store, &f.request).await, committed);
    }
    assert_eq!(
        f.execution
            .mutate_phase(f.request.resolution.clone(), None)
            .await
            .unwrap_err(),
        PhaseError::SemanticConflict
    );
    let mut fresh_key = f.request.clone();
    fresh_key.resolution.id = ControlId::new();
    fresh_key.resolution.expected_revision = receipt.revision;
    assert_eq!(
        store.resolve_uncertain_phase(fresh_key).await.unwrap_err(),
        PhaseError::InvalidTransition
    );
    // Domain conflict in the other direction, reusing an earlier metadata key.
    let call = crate::phase_contract::mutation(
        receipt.revision,
        PhaseCommand::RecordQuiescence {
            target: receipt.phase.target.clone(),
            result: crate::PhaseQuiescenceResult::Complete,
            host_claim: Some(crate::phase_contract::resolver()),
        },
    );
    let metadata = f.execution.mutate_phase(call.clone(), None).await.unwrap();
    let metadata = crate::phase_contract::applied(metadata).0;
    let mut conflict = f.request.clone();
    conflict.resolution.id = call.id;
    conflict.resolution.expected_revision = metadata.revision;
    assert_eq!(
        store.resolve_uncertain_phase(conflict).await.unwrap_err(),
        PhaseError::SemanticConflict
    );
    // Later cleanup does not invalidate the historical resolution receipt.
    assert_eq!(
        store
            .resolve_uncertain_phase(f.request.clone())
            .await
            .unwrap(),
        PhaseResolutionOutcome::Replayed(receipt)
    );
    // The old phase worker cannot dispatch or rewrite its host-selected result.
    let begin = crate::phase_contract::mutation(
        metadata.revision,
        PhaseCommand::Begin {
            target: f.request.resolution.command.target().clone(),
        },
    );
    assert!(
        f.execution
            .mutate_phase(begin, Some(crate::PhaseWriteAuthority::Phase(f.worker)))
            .await
            .is_err()
    );
    races(store).await;
    non_uncertain(store).await;
}
async fn wrong_scope(store: &dyn Store, f: &Prepared) {
    let before = evidence(store, &f.request).await;
    // Malformed exact identities must not change any durable evidence.
    for variant in 0..7 {
        let mut changed = f.request.clone();
        match variant {
            0 => changed.session_id = SessionId::new(),
            1 => changed.run_id = RunId::new(),
            2 => {
                changed.resolution.expected_revision =
                    changed.resolution.expected_revision.next().unwrap();
            }
            _ => {
                let PhaseCommand::ResolveUncertain { target, .. } = &mut changed.resolution.command
                else {
                    unreachable!()
                };
                match variant {
                    3 => target.id = PhaseId::new(),
                    4 => target.call_id = ToolCallId::new(),
                    5 => target.generation = target.generation.next().unwrap(),
                    _ => target.attempt.generation = target.attempt.generation.next().unwrap(),
                }
            }
        }
        assert!(store.resolve_uncertain_phase(changed).await.is_err());
        assert_eq!(evidence(store, &f.request).await, before);
    }
    // Raw metadata resolution is no longer an alternate success path.
    assert_eq!(
        f.execution
            .mutate_phase(f.request.resolution.clone(), None)
            .await
            .unwrap_err(),
        PhaseError::InvalidRequest
    );
    assert_eq!(evidence(store, &f.request).await, before);
}
async fn races(store: &dyn Store) {
    let f = prepare(
        store,
        UncertainPhaseChoice::Success {
            output: mixed_output(),
        },
        PhaseQuiescence::Pending,
        true,
    )
    .await;
    let (a, b) = futures::join!(
        store.resolve_uncertain_phase(f.request.clone()),
        store.resolve_uncertain_phase(f.request.clone())
    );
    let outcomes = [a.unwrap(), b.unwrap()];
    assert_eq!(
        outcomes
            .iter()
            .filter(|r| matches!(r, PhaseResolutionOutcome::Applied(_)))
            .count(),
        1
    );
    assert_eq!(
        outcomes
            .iter()
            .filter(|r| matches!(r, PhaseResolutionOutcome::Replayed(_)))
            .count(),
        1
    );
    let f = prepare(
        store,
        UncertainPhaseChoice::Success {
            output: mixed_output(),
        },
        PhaseQuiescence::Pending,
        true,
    )
    .await;
    let mut other = f.request.clone();
    other.resolution.id = ControlId::new();
    if let PhaseCommand::ResolveUncertain { choice, .. } = &mut other.resolution.command {
        *choice = UncertainPhaseChoice::Failure { output: None };
    }
    let (a, b) = futures::join!(
        store.resolve_uncertain_phase(f.request),
        store.resolve_uncertain_phase(other)
    );
    assert_eq!([&a, &b].iter().filter(|r| r.is_ok()).count(), 1);
    assert!(
        [a, b]
            .into_iter()
            .any(|r| matches!(r, Err(PhaseError::RevisionConflict)))
    );
}
async fn non_uncertain(store: &dyn Store) {
    let (admitted, execution, owner, claim) = crate::phase_contract::fixture(store).await;
    let (claimed, worker) = crate::phase_contract::applied(
        execution
            .mutate_phase(claim, Some(crate::PhaseWriteAuthority::Attempt(owner)))
            .await
            .unwrap(),
    );
    let command = crate::phase_contract::mutation(
        claimed.revision,
        PhaseCommand::ResolveUncertain {
            target: claimed.phase.target.clone(),
            claim: crate::phase_contract::resolver(),
            choice: UncertainPhaseChoice::Refuse,
        },
    );
    let mut request = descriptor(admitted.session.id, admitted.run.id, command);
    let before = evidence(store, &request).await;
    assert_eq!(
        store
            .resolve_uncertain_phase(request.clone())
            .await
            .unwrap_err(),
        PhaseError::InvalidTransition
    );
    assert_eq!(evidence(store, &request).await, before);
    let begun = crate::phase_contract::applied(
        execution
            .mutate_phase(
                crate::phase_contract::mutation(
                    claimed.revision,
                    PhaseCommand::Begin {
                        target: claimed.phase.target,
                    },
                ),
                Some(crate::PhaseWriteAuthority::Phase(worker.unwrap())),
            )
            .await
            .unwrap(),
    )
    .0;
    request.resolution.expected_revision = begun.revision;
    let before = evidence(store, &request).await;
    assert_eq!(
        store
            .resolve_uncertain_phase(request.clone())
            .await
            .unwrap_err(),
        PhaseError::InvalidTransition
    );
    assert_eq!(evidence(store, &request).await, before);
}
