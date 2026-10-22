use super::{
    AttemptControlOutcome, AttemptError, AttemptRetirementOutcome, ControlId, MessageId,
    OffsetDateTime, PartId, PhaseCleanup, PhaseCommand, PhaseControlOutcome, PhaseQuiescenceResult,
    PhaseResolution, PhaseResolutionOutcome, PhaseStart, PhaseUncertainty, PhaseUncertaintyCause,
    PhaseWriteAuthority, RunStatus, Store, UncertainPhaseChoice, fresh, read_calls, retirement,
    uncertainty,
};
pub(super) async fn run(store: &dyn Store) {
    for result in [false, true] {
        for cleanup_first in [false, true] {
            reconcile(store, result, cleanup_first).await;
        }
    }
}
#[allow(clippy::too_many_lines)] // Both result/cleanup orders share one exact lifecycle fixture.
async fn reconcile(store: &dyn Store, failed: bool, cleanup_first: bool) {
    let (admitted, execution, owner, planned) = crate::phase_contract::fixture(store).await;
    let (claimed, worker) = crate::phase_contract::applied(
        execution
            .mutate_phase(planned, Some(PhaseWriteAuthority::Attempt(owner)))
            .await
            .unwrap(),
    );
    let (started, _) = crate::phase_contract::applied(
        execution
            .start_tool_phase(
                PhaseStart {
                    begin: crate::phase_contract::mutation(
                        claimed.revision,
                        PhaseCommand::Begin {
                            target: claimed.phase.target.clone(),
                        },
                    ),
                    turn_id: None,
                    created_at: OffsetDateTime::UNIX_EPOCH,
                },
                worker,
            )
            .await
            .unwrap(),
    );
    let request = uncertainty(
        &admitted,
        started.phase.target.attempt.clone(),
        started.revision,
    );
    let marked = fresh(store.mark_attempt_uncertain(request.clone()).await.unwrap());
    assert_eq!(
        store
            .tool_phase(&request.run_id, &started.phase.target.id)
            .await
            .unwrap()
            .phase,
        started.phase
    );
    assert_eq!(
        store
            .retire_attempt(retirement(&request, marked.revision))
            .await
            .unwrap_err(),
        AttemptError::CleanupBlocked
    );
    let phase_request = PhaseUncertainty {
        session_id: request.session_id.clone(),
        run_id: request.run_id.clone(),
        id: ControlId::new(),
        expected_revision: marked.revision,
        target: started.phase.target,
        cause: PhaseUncertaintyCause::ExpiredOrRevokedOwner,
    };
    let PhaseControlOutcome::Applied(uncertain) = store
        .mark_phase_uncertain(phase_request.clone())
        .await
        .unwrap()
    else {
        panic!("phase uncertainty after parent revocation");
    };
    let mut revision = uncertain.revision;
    if cleanup_first {
        let PhaseControlOutcome::Applied(cleaned) = store
            .record_phase_cleanup(PhaseCleanup {
                session_id: request.session_id.clone(),
                run_id: request.run_id.clone(),
                id: ControlId::new(),
                expected_revision: revision,
                target: phase_request.target.clone(),
                result: PhaseQuiescenceResult::Complete,
                claim: crate::phase_contract::resolver(),
            })
            .await
            .unwrap()
        else {
            panic!("cleanup");
        };
        revision = cleaned.revision;
    }
    assert_eq!(
        store
            .retire_attempt(retirement(&request, revision))
            .await
            .unwrap_err(),
        AttemptError::CleanupBlocked
    );
    let resolution = PhaseResolution {
        session_id: request.session_id.clone(),
        run_id: request.run_id.clone(),
        resolution: crate::phase_contract::mutation(
            revision,
            PhaseCommand::ResolveUncertain {
                target: phase_request.target.clone(),
                claim: crate::phase_contract::resolver(),
                choice: if failed {
                    UncertainPhaseChoice::Failure { output: None }
                } else {
                    UncertainPhaseChoice::Success {
                        output: crate::phase_contract::output(),
                    }
                },
            },
        ),
        message_id: MessageId::new(),
        part_id: PartId::new(),
        turn_id: None,
        created_at: OffsetDateTime::UNIX_EPOCH,
        duration_ms: 0,
    };
    let PhaseResolutionOutcome::Applied(resolved) =
        store.resolve_uncertain_phase(resolution).await.unwrap()
    else {
        panic!("known result");
    };
    revision = resolved.revision;
    if !cleanup_first {
        assert_eq!(
            store
                .retire_attempt(retirement(&request, revision))
                .await
                .unwrap_err(),
            AttemptError::CleanupBlocked
        );
        let PhaseControlOutcome::Applied(cleaned) = store
            .record_phase_cleanup(PhaseCleanup {
                session_id: request.session_id.clone(),
                run_id: request.run_id.clone(),
                id: ControlId::new(),
                expected_revision: revision,
                target: phase_request.target,
                result: PhaseQuiescenceResult::Complete,
                claim: crate::phase_contract::resolver(),
            })
            .await
            .unwrap()
        else {
            panic!("cleanup");
        };
        revision = cleaned.revision;
    }
    let before = read_calls(store, &admitted.session.id).await;
    let mut retire = retirement(&request, revision);
    retire.outcome = AttemptRetirementOutcome::Failed;
    let settled = fresh(store.retire_attempt(retire.clone()).await.unwrap());
    assert_eq!(
        store.retire_attempt(retire).await.unwrap(),
        AttemptControlOutcome::Replayed(settled)
    );
    assert_eq!(read_calls(store, &admitted.session.id).await, before);
    assert_eq!(
        store
            .get_run(&request.run_id)
            .await
            .unwrap()
            .unwrap()
            .status,
        RunStatus::Failed
    );
}
