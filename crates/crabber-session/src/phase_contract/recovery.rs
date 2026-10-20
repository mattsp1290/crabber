use super::{
    Error, ManualClock, OffsetDateTime, Outcome, PhaseCommand, PhaseId, PhaseQuiescence,
    PhaseState, PhaseWriteAuthority, Store, applied, fixture, mutation,
};

pub(super) async fn run(store: &impl Store, clock: &ManualClock) {
    // A lost claim is uncertain even if the phase may never have dispatched.
    // The same rule applies after a committed Begin, including retry_safe calls.
    for started in [false, true] {
        clock.set(OffsetDateTime::UNIX_EPOCH);
        let (admitted, execution, owner, claim) = fixture(store).await;
        let (mut last, worker) = applied(
            execution
                .mutate_phase(claim.clone(), Some(PhaseWriteAuthority::Attempt(owner)))
                .await
                .unwrap(),
        );
        let worker = worker.unwrap();
        if started {
            let begin = mutation(
                last.revision,
                PhaseCommand::Begin {
                    target: last.phase.target.clone(),
                },
            );
            last = applied(
                execution
                    .mutate_phase(begin, Some(PhaseWriteAuthority::Phase(worker.clone())))
                    .await
                    .unwrap(),
            )
            .0;
        }
        clock.set(admitted.run.lease_until);
        let replacement = store
            .claim_expired_run(&admitted.run.id, "recovery-host")
            .await
            .unwrap();
        let replacement = store.execution(replacement).await.unwrap();
        assert!(matches!(
            execution.mutate_phase(claim, None).await.unwrap(),
            Outcome::Replayed(_)
        ));
        let expired = mutation(
            last.revision,
            PhaseCommand::MarkExpiredUncertain {
                target: last.phase.target.clone(),
            },
        );
        let (uncertain, _) = applied(replacement.mutate_phase(expired, None).await.unwrap());
        assert_eq!(uncertain.phase.state, PhaseState::Uncertain);
        assert_eq!(uncertain.phase.quiescence, PhaseQuiescence::Pending);
        let begin = mutation(
            uncertain.revision,
            PhaseCommand::Begin {
                target: uncertain.phase.target.clone(),
            },
        );
        assert_eq!(
            replacement
                .mutate_phase(begin, Some(PhaseWriteAuthority::Phase(worker)))
                .await
                .unwrap_err(),
            Error::StaleOwner
        );
        let mut retry = uncertain.phase.target;
        retry.id = PhaseId::new();
        let retry = mutation(uncertain.revision, PhaseCommand::Claim { target: retry });
        // Even with a fresh run fence the original attempt is never reclaimed.
        assert_eq!(
            replacement.mutate_phase(retry, None).await.unwrap_err(),
            Error::StaleOwner
        );
    }
}
