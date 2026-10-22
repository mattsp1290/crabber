//! Retained native journals make every raw denial assertion non-vacuous.
use super::*;
use crate::InboxKind;
use crabber_core::ManualClock;

pub(super) async fn seed(admin: &PostgresStore) {
    crate::control_contract::run_contract(|clock| admin.clone().with_clock(clock)).await;
    let clock = Arc::new(ManualClock::new(OffsetDateTime::UNIX_EPOCH));
    let timed = admin.clone().with_clock(clock.clone());
    let approval = crate::approval_contract::fixture::staged(&timed, &clock).await;
    approval
        .execution
        .suspend_for_approval(approval.batch, approval.owner)
        .await
        .unwrap();
    crate::tool_continuation_contract::corruption::suspended(&timed).await;
    crate::frontend_contract::fixture::paused(&timed).await;
    // Owned original sources retain the input-publication journals used by
    // successor preparation, beyond the public continuation rows above.
    crate::successor_driver_contract::approval::pending::unprepared(&timed, &clock, false, false)
        .await;
    crate::successor_driver_contract::tools::pending::unprepared(&timed, &clock, 0, false, false)
        .await;

    let fork = crate::fork_pending_contract::fixture(&timed).await;
    // Dormant legacy journal has no current API writer. Match the retained-row
    // deletion fixture using a genuine native source and all constraints enabled.
    sqlx::query("INSERT INTO session_invocations(id,session_id,parent_run_id,parent_call_id,generation,owner,claim_token,lease_until,state,cleanup_state,data_json) VALUES($1,$2,$3,$4,1,'fixture','',0,'Completed','Complete','{}')")
        .bind(uuid::Uuid::new_v4().to_string())
        .bind(&fork.request.source_session_id.0)
        .bind(&fork.run.0)
        .bind(&fork.target.call_id.0)
        .execute(&admin.pool).await.unwrap();
    timed
        .record_phase_cleanup(crate::PhaseCleanup {
            session_id: fork.request.source_session_id.clone(),
            run_id: fork.run,
            id: crabber_core::ControlId::new(),
            expected_revision: timed
                .session_revision(&fork.request.source_session_id)
                .await
                .unwrap(),
            target: fork.target,
            result: crate::PhaseQuiescenceResult::Complete,
            claim: crate::PhaseResolverClaim {
                resolver_id: "isolation-host".into(),
                authority_claim: "source worker joined".into(),
            },
        })
        .await
        .unwrap();
    let mut request = fork.request;
    request.expected_source_revision = timed
        .session_revision(&request.source_session_id)
        .await
        .unwrap();
    assert!(matches!(
        timed.fork_session(request).await.unwrap(),
        crate::ForkOutcome::Applied(_)
    ));

    let admitted = admit(&timed, "foreign-isolation-journal").await;
    timed
        .execution(admitted.fence.clone())
        .await
        .unwrap()
        .put_extension_state(
            "isolation",
            vec![("private".into(), Some("foreign".into()))],
        )
        .await
        .unwrap();
    timed
        .enqueue_inbox(
            &admitted.session.id,
            InboxKind::FollowUp,
            tests::request(&admitted.session.id).user_message,
        )
        .await
        .unwrap();
    timed
        .abandon_run(crate::abandonment_contract::abandon_request(
            &admitted,
            crabber_core::AbandonAuthority::HostStoppedOwner,
        ))
        .await
        .unwrap();
}
