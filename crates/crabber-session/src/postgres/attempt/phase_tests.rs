use crate::{AttemptCleanupResult, AttemptCommand, AttemptError, AttemptMutation, Store};
use crabber_core::ControlId;
#[tokio::test]
async fn cleanup_checks_the_independent_phase_quiescence_rows() {
    let Some(url) = crate::postgres::tests::test_url() else {
        return;
    };
    let _guard = crate::postgres::tests::TEST_LOCK.lock().await;
    super::super::PostgresStore::migrate(&url).await.unwrap();
    let store = super::super::PostgresStore::connect(&url).await.unwrap();
    let crate::attempt_contract::phase_fixture::Fixture {
        admitted,
        execution,
        target,
        authority,
        call,
        revision,
    } = crate::attempt_contract::phase_fixture::fixture(&store).await;
    let phase = uuid::Uuid::new_v4().to_string();
    sqlx::query("INSERT INTO tool_phases(id,run_id,call_id,attempt_id,generation,claim_token,state,quiescence_state) VALUES($1,$2,$3,$4,1,$5,'Started','Complete')")
        .bind(&phase).bind(&admitted.run.id.0).bind(&call.0).bind(&target.id.0).bind(uuid::Uuid::new_v4().to_string()).execute(&store.pool).await.unwrap();
    for (state, quiet) in [
        ("Started", "Complete"),
        ("Suspended", "Incomplete"),
        ("Uncertain", "Complete"),
    ] {
        // Seed each independent persisted liveness predicate without dispatching a worker.
        sqlx::query("UPDATE tool_phases SET state=$2,quiescence_state=$3 WHERE id=$1")
            .bind(&phase)
            .bind(state)
            .bind(quiet)
            .execute(&store.pool)
            .await
            .unwrap();
        let request = AttemptMutation {
            id: ControlId::new(),
            expected_revision: revision,
            command: AttemptCommand::RecordCleanup {
                target: target.clone(),
                result: AttemptCleanupResult::Complete,
            },
        };
        let id = request.id.clone();
        assert_eq!(
            execution
                .mutate_attempt(request, authority.clone())
                .await
                .unwrap_err(),
            AttemptError::CleanupBlocked
        );
        assert_eq!(
            store
                .lookup_attempt_mutation(&admitted.session.id, &id)
                .await
                .unwrap(),
            None
        );
        assert_eq!(
            store.session_revision(&admitted.session.id).await.unwrap(),
            revision
        );
    }
}
