//! Actual completed fixed Initial executions exercise production report/readiness.
use super::*;
use crate::deletion::coverage::CoverageRead;
use crate::native_enrollment::initial_consumer::InitialConsumption;
use crate::public_session::v2::proof::Failure as ProofFailure;
use crabber_core::RunId;

pub(in crate::sqlite) async fn verify(store: &crate::SqliteStore, session: &SessionId) {
    let mut tx = store.begin_write().await.unwrap();
    let id: String = sqlx::query_scalar("SELECT id FROM runs WHERE session_id=$1")
        .bind(&session.0)
        .fetch_one(&mut *tx)
        .await
        .unwrap();
    let id = RunId(id);
    let classify = crate::sqlite::native_enrollment::initial_consumer::classify_selected_run;
    let mut proof = Proof::bounded(MAX_PROOF_BYTES);
    let class = Box::pin(classify(&mut tx, session, &id, &mut proof))
        .await
        .unwrap();
    let used = proof.budget().used;
    let exact = Box::pin(classify(&mut tx, session, &id, &mut Proof::bounded(used))).await;
    let short = Box::pin(classify(
        &mut tx,
        session,
        &id,
        &mut Proof::bounded(used - 1),
    ))
    .await;
    let report = Box::pin(native::runs::verify(
        &mut tx,
        session,
        &id,
        crabber_core::RunStatus::Completed,
        &mut Proof::bounded(MAX_PROOF_BYTES),
    ))
    .await
    .unwrap_or_else(|_| panic!("actual historical report failed"));
    let CoverageRead::Verified(coverage) =
        crate::sqlite::ownership_origin::coverage(&mut tx, session)
            .await
            .unwrap()
    else {
        panic!("actual managed creation coverage");
    };
    let live_resources: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM session_owner_current WHERE session_id=$1 AND json_extract(data_json,'$.snapshot.state')='Active' AND json_type(data_json,'$.resource')='object'"
    ).bind(&session.0).fetch_one(&mut *tx).await.unwrap();
    let ready_budget = Box::pin(readiness_bound(&mut tx, session, &coverage)).await;
    let original_loss = Box::pin(original_loss(&mut tx, session, &coverage)).await;
    let child_loss = Box::pin(child_activation_loss(&mut tx, session, &id, &coverage)).await;
    tx.rollback().await.unwrap();
    assert_eq!(class, InitialConsumption::CompletedSource);
    assert!(matches!(exact, Ok(InitialConsumption::CompletedSource)));
    assert!(matches!(short, Err(ProofFailure::Limited(_))));
    assert_eq!(
        report.initial_source,
        crate::deletion::history::Fact::Authenticated
    );
    assert_eq!(
        report.initial_consumption,
        InitialConsumption::CompletedSource
    );
    assert_eq!(
        report.main_join,
        crate::deletion::history::Fact::Authenticated
    );
    assert_eq!(
        report.resource_policy,
        crate::deletion::history::Fact::Unproven
    );
    assert!(!report.complete());
    assert_eq!(
        live_resources, 1,
        "actual original resource owner remains live"
    );
    assert_eq!(
        ready_budget,
        ("session_owner", "session_owner", "proof_limit"),
        "intact, exact N, and N-minus-one must retain their precise blockers"
    );
    assert_eq!(original_loss.0, 1);
    assert!(
        original_loss.1,
        "production readiness must reach native originals despite Pending rows"
    );
    assert_eq!(child_loss.0, 1);
    assert!(
        child_loss.1,
        "classifier must require the ORIGINAL preparation child Activate"
    );
    assert!(
        child_loss.2,
        "production readiness must reach the missing original child Activate"
    );
}

async fn readiness_bound(
    tx: &mut crate::transaction::Transaction<'_, Sqlite>,
    session: &SessionId,
    coverage: &VerifiedDeletionCoverage,
) -> (&'static str, &'static str, &'static str) {
    let mut proof = Proof::bounded(MAX_PROOF_BYTES);
    let result = Box::pin(check_with_proof(tx, session, coverage, &mut proof)).await;
    let used = proof.budget().used;
    let exact = Box::pin(check_with_proof(
        tx,
        session,
        coverage,
        &mut Proof::bounded(used),
    ))
    .await;
    let short = Box::pin(check_with_proof(
        tx,
        session,
        coverage,
        &mut Proof::bounded(used - 1),
    ))
    .await;
    // Driver retirement does not retire the independently retained preparation
    // resource child. Its authenticated live owner is the earlier exact blocker;
    // deferring the Pending gate must not hide or erase that original lifetime.
    (
        readiness_label(&result),
        readiness_label(&exact),
        readiness_label(&short),
    )
}

fn readiness_label(
    result: &crate::sql_deletion::readiness::Result<crate::sql_deletion::VerifiedScope>,
) -> &'static str {
    match result {
        Ok(_) => "ready",
        Err(Failure::Blocked(DeletionBlocker::SessionOwner)) => "session_owner",
        Err(Failure::Blocked(DeletionBlocker::ProofLimit)) => "proof_limit",
        Err(Failure::Blocked(DeletionBlocker::OwnershipUnknown)) => "ownership_unknown",
        Err(Failure::Blocked(_)) => "other_blocker",
        Err(Failure::Error(DeletionError::UnknownStoreOutcome)) => "unknown_store_outcome",
        Err(Failure::Error(_)) => "other_error",
    }
}

async fn original_loss(
    tx: &mut crate::transaction::Transaction<'_, Sqlite>,
    session: &SessionId,
    coverage: &VerifiedDeletionCoverage,
) -> (u64, bool) {
    let run: String = sqlx::query_scalar("SELECT id FROM runs WHERE session_id=$1")
        .bind(&session.0)
        .fetch_one(&mut **tx)
        .await
        .unwrap();
    let mut proof = Proof::bounded(MAX_PROOF_BYTES);
    let cursor = crate::sqlite::native_enrollment::initial_consumer::original_started_cursor(
        tx,
        session,
        &RunId(run),
        &mut proof,
    )
    .await
    .unwrap();
    sqlx::query("SAVEPOINT classifier_original")
        .execute(&mut **tx)
        .await
        .unwrap();
    let removed = sqlx::query("DELETE FROM events WHERE session_id=$1 AND seq=$2")
        .bind(&session.0)
        .bind(i64::try_from(cursor.0).unwrap())
        .execute(&mut **tx)
        .await
        .unwrap()
        .rows_affected();
    let result = Box::pin(check_with_proof(
        tx,
        session,
        coverage,
        &mut Proof::bounded(MAX_PROOF_BYTES),
    ))
    .await;
    sqlx::query("ROLLBACK TO SAVEPOINT classifier_original")
        .execute(&mut **tx)
        .await
        .unwrap();
    (
        removed,
        matches!(
            result,
            Err(Failure::Error(DeletionError::UnknownStoreOutcome))
        ),
    )
}

/// Delete only the actual preparation child activation Audit. Protected native
/// publications and all birth/consumer copies remain unchanged throughout.
async fn child_activation_loss(
    tx: &mut crate::transaction::Transaction<'_, Sqlite>,
    session: &SessionId,
    run: &RunId,
    coverage: &VerifiedDeletionCoverage,
) -> (u64, bool, bool) {
    let original =
        crate::sqlite::native_enrollment::initial_consumer::original_child_activation_id(
            tx,
            session,
            run,
            &mut Proof::bounded(MAX_PROOF_BYTES),
        )
        .await
        .unwrap();
    sqlx::query("SAVEPOINT classifier_child_activation")
        .execute(&mut **tx)
        .await
        .unwrap();
    let removed =
        sqlx::query("DELETE FROM session_owner_audits WHERE session_id=$1 AND operation_id=$2")
            .bind(&session.0)
            .bind(&original.0)
            .execute(&mut **tx)
            .await
            .unwrap()
            .rows_affected();
    let classified = Box::pin(
        crate::sqlite::native_enrollment::initial_consumer::classify_selected_run(
            tx,
            session,
            run,
            &mut Proof::bounded(MAX_PROOF_BYTES),
        ),
    )
    .await;
    let ready = Box::pin(check_with_proof(
        tx,
        session,
        coverage,
        &mut Proof::bounded(MAX_PROOF_BYTES),
    ))
    .await;
    sqlx::query("ROLLBACK TO SAVEPOINT classifier_child_activation")
        .execute(&mut **tx)
        .await
        .unwrap();
    (
        removed,
        matches!(
            classified,
            Err(ProofFailure::Error(
                crate::PublicSessionError::UnknownStoreOutcome
            ))
        ),
        matches!(
            ready,
            Err(Failure::Error(DeletionError::UnknownStoreOutcome))
        ),
    )
}

/// Use the actual bounded current Run/status and the production report builder;
/// no terminal status or completed-source label is supplied by the fixture.
pub(in crate::sqlite) async fn report_for_fixture(
    tx: &mut crate::transaction::Transaction<'_, Sqlite>,
    session: &SessionId,
    id: &RunId,
    proof: &mut Proof,
) -> crate::sql_deletion::readiness::Result<crate::deletion::history::RunHistoryReport> {
    let selected = crate::sqlite::records::load_run_with_proof(tx, id, proof).await;
    proof.charge(0)?;
    let run = selected.map_err(unknown)?;
    if run.session_id != *session || run.id != *id {
        return Err(unknown("actual report fixture Run route"));
    }
    Box::pin(native::runs::verify(tx, session, id, run.status, proof)).await
}
