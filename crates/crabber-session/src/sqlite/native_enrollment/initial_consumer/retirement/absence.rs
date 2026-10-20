//! Authenticate every original Session Driver before excluding unrelated epochs.
use super::{Failure, Proof, Sqlite, Transaction, domain, unknown};
pub(super) async fn verify(
    tx: &mut Transaction<'_, Sqlite>,
    view: &domain::Selection<'_>,
    proof: &mut Proof,
) -> Result<(), Failure> {
    let correlation = view
        .origin
        .map(domain::Origin::correlation)
        .or_else(|| view.assignment.map(domain::AssignmentLink::correlation));
    // Fresh NoOpenedMember has no origin yet: its expected route is supplied by
    // the genuine retainer, not inferred from absence.
    let correlation = correlation.ok_or_else(|| unknown("NoDriver expected source route"))?;
    verify_route(tx, view, correlation, proof).await
}
pub(in super::super) async fn verify_route(
    tx: &mut Transaction<'_, Sqlite>,
    view: &domain::Selection<'_>,
    expected: &domain::Correlation,
    proof: &mut Proof,
) -> Result<(), Failure> {
    if let Some(assignment) = view.assignment {
        assigned(tx, &expected.key.session_id, assignment.run_id(), proof).await?;
    }
    let records =
        crate::sqlite::session_owner::initial_driver_roster(tx, &expected.key.session_id, proof)
            .await
            .map_err(super::owner_failure)?;
    for record in records {
        let Some(driver) = record.driver.as_deref() else {
            continue;
        };
        let original = Box::pin(
            crate::sqlite::session_owner::drivers::source::original_read(
                tx,
                &expected.key.session_id,
                driver.run_id(),
                proof,
            ),
        )
        .await
        .map_err(crate::public_session::v2::proof::ReadError::public)?;
        original.verify_link(driver).map_err(unknown)?;
        let status = Box::pin(crate::sqlite::native_enrollment::original(
            tx,
            &expected.key.session_id,
            driver.run_id(),
            proof,
        ))
        .await?;
        if status == crate::native_enrollment::Status::Pending {
            let actual = Box::pin(
                crate::sqlite::native_enrollment::pending_initial::assignment_for_run(
                    tx,
                    &expected.key.session_id,
                    driver.run_id(),
                    proof,
                ),
            )
            .await?;
            if actual.correlation == *expected {
                return Err(unknown(
                    "NoDriver conflicts with actual current-epoch Driver",
                ));
            }
        } else if view
            .assignment
            .is_some_and(|value| value.run_id() == driver.run_id())
        {
            return Err(unknown(
                "NoDriver current Run lacks original Pending origin",
            ));
        }
    }
    // A protected pending origin cannot survive a missing selected assignment.
    proof.charge(1)?;
    let orphan: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM native_execution_origins WHERE pending_epoch=$1)",
    )
    .bind(&expected.identity.epoch.0)
    .fetch_one(&mut **tx)
    .await
    .map_err(unknown)?;
    if orphan && view.assignment.is_none() {
        return Err(unknown(
            "NoDriver missing selected birth/consumer assignment",
        ));
    }
    Ok(())
}

async fn assigned(
    tx: &mut Transaction<'_, Sqlite>,
    session: &crabber_core::SessionId,
    run: &crabber_core::RunId,
    proof: &mut Proof,
) -> Result<(), Failure> {
    proof.measure(&(session, run))?;
    let hash = crate::session_owner::validation::digest(&(
        "crabber.execution-driver.initial.identity.v1",
        session,
        run,
    ))
    .map_err(unknown)?;
    proof.charge(1)?;
    let present:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM session_owner_current WHERE owner_id='driver-owner-'||$1 UNION ALL SELECT 1 FROM session_owner_audits WHERE owner_id='driver-owner-'||$1 OR operation_id IN ('driver-reserve-'||$1,'driver-begin-'||$1) UNION ALL SELECT 1 FROM session_owner_publications WHERE owner_id='driver-owner-'||$1 OR operation_id IN ('driver-reserve-'||$1,'driver-begin-'||$1) UNION ALL SELECT 1 FROM session_owner_publication_cohorts WHERE owner_id='driver-owner-'||$1)").bind(hash.as_str()).fetch_one(&mut **tx).await.map_err(unknown)?;
    if present {
        return Err(unknown("NoDriver actual assigned Driver original exists"));
    }
    Ok(())
}
