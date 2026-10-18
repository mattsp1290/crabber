//! Historical source classification supplies no dispatch or whole-resource authority.
use crate::native_enrollment::{Status, initial_consumer as domain, pending_initial as birth};
use crate::public_session::v2::proof::{Failure, Proof, ReadError, unknown};
use crate::sqlite as backend;
use crate::transaction::Transaction;
use crabber_core::{Run, RunId, SessionId};
use sqlx::Sqlite;

pub(in crate::sqlite) async fn classify_run(
    tx: &mut Transaction<'_, Sqlite>,
    run: &Run,
    proof: &mut Proof,
) -> Result<domain::InitialConsumption, Failure> {
    let status = backend::native_enrollment::original(tx, &run.session_id, &run.id, proof).await?;
    if status != Status::Pending {
        proof.charge(1)?;
        let retained: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM pending_initial_assignments WHERE run_id=$1 OR json_extract(data_json,'$.run_id')=$1 UNION ALL SELECT 1 FROM initial_consumer_assignments WHERE run_id=$1 OR json_extract(data_json,'$.assignment.run_id')=$1)"
        ).bind(&run.id.0).fetch_one(&mut **tx).await.map_err(unknown)?;
        if retained {
            return Err(unknown("Pending assignment without matching native origin"));
        }
        return Ok(domain::InitialConsumption::Unproven);
    }
    let current = Box::pin(backend::native_parent::source_protocol::current(
        tx,
        &run.session_id,
        &run.id,
        proof,
    ))
    .await
    .map_err(ReadError::public)?
    .ok_or_else(|| unknown("Pending classifier current native issuance"))?;
    current.verify(run).map_err(unknown)?;
    let assignment = backend::native_enrollment::pending_initial::assignment_for_run(
        tx,
        &run.session_id,
        &run.id,
        proof,
    )
    .await?;
    let expected = &assignment.correlation;
    let original = backend::native_enrollment::pending_initial::storage::load(
        tx,
        &expected.key,
        &expected.identity.epoch,
        proof,
    )
    .await?;
    let selected = super::storage::load(tx, expected, proof).await?;
    let view = selected.selection(proof)?;
    original.view().verify(&expected.key, proof)?;
    view.verify(expected, proof)?;
    if !view.empty() {
        view.verify_birth(original.view(), proof)?;
    }
    Box::pin(super::members::verify(tx, &view, proof)).await?;
    Box::pin(super::retirement::corroborate(tx, &view, proof)).await?;
    Box::pin(historical(tx, run, expected, proof)).await?;
    domain::classify_closed(&view, original.view(), run, proof)
}

async fn historical(
    tx: &mut Transaction<'_, Sqlite>,
    run: &Run,
    expected: &birth::Correlation,
    proof: &mut Proof,
) -> Result<(), Failure> {
    let original =
        backend::session_owner::drivers::source::original_read(tx, &run.session_id, &run.id, proof)
            .await
            .map_err(ReadError::public)?;
    // original_read authenticates each actual selected publication; its honest
    // legacy fallback is insufficient for this independently protected branch.
    if original.audits.iter().any(|audit| audit.version != 3) {
        return Err(unknown("Pending historical owner protocol"));
    }
    let id = crate::session_owner::owned_scope::source::activation_key(
        &run.session_id,
        &original.audits[4].record.snapshot.target,
    )
    .map_err(unknown)?;
    let activated =
        backend::session_owner::drivers::selected_consumer_audit(tx, &run.session_id, &id, proof)
            .await
            .map_err(ReadError::public)?
            .ok_or_else(|| unknown("original child Activate"))?;
    if activated.version != 3 {
        return Err(unknown("Pending historical Activate protocol"));
    }
    let binding = backend::execution_binding::read_with_proof(tx, &run.session_id, &run.id, proof)
        .await
        .map_err(ReadError::public)?
        .ok_or_else(|| unknown("original initial binding"))?;
    let keyed = if original.init.is_some() {
        Some(
            backend::admission_execution::load_record_with_proof(tx, &run.id, proof)
                .await
                .map_err(ReadError::public)?
                .ok_or_else(|| unknown("original keyed record"))?,
        )
    } else {
        None
    };
    birth::prefix::HistoricalInitialSources {
        session: &original.session,
        initial: &original.initial,
        witness: &original.witness,
        audits: original.audits(),
        activated: &activated,
        binding: &binding,
        keyed: keyed.as_ref().zip(original.init.as_ref()),
    }
    .verify(expected, proof)
}

/// Reports receive a full current Run only after its actual body is charged.
/// Unlike mutation-oriented loaders this leaf preserves budget exhaustion.
pub(in crate::sqlite) async fn classify_selected_run(
    tx: &mut Transaction<'_, Sqlite>,
    session: &SessionId,
    id: &RunId,
    proof: &mut Proof,
) -> Result<domain::InitialConsumption, Failure> {
    backend::read_proof::charge(tx,
        sqlx::query_scalar("SELECT CAST(length(CAST(data AS BLOB)) AS BIGINT) FROM runs WHERE session_id=$1 AND id=$2")
            .bind(&session.0).bind(&id.0), proof,
    ).await?;
    let raw: Option<String> = sqlx::query_scalar(
        "SELECT CASE WHEN length(CAST(data AS BLOB))<=33554432 THEN data END FROM runs WHERE session_id=$1 AND id=$2"
    ).bind(&session.0).bind(&id.0).fetch_optional(&mut **tx).await.map_err(unknown)?.flatten();
    let run: Run = serde_json::from_str(&raw.ok_or_else(|| unknown("selected classifier Run"))?)
        .map_err(unknown)?;
    if run.id != *id || run.session_id != *session {
        return Err(unknown("classifier Run routing"));
    }
    Box::pin(super::classify_run(tx, &run, proof)).await
}

/// Diagnostic selector for the actual captured `RunStarted` identity. Kept within
/// the owning native module so fixtures cannot bypass production leaf privacy.
#[cfg(test)]
pub(in crate::sqlite) async fn original_started_cursor(
    tx: &mut Transaction<'_, Sqlite>,
    session: &SessionId,
    run: &RunId,
    proof: &mut Proof,
) -> Result<crabber_core::EventCursor, Failure> {
    let assignment =
        backend::native_enrollment::pending_initial::assignment_for_run(tx, session, run, proof)
            .await?;
    let selected = super::storage::load(tx, &assignment.correlation, proof).await?;
    selected
        .results
        .iter()
        .find_map(|result| match &result.original {
            domain::Original::Lifecycle(value)
                if value.kind == domain::InitialLifecycleKind::RunStarted =>
            {
                Some(value.cursor)
            }
            _ => None,
        })
        .ok_or_else(|| unknown("actual original RunStarted cursor"))
}

#[cfg(test)]
pub(in crate::sqlite) async fn original_child_activation_id(
    tx: &mut Transaction<'_, Sqlite>,
    session: &SessionId,
    run: &RunId,
    proof: &mut Proof,
) -> Result<crabber_core::SessionOwnerOperationId, Failure> {
    let original = backend::session_owner::drivers::source::original_read(tx, session, run, proof)
        .await
        .map_err(ReadError::public)?;
    crate::session_owner::owned_scope::source::activation_key(
        session,
        &original.audits[4].record.snapshot.target,
    )
    .map_err(unknown)
}
