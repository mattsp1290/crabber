use super::reads;
use crate::public_session::{budget::Budget, v2::proof::Proof};
use crate::transaction::Transaction;
use crabber_core::Run;
use sqlx::Sqlite;

pub(in super::super) async fn accepted_event_count(
    tx: &mut Transaction<'_, Sqlite>,
    run: &Run,
    proof: &mut Proof,
) -> Result<usize, crate::AttemptError> {
    let mut safe = Budget::new(usize::MAX, crate::PublicSessionLimit::HeaderBytes);
    let summaries = super::public::controls(
        tx,
        &run.session_id,
        &run.id,
        run.status,
        64,
        &mut safe,
        proof,
    )
    .await
    .map_err(|_| crate::AttemptError::UnknownStoreOutcome)?;
    let records =
        reads::records_from_summaries(tx, &crate::RunControlTarget::of(run), summaries, proof)
            .await
            .map_err(|_| crate::AttemptError::UnknownStoreOutcome)?;
    crate::attempt::pristine::accepted_control_events(run, &records)
}
