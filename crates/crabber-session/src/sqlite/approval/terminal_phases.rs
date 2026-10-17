//! Walk bounded phase pages; terminal metadata alone cannot authenticate a result.
use super::Error;
use crate::{PhasePageRequest, PhaseQuiescence, PhaseState};
use crabber_core::{Run, SessionRevision};
use sqlx::{Sqlite, Transaction};

pub(super) async fn validate(
    tx: &mut Transaction<'_, Sqlite>,
    run: &Run,
    revision: SessionRevision,
) -> Result<(), Error> {
    validate_with_proof(
        tx,
        run,
        revision,
        &mut crate::public_session::v2::proof::Proof::legacy(),
    )
    .await
}
pub(super) async fn validate_with_proof(
    tx: &mut Transaction<'_, Sqlite>,
    run: &Run,
    revision: SessionRevision,
    proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<(), Error> {
    let mut request = PhasePageRequest {
        session_id: run.session_id.clone(),
        run_id: run.id.clone(),
        limit: 64,
        after: None,
        expected_revision: Some(revision),
    };
    loop {
        let page = super::super::super::phase::metadata_page_with_proof(tx, &request, proof)
            .await
            .map_err(|_| Error::UnknownStoreOutcome)?;
        for snapshot in &page.phases {
            if !matches!(
                snapshot.state,
                PhaseState::Completed | PhaseState::Failed | PhaseState::Suspended
            ) || snapshot.quiescence != PhaseQuiescence::Complete
            {
                return Err(Error::CleanupBlocked);
            }
            let phase = super::super::super::phase::load_with_proof(
                tx,
                &run.id,
                &snapshot.target.id,
                proof,
            )
            .await
            .map_err(|_| Error::UnknownStoreOutcome)?
            .ok_or(Error::UnknownStoreOutcome)?;
            let call = super::super::super::phase::load_call_with_proof(
                tx,
                run,
                &snapshot.target.call_id,
                proof,
            )
            .await
            .map_err(|_| Error::UnknownStoreOutcome)?;
            if phase.snapshot != *snapshot {
                return Err(Error::UnknownStoreOutcome);
            }
            if phase.snapshot.state == PhaseState::Suspended {
                super::super::terminal_history::validate_with_proof(tx, run, &phase, &call, proof)
                    .await?;
            } else {
                crate::approval::validate_termination_phase(run, &phase, &call)?;
                super::super::super::phase::terminal_history::validate_with_proof(
                    tx, run, &phase, &call, proof,
                )
                .await
                .map_err(|_| Error::UnknownStoreOutcome)?;
            }
        }
        let Some(after) = page.next_after else {
            return Ok(());
        };
        request.after = Some(after);
    }
}
