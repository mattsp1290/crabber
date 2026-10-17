//! Capture only the actual rotating v3 writer, before status-only publications.
use crate::public_session::v2::proof::Proof;
use crate::session_owner::preparation::proof::VerifiedPreparation;
use crate::{ToolContinuationError as Error, native_parent::CompletionSource};
use crabber_core::Run;
use sqlx::{Postgres, Transaction};
pub(super) async fn save(
    tx: &mut Transaction<'_, Postgres>,
    run: &Run,
    completion: Option<&CompletionSource>,
    preparation: Option<&VerifiedPreparation<'_>>,
    proof: &mut Proof,
) -> Result<(), Error> {
    let captured = super::super::save_run_captured_with_proof(tx, run, completion, proof)
        .await
        .map_err(|_| Error::UnknownStoreOutcome)?;
    if let Some(preparation) = preparation {
        preparation
            .record_continuation_claim(captured)
            .map_err(|_| Error::UnknownStoreOutcome)?;
    }
    Ok(())
}
