//! The permanent receipt is independent of erased operation history.
use crate::deletion::Tombstone;
use crate::{DeleteSession, DeletionError as Error, DeletionOutcome, DeletionReceipt};
use crabber_core::InputFingerprint;
use time::OffsetDateTime;

pub(crate) fn replay(
    saved: &Tombstone,
    request: &DeleteSession,
    digest: &InputFingerprint,
) -> Result<DeletionOutcome, Error> {
    // Memory callers must reject oversized corruption before verify clones IDs.
    crate::snapshot::encoded_bytes(saved, 8192).ok_or(Error::UnknownStoreOutcome)?;
    saved.verify(&request.session_id)?;
    if saved.receipt.operation_id != request.id {
        return Err(Error::Deleted);
    }
    if &saved.receipt.semantic_digest != digest {
        return Err(Error::SemanticConflict);
    }
    Ok(DeletionOutcome::Replayed(saved.receipt.clone()))
}

pub(crate) fn tombstone(
    request: DeleteSession,
    digest: InputFingerprint,
    now: OffsetDateTime,
) -> Result<Tombstone, Error> {
    let final_revision = request
        .expected_revision
        .next()
        .map_err(|_| Error::RevisionExhausted)?;
    let saved = Tombstone {
        format_version: 1,
        expected_revision: request.expected_revision,
        receipt: DeletionReceipt {
            session_id: request.session_id,
            operation_id: request.id,
            semantic_digest: digest,
            final_revision,
            deleted_at: now,
        },
    };
    saved.verify(&saved.receipt.session_id)?;
    Ok(saved)
}
