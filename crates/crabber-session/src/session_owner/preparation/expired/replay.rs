//! Bounded immutable source reconciliation; never current Run or authority.
use super::{Error, ExpiredRunOrigin};
use crate::native_parent::RunIssuance;
use crabber_core::{EventRecord, RunStatus};
pub(crate) enum Native<'a> {
    Unkeyed(&'a super::unkeyed::Original),
    Suspended(&'a crate::AttemptReceipt, &'a EventRecord),
    Initial {
        init: &'a crate::admission_journal::Init,
        begin: &'a crate::admission_journal::Begin,
        receipt: &'a crabber_core::AdmissionReceipt,
        initial: &'a RunIssuance,
        begun: &'a RunIssuance,
    },
}
pub(super) fn verify_native(
    session: &crabber_core::SessionId,
    run: &crabber_core::RunId,
    target: &super::ExpiredRunTarget,
    status: RunStatus,
    native: Native<'_>,
) -> Result<(), Error> {
    let digest = match (&target.origin, native) {
        (ExpiredRunOrigin::UnkeyedAdmission(id), Native::Unkeyed(original)) => {
            if status != RunStatus::Running
                || original.witness.session_id() != session
                || original.witness.run_id() != run
                || original.witness.join_id()? != *id
            {
                return Err(Error::SemanticConflict);
            }
            original.digest()?
        }

        (ExpiredRunOrigin::Suspended(id), Native::Suspended(receipt, event)) => {
            if crate::snapshot::encoded_bytes(event, 65536).is_none()
                || crate::snapshot::encoded_bytes(receipt, 8192).is_none()
            {
                return Err(Error::UnknownStoreOutcome);
            }
            crate::attempt::verify_receipt(receipt, event).map_err(unknown)?;
            if status != RunStatus::Paused
                || receipt.operation_id != *id
                || receipt.attempt.session_id != *session
                || receipt.attempt.run_id != *run
                || receipt.attempt.state != crate::AttemptState::Suspended
                || receipt.attempt.cleanup != crate::AttemptCleanup::Complete
            {
                return Err(Error::SemanticConflict);
            }
            crate::session_owner::digest(&(
                "crabber.preparation.expired-suspension.v1",
                &receipt.semantic_digest,
                receipt,
            ))?
        }
        (
            ExpiredRunOrigin::KeyedInitial(key),
            Native::Initial {
                init,
                begin,
                receipt,
                initial,
                begun,
            },
        ) => {
            if status != RunStatus::Running || init.driver_key() != key {
                return Err(Error::SemanticConflict);
            }
            for issuer in [initial, begun] {
                if crate::snapshot::encoded_bytes(issuer, 2048).is_none() {
                    return Err(Error::UnknownStoreOutcome);
                }
                issuer.validate().map_err(unknown)?;
            }
            if crate::snapshot::encoded_bytes(init, 8192).is_none()
                || crate::snapshot::encoded_bytes(begin, 8192).is_none()
                || crate::snapshot::encoded_bytes(receipt, 8192).is_none()
            {
                return Err(Error::UnknownStoreOutcome);
            }
            init.verify_driver_origin(receipt, initial)
                .map_err(unknown)?;
            begin.verify(init).map_err(unknown)?;
            if &begin.issuance != begun
                || initial.state.session_id != *session
                || initial.state.run_id != *run
            {
                return Err(Error::SemanticConflict);
            }
            crate::session_owner::digest(&(
                "crabber.preparation.expired-initial.v1",
                init.digest().map_err(unknown)?,
            ))?
        }
        _ => return Err(Error::SemanticConflict),
    };
    if digest != target.native_source {
        return Err(Error::SemanticConflict);
    }
    Ok(())
}
fn unknown(_: impl std::fmt::Debug) -> Error {
    Error::UnknownStoreOutcome
}
