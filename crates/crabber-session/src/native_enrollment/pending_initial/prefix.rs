//! Historical original preparation sources, independent of current eligibility.
//! A verified prefix is transaction-local corroboration, never a task join.
#[path = "prefix/historical.rs"]
mod historical;
use super::{Correlation, Identity, SourceKey};
use crate::native_enrollment::caller::initial::{SelectedOriginal, input_preparation};
use crate::public_session::v2::proof::{Failure, Proof};
use crate::session_owner::{
    SessionOwnerError as OwnerError, SessionOwnerOperation as Operation,
    audit::{self, Audit},
    owned_scope,
    preparation::{self, JoinedPreparation, PreparationCommitError, PreparedOperation},
};
use crabber_core::{InputFingerprint, SessionOwnerOperationId};
pub(crate) use historical::HistoricalInitialSources;
use serde::Serialize;
use sha2::{Digest, Sha256};

type Result<T> = std::result::Result<T, PreparationCommitError>;

pub(crate) struct PrefixIds {
    pub root_reserve: SessionOwnerOperationId,
    pub root_begin: SessionOwnerOperationId,
    pub child_reserve: SessionOwnerOperationId,
    pub child_begin: SessionOwnerOperationId,
    pub activated: SessionOwnerOperationId,
}

/// These must be independently selected protected originals under the native
/// gate. SQL adapters also require their protected protocol membership; an
/// Audit version or a current owner projection alone cannot supply that fact.
#[derive(Clone, Copy)]
pub(crate) struct InitialPrefixAudits<'a> {
    pub root_reserve: &'a Audit,
    pub root_begin: &'a Audit,
    pub child_reserve: &'a Audit,
    pub child_begin: &'a Audit,
    pub activated: &'a Audit,
}

/// Pre-I/O correlation computed from the retained original. This contains no
/// native eligibility or join permission and is not an accounting certificate.
pub(crate) struct ExpectedSource {
    key: SourceKey,
    prefix_digest: InputFingerprint,
    intent_digest: InputFingerprint,
}
impl ExpectedSource {
    pub(crate) fn correlation(
        &self,
        identity: &Identity,
        proof: &mut Proof,
    ) -> Result<Correlation> {
        identity.validate().map_err(|error| failure(&error))?;
        measure(
            proof,
            &(
                identity,
                &self.key,
                &self.prefix_digest,
                &self.intent_digest,
            ),
        )?;
        Ok(Correlation {
            identity: identity.clone(),
            key: self.key.clone(),
            prefix_digest: self.prefix_digest.clone(),
            intent_digest: self.intent_digest.clone(),
        })
    }
}

pub(crate) struct HistoricalInitialPrefix<'a> {
    original: &'a JoinedPreparation,
    _audits: InitialPrefixAudits<'a>,
    expected: ExpectedSource,
}
impl HistoricalInitialPrefix<'_> {
    #[cfg(test)]
    pub(crate) fn key(&self) -> &SourceKey {
        &self.expected.key
    }
    pub(crate) fn original(&self) -> &JoinedPreparation {
        self.original
    }
    pub(crate) fn correlation(
        &self,
        identity: &Identity,
        proof: &mut Proof,
    ) -> Result<Correlation> {
        self.expected.correlation(identity, proof)
    }
}

pub(crate) fn select(original: &JoinedPreparation, proof: &mut Proof) -> Result<PrefixIds> {
    select_selected(original, None, proof)
}

pub(crate) fn select_selected(
    original: &JoinedPreparation,
    selected: Option<&SelectedOriginal<'_>>,
    proof: &mut Proof,
) -> Result<PrefixIds> {
    let grant = original.observed_grant();
    let registration = original
        .observed_registration()
        .ok_or(OwnerError::Unsupported)?;
    let reservation = grant.original_reservation();
    // Reconciliation validates/hashes the actual reservation before selecting
    // IDs; charge that input and every scalar that will be copied first.
    match selected {
        Some(selected) => selected
            .verify(original, proof)
            .map_err(|error| failure(&error))?,
        None => measure(proof, &(reservation, registration))?,
    }
    let root = reservation.reconciliation()?;
    measure(
        proof,
        &(
            &root.reserve_id,
            &root.begin_id,
            &registration.source.reserve,
            &registration.source.begin,
            &registration.id,
        ),
    )?;
    Ok(PrefixIds {
        root_reserve: root.reserve_id,
        root_begin: root.begin_id,
        child_reserve: registration.source.reserve.clone(),
        child_begin: registration.source.begin.clone(),
        activated: registration.id.clone(),
    })
}

pub(crate) fn prepare(original: &JoinedPreparation, proof: &mut Proof) -> Result<ExpectedSource> {
    prepare_selected(original, None, proof)
}

fn prepare_selected(
    original: &JoinedPreparation,
    selected: Option<&SelectedOriginal<'_>>,
    proof: &mut Proof,
) -> Result<ExpectedSource> {
    input_preparation(original, selected, proof).map_err(|error| failure(&error))?;
    let grant = original.observed_grant();
    let reservation = grant.original_reservation();
    let registration = original
        .observed_registration()
        .ok_or(OwnerError::Unsupported)?;
    let child = grant
        .retained_resources_authority()
        .ok_or(OwnerError::Unsupported)?;
    if reservation.resource_contract.is_none()
        || !matches!(
            reservation.purpose,
            preparation::PreparationPurpose::OrdinaryAdmission
        )
    {
        return Err(OwnerError::Unsupported.into());
    }
    original.verify_original_intent()?;
    let intent_digest = match original.observed_operation() {
        PreparedOperation::Admit(bound) => hash(&(
            "crabber.pending-initial.unkeyed-intent.v1",
            &bound.request,
            &bound.binding,
        ))?,
        PreparedOperation::AdmitKeyed(bound) => {
            hash(&("crabber.pending-initial.keyed-intent.v1", bound))?
        }
        _ => return Err(OwnerError::Unsupported.into()),
    };
    let root = grant.owner_authority();
    let prefix_digest = hash(&(
        "crabber.pending-initial.original-prefix.v1",
        reservation,
        (&root.session_id, &root.target, &root.token),
        (&child.session_id, &child.target, &child.token),
        registration,
    ))?;
    let key = SourceKey::expected(original, proof)?;
    Ok(ExpectedSource {
        key,
        prefix_digest,
        intent_digest,
    })
}

pub(crate) fn expected(
    original: &JoinedPreparation,
    identity: &Identity,
    proof: &mut Proof,
) -> Result<Correlation> {
    prepare(original, proof)?.correlation(identity, proof)
}

#[cfg(test)]
pub(crate) fn verify<'a>(
    original: &'a JoinedPreparation,
    audits: InitialPrefixAudits<'a>,
    proof: &mut Proof,
) -> Result<HistoricalInitialPrefix<'a>> {
    verify_selected(original, audits, None, proof)
}

pub(crate) fn verify_selected<'a>(
    original: &'a JoinedPreparation,
    audits: InitialPrefixAudits<'a>,
    selected: Option<&SelectedOriginal<'_>>,
    proof: &mut Proof,
) -> Result<HistoricalInitialPrefix<'a>> {
    let expected = prepare_selected(original, selected, proof)?;
    for audit in [
        audits.root_reserve,
        audits.root_begin,
        audits.child_reserve,
        audits.child_begin,
        audits.activated,
    ] {
        measure(proof, audit)?;
        audit.validate()?;
        if !matches!(audit.version, 2 | 3) || audit.publication.is_none() {
            return Err(OwnerError::Unsupported.into());
        }
    }
    verify_root(original, audits)?;
    verify_child(original, audits)?;
    Ok(HistoricalInitialPrefix {
        original,
        _audits: audits,
        expected,
    })
}

fn verify_root(original: &JoinedPreparation, audits: InitialPrefixAudits<'_>) -> Result<()> {
    let grant = original.observed_grant();
    let reservation = grant.original_reservation();
    let authority = grant.owner_authority();
    let root = &audits.root_begin.record;
    audit::verify_current(root, audits.root_reserve, audits.root_begin)?;
    let reconciliation = reservation.reconciliation()?;
    let source = owned_scope::OwnedScopeSource::from_receipts(
        &audits.root_reserve.receipt,
        &audits.root_begin.receipt,
    )?;
    if source.reserve != reconciliation.reserve_id
        || source.begin != reconciliation.begin_id
        || source.reserve_digest != reservation.fingerprint()?
        || root.snapshot.binding != reservation.owner.binding
        || root.preparation.as_ref() != Some(&preparation::origin(reservation))
        || !authority.authenticates(
            &root.snapshot.binding.session_id,
            &root.snapshot.target,
            &root.token,
        )
    {
        return Err(OwnerError::SemanticConflict.into());
    }
    // Reserve may predate a legitimate Reclaim. The exact original Begin and
    // private current-generation carrier agree; their Reserve generations need
    // not be equal. No present lease/current-owner check belongs in this reader.
    Ok(())
}

fn verify_child(original: &JoinedPreparation, audits: InitialPrefixAudits<'_>) -> Result<()> {
    let grant = original.observed_grant();
    let authority = grant
        .retained_resources_authority()
        .ok_or(OwnerError::Unsupported)?;
    let registered = original
        .observed_registration()
        .ok_or(OwnerError::Unsupported)?;
    let child = &audits.child_begin.record;
    audit::verify_current(child, audits.child_reserve, audits.child_begin)?;
    preparation::resource_source::verify(child, audits.root_reserve, audits.root_begin)?;
    let link = preparation::PreparationResourceLink::for_grant(grant)?;
    if child.resource.as_ref() != Some(&link)
        || audits.child_begin.receipt.operation != Operation::Begin
        || audits.activated.record.owned_scope.as_ref() != Some(registered)
    {
        return Err(OwnerError::SemanticConflict.into());
    }
    for record in [child, &audits.activated.record] {
        if !authority.authenticates(
            &record.snapshot.binding.session_id,
            &record.snapshot.target,
            &record.token,
        ) {
            return Err(OwnerError::SemanticConflict.into());
        }
    }
    owned_scope::source::current(
        &audits.activated.record,
        audits.child_reserve,
        audits.child_begin,
        Some(audits.activated),
    )?;
    Ok(())
}

fn measure(proof: &mut Proof, value: &impl Serialize) -> Result<()> {
    proof.measure(value).map_err(|error| failure(&error))
}
fn failure(error: &Failure) -> PreparationCommitError {
    match error {
        Failure::Limited(_) => OwnerError::Capacity,
        Failure::Error(_) => OwnerError::UnknownStoreOutcome,
    }
    .into()
}

/// Stream the already measured exact carrier; do not allocate another large
/// JSON tree merely to retain its pre-I/O correlation.
fn hash(value: &impl Serialize) -> Result<InputFingerprint> {
    struct Writer(Sha256);
    impl std::io::Write for Writer {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.update(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut writer = Writer(Sha256::new());
    serde_json::to_writer(&mut writer, value).map_err(|_| OwnerError::InvalidRequest)?;
    InputFingerprint::new(format!("{:x}", writer.0.finalize()))
        .map_err(|_| OwnerError::InvalidRequest.into())
}
