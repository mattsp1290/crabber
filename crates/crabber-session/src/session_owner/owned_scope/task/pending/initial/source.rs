use super::super::{
    Error, PendingSuccessorError,
    core::{
        Control, Notice, Replay,
        execute::{Fresh, Prepared, Registration, administrative},
    },
};
use super::{InitialAdmissionMetadata, InitialAdmissionReplay, PendingInitialCompletion};
use crate::public_session::v2::proof::Proof;
use crate::{
    AdmitOutcome, KeyedAdmitOutcome, Store,
    session_owner::{
        driver::RegisterExecutionDriver,
        preparation::{
            JoinedPreparation, PreparationCompletion, PreparationKey, PreparationPurpose,
            PreparedOperation, inspection,
        },
    },
};
use crabber_core::{AdmissionKey, AdmissionReceipt, SessionContext};

pub(in crate::session_owner::owned_scope::task::pending) enum Source {
    Unkeyed,
    Keyed(AdmissionKey),
}
impl Source {
    pub(in crate::session_owner::owned_scope::task::pending) fn new(
        store: &dyn Store,
        context: &SessionContext,
        original: &JoinedPreparation,
    ) -> Result<Self, crate::session_owner::preparation::PreparationCommitError> {
        Self::with_proof(
            store,
            context,
            original,
            &mut Proof::bounded(inspection::MAX_PROOF),
        )
    }
    fn with_proof(
        store: &dyn Store,
        context: &SessionContext,
        original: &JoinedPreparation,
        proof: &mut Proof,
    ) -> Result<Self, crate::session_owner::preparation::PreparationCommitError> {
        let grant = original.observed_grant();
        let reservation = grant.original_reservation();
        if reservation.resource_contract.is_none()
            || original.observed_registration().is_none()
            || !matches!(reservation.purpose, PreparationPurpose::OrdinaryAdmission)
        {
            return Err(Error::Unsupported.into());
        }
        let identity = &reservation.owner.binding;
        if context.session_id() != &identity.session_id
            || context.workspace_id() != identity.workspace_id
            || context.directory() != identity.directory
        {
            return Err(Error::IdentityMismatch.into());
        }
        // All actual remaining cloned scalar authority carriers, before hashing.
        let root = grant.owner_authority();
        let child = grant
            .retained_resources_authority()
            .ok_or(Error::Unsupported)?;
        proof
            .measure(&(
                &root.session_id,
                &root.target,
                &root.token,
                &child.session_id,
                &child.target,
                &child.token,
            ))
            .map_err(|error| {
                proof.admission_failure(
                    &error,
                    crate::session_owner::preparation::PreparationCommitError::Owner(
                        Error::Capacity,
                    ),
                )
            })?;
        match (original.observed_operation(), &reservation.key) {
            (PreparedOperation::Admit(_), PreparationKey::Unkeyed(_)) => {
                if !store.supports_initial_preparation_admission_inspection() {
                    return Err(Error::Unsupported.into());
                }
                inspection::original(original, proof).map_err(|error| match error {
                    crate::session_owner::preparation::PreparationCommitError::Owner(_)
                    | crate::session_owner::preparation::PreparationCommitError::Store(
                        crate::StoreError::AdmissionLimit(_),
                    ) => error,
                    _ => Error::InvalidRequest.into(),
                })?;
                Ok(Self::Unkeyed)
            }
            (PreparedOperation::AdmitKeyed(bound), PreparationKey::Admission(key))
                if key == &bound.keyed.options.key =>
            {
                proof
                    .measure(&(reservation, original.observed_registration(), bound))
                    .map_err(|error| {
                        proof.admission_failure(
                            &error,
                            crate::session_owner::preparation::PreparationCommitError::Owner(
                                Error::Capacity,
                            ),
                        )
                    })?;
                original
                    .verify_original_intent()
                    .map_err(|_| Error::InvalidRequest)?;
                Ok(Self::Keyed(key.clone()))
            }
            _ => Err(Error::Unsupported.into()),
        }
    }
    fn validate_receipt(
        &self,
        context: &SessionContext,
        original: &JoinedPreparation,
        receipt: &InitialAdmissionReplay,
    ) -> Result<(), Error> {
        crate::session_owner::identifier(&receipt.run_id().0, 128, false)
            .map_err(|_| Error::SemanticConflict)?;
        let bounded = match (self, receipt) {
            (Self::Keyed(_), InitialAdmissionReplay::Keyed(value)) => {
                crate::snapshot::encoded_bytes(value, 8192).is_some()
            }
            (Self::Unkeyed, InitialAdmissionReplay::Unkeyed(value)) => {
                let PreparationKey::Unkeyed(operation) =
                    &original.observed_grant().original_reservation().key
                else {
                    return Err(Error::SemanticConflict);
                };
                if &value.operation_id != operation {
                    return Err(Error::SemanticConflict);
                }
                crate::snapshot::encoded_bytes(
                    &(&value.session_id, &value.run_id, &value.operation_id),
                    8192,
                )
                .is_some()
            }
            _ => return Err(Error::SemanticConflict),
        };
        if !bounded || receipt.session_id() != context.session_id() {
            return Err(Error::SemanticConflict);
        }
        Ok(())
    }
    fn admitted<'a>(
        &self,
        completion: &'a PreparationCompletion,
    ) -> Result<&'a AdmitOutcome, Error> {
        match (self, completion) {
            (Self::Unkeyed, PreparationCompletion::Admitted(value)) => Ok(value),
            (
                Self::Keyed(_),
                PreparationCompletion::Keyed(KeyedAdmitOutcome::Started { admitted, .. }),
            ) => Ok(admitted),
            _ => Err(Error::SemanticConflict),
        }
    }
    fn validate_fresh(
        &self,
        context: &SessionContext,
        original: &JoinedPreparation,
        completion: &PreparationCompletion,
    ) -> Result<(), Error> {
        let admitted = self.admitted(completion)?;
        let request = match original.observed_operation() {
            PreparedOperation::Admit(bound) => &bound.request,
            PreparedOperation::AdmitKeyed(bound) => &bound.keyed.request,
            _ => return Err(Error::SemanticConflict),
        };
        crate::session_owner::identifier(&admitted.run.id.0, 128, false)?;
        crate::session_owner::identifier(&admitted.fence.claim_token, 512, false)?;
        if &admitted.session.id != context.session_id()
            || admitted.session.workspace_id != context.workspace_id()
            || admitted.session.directory != context.directory()
            || admitted.run.session_id != admitted.session.id
            || admitted.run.id != admitted.fence.run_id
            || admitted.run.claim_token != admitted.fence.claim_token
            || admitted.run.owner != request.owner
            || admitted.run.config_hash != request.config_hash
            || admitted.run.plan_fingerprint != request.plan_fingerprint
        {
            return Err(Error::SemanticConflict);
        }
        Ok(())
    }
}

pub(in crate::session_owner::owned_scope::task::pending) async fn prepare(
    store: &dyn Store,
    context: SessionContext,
    original: JoinedPreparation,
    source: Source,
    control: &Control,
) -> Result<Prepared, PendingSuccessorError> {
    // Source::new already streamed the full retained argument before this clone.
    // The actual original proof remains here for protected post-Complete inspection.
    let completion = {
        let Some(_call) = control.enter_call()? else {
            return Ok(Prepared::Stopped);
        };
        administrative(async { store.complete_preparation(original.clone()).await }).await?
    };
    if !completion.is_replayed() {
        source.validate_fresh(&context, &original, &completion)?;
    }
    let receipt = {
        let Some(_call) = control.enter_call()? else {
            return Ok(Prepared::Stopped);
        };
        match &source {
            Source::Unkeyed => {
                let receipt = administrative(async {
                    store.inspect_initial_preparation_admission(&original).await
                })
                .await?
                .ok_or(Error::UnknownStoreOutcome)?;
                InitialAdmissionReplay::Unkeyed(receipt)
            }
            Source::Keyed(key) => {
                let receipt = administrative(async {
                    store
                        .lookup_admission(context.session_id(), key)
                        .await
                        .map_err(crate::session_owner::preparation::PreparationCommitError::from)
                })
                .await?
                .ok_or(Error::UnknownStoreOutcome)?;
                validate_keyed(&original, &receipt, &completion)?;
                InitialAdmissionReplay::Keyed(receipt)
            }
        }
    };
    source.validate_receipt(&context, &original, &receipt)?;
    if completion.is_replayed() {
        let valid = match (&receipt, &completion) {
            (
                InitialAdmissionReplay::Unkeyed(actual),
                PreparationCompletion::AdmissionReplayed(ack),
            ) => actual == ack,
            (
                InitialAdmissionReplay::Keyed(actual),
                PreparationCompletion::Keyed(KeyedAdmitOutcome::Replayed(ack)),
            ) => actual == ack,
            _ => false,
        };
        if !valid {
            return Err(Error::SemanticConflict.into());
        }
        return Ok(Prepared::Replayed(Replay::Initial(receipt)));
    }
    let admitted = source.admitted(&completion)?;
    if receipt.run_id() != &admitted.run.id {
        return Err(Error::SemanticConflict.into());
    }
    let request = RegisterExecutionDriver {
        context,
        fence: admitted.fence.clone(),
    };
    Ok(Prepared::Fresh(Box::new(Fresh {
        registration: Registration::Initial(request),
        notice: Notice::Initial(PendingInitialCompletion::Fresh(InitialAdmissionMetadata {
            receipt,
        })),
        completion,
    })))
}
fn validate_keyed(
    original: &JoinedPreparation,
    receipt: &AdmissionReceipt,
    completion: &PreparationCompletion,
) -> Result<(), Error> {
    let PreparedOperation::AdmitKeyed(bound) = original.observed_operation() else {
        return Err(Error::SemanticConflict);
    };
    if crate::snapshot::encoded_bytes(receipt, 8192).is_none()
        || receipt.semantic_digest_version
            != if bound.keyed.request.input_envelope.is_some() {
                2
            } else {
                1
            }
        || receipt.fingerprint != bound.keyed.options.fingerprint
        || receipt.semantic_digest
            != bound
                .keyed
                .semantic_digest()
                .map_err(|_| Error::InvalidRequest)?
    {
        return Err(Error::SemanticConflict);
    }
    let valid = match completion {
        PreparationCompletion::Keyed(KeyedAdmitOutcome::Started { receipt: ack, .. }) => {
            ack == receipt && receipt.user_message_id == bound.keyed.request.user_message.id
        }
        PreparationCompletion::Keyed(KeyedAdmitOutcome::Replayed(ack)) => ack == receipt,
        _ => false,
    };
    if !valid {
        return Err(Error::SemanticConflict);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session_owner::preparation::PreparationCommitError;

    async fn prepared(
        store: &crate::MemoryStore,
        keyed: bool,
    ) -> (SessionContext, JoinedPreparation) {
        use crate::session_owner::preparation_contract::resources;
        let original = if keyed {
            let (root, bound) = resources::declaration::setup(store).await;
            resources::initial_driver_observation(
                store,
                root,
                PreparedOperation::AdmitKeyed(Box::new(bound)),
            )
            .await
        } else {
            resources::initial_unkeyed_observation(store).await
        };
        let binding = &original
            .observed_grant()
            .original_reservation()
            .owner
            .binding;
        let context = SessionContext::new(
            binding.session_id.clone(),
            binding.workspace_id.clone(),
            binding.directory.clone(),
        );
        (context, original)
    }

    #[tokio::test]
    async fn initial_source_preserves_unkeyed_and_keyed_inspection_limits() {
        for keyed in [false, true] {
            let store = crate::MemoryStore::new();
            let (context, original) = prepared(&store, keyed).await;
            let mut authority = Proof::bounded(inspection::MAX_PROOF);
            let grant = original.observed_grant();
            let root = grant.owner_authority();
            let child = grant.retained_resources_authority().unwrap();
            authority
                .measure(&(
                    &root.session_id,
                    &root.target,
                    &root.token,
                    &child.session_id,
                    &child.target,
                    &child.token,
                ))
                .unwrap();
            let cap = authority.used();
            let error = Source::with_proof(&store, &context, &original, &mut Proof::bounded(cap))
                .err()
                .unwrap();
            let PreparationCommitError::Store(error) = error else {
                panic!("source must preserve its own inspection proof limit");
            };
            assert_eq!(
                error.admission_limit().unwrap().limit_bytes(),
                Some(u64::try_from(cap).unwrap())
            );
            assert!(Source::new(&store, &context, &original).is_ok());
        }
    }

    #[tokio::test]
    async fn invalid_unkeyed_binding_retains_original_owner_classification() {
        let store = crate::MemoryStore::new();
        let (context, mut original) = prepared(&store, false).await;
        let PreparedOperation::Admit(bound) = &mut original.operation else {
            unreachable!()
        };
        bound.binding.version = 0;
        assert!(matches!(
            Source::new(&store, &context, &original),
            Err(PreparationCommitError::Owner(Error::InvalidRequest))
        ));
    }
}
