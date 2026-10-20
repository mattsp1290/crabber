use super::{
    Arc, Error, JoinedPreparation, MAX_PROOF, PreparationCommitError, Proof, SessionContext,
};
use crate::native_enrollment::pending_initial::prefix;

pub(super) struct PreparedSource {
    pub(super) source: Arc<RetainedInitialSource>,
    pub(super) expected: prefix::ExpectedSource,
    pub(super) proof: Proof,
}
impl PreparedSource {
    pub(super) fn new(
        context: SessionContext,
        original: JoinedPreparation,
    ) -> Result<Self, PreparationCommitError> {
        Self::bounded(context, original, MAX_PROOF).0
    }
    pub(super) fn bounded(
        context: SessionContext,
        original: JoinedPreparation,
        cap: usize,
    ) -> (Result<Self, PreparationCommitError>, usize) {
        let mut proof = Proof::bounded(cap);
        let validated = Self::validate(context, original, &mut proof);
        let used = proof.used();
        (
            validated.map(|(source, expected)| Self {
                source,
                expected,
                proof,
            }),
            used,
        )
    }
    fn validate(
        context: SessionContext,
        original: JoinedPreparation,
        proof: &mut Proof,
    ) -> Result<(Arc<RetainedInitialSource>, prefix::ExpectedSource), PreparationCommitError> {
        proof
            .measure(&(
                context.session_id(),
                context.workspace_id(),
                context.directory(),
            ))
            .map_err(|_| Error::Capacity)?;
        let binding = &original
            .observed_grant()
            .original_reservation()
            .owner
            .binding;
        if context.session_id() != &binding.session_id
            || context.workspace_id() != binding.workspace_id
            || context.directory() != binding.directory
        {
            return Err(Error::IdentityMismatch.into());
        }
        // Original moves once into the retained allocation; no data clone is
        // needed to let the existing supervisor retain the same source.
        let expected = prefix::prepare(&original, proof)?;
        Ok((
            Arc::new(RetainedInitialSource { original, context }),
            expected,
        ))
    }
}

pub(super) struct RetainedInitialSource {
    pub(super) original: JoinedPreparation,
    pub(super) context: SessionContext,
}
