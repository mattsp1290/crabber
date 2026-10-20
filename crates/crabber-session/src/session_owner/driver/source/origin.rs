//! Actual initial issuer and original admission association.
use super::{
    DriverLink, Error, InputFingerprint, RegisterExecutionDriver, Run, RunIssuance, Session,
    bounded,
};
enum InitialInput {
    Keyed(InputFingerprint),
    Unkeyed(InputFingerprint),
}
impl DriverLink {
    pub(crate) fn initial(
        request: &RegisterExecutionDriver,
        session: &Session,
        run: &Run,
        issuance: &RunIssuance,
        witness: &crate::session_owner::preparation::admission::Witness,
        sources: &crate::session_owner::preparation::admission::SourceAudits<'_>,
        init: &crate::admission_journal::Init,
    ) -> Result<Self, Error> {
        bounded(init, crate::admission_journal::MAX_BYTES)?;
        Self::initial_source(
            request,
            session,
            run,
            issuance,
            witness,
            sources,
            InitialInput::Keyed(init.digest().map_err(|_| Error::UnknownStoreOutcome)?),
        )
    }
    pub(crate) fn initial_unkeyed(
        request: &RegisterExecutionDriver,
        session: &Session,
        run: &Run,
        issuance: &RunIssuance,
        witness: &crate::session_owner::preparation::admission::Witness,
        sources: &crate::session_owner::preparation::admission::SourceAudits<'_>,
        binding: &crate::ExecutionBindingCapsule,
    ) -> Result<Self, Error> {
        let input = witness.verify_unkeyed_binding(binding)?.clone();
        Self::initial_source(
            request,
            session,
            run,
            issuance,
            witness,
            sources,
            InitialInput::Unkeyed(input),
        )
    }
    fn initial_source(
        request: &RegisterExecutionDriver,
        session: &Session,
        run: &Run,
        issuance: &RunIssuance,
        witness: &crate::session_owner::preparation::admission::Witness,
        sources: &crate::session_owner::preparation::admission::SourceAudits<'_>,
        input: InitialInput,
    ) -> Result<Self, Error> {
        let request_digest = request.fingerprint()?;
        if matches!(input, InitialInput::Unkeyed(_)) != witness.is_unkeyed() {
            return Err(Error::SemanticConflict);
        }
        bounded(session, 16384)?;
        bounded(run, 32768)?;
        bounded(issuance, 2048)?;
        bounded(witness, 8192)?;
        for source in [
            sources.reserve,
            sources.begin,
            sources.join,
            sources.child_reserve,
            sources.child_begin,
        ] {
            bounded(source, 32768)?;
        }
        issuance
            .verify(run)
            .map_err(|_| Error::UnknownStoreOutcome)?;
        witness.verify_sources(session, issuance, sources)?;
        let declared = sources
            .reserve
            .record
            .preparation
            .as_ref()
            .and_then(|origin| origin.resource_contract.as_ref());
        if declared.is_none()
            || sources.reserve.record.snapshot.binding.component_id
                != crate::session_owner::preparation::OWNED_PREPARATION_COMPONENT
        {
            return Err(Error::Unsupported);
        }
        if issuance.sequence != 1
            || issuance.version != 2
            || run.status != crabber_core::RunStatus::Running
            || run.id != request.fence.run_id
            || run.claim_token != request.fence.claim_token
            || run.session_id != *request.context.session_id()
            || session.id != run.session_id
            || session.workspace_id != request.context.workspace_id()
            || session.directory != request.context.directory()
            || run.checkpoint.is_some()
            || run.error.is_some()
        {
            return Err(Error::StaleOwner);
        }
        let (version, init_digest, unkeyed_input) = match input {
            InitialInput::Keyed(digest) => (1, Some(digest), None),
            InitialInput::Unkeyed(digest) => (2, None, Some(digest)),
        };
        let source = Self {
            version,
            session_id: session.id.clone(),
            run_id: run.id.clone(),
            init_digest,
            unkeyed_input,
            issuer_digest: crate::native_parent::issuance_digest(issuance)
                .map_err(|_| Error::UnknownStoreOutcome)?,
            admission_handoff: issuance
                .ownership_handoff
                .clone()
                .ok_or(Error::Unsupported)?,
            config_hash: InputFingerprint::new(run.config_hash.clone())
                .map_err(|_| Error::UnknownStoreOutcome)?,
            plan_fingerprint: InputFingerprint::new(run.plan_fingerprint.clone())
                .map_err(|_| Error::UnknownStoreOutcome)?,
            request_digest,
        };
        source.validate()?;
        Ok(source)
    }
    pub(crate) fn validate(&self) -> Result<(), Error> {
        crate::session_owner::identifier(&self.session_id.0, 512, false)?;
        crate::session_owner::identifier(&self.run_id.0, 128, false)?;
        if !matches!(
            (
                self.version,
                self.init_digest.is_some(),
                self.unkeyed_input.is_some()
            ),
            (1, true, false) | (2, false, true)
        ) {
            return Err(Error::UnknownStoreOutcome);
        }
        bounded(self, 2048)
    }

    pub(crate) fn verify_original(
        &self,
        session: &Session,
        issuer: &RunIssuance,
        witness: &crate::session_owner::preparation::admission::Witness,
        sources: &crate::session_owner::preparation::admission::SourceAudits<'_>,
        init: &crate::admission_journal::Init,
    ) -> Result<(), Error> {
        self.validate()?;
        bounded(init, crate::admission_journal::MAX_BYTES)?;
        if self.version != 1
            || witness.is_unkeyed()
            || self.init_digest.as_ref()
                != Some(&init.digest().map_err(|_| Error::UnknownStoreOutcome)?)
        {
            return Err(Error::UnknownStoreOutcome);
        }
        self.verify_original_source(session, issuer, witness, sources)
    }
    pub(crate) fn is_unkeyed(&self) -> bool {
        self.version == 2
    }
    pub(crate) fn verify_original_unkeyed(
        &self,
        session: &Session,
        issuer: &RunIssuance,
        witness: &crate::session_owner::preparation::admission::Witness,
        sources: &crate::session_owner::preparation::admission::SourceAudits<'_>,
    ) -> Result<(), Error> {
        self.validate()?;
        if self.version != 2
            || self.unkeyed_input.as_ref() != Some(witness.unkeyed_binding_digest()?)
        {
            return Err(Error::UnknownStoreOutcome);
        }
        self.verify_original_source(session, issuer, witness, sources)
    }
    fn verify_original_source(
        &self,
        session: &Session,
        issuer: &RunIssuance,
        witness: &crate::session_owner::preparation::admission::Witness,
        sources: &crate::session_owner::preparation::admission::SourceAudits<'_>,
    ) -> Result<(), Error> {
        witness.verify_sources(session, issuer, sources)?;
        if issuer.sequence != 1
            || issuer.version != 2
            || issuer.state.session_id != self.session_id
            || issuer.state.run_id != self.run_id
            || issuer.ownership_handoff.as_ref() != Some(&self.admission_handoff)
            || crate::native_parent::issuance_digest(issuer)
                .map_err(|_| Error::UnknownStoreOutcome)?
                != self.issuer_digest
        {
            return Err(Error::UnknownStoreOutcome);
        }
        Ok(())
    }
}
