//! Exact native source/CAS eligibility before any preparation factory grant.
use super::{Error, reads, unknown};
use crate::session_owner::preparation::ReservePreparation;
impl super::SqliteStore {
    pub(super) async fn preparation_preflight(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        preparation: &ReservePreparation,
        actual: crabber_core::SessionRevision,
        proof: &mut crate::public_session::v2::proof::Proof,
    ) -> Result<(), Error> {
        let binding = &preparation.owner.binding;
        let running: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM runs WHERE session_id=$1 AND status IN ('pending','running','paused'))")
                .bind(&binding.session_id.0).fetch_one(&mut **tx).await.map_err(unknown)?;
        if (running
            && matches!(
                preparation.purpose,
                crate::session_owner::preparation::PreparationPurpose::OrdinaryAdmission
            ))
            || reads::has_preparation(tx, &binding.session_id).await?
        {
            return Err(Error::Busy);
        }
        if actual != preparation.expected_revision {
            return Err(Error::RevisionConflict);
        }
        self.preparation_source_context(tx, preparation, proof)
            .await
    }
    pub(super) async fn preparation_source_context(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        preparation: &ReservePreparation,
        proof: &mut crate::public_session::v2::proof::Proof,
    ) -> Result<(), Error> {
        let binding = &preparation.owner.binding;
        match &preparation.purpose {
            crate::session_owner::preparation::PreparationPurpose::OrdinaryAdmission => (),
            crate::session_owner::preparation::PreparationPurpose::Queue { authority } => {
                self.queue_preparation_context(tx, authority, &preparation.intent, proof)
                    .await
                    .map_err(unknown)?;
            }
            crate::session_owner::preparation::PreparationPurpose::ExistingRun {
                run_id, ..
            } => {
                let original =
                    super::super::execution_binding::read(tx, &binding.session_id, run_id)
                        .await
                        .map_err(unknown)?
                        .ok_or(Error::UnknownStoreOutcome)?;
                if crate::session_owner::preparation::resource_contract(
                    &original.capsule.runtime_semantics,
                )? != preparation.resource_contract
                {
                    return Err(Error::SemanticConflict);
                }
                let run = if let Some(
                    crate::session_owner::preparation::PreparationSource::ExpiredRun(source),
                ) = &preparation.source
                {
                    super::super::expired::load_run(tx, source.request()).await?
                } else {
                    super::super::records::load_run(tx, run_id)
                        .await
                        .map_err(unknown)?
                };
                if run.session_id != binding.session_id {
                    return Err(Error::NotFound);
                }
                super::super::native_parent::verify_run(tx, &run)
                    .await
                    .map_err(unknown)?;
                let source = preparation.source.as_ref().ok_or(Error::Unsupported)?;
                crate::session_owner::preparation::source_validation::validate(
                    source,
                    preparation,
                )?;
                self.verify_preparation_native_source(tx, &run, source)
                    .await?;
            }
        }

        Ok(())
    }
    async fn verify_preparation_native_source(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        run: &crabber_core::Run,
        source: &crate::session_owner::preparation::PreparationSource,
    ) -> Result<(), Error> {
        match source {
            crate::session_owner::preparation::PreparationSource::ExpiredRun(source) => {
                let current =
                    super::super::expired::context(tx, source.request().clone(), self.clock.now())
                        .await?;
                source.verify_source(&current)?;
            }
            crate::session_owner::preparation::PreparationSource::AdmissionRecovery(recovery) => {
                super::super::admission_execution::validate_source(tx, recovery, self.clock.now())
                    .await
                    .map_err(unknown)?;
            }
            crate::session_owner::preparation::PreparationSource::ApprovalClaim(claim) => {
                super::super::approval::preparation_context(tx, run, claim)
                    .await
                    .map_err(Error::ApprovalSource)?;
            }
            crate::session_owner::preparation::PreparationSource::ToolClaim(claim) => {
                super::super::tool_continuation::preparation_context(tx, run, claim)
                    .await
                    .map_err(Error::ToolSource)?;
            }
            crate::session_owner::preparation::PreparationSource::FrontendClaim(claim) => {
                super::super::tool_continuation::preparation_frontend_context(tx, run, claim)
                    .await
                    .map_err(Error::ToolSource)?;
            }
            crate::session_owner::preparation::PreparationSource::FrontendRecovery(recovery) => {
                super::super::tool_continuation::preparation_frontend_recovery_context(
                    tx,
                    run,
                    recovery,
                    self.clock.now(),
                )
                .await
                .map_err(Error::ToolSource)?;
            }
            crate::session_owner::preparation::PreparationSource::ApprovalRecovery(recovery) => {
                super::super::approval::preparation_recovery_context(
                    tx,
                    run,
                    recovery,
                    self.clock.now(),
                )
                .await
                .map_err(Error::ApprovalSource)?;
            }
            crate::session_owner::preparation::PreparationSource::ToolRecovery(recovery) => {
                super::super::tool_continuation::preparation_recovery_context(
                    tx,
                    run,
                    recovery,
                    self.clock.now(),
                )
                .await
                .map_err(Error::ToolSource)?;
            }
        }
        Ok(())
    }
}
