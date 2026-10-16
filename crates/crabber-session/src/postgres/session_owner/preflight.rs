//! Exact native source/CAS eligibility before any preparation factory grant.
use super::{Error, reads, unknown};
use crate::session_owner::preparation::ReservePreparation;
impl super::PostgresStore {
    pub(super) async fn preparation_preflight(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        preparation: &ReservePreparation,
        actual: crabber_core::SessionRevision,
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
        self.preparation_source_context(tx, preparation).await
    }
    pub(super) async fn preparation_source_context(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        preparation: &ReservePreparation,
    ) -> Result<(), Error> {
        let binding = &preparation.owner.binding;
        match &preparation.purpose {
            crate::session_owner::preparation::PreparationPurpose::OrdinaryAdmission => (),
            crate::session_owner::preparation::PreparationPurpose::Queue { authority } => {
                self.queue_preparation_context(tx, authority, &preparation.intent)
                    .await
                    .map_err(unknown)?;
            }
            crate::session_owner::preparation::PreparationPurpose::ExistingRun {
                run_id, ..
            } => {
                super::super::execution_binding::read(tx, &binding.session_id, run_id)
                    .await
                    .map_err(unknown)?
                    .ok_or(Error::UnknownStoreOutcome)?;
                let run = super::super::load_run(tx, run_id, true)
                    .await
                    .map_err(unknown)?;
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
                match source {
                    crate::session_owner::preparation::PreparationSource::ApprovalClaim(claim) => {
                        super::super::approval::preparation_context(tx, &run, claim)
                            .await
                            .map_err(Error::ApprovalSource)?;
                    }
                    crate::session_owner::preparation::PreparationSource::ToolClaim(claim) => {
                        super::super::tool_continuation::preparation_context(tx, &run, claim)
                            .await
                            .map_err(Error::ToolSource)?;
                    }
                    crate::session_owner::preparation::PreparationSource::ApprovalRecovery(
                        recovery,
                    ) => {
                        super::super::approval::preparation_recovery_context(
                            tx,
                            &run,
                            recovery,
                            self.clock.now(),
                        )
                        .await
                        .map_err(Error::ApprovalSource)?;
                    }
                    crate::session_owner::preparation::PreparationSource::ToolRecovery(
                        recovery,
                    ) => {
                        super::super::tool_continuation::preparation_recovery_context(
                            tx,
                            &run,
                            recovery,
                            self.clock.now(),
                        )
                        .await
                        .map_err(Error::ToolSource)?;
                    }
                }
            }
        }

        Ok(())
    }
}
