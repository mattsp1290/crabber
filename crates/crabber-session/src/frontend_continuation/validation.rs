//! Pure frozen frontend declaration and complete-set verification.
use super::{
    Error, FrontendContinuationClaim, FrontendContinuationRecovery, FrozenFrontendCall,
    frontend_descriptor_fingerprint, validate_frontend_result,
};
use crate::ToolContinuationReceipt;
impl FrozenFrontendCall {
    /// Validate the frozen declaration and bounded original input.
    /// # Errors
    /// Rejects changed descriptor identity, schema or initial phase generation.
    pub fn validate(&self) -> Result<(), Error> {
        if self.descriptor_fingerprint != frontend_descriptor_fingerprint(&self.descriptor)?
            || self.target.generation != crabber_core::PhaseGeneration::INITIAL
        {
            return Err(Error::InvalidRequest);
        }
        crabber_core::AcceptedToolResponse::validate_value(&self.arguments)
            .map_err(|_| Error::InvalidRequest)?;
        crate::ensure_storable(&self.arguments).map_err(|_| Error::InvalidRequest)?;
        jsonschema::validator_for(&self.descriptor.input_parameters)
            .map_err(|_| Error::InvalidRequest)?
            .validate(&self.arguments)
            .map_err(|_| Error::InvalidRequest)?;
        Ok(())
    }
}
impl FrontendContinuationClaim {
    /// Check the complete result set against its frozen suspension.
    /// # Errors
    /// Rejects missing/foreign calls, changed generation or output contract.
    pub fn validate_for_batch(
        &self,
        batch: &crate::ToolContinuationSuspension,
    ) -> Result<(), Error> {
        self.fingerprint()?;
        batch.fingerprint(&self.claim.session_id, &self.claim.run_id)?;
        if batch.frontend.is_empty()
            || self.results.len() != batch.frontend.len()
            || self.claim.continuation != batch.continuation
            || self.claim.expected_plan != batch.expected_plan
        {
            return Err(Error::SemanticConflict);
        }
        for frozen in &batch.frontend {
            let result = self
                .results
                .iter()
                .find(|result| result.call_id() == &frozen.target.call_id)
                .ok_or(Error::SemanticConflict)?;
            validate_frontend_result(&frozen.descriptor, result)?;
        }
        Ok(())
    }
}
impl FrontendContinuationRecovery {
    /// Check metadata without returning authority or reconstructing a worker.
    /// # Errors
    /// Rejects changed original results, scope or recovery target.
    pub fn verify_receipt(&self, receipt: &ToolContinuationReceipt) -> Result<(), Error> {
        super::record::verify_recovery_receipt(
            &self.id,
            &self.fingerprint()?,
            &self.original.claim,
            &self.attempt,
            receipt,
        )
    }
}
