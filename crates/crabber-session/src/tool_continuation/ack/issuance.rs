//! One actual outgoing grant and receipt, completed before their native commit.
use super::{Error, Proof, ToolContinuationGrant, ToolContinuationReceipt};
pub(crate) struct Issuance<'a> {
    pub receipt: &'a ToolContinuationReceipt,
    pub token: &'a str,
    pub run_token: &'a str,
    pub proof: &'a mut Proof,
}
pub(crate) struct Completion<'a> {
    receipt: ToolContinuationReceipt,
    token: &'a str,
    run_token: &'a str,
    proof: &'a mut Proof,
}
impl<'a> Completion<'a> {
    pub(crate) fn new(
        receipt: ToolContinuationReceipt,
        credentials: (&'a str, &'a str),
        proof: &'a mut Proof,
    ) -> Self {
        Self {
            receipt,
            token: credentials.0,
            run_token: credentials.1,
            proof,
        }
    }
    pub(crate) fn finish(
        self,
        mut grant: Box<ToolContinuationGrant>,
    ) -> Result<crate::ToolContinuationClaimOutcome, Error> {
        let Self {
            receipt,
            token,
            run_token,
            proof,
        } = self;
        super::issue(
            &mut grant,
            Issuance {
                receipt: &receipt,
                token,
                run_token,
                proof,
            },
        )?;
        Ok(crate::ToolContinuationClaimOutcome::Applied { receipt, grant })
    }
}
