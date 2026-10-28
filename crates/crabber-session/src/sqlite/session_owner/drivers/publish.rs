//! The same canonical Reserve/Begin publisher, using an already-held transaction.
use super::{Audit, Error, Record, SessionOwnerOperation, publish_revision, reads, unknown};
use crate::public_session::v2::proof::Proof;
use crate::session_owner::driver::{
    DriverLink, ExecutionDriverGrant, ExecutionDriverOutcome, ExecutionDriverReceipt,
    RegisterExecutionDriver,
};
use crate::sqlite::SqliteStore;
use crate::transaction::Transaction;
use sqlx::Sqlite;
/// Actual publisher outputs retained for the closed consumer's original capture.
pub(in crate::sqlite) struct DriverPublication {
    pub outcome: ExecutionDriverOutcome,
    pub reserve: Audit,
    pub begin: Audit,
}
impl SqliteStore {
    pub(in crate::sqlite) async fn publish_initial_driver(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        request: &RegisterExecutionDriver,
        link: DriverLink,
        proof: &mut Proof,
    ) -> Result<DriverPublication, Error> {
        link.matches_request(request)?;
        let reservation = link.reservation(request)?;
        // Fresh source validation already authenticated the current child context.
        proof.charge(9).map_err(unknown)?; // Capacity count and target-existence scalar.
        reads::capacity(tx, request.context.session_id()).await?;
        let exists: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM session_owner_current WHERE session_id=$1 AND owner_id=$2)").bind(&reservation.binding.session_id.0).bind(&reservation.binding.id.0).fetch_one(&mut **tx).await.map_err(unknown)?;
        if exists {
            return Err(Error::SemanticConflict);
        }
        let mut record = Record::reserve(&reservation, None, self.clock.now())?;
        record.driver = Some(Box::new(link.clone()));
        let authority = record.grant()?;
        let reserve = record.receipt(
            reservation.id.clone(),
            link.fingerprint(&reservation)?,
            SessionOwnerOperation::Reserve,
        );
        let publication = publish_revision(tx, request.context.session_id(), true).await?;
        let reserve_audit = Audit {
            version: 2,
            receipt: reserve.clone(),
            record: record.clone(),
            publication: Some(publication),
        };
        let reserve_audit = reads::publish_captured_with_proof(tx, &reserve_audit, true, proof)
            .await
            .map_err(crate::public_session::v2::proof::ReadError::owner)?;
        let command = crate::session_owner::MutateSessionOwner {
            id: request.identities()?.begin,
            session_id: request.context.session_id().clone(),
            target: authority.target().clone(),
            command: crate::session_owner::SessionOwnerCommand::Begin,
        };
        record.mutate(&command, Some(&authority), self.clock.now())?;
        let begin = record.receipt(
            command.id.clone(),
            command.fingerprint()?,
            SessionOwnerOperation::Begin,
        );
        let publication = publish_revision(tx, request.context.session_id(), true).await?;
        let begin_audit = Audit {
            version: 2,
            receipt: begin.clone(),
            record,
            publication: Some(publication),
        };
        let begin_audit = reads::publish_captured_with_proof(tx, &begin_audit, false, proof)
            .await
            .map_err(crate::public_session::v2::proof::ReadError::owner)?;
        let receipt = ExecutionDriverReceipt {
            session_id: request.context.session_id().clone(),
            run_id: request.fence.run_id.clone(),
            request_digest: request.fingerprint()?,
            reserve,
            begin,
        };
        let grant = ExecutionDriverGrant::from_fresh_begin(
            receipt.clone(),
            authority,
            request.clone(),
            link.identity()?,
        )?;
        Ok(DriverPublication {
            outcome: ExecutionDriverOutcome::Applied {
                receipt,
                grant: Box::new(grant),
            },
            reserve: reserve_audit,
            begin: begin_audit,
        })
    }
}

/// The actual canonical activation Audit; no pre-commit dispatch permit.
pub(in crate::sqlite) struct ActivationPublication {
    pub audit: Audit,
}
impl SqliteStore {
    pub(in crate::sqlite) async fn publish_driver_activation(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        request: &crate::session_owner::owned_scope::OwnedScopeActivation,
        record: Record,
        proof: &mut Proof,
    ) -> Result<ActivationPublication, Error> {
        let receipt = record.receipt(
            request.id.clone(),
            request.fingerprint()?,
            SessionOwnerOperation::ActivateScope,
        );
        let publication = publish_revision(tx, request.session_id(), true).await?;
        let audit = Audit {
            version: 2,
            receipt,
            record,
            publication: Some(publication),
        };
        let audit = reads::publish_captured_with_proof(tx, &audit, false, proof)
            .await
            .map_err(crate::public_session::v2::proof::ReadError::owner)?;
        Ok(ActivationPublication { audit })
    }
}
