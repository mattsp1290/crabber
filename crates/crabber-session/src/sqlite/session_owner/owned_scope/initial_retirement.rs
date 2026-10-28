//! Private original Driver selectors and close-only publishers.
use super::super::{Error, reads};
use crate::public_session::v2::proof::Proof;
use crate::transaction::Transaction;
use sqlx::Sqlite;

pub(in crate::sqlite) async fn driver_roster(
    tx: &mut Transaction<'_, Sqlite>,
    session: &crabber_core::SessionId,
    proof: &mut Proof,
) -> Result<Vec<crate::session_owner::record::Record>, Error> {
    proof.measure(session).map_err(|_| Error::Capacity)?;
    reads::scope(tx, session).await?;
    let (count,bytes):(i64,i64)=sqlx::query_as("SELECT count(*),CAST(COALESCE(sum(length(CAST(owner_id AS BLOB))),0) AS BIGINT) FROM session_owner_current WHERE session_id=$1 OR (json_extract(data_json,'$.snapshot.binding.session_id'))=$1").bind(&session.0).fetch_one(&mut **tx).await.map_err(|_|Error::UnknownStoreOutcome)?;
    proof.charge(16).map_err(|_| Error::Capacity)?;
    proof
        .charge(usize::try_from(bytes).map_err(|_| Error::UnknownStoreOutcome)?)
        .map_err(|_| Error::Capacity)?;
    let count = usize::try_from(count).map_err(|_| Error::UnknownStoreOutcome)?;
    proof
        .charge(
            count
                .checked_mul(
                    std::mem::size_of::<crate::session_owner::record::Record>()
                        + std::mem::size_of::<String>(),
                )
                .ok_or(Error::Capacity)?,
        )
        .map_err(|_| Error::Capacity)?;
    let owners:Vec<String>=sqlx::query_scalar("SELECT owner_id FROM session_owner_current WHERE session_id=$1 OR (json_extract(data_json,'$.snapshot.binding.session_id'))=$1 ORDER BY owner_id").bind(&session.0).fetch_all(&mut **tx).await.map_err(|_|Error::UnknownStoreOutcome)?;
    if owners.len() != count {
        return Err(Error::UnknownStoreOutcome);
    }
    let mut records = Vec::with_capacity(count);
    for owner in owners {
        records.push(
            Box::pin(reads::current_with_proof(
                tx,
                session,
                &crabber_core::SessionOwnerId(owner),
                proof,
            ))
            .await
            .map_err(crate::public_session::v2::proof::ReadError::owner)?,
        );
    }
    Ok(records)
}

impl crate::sqlite::SqliteStore {
    pub(in crate::sqlite) async fn retire_unactivated_initial_in(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        parts: &mut crate::session_owner::owned_scope::NativeInitialParts<'_>,
        witness: &crate::session_owner::owned_scope::InitialRetirementWitness<'_>,
        register: &crate::native_enrollment::initial_consumer::RegisterOriginal,
    ) -> Result<
        crate::session_owner::audit::Audit,
        crate::session_owner::preparation::PreparationCommitError,
    > {
        use crate::native_enrollment::initial_consumer as domain;
        use crate::session_owner::{SessionOwnerOperation, audit::Audit};
        let request = domain::unactivated_request(parts.expected, register, parts.proof)
            .map_err(|error| domain::failure(&error))?;
        let current = Box::pin(reads::current_with_proof(
            tx,
            &request.session_id,
            &request.target.id,
            parts.proof,
        ))
        .await
        .map_err(|error| {
            crate::session_owner::preparation::inspection::read_error(parts.proof, error)
        })?;
        for expected in [&register.reserve, &register.begin] {
            let actual = super::super::drivers::selected_consumer_audit(
                tx,
                &request.session_id,
                &expected.receipt.operation_id,
                parts.proof,
            )
            .await
            .map_err(|error| error.into_domain_or(Error::Capacity))?
            .ok_or(Error::UnknownStoreOutcome)?;
            if domain::digest(&actual).map_err(|error| domain::failure(&error))?
                != domain::digest(expected).map_err(|error| domain::failure(&error))?
            {
                return Err(Error::UnknownStoreOutcome.into());
            }
        }
        if let Some(audit) = super::super::drivers::selected_consumer_audit(
            tx,
            &request.session_id,
            &request.id,
            parts.proof,
        )
        .await
        .map_err(|error| error.into_domain_or(Error::Capacity))?
        {
            if audit.receipt.operation != SessionOwnerOperation::Join
                || audit.receipt.semantic_digest != request.fingerprint()?
                || audit.record != current
            {
                return Err(Error::UnknownStoreOutcome.into());
            }
            return Ok(audit);
        }
        let verified = domain::verify_unactivated_retirement(
            witness,
            (parts.original, parts.context, parts.expected),
            register,
            &current,
            parts.proof,
        )?;
        let mut record = current;
        let request = record.retire_unactivated_initial(verified, self.clock.now())?;
        parts
            .proof
            .measure(&(&request.id, &record.snapshot))
            .map_err(|error| domain::failure(&error))?;
        let receipt = record.receipt(
            request.id.clone(),
            request.fingerprint()?,
            SessionOwnerOperation::Join,
        );
        let publication = super::super::publish_revision(tx, &request.session_id, true).await?;
        let audit = Audit {
            version: 2,
            receipt,
            record,
            publication: Some(publication),
        };
        parts
            .proof
            .measure(&audit)
            .map_err(|error| domain::failure(&error))?;
        let actual = Box::pin(reads::publish_captured_with_proof(
            tx,
            &audit,
            false,
            parts.proof,
        ))
        .await
        .map_err(|error| {
            crate::session_owner::preparation::inspection::read_error(parts.proof, error)
        })?;
        Ok(actual)
    }
}
