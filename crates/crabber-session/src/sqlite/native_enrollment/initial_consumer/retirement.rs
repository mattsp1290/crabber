//! Exact native Driver retirement under the original paired Seal transaction.
pub(super) mod absence;
use crate::native_enrollment::initial_consumer as domain;
use crate::public_session::v2::proof::{Failure, Proof, unknown};
use crate::session_owner::{
    SessionOwnerError as Error,
    owned_scope::{NativeInitialParts, OwnedMainDisposition},
    preparation::PreparationCommitError,
};
use crate::sqlite::SqliteStore;
use crate::transaction::Transaction;
use sqlx::Sqlite;

pub(super) async fn corroborate(
    tx: &mut Transaction<'_, Sqlite>,
    view: &domain::Selection<'_>,
    proof: &mut Proof,
) -> Result<(), Failure> {
    let saved = if let Some(close) = view.close {
        Some(&close.retirement)
    } else {
        match view.origin {
            Some(domain::Origin::NoOpenedMember(value)) => Some(&value.retirement),
            _ => None,
        }
    };
    if let Some(saved) = saved {
        verify_native(tx, view, saved, proof).await?;
    }
    Ok(())
}
async fn verify_native(
    tx: &mut Transaction<'_, Sqlite>,
    view: &domain::Selection<'_>,
    retirement: &domain::DriverRetirement,
    proof: &mut Proof,
) -> Result<(), Failure> {
    match retirement {
        domain::DriverRetirement::NoDriver => absence::verify(tx, view, proof).await,
        domain::DriverRetirement::Activated { audit }
        | domain::DriverRetirement::Unactivated { audit } => {
            let actual = crate::sqlite::session_owner::drivers::selected_consumer_audit(
                tx,
                &audit.receipt.session_id,
                &audit.receipt.operation_id,
                proof,
            )
            .await
            .map_err(crate::public_session::v2::proof::ReadError::public)?
            .ok_or_else(|| unknown("retired Driver native Join missing"))?;
            if domain::digest(&actual)? != domain::digest(audit.as_ref())? {
                return Err(unknown("changed retired Driver native Join"));
            }
            let current = crate::sqlite::session_owner::current_owner_with_proof(
                tx,
                &audit.receipt.session_id,
                &audit.record.snapshot.target.id,
                proof,
            )
            .await
            .map_err(owner_failure)?;
            if current != actual.record {
                return Err(unknown("retired Driver current/source mismatch"));
            }
            Ok(())
        }
    }
}
pub(super) async fn produce(
    store: &SqliteStore,
    tx: &mut Transaction<'_, Sqlite>,
    parts: &mut NativeInitialParts<'_>,
    view: &domain::Selection<'_>,
) -> Result<(domain::DriverRetirement, OwnedMainDisposition), PreparationCommitError> {
    let witness = parts.retirement.ok_or(Error::Unsupported)?;
    let local = witness.verify(parts.original, parts.context, parts.expected, parts.proof)?;
    // ACK-loss replay authenticates the saved Join/current record with its cleared
    // token; it never re-enters the fresh original-token mutation checks.
    if let Some(close) = view.close {
        if close.disposition != local.disposition {
            return Err(Error::IdentityMismatch.into());
        }
        verify_native(tx, view, &close.retirement, parts.proof)
            .await
            .map_err(|error| domain::failure(&error))?;
        parts
            .proof
            .measure(&close.retirement)
            .map_err(|error| domain::failure(&error))?;
        return Ok((close.retirement.clone(), local.disposition));
    }
    if let Some(domain::Origin::NoOpenedMember(value)) = view.origin {
        if value.disposition != local.disposition {
            return Err(Error::IdentityMismatch.into());
        }
        verify_native(tx, view, &value.retirement, parts.proof)
            .await
            .map_err(|error| domain::failure(&error))?;
        parts
            .proof
            .measure(&value.retirement)
            .map_err(|error| domain::failure(&error))?;
        return Ok((value.retirement.clone(), local.disposition));
    }
    let register = view
        .results
        .iter()
        .find_map(|result| match &result.original {
            domain::Original::Register(value) => Some(value.as_ref()),
            _ => None,
        });
    let Some(register) = register else {
        absence::verify_route(tx, view, parts.expected, parts.proof)
            .await
            .map_err(|error| domain::failure(&error))?;
        return Ok((domain::DriverRetirement::NoDriver, local.disposition));
    };
    let activated = view
        .results
        .iter()
        .find_map(|result| match &result.original {
            domain::Original::Activate(value) => Some(value.as_ref()),
            _ => None,
        });
    let retirement = if let Some(activated) = activated {
        let activation = activated
            .audit
            .record
            .owned_scope
            .as_ref()
            .ok_or(Error::UnknownStoreOutcome)?;
        let joined = witness.joined_scope(activation, parts.proof)?;
        let grant = local.grant.ok_or(Error::UnknownStoreOutcome)?;
        let (_, audit) =
            Box::pin(store.join_owned_scope_in(tx, &joined, grant.authority(), parts.proof))
                .await?;
        parts
            .proof
            .charge(std::mem::size_of::<crate::session_owner::audit::Audit>())
            .map_err(|error| domain::failure(&error))?;
        domain::DriverRetirement::Activated {
            audit: Box::new(audit),
        }
    } else {
        let audit =
            Box::pin(store.retire_unactivated_initial_in(tx, parts, witness, register)).await?;
        parts
            .proof
            .charge(std::mem::size_of::<crate::session_owner::audit::Audit>())
            .map_err(|error| domain::failure(&error))?;
        domain::DriverRetirement::Unactivated {
            audit: Box::new(audit),
        }
    };
    Ok((retirement, local.disposition))
}

fn owner_failure(error: Error) -> Failure {
    match error {
        Error::Capacity => Failure::Limited(crate::PublicSessionLimitV2::ProofBytes),
        error => unknown(error),
    }
}
