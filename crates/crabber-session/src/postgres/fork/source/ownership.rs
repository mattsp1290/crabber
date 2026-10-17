//! Authenticate the immutable original imported prefix in the caller's budget.
use super::{Failure, Proof};
use crate::postgres::transaction::Transaction;
use crate::public_session::v2::proof::{Failure as ReadFailure, Proof as SharedProof, unknown};
use crabber_core::SessionId;
use sqlx::Postgres;

pub(in crate::postgres) async fn verify_imported_history(
    tx: &mut Transaction<'_, Postgres>,
    session: &SessionId,
    proof: &mut SharedProof,
) -> Result<(), ReadFailure> {
    let present: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM session_fork_audit WHERE session_id=$1 UNION ALL SELECT 1 FROM imported_message_provenance WHERE session_id=$1 UNION ALL SELECT 1 FROM imported_tool_calls WHERE session_id=$1 UNION ALL SELECT 1 FROM messages WHERE session_id=$1 AND run_id IS NULL)")
        .bind(&session.0).fetch_one(&mut **tx).await.map_err(unknown)?;
    if !present {
        return Ok(());
    }
    let invalid: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM imported_tool_calls c WHERE c.session_id=$1 AND NOT EXISTS(SELECT 1 FROM tool_call_identities i WHERE i.id=c.id AND i.kind='imported'))")
        .bind(&session.0).fetch_one(&mut **tx).await.map_err(unknown)?;
    if invalid {
        return Err(unknown("imported identity ownership"));
    }
    let mut committed = std::collections::BTreeSet::new();
    super::imported::imported(tx, session, &mut Proof(proof.budget()), &mut committed)
        .await
        .map(|_| ())
        .map_err(|error| match error {
            Failure::Limited | Failure::RawLimited(_) => {
                ReadFailure::Limited(crate::PublicSessionLimitV2::ProofBytes)
            }
            Failure::Error(error) => unknown(error),
        })
}
