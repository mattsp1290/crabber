//! Existing coherent domain proofs run before any mutable terminal-state filter.
use crate::public_session::{budget::Budget, v2::proof::Proof};
use crate::sql_deletion::readiness::{MAX_PROOF_BYTES, Result, blocked, unknown};
use crate::{DeletionBlocker, PublicSessionLimit, RunControlStatus};
use crabber_core::SessionId;
use sqlx::{Sqlite, Transaction};
fn safe() -> Budget {
    Budget::new(MAX_PROOF_BYTES, PublicSessionLimit::HeaderBytes)
}
pub(super) async fn continuations(
    tx: &mut Transaction<'_, Sqlite>,
    session: &SessionId,
    proof: &mut Proof,
) -> Result<()> {
    super::super::super::approval::current::session_with_proof(tx, session, proof).await?;
    let approval:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM continuations WHERE session_id=$1 AND state<>'released' UNION ALL SELECT 1 FROM continuation_barriers WHERE session_id=$1)")
        .bind(&session.0).fetch_one(&mut **tx).await.map_err(unknown)?;
    if approval {
        return Err(blocked(DeletionBlocker::Continuation));
    }
    let tool =
        super::super::super::tool_continuation::scan::scan_with_proof(tx, session, None, proof)
            .await?;
    if tool.active.is_some() {
        return Err(blocked(DeletionBlocker::Continuation));
    }
    let barrier: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM tool_continuation_barriers WHERE session_id=$1)",
    )
    .bind(&session.0)
    .fetch_one(&mut **tx)
    .await
    .map_err(unknown)?;
    if barrier {
        return Err(blocked(DeletionBlocker::Continuation));
    }
    Ok(())
}
pub(super) async fn queue(
    tx: &mut Transaction<'_, Sqlite>,
    session: &SessionId,
    proof: &mut Proof,
) -> Result<()> {
    let pending =
        super::super::super::queue::public::pending(tx, session, u16::MAX, &mut safe(), proof)
            .await?;
    if !pending.is_empty() {
        return Err(blocked(DeletionBlocker::Queue));
    }
    Ok(())
}
pub(super) async fn pending_input(
    tx: &mut Transaction<'_, Sqlite>,
    session: &SessionId,
) -> Result<()> {
    let pending: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM inbox WHERE session_id=$1 AND consumed_by_run IS NULL)",
    )
    .bind(&session.0)
    .fetch_one(&mut **tx)
    .await
    .map_err(unknown)?;
    if pending {
        return Err(blocked(DeletionBlocker::PendingInput));
    }
    Ok(())
}
pub(super) async fn controls(
    tx: &mut Transaction<'_, Sqlite>,
    session: &SessionId,
    proof: &mut Proof,
) -> Result<()> {
    let mut after = String::new();
    loop {
        let ids = super::native::run_ids(tx, session, &after, proof).await?;
        if ids.is_empty() {
            break;
        }
        for id in ids {
            let facts = super::super::super::run_facts::read(tx, session, &id, proof).await?;
            let controls = super::super::super::run_control::public::controls(
                tx,
                session,
                &id,
                facts.status,
                64,
                &mut safe(),
                proof,
            )
            .await?;
            if controls.iter().any(|control| {
                matches!(
                    control.status,
                    RunControlStatus::Accepted | RunControlStatus::Observed
                )
            }) {
                return Err(blocked(DeletionBlocker::PendingControl));
            }
            after = id.0;
        }
    }
    Ok(())
}
pub(super) async fn owners(
    tx: &mut Transaction<'_, Sqlite>,
    session: &SessionId,
    proof: &mut Proof,
) -> Result<bool> {
    let orphan: bool = sqlx::query_scalar(OWNER_SCOPE)
        .bind(&session.0)
        .fetch_one(&mut **tx)
        .await
        .map_err(unknown)?;
    if orphan {
        return Err(unknown("owner source membership"));
    }
    let owners = super::super::super::session_owner::public::deletion_registered(
        tx,
        session,
        &mut safe(),
        proof,
    )
    .await?;
    if !owners.live.is_empty() {
        return Err(blocked(DeletionBlocker::SessionOwner));
    }
    let pending:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM session_invocations WHERE session_id=$1 AND (state NOT IN ('Completed','Failed','Interrupted') OR cleanup_state<>'Complete'))")
        .bind(&session.0).fetch_one(&mut **tx).await.map_err(unknown)?;
    if pending {
        return Err(blocked(DeletionBlocker::SessionOwner));
    }
    // Legacy scaffolding has no independently authenticated join vocabulary.
    let legacy: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM session_invocations WHERE session_id=$1)")
            .bind(&session.0)
            .fetch_one(&mut **tx)
            .await
            .map_err(unknown)?;
    Ok(legacy || owners.unclassified)
}
const OWNER_SCOPE:&str="SELECT EXISTS(
 SELECT 1 FROM session_owner_audits a WHERE a.session_id=$1 AND NOT EXISTS(SELECT 1 FROM session_owner_current c WHERE c.session_id=a.session_id AND c.owner_id=a.owner_id)
 UNION ALL SELECT 1 FROM session_owner_contexts c WHERE c.session_id=$1 AND NOT EXISTS(SELECT 1 FROM session_owner_audits a WHERE a.session_id=c.session_id AND a.operation='Reserve')
)";
