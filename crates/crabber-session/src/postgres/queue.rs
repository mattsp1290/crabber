mod admission;
mod inspection;
pub(in crate::postgres) mod protocol;
pub(super) mod public;
pub(super) mod reads;
use super::{PostgresStore, db};
use crate::postgres::transaction::Transaction;
use crate::{
    StoreError,
    queue::{
        AdmissionGuard, Audit, MAX_PENDING, MAX_PENDING_BYTES, QueueAction, QueueAuthority,
        QueueMutation, QueueOutcome, QueueReceipt, Record, complete, corrupt, encode, invalid,
        target,
    },
};
use crabber_core::SessionRevision;
use sqlx::{Postgres, Row};
impl PostgresStore {
    pub(super) async fn queue_mutate(
        &self,
        request: QueueMutation,
    ) -> Result<QueueOutcome, StoreError> {
        let digest = request.fingerprint()?;
        let mut tx = self.session_transaction(&request.session_id).await?;
        if let Some(saved) = reads::receipt(&mut tx, &request.session_id, &request.id).await? {
            if saved.semantic_digest != digest {
                return Err(StoreError::AdmissionConflict);
            }
            tx.commit().await.map_err(db)?;
            return Ok(QueueOutcome::Replayed(saved));
        }
        let result = Box::pin(self.queue_apply_guarded(
            &mut tx,
            request,
            digest,
            None,
            &mut crate::public_session::v2::proof::Proof::bounded(32 << 20),
        ))
        .await?;
        #[cfg(test)]
        crate::binding_fault::trip(&self.queue_fault, 1)?;
        tx.commit().await.map_err(db)?;
        #[cfg(test)]
        crate::binding_fault::trip(&self.queue_fault, 2)?;
        Ok(result)
    }
    pub(super) async fn queue_apply_guarded(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        request: QueueMutation,
        digest: crabber_core::InputFingerprint,
        preparation: Option<&crate::session_owner::preparation::proof::VerifiedPreparation<'_>>,
        read_proof: &mut crate::public_session::v2::proof::Proof,
    ) -> Result<QueueOutcome, StoreError> {
        let now = self.clock.now();
        protocol::routing(tx, &request.session_id, read_proof)
            .await
            .map_err(protocol::domain)?;
        let (session, metadata, revision) =
            reads::current_with_proof(tx, &request.session_id, read_proof)
                .await
                .map_err(protocol::domain)?
                .ok_or(StoreError::NotFound)?;
        let summary = crate::management::summary(&session, metadata, revision);
        let head = reads::head(tx, &request.session_id).await?;
        let id = target(&request.action).or_else(|| head.as_ref().map(|r| &r.queue_id));
        let original = if let Some(id) = id {
            reads::record_with_proof(tx, &request.session_id, id, read_proof).await?
        } else {
            None
        };
        let previous = if let Some(id) = id {
            protocol::verified_head(
                tx,
                &crate::queue::source::Scope {
                    session: request.session_id.clone(),
                    queue: id.clone(),
                },
                read_proof,
            )
            .await
            .map_err(protocol::domain)?
        } else {
            None
        };
        self.queue_guards(tx, &request).await?;
        let sequence = if matches!(request.action, QueueAction::Enqueue { .. }) {
            let previous: Option<i64> =
                sqlx::query_scalar("SELECT MAX(sequence) FROM future_queue WHERE session_id=$1")
                    .bind(&request.session_id.0)
                    .fetch_one(&mut **tx)
                    .await
                    .map_err(db)?;
            let sequence = u64::try_from(previous.unwrap_or(0))
                .map_err(|_| corrupt())?
                .checked_add(1)
                .ok_or_else(invalid)?;
            i64::try_from(sequence).map_err(|_| invalid())?;
            sequence
        } else {
            0
        };
        let change = crate::queue::apply_guarded(
            &request,
            &summary,
            head.as_ref(),
            original,
            sequence,
            now,
            preparation,
        )?;
        Box::pin(self.queue_admit_and_publish(
            tx,
            (request, digest, previous, now),
            change,
            preparation,
            read_proof,
        ))
        .await
    }
    async fn queue_guards(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        request: &QueueMutation,
    ) -> Result<(), StoreError> {
        if let QueueAction::Enqueue { input, .. } = &request.action {
            let exists: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM future_queue WHERE session_id=$1 AND admission_key=$2) OR EXISTS(SELECT 1 FROM admission_receipts WHERE session_id=$1 AND admission_key=$2)").bind(&request.session_id.0).bind(input.admission.key.as_str()).fetch_one(&mut **tx).await.map_err(db)?;
            if exists {
                return Err(StoreError::AdmissionConflict);
            }
        }
        if matches!(
            request.action,
            QueueAction::Enqueue { .. } | QueueAction::Prepare { .. }
        ) {
            let row = sqlx::query("SELECT COUNT(*) AS count, CAST(COALESCE(SUM(octet_length(data_json)),0) AS BIGINT) AS bytes FROM future_queue WHERE session_id=$1 AND state IN ('Queued','Claimed')")
                .bind(&request.session_id.0).fetch_one(&mut **tx).await.map_err(db)?;
            let count: i64 = row.try_get("count").map_err(db)?;
            let bytes: i64 = row.try_get("bytes").map_err(db)?;
            // Enqueue reserves a separate bounded slot for the FIFO head's
            // prepared capsule, so a full backlog can still make progress.
            let budget = if matches!(request.action, QueueAction::Prepare { .. }) {
                MAX_PENDING_BYTES + crate::queue::MAX_RECORD
            } else {
                MAX_PENDING_BYTES
            };
            if (matches!(request.action, QueueAction::Enqueue { .. })
                && count >= i64::try_from(MAX_PENDING).map_err(|_| invalid())?)
                || bytes + i64::try_from(encode(request)?.len()).map_err(|_| invalid())?
                    > i64::try_from(budget).map_err(|_| invalid())?
            {
                return Err(StoreError::Busy);
            }
        }
        if matches!(
            request.action,
            QueueAction::Claim { .. } | QueueAction::Prepare { .. }
        ) {
            let busy: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM runs WHERE session_id=$1 AND status IN ('pending','running','paused'))")
                .bind(&request.session_id.0).fetch_one(&mut **tx).await.map_err(db)?;
            if super::has_pending_continuation(tx, &request.session_id).await? || busy {
                return Err(StoreError::Busy);
            }
        }
        Ok(())
    }
}
async fn publish_queue_revision(
    tx: &mut Transaction<'_, Postgres>,
    request: &QueueMutation,
    digest: crabber_core::InputFingerprint,
    summary: crate::QueueSummary,
) -> Result<QueueReceipt, StoreError> {
    super::bump_public_revision(tx, &request.session_id).await?;
    let raw: i64 = sqlx::query_scalar("SELECT public_revision FROM sessions WHERE id=$1")
        .bind(&request.session_id.0)
        .fetch_one(&mut **tx)
        .await
        .map_err(db)?;
    let saved = QueueReceipt {
        session_id: request.session_id.clone(),
        operation_id: request.id.clone(),
        semantic_digest: digest,
        revision: SessionRevision::new(u64::try_from(raw).map_err(|_| corrupt())?)?,
        summary,
    };
    Ok(saved)
}

async fn save(
    tx: &mut Transaction<'_, Postgres>,
    audit: &Audit,
    record: &Record,
    previous: Option<crate::queue::source::Source>,
    proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<(), StoreError> {
    let request = &audit.request;
    let saved = &audit.receipt;
    record.validate(
        &request.session_id,
        &saved.summary.queue_id,
        saved.summary.sequence,
    )?;
    let seq = i64::try_from(record.summary.sequence).map_err(|_| invalid())?;
    let fresh = matches!(request.action, QueueAction::Enqueue { .. });
    if fresh {
        sqlx::query("INSERT INTO future_queue(session_id,queue_id,sequence,state,prepared,summary_json,data_json,admission_key,source_protocol) VALUES($1,$2,$3,$4,$5,$6,$7,$8,1)")
            .bind(&request.session_id.0).bind(&record.summary.queue_id.0).bind(seq).bind(format!("{:?}",record.summary.state)).bind(record.summary.prepared).bind(encode(&record.summary)?).bind(encode(record)?).bind(record.input.admission.key.as_str()).execute(&mut **tx).await.map_err(db)?;
    } else {
        let changed = sqlx::query("UPDATE future_queue SET state=$3,prepared=$4,summary_json=$5,data_json=$6 WHERE session_id=$1 AND queue_id=$2")
            .bind(&request.session_id.0).bind(&record.summary.queue_id.0).bind(format!("{:?}",record.summary.state)).bind(record.summary.prepared).bind(encode(&record.summary)?).bind(encode(record)?).execute(&mut **tx).await.map_err(db)?;
        if changed.rows_affected() != 1 {
            return Err(corrupt());
        }
    }
    protocol::publish(tx, audit, previous, fresh, proof)
        .await
        .map_err(protocol::domain)?;
    sqlx::query("INSERT INTO operation_receipts(session_id,operation_id,kind,semantic_digest,event_seq,data_json) VALUES($1,$2,'future_queue',$3,NULL,$4)")
        .bind(&request.session_id.0).bind(&request.id.0).bind(saved.semantic_digest.as_str()).bind(encode(saved)?).execute(&mut **tx).await.map_err(db)?;
    sqlx::query(
        "INSERT INTO future_queue_audit(session_id,operation_id,queue_id,revision,data_json) VALUES($1,$2,$3,$4,$5)",
    )
    .bind(&request.session_id.0)
    .bind(&request.id.0)
    .bind(&record.summary.queue_id.0)
    .bind(i64::try_from(saved.revision.get()).map_err(|_| invalid())?)
    .bind(encode(audit)?)
    .execute(&mut **tx)
    .await
    .map_err(db)?;
    Ok(())
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod process_tests;

#[cfg(test)]
mod corruption_tests;

impl PostgresStore {
    pub(super) async fn queue_replay(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        request: &QueueMutation,
    ) -> Result<Option<QueueOutcome>, StoreError> {
        if let Some(saved) = reads::receipt(tx, &request.session_id, &request.id).await? {
            if saved.semantic_digest != request.fingerprint()? {
                return Err(StoreError::AdmissionConflict);
            }
            return Ok(Some(QueueOutcome::Replayed(saved)));
        }
        Ok(None)
    }
    pub(super) async fn queue_preparation_context(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        authority: &QueueAuthority,
        intent: &crabber_core::InputFingerprint,
        proof: &mut crate::public_session::v2::proof::Proof,
    ) -> Result<(), StoreError> {
        reads::current_with_proof(tx, &authority.session_id, proof)
            .await
            .map_err(protocol::domain)?
            .ok_or(StoreError::NotFound)?;
        protocol::routing(tx, &authority.session_id, proof)
            .await
            .map_err(protocol::domain)?;
        let saved = reads::record_with_proof(tx, &authority.session_id, &authority.queue_id, proof)
            .await?
            .ok_or(StoreError::NotFound)?;
        if !protocol::protocol(
            tx,
            &crate::queue::source::Scope {
                session: authority.session_id.clone(),
                queue: authority.queue_id.clone(),
            },
            proof,
        )
        .await
        .map_err(protocol::domain)?
        {
            return Err(StoreError::HostUnsupported);
        }
        if let Some(prepared) = &saved.prepared
            && crate::session_owner::preparation::queue_admission_intent(prepared)
                .map_err(|_| StoreError::AdmissionConflict)?
                != *intent
        {
            return Err(StoreError::AdmissionConflict);
        }
        let claim = saved.claim.as_ref().ok_or(StoreError::Conflict)?;
        if saved.summary.state != crate::QueueState::Claimed
            || claim.authority != *authority
            || claim.expires <= self.clock.now()
            || reads::head(tx, &authority.session_id)
                .await?
                .is_none_or(|head| head.queue_id != authority.queue_id)
        {
            return Err(StoreError::Conflict);
        }
        let busy: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM runs WHERE session_id=$1 AND status IN ('pending','running','paused'))")
            .bind(&authority.session_id.0).fetch_one(&mut **tx).await.map_err(db)?;
        if busy || super::has_pending_continuation(tx, &authority.session_id).await? {
            return Err(StoreError::Busy);
        }
        Ok(())
    }
}

#[cfg(test)]
mod publication_tests;

#[cfg(test)]
mod migration_tests;
