//! Atomic attempt transitions; no provider or tool work occurs inside a transaction.
use crate::native_enrollment::caller::{self, Caller, Fresh};
pub(super) mod accounting;
mod prepare;
pub(super) use records::{load as load_host_attempt, save as save_host_attempt};
pub(super) use records::{
    load_with_proof as load_host_attempt_with_proof, receipt_with_proof as host_receipt_with_proof,
};
mod control;
pub(super) use control::host_turn;
mod pristine;
pub(super) mod records;
use super::{SqliteExecution, SqliteStore, admission_execution, db};
use crate::transaction::Transaction;
use crate::{
    AttemptAuthority, AttemptCommand, AttemptError as Error, AttemptMutation,
    AttemptMutationOutcome as Outcome, AttemptReceipt, VersionedAttempt,
    attempt::{self, sql},
};
use crabber_core::{AttemptId, ControlId, Run, RunId, SessionId};
use sqlx::Sqlite;

async fn finish<T>(tx: Transaction<'_, Sqlite>, result: Result<T, Error>) -> Result<T, Error> {
    super::transactions::finish(tx, result, sql::unknown).await
}
impl SqliteStore {
    pub(super) async fn read_attempt(
        &self,
        run: &RunId,
        id: Option<&AttemptId>,
    ) -> Result<Option<VersionedAttempt>, Error> {
        if let Some(id) = id {
            attempt::validate_identifier(&id.0)?;
        }
        let mut tx = crate::transaction::begin(&self.readers)
            .await
            .map_err(db)
            .map_err(sql::unknown)?;
        let result = async {
            if let Some((record, revision)) = records::load(&mut tx, run, id, false).await? {
                return Ok(Some(VersionedAttempt {
                    revision,
                    attempt: record.snapshot,
                }));
            }
            let exists: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM runs WHERE id=$1)")
                .bind(&run.0)
                .fetch_one(&mut *tx)
                .await
                .map_err(db)
                .map_err(sql::unknown)?;
            if !exists {
                return Err(Error::NotFound);
            }
            Ok(None)
        }
        .await;
        finish(tx, result).await
    }
    pub(super) async fn attempt_receipt(
        &self,
        session: &SessionId,
        id: &ControlId,
    ) -> Result<Option<AttemptReceipt>, Error> {
        attempt::validate_identifier(&id.0)?;
        let mut tx = crate::transaction::begin(&self.readers)
            .await
            .map_err(db)
            .map_err(sql::unknown)?;
        let result = records::receipt(&mut tx, session, id).await;
        finish(tx, result).await
    }
}
impl SqliteExecution {
    pub(super) async fn change_attempt(
        &self,
        request: AttemptMutation,
        authority: Option<AttemptAuthority>,
    ) -> Result<Outcome, Error> {
        self.change_attempt_inner(request, authority, None).await
    }
    pub(super) async fn conclude_owned_attempt(
        &self,
        conclusion: crate::AttemptConclusion,
        authority: Option<AttemptAuthority>,
    ) -> Result<Outcome, Error> {
        conclusion.validate()?;
        self.change_attempt_inner(conclusion.finish.clone(), authority, Some(conclusion))
            .await
    }
    async fn change_attempt_inner(
        &self,
        request: AttemptMutation,
        authority: Option<AttemptAuthority>,
        conclusion: Option<crate::AttemptConclusion>,
    ) -> Result<Outcome, Error> {
        self.change_attempt_with_proof(
            request,
            authority,
            conclusion,
            &mut crate::public_session::v2::proof::Proof::bounded(32 << 20),
        )
        .await
    }
    pub(super) async fn change_attempt_with_proof(
        &self,
        request: AttemptMutation,
        authority: Option<AttemptAuthority>,
        conclusion: Option<crate::AttemptConclusion>,
        read_proof: &mut crate::public_session::v2::proof::Proof,
    ) -> Result<Outcome, Error> {
        attempt::validate_request(&request)?;
        read_proof
            .measure(&(&self.fence, &request, &conclusion))
            .map_err(|_| Error::UnknownStoreOutcome)?;
        caller::measure_authority(authority.as_ref(), read_proof)
            .map_err(|_| Error::UnknownStoreOutcome)?;
        let mut tx = self.store.begin_write().await.map_err(sql::unknown)?;
        let result = async {
            let run = accounting::load(&mut tx, &self.fence.run_id, read_proof).await?;
            Box::pin(self.apply_attempt_inner(
                &mut tx,
                &run,
                &request,
                (authority.as_ref(), conclusion.as_ref(), false),
                Caller::Raw,
                read_proof,
            ))
            .await
        }
        .await;
        #[cfg(test)]
        let result = if self
            .store
            .attempt_fault
            .load(std::sync::atomic::Ordering::SeqCst)
            == 1
            && matches!(&result, Ok(Outcome::Applied { .. }))
        {
            Err(Error::UnknownStoreOutcome)
        } else {
            result
        };
        let result = finish(tx, result).await;
        #[cfg(test)]
        if self
            .store
            .attempt_fault
            .load(std::sync::atomic::Ordering::SeqCst)
            == 2
            && matches!(&result, Ok(Outcome::Applied { .. }))
        {
            return Err(Error::UnknownStoreOutcome);
        }
        result
    }
    pub(super) async fn apply_attempt(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        run: &Run,
        request: &AttemptMutation,
        authority: Option<&AttemptAuthority>,
        conclusion: Option<&crate::AttemptConclusion>,
    ) -> Result<Outcome, Error> {
        Box::pin(self.apply_attempt_inner(
            tx,
            run,
            request,
            (authority, conclusion, false),
            Caller::Raw,
            &mut crate::public_session::v2::proof::Proof::bounded(32 << 20),
        ))
        .await
    }
    pub(super) async fn apply_claim_attempt(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        run: &Run,
        request: &AttemptMutation,
        read_proof: &mut crate::public_session::v2::proof::Proof,
    ) -> Result<Outcome, Error> {
        Box::pin(self.apply_attempt_inner(
            tx,
            run,
            request,
            (None, None, true),
            Caller::Raw,
            read_proof,
        ))
        .await
    }
    pub(super) async fn apply_attempt_with_proof(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        run: &Run,
        request: &AttemptMutation,
        authority: Option<&AttemptAuthority>,
        conclusion: Option<&crate::AttemptConclusion>,
        read_proof: &mut crate::public_session::v2::proof::Proof,
    ) -> Result<Outcome, Error> {
        Box::pin(self.apply_attempt_inner(
            tx,
            run,
            request,
            (authority, conclusion, false),
            Caller::Raw,
            read_proof,
        ))
        .await
    }
    async fn apply_attempt_inner(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        run: &Run,
        request: &AttemptMutation,
        context: (
            Option<&AttemptAuthority>,
            Option<&crate::AttemptConclusion>,
            bool,
        ),
        caller: Caller,
        read_proof: &mut crate::public_session::v2::proof::Proof,
    ) -> Result<Outcome, Error> {
        let (authority, conclusion, claiming) = context;
        let (digest, replayed) =
            accounting::request(tx, run, request, authority, conclusion, read_proof).await?;
        if let Some(replayed) = replayed {
            return Ok(replayed);
        }
        let now = self.store.clock.now();
        attempt::verify_run(run, &self.fence, now)?;
        if admission_execution::load_record_with_proof(tx, &run.id, read_proof)
            .await
            .map_err(|_| Error::UnknownStoreOutcome)?
            .is_some_and(|r| r.state == crate::AdmissionExecutionState::Unstarted)
        {
            return Err(Error::StaleOwner);
        }
        super::approval::guard_attempt_with_proof(tx, run, request, claiming, read_proof).await?;
        super::tool_continuation::guard_attempt_with_proof(tx, run, request, read_proof).await?;
        let current = records::revision(tx, &run.session_id).await?;
        if current != request.expected_revision {
            return Err(Error::RevisionConflict);
        }
        if let Some(conclusion) = conclusion {
            check_boundary(tx, run, conclusion).await?;
        }
        if matches!(request.command, AttemptCommand::ReserveBound { .. }) {
            pristine::check_with_proof(tx, run, read_proof).await?;
        }
        let insert = matches!(
            request.command,
            AttemptCommand::Reserve { .. } | AttemptCommand::ReserveBound { .. }
        );
        let (record, grant) = prepare::record(
            tx,
            run,
            request,
            authority,
            now,
            conclusion.is_some(),
            read_proof,
        )
        .await?;
        caller::measure_record(&record, read_proof).map_err(|_| Error::UnknownStoreOutcome)?;
        let authority = grant.then(|| record.authority()).transpose()?;
        read_proof
            .measure(&(&request.command, &record.snapshot))
            .map_err(|_| Error::UnknownStoreOutcome)?;
        let settlement =
            apply_run_transition(tx, run, request, conclusion, now, read_proof).await?;
        let revision = records::revision(tx, &run.session_id)
            .await?
            .next()
            .map_err(|_| Error::RevisionExhausted)?;
        let event = crate::attempt_conclusion::publication(
            run,
            request,
            conclusion,
            &record.snapshot,
            digest.clone(),
            settlement,
            now,
        )?;
        read_proof
            .measure(&event)
            .map_err(|_| Error::UnknownStoreOutcome)?;
        let source_event = event.clone();
        records::save(tx, &record, insert).await?;
        let receipt =
            records::publish(tx, run, request, &record.snapshot, digest, revision, event).await?;
        if caller::relevant(&request.command) {
            super::native_enrollment::record_raw_native(
                tx,
                caller,
                &Fresh::Attempt(caller::Attempt {
                    run,
                    request,
                    record: &record,
                    receipt: &receipt,
                    event: &source_event,
                    issued: authority.as_ref(),
                }),
                read_proof,
                #[cfg(test)]
                &self.store.native_raw_probe,
            )
            .await
            .map_err(|_| Error::UnknownStoreOutcome)?;
        }
        super::native_parent::record_attempt(tx, run, request, &record, &receipt, read_proof)
            .await?;
        super::run_control::suspension::save(tx, run, conclusion, &receipt)
            .await
            .map_err(|_| Error::UnknownStoreOutcome)?;
        Ok(Outcome::Applied { receipt, authority })
    }
}
pub(super) async fn renew(
    tx: &mut Transaction<'_, Sqlite>,
    run: &Run,
) -> Result<(), crate::StoreError> {
    records::renew(tx, run).await
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod phase_tests;

#[cfg(test)]
mod metadata_tests;

pub(super) async fn load_owned(
    tx: &mut Transaction<'_, Sqlite>,
    run: &RunId,
    id: &AttemptId,
) -> Result<Option<attempt::AttemptRecord>, Error> {
    records::load(tx, run, Some(id), true)
        .await
        .map(|v| v.map(|(r, _)| r))
}

async fn check_boundary(
    tx: &mut Transaction<'_, Sqlite>,
    run: &Run,
    conclusion: &crate::AttemptConclusion,
) -> Result<(), Error> {
    if let crate::RunBoundary::Terminal { status, .. } = &conclusion.boundary {
        if *status == crabber_core::RunStatus::Completed {
            let pending: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM inbox WHERE session_id=$1 AND consumed_by_run IS NULL)").bind(&run.session_id.0).fetch_one(&mut **tx).await.map_err(db).map_err(sql::unknown)?;
            if pending
                || super::run_control::pending(tx, run)
                    .await
                    .map_err(|_| Error::UnknownStoreOutcome)?
            {
                return Err(Error::PendingInput);
            }
        }
        let unfinished: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM tool_calls WHERE run_id=$1 AND status IN ('pending','running','waiting'))").bind(&run.id.0).fetch_one(&mut **tx).await.map_err(db).map_err(sql::unknown)?;
        if unfinished {
            return Err(Error::CleanupBlocked);
        }
    }
    Ok(())
}

async fn apply_run_transition(
    tx: &mut Transaction<'_, Sqlite>,
    run: &Run,
    request: &AttemptMutation,
    conclusion: Option<&crate::AttemptConclusion>,
    now: time::OffsetDateTime,
    read_proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<Option<crabber_core::EventRecord>, Error> {
    let mut settlement = None;
    if let Some(conclusion) = conclusion {
        let mut source = conclusion.materialize(run)?;
        super::records::insert_event(tx, run, &source)
            .await
            .map_err(sql::unknown)?;
        let seq: i64 = sqlx::query_scalar(
            "SELECT seq FROM events WHERE session_id=$1 AND run_id=$2 ORDER BY seq DESC LIMIT 1",
        )
        .bind(&run.session_id.0)
        .bind(&run.id.0)
        .fetch_one(&mut **tx)
        .await
        .map_err(db)
        .map_err(sql::unknown)?;
        source.cursor = Some(crabber_core::EventCursor(
            u64::try_from(seq).map_err(|_| Error::UnknownStoreOutcome)?,
        ));
        settlement = Some(source);
        let mut updated = run.clone();
        conclusion.apply_run(&mut updated, now)?;
        super::run_control::finalize(tx, &updated, now)
            .await
            .map_err(|_| Error::UnknownStoreOutcome)?;
        super::records::save_run_with_proof(tx, &updated, None, read_proof)
            .await
            .map_err(sql::unknown)?;
        if matches!(conclusion.boundary, crate::RunBoundary::Terminal { .. }) {
            super::approval::release_terminal(tx, run, request).await?;
            super::tool_continuation::release_terminal(tx, run)
                .await
                .map_err(|_| Error::UnknownStoreOutcome)?;
        }
    } else if matches!(
        request.command,
        AttemptCommand::Reserve { .. } | AttemptCommand::ReserveBound { .. }
    ) && run.status == crabber_core::RunStatus::Paused
    {
        let mut activated = run.clone();
        activated.status = crabber_core::RunStatus::Running;
        activated.updated_at = now;
        super::records::save_run_with_proof(tx, &activated, None, read_proof)
            .await
            .map_err(sql::unknown)?;
    }
    Ok(settlement)
}

/// Pure no-parent eligibility; caller holds the same session transaction.
pub(super) async fn pristine_for_expired(
    tx: &mut crate::transaction::Transaction<'_, Sqlite>,
    run: &crabber_core::Run,
) -> Result<(), crate::AttemptError> {
    pristine::check(tx, run).await
}

pub(super) async fn pristine_for_expired_with_proof(
    tx: &mut crate::transaction::Transaction<'_, sqlx::Sqlite>,
    run: &crabber_core::Run,
    proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<(), crate::AttemptError> {
    if !proof.active() {
        return pristine_for_expired(tx, run).await;
    }
    pristine::check_with_proof(tx, run, proof).await
}
