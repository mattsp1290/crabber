#[cfg(test)]
use super::InitialOperationUsage;
use super::{
    Arc, Error, LocalLease, NATIVE_BOUND, Operation, OwnedMainDisposition,
    PendingInitialProducerObservation, PendingInitialProducerStatus, PreparationCommitError, Reply,
    RetainedInitial, protocol,
};

/// Only the existing supervisor constructs this after the actual original join.
struct JoinedInitialOriginal {
    retained: Arc<RetainedInitial>,
    disposition: OwnedMainDisposition,
}

/// Reconcile only this actually joined original's exact Seal. No grant or
/// replacement task can be recovered, including after process restart.
pub struct LocallyJoinedInitialSeal {
    original: JoinedInitialOriginal,
    terminal: Option<PendingInitialProducerStatus>,
}
impl LocallyJoinedInitialSeal {
    async fn attempt(&mut self) -> Result<PendingInitialProducerStatus, PreparationCommitError> {
        let retained = &self.original.retained;
        if let Some(resources) = &retained.consumer {
            let drained = resources.drain().await;
            drained?;
        }
        if let Some(terminal) = &self.terminal {
            return Ok(terminal.clone());
        }
        if retained.lock().identity.is_none() {
            let status = retained.lock().status.clone();
            self.terminal = Some(status.clone());
            if let Some(resources) = &retained.consumer {
                retained.calls.dispose_staged(resources).await;
            }
            return Ok(status);
        }
        let identity = retained.identity()?;
        let mut proof = RetainedInitial::new_proof(
            Operation::Seal,
            #[cfg(test)]
            &retained.budget,
        );
        let expected =
            protocol::prefix::expected(&retained.source.original, &identity, &mut proof)?;
        let lease = LocalLease {
            retained: retained.clone(),
            _call: None,
            joined: Some(self.original.disposition),
        };
        let outcome = retained
            .invoke(
                Operation::Seal,
                &expected,
                None,
                &mut proof,
                lease,
                Some(tokio::time::Instant::now() + NATIVE_BOUND),
            )
            .await;
        let accepted = outcome.and_then(|outcome| {
            Ok(match outcome {
                Reply::Closed(record) => match record.outcome {
                    protocol::CloseOutcome::NoPublication => {
                        PendingInitialProducerStatus::ClosedNoPublication
                    }
                    protocol::CloseOutcome::Assigned { .. } => {
                        PendingInitialProducerStatus::ClosedAssignment
                    }
                },
                Reply::RawCompleted(record) => {
                    PendingInitialProducerStatus::SourceAlreadyCompletedRaw {
                        run_id: record.run_id,
                    }
                }
                Reply::Opened(_)
                | Reply::AlreadyOpened(_)
                | Reply::Completed(_)
                | Reply::AlreadyPublished(_) => return Err(Error::UnknownStoreOutcome.into()),
            })
        });
        let accepted = accepted.and_then(|status| {
            if let PendingInitialProducerStatus::SourceAlreadyCompletedRaw { run_id } = &status {
                proof
                    .measure(&(run_id, run_id))
                    .map_err(|_| Error::Capacity)?;
            }
            retained.status(status.clone());
            self.terminal = Some(status.clone());
            Ok(status)
        });
        #[cfg(test)]
        if let Some(last) = retained.budget.usage.lock().unwrap().seal.last_mut() {
            *last = InitialOperationUsage {
                used: proof.used(),
                acknowledged: accepted.is_ok(),
            };
        }
        if accepted.is_ok()
            && let Some(resources) = &retained.consumer
        {
            retained.calls.dispose_staged(resources).await;
        }
        accepted
    }
    /// One bounded same-identity native attempt; no implicit retry loop.
    /// Physically blocked literal Drop stays visibly pending until observed.
    /// # Errors
    /// Native refusal, unknown acknowledgment, or failed literal observation.
    pub async fn retry(&mut self) -> Result<PendingInitialProducerStatus, PreparationCommitError> {
        let result = self.attempt().await;
        if result.is_err() {
            self.original.retained.fail();
        }
        result
    }
}

impl RetainedInitial {
    pub(in crate::session_owner::owned_scope::task::pending) fn resources_pending(&self) -> bool {
        self.consumer
            .as_ref()
            .is_some_and(|resources| resources.owner.snapshot().pending != 0)
    }
    pub(in crate::session_owner::owned_scope::task::pending) async fn join_original_resources(
        &self,
    ) {
        if let Some(resources) = &self.consumer {
            let _report = resources.owner.join().await;
        }
    }

    pub(in crate::session_owner::owned_scope::task::pending) async fn after_join(
        self: Arc<Self>,
        disposition: OwnedMainDisposition,
    ) -> PendingInitialProducerObservation {
        {
            let mut state = self.lock();
            state.joined = true;
            state.status = PendingInitialProducerStatus::OriginalJoined { disposition };
        }
        if let Some(resources) = &self.consumer
            && resources.original_joined().is_err()
        {
            self.lock().observation_failed = true;
            self.fail();
        }
        // No arming means no enrolled future could have been constructed.
        let armed = self.lock().identity.is_some();
        if !armed && self.consumer.is_none() {
            let (status, observation_failed) = {
                let state = self.lock();
                (state.status.clone(), state.observation_failed)
            };
            return PendingInitialProducerObservation {
                status,
                disposition,
                retry: None,
                observation_failed,
                error: None,
            };
        }
        let mut retry = LocallyJoinedInitialSeal {
            original: JoinedInitialOriginal {
                retained: self.clone(),
                disposition,
            },
            terminal: None,
        };
        let status = match retry.retry().await {
            Ok(status) => status,
            Err(_) => PendingInitialProducerStatus::Unresolved,
        };
        // Actual private result stays retained through join/native close attempt.
        // No caller receives a public completion or execution capability.
        if self.consumer.is_none() || retry.terminal.is_some() {
            let (staged, enrolled) = {
                let mut state = self.lock();
                (state.staged.take(), state.enrolled.take())
            };
            super::super::retain::discard((staged, enrolled));
        }
        PendingInitialProducerObservation {
            status,
            disposition,
            retry: if retry.terminal.is_some() {
                None
            } else {
                Some(retry)
            },
            observation_failed: self.lock().observation_failed,
            error: None,
        }
    }
}
