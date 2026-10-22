//! Transactional private retention around public runtime registration.
use super::*;
use crabber::extension::{SessionInvocationAdmission, SessionInvocationError};

impl Policy {
    pub(super) async fn start(
        self: &Arc<Self>,
        context: ToolContext,
        arguments: Value,
    ) -> Result<Value, ExtensionError> {
        if context.cancel.is_cancelled() {
            return Err(failure("cancelled"));
        }
        let (task, profile) = input::normalize(arguments, &self.limits)?;
        let owner = Owner::from_context(&context)?;
        let invocation = invocation_request(&context, &self.hash)?;
        {
            let tasks = lock(&self.tasks);
            if tasks.closing {
                return Ok(json!({"status":"unavailable"}));
            }
            if let Some(record) = tasks
                .records
                .values()
                .find(|record| record.owner == owner && record.invocation == invocation)
            {
                // Only a positive Started acknowledgement proves this local
                // metadata replay. Pending/unknown never fabricates success.
                return Ok(if record.acknowledged.load(Ordering::Acquire) {
                    json!({"status":"started","task_id":record.id})
                } else {
                    json!({"status":"unavailable"})
                });
            }
        }
        // Reserve reconciliation space before registration, including caller-drop
        // uncertainty. Every unknown acknowledgement keeps its exact request.
        let Ok(slot) = self.reconciliation.clone().try_acquire_owned() else {
            return Ok(json!({"status":"unavailable"}));
        };
        let shared_permit = match self.shared.as_ref().map(|pool| pool.acquire()).transpose() {
            Ok(permit) => permit,
            Err(()) => return Ok(json!({"status":"unavailable"})),
        };
        let Ok(permit) = self.capacity.clone().try_acquire_owned() else {
            return Ok(json!({"status":"unavailable"}));
        };
        let ReservedRecord { record, evicted } = {
            let mut tasks = lock(&self.tasks);
            let Some(reservation) =
                tasks.admit(owner, self.limits.max_tracked, invocation.clone())?
            else {
                return Ok(json!({"status":"unavailable"}));
            };
            reservation
        };
        let dispatch = Arc::new(Mutex::new(Dispatch::Pending));
        let mut registration = Registration {
            policy: self.clone(),
            record: record.clone(),
            evicted,
            dispatch: dispatch.clone(),
            slot: Some(slot),
        };
        let request = Request::new(
            &context,
            (task, profile),
            record.token.child_token(),
            self.limits.max_background_duration,
            true,
        );
        let lifecycle = RunnerLifecycle::new(self.clone(), record, permit, shared_permit);
        let admitted = context
            .session_invocations()
            .spawn(invocation.clone(), move |inv| {
                let admitted = {
                    let mut state = lock(&dispatch);
                    if matches!(*state, Dispatch::Abandoned) {
                        false
                    } else {
                        *state = Dispatch::Dispatched;
                        true
                    }
                };
                async move {
                    if admitted {
                        lifecycle.run(inv, request).await
                    } else {
                        drop(lifecycle);
                        Ok(())
                    }
                }
            })
            .await;
        match admitted {
            Ok(SessionInvocationAdmission::Started) => {
                let id = registration.commit_started();
                Ok(json!({"status":"started","task_id":id}))
            }
            outcome => {
                registration.rollback();
                if !matches!(outcome, Err(SessionInvocationError::UnknownOutcome)) {
                    registration.known();
                }
                match outcome {
                    Ok(SessionInvocationAdmission::Replayed) => {
                        let tasks = lock(&self.tasks);
                        tasks
                            .records
                            .values()
                            .find(|r| {
                                r.owner == registration.record.owner
                                    && r.invocation == invocation
                                    && r.acknowledged.load(Ordering::Acquire)
                            })
                            .map(|r| json!({"status":"started","task_id":r.id}))
                            .ok_or_else(|| failure("session-jobs"))
                    }
                    Err(SessionInvocationError::Capacity | SessionInvocationError::Rejected) => {
                        Ok(json!({"status":"unavailable"}))
                    }
                    _ => Err(failure("session-jobs")),
                }
            }
        }
    }
}

fn invocation_request(
    context: &ToolContext,
    hash: &str,
) -> Result<SessionInvocationRequest, ExtensionError> {
    let key = crate::config_hash(&(&context.session_id, &context.run_id, &context.call_id));
    let key = format!("delegate-task.{}", &key[..32]);
    Ok(SessionInvocationRequest {
        id: SessionOwnerId::from(key.clone()),
        reserve_id: SessionOwnerOperationId::from(format!("{key}.reserve")),
        begin_id: SessionOwnerOperationId::from(format!("{key}.begin")),
        join_id: SessionOwnerOperationId::from(format!("{key}.join")),
        component_id: COMPONENT_ID.into(),
        behavior_fingerprint: InputFingerprint::new(hash).map_err(|_| failure("session-jobs"))?,
    })
}
enum Dispatch {
    Pending,
    Dispatched,
    Abandoned,
}
/// Owns a reserved registry slot and its former terminal result across await.
/// An unacknowledged record cannot be selected by another admission.
struct Registration {
    policy: Arc<Policy>,
    record: Arc<TaskRecord>,
    evicted: Option<Arc<TaskRecord>>,
    dispatch: Arc<Mutex<Dispatch>>,
    slot: Option<tokio::sync::OwnedSemaphorePermit>,
}
impl Registration {
    fn known(&mut self) {
        self.evicted.take();
        self.slot.take();
    }
    fn commit_started(self) -> String {
        self.commit_started_observed(|| {})
    }
    // The private observer qualifies the commit boundary without a host hook.
    // No guard-owned output may outlive publishing its row as prunable.
    fn commit_started_observed(mut self, committed: impl FnOnce()) -> String {
        let policy = self.policy.clone();
        let _tasks = lock(&policy.tasks);
        let id = self.record.id.clone();
        self.known();
        self.record.acknowledged.store(true, Ordering::Release);
        drop(self); // no slot remains, so Drop cannot re-enter the registry
        committed();
        id
    }
    fn rollback(&mut self) {
        let mut dispatch = lock(&self.dispatch);
        // Factory dispatch proves fresh runtime ownership. A lost caller must
        // retain this real job; nondispatched rollback never discards its row.
        if matches!(*dispatch, Dispatch::Dispatched) {
            return;
        }
        *dispatch = Dispatch::Abandoned;
        let mut tasks = lock(&self.policy.tasks);
        tasks.records.remove(&self.record.id);
        if let Some(old) = self.evicted.take() {
            tasks.records.insert(old.id.clone(), old);
        }
    }
}
impl Drop for Registration {
    fn drop(&mut self) {
        if let Some(slot) = self.slot.take() {
            self.rollback();
            let mut tasks = lock(&self.policy.tasks);
            if !tasks
                .unresolved
                .iter()
                .any(|(r, _)| r.id == self.record.invocation.id)
            {
                tasks
                    .unresolved
                    .push((self.record.invocation.clone(), slot));
            }
        }
    }
}

#[cfg(test)]
mod tests;
