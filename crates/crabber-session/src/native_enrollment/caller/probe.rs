//! Per-Store test tripwire. Observations cannot become native evidence.
use super::Fresh;
use super::verify::Scope;
use crate::public_session::v2::proof::{Failure, unknown};
use crabber_core::{ControlId, InputFingerprint, RunId, SessionId, ToolCallId};
use std::sync::Mutex;

#[derive(Default)]
pub(crate) struct Probe(Mutex<Option<Selection>>);
struct Selection {
    session: SessionId,
    operation: Identity,
    refuse: bool,
    lose_ack: bool,
    hits: Vec<InputFingerprint>,
}
#[derive(PartialEq, Eq)]
enum Identity {
    Initial,
    Control(ControlId),
    LegacyTool(RunId, ToolCallId),
    AdmissionBegin(RunId),
    AdmissionRecovery(RunId),
}
impl Identity {
    fn matches(&self, fresh: &Fresh<'_>) -> bool {
        match (self, fresh) {
            (Self::Initial, Fresh::Initial(_)) => true,
            (Self::Control(id), Fresh::Attempt(a)) => *id == a.request.id,
            (Self::Control(id), Fresh::Phase(p)) => *id == p.request.id,
            (Self::AdmissionBegin(run), Fresh::AdmissionBegin(a)) => *run == a.run.id,
            (Self::AdmissionRecovery(run), Fresh::AdmissionRecovery(a)) => *run == a.previous.id,
            (Self::LegacyTool(run, id), Fresh::LegacyTool(a)) => *run == a.run.id && id == a.id,
            _ => false,
        }
    }
}
impl Probe {
    pub(crate) fn arm(&self, session: &SessionId, operation: &ControlId, refuse: bool) {
        *self.0.lock().unwrap() = Some(Selection {
            session: session.clone(),
            operation: Identity::Control(operation.clone()),
            refuse,
            lose_ack: false,
            hits: Vec::new(),
        });
    }
    pub(crate) fn arm_initial(&self, session: &SessionId, refuse: bool) {
        *self.0.lock().unwrap() = Some(Selection {
            session: session.clone(),
            operation: Identity::Initial,
            refuse,
            lose_ack: false,
            hits: Vec::new(),
        });
    }
    pub(crate) fn arm_legacy(
        &self,
        session: &SessionId,
        run: &RunId,
        call: &ToolCallId,
        refuse: bool,
    ) {
        *self.0.lock().unwrap() = Some(Selection {
            session: session.clone(),
            operation: Identity::LegacyTool(run.clone(), call.clone()),
            refuse,
            lose_ack: false,
            hits: Vec::new(),
        });
    }
    pub(crate) fn arm_begin(&self, session: &SessionId, run: &RunId, refuse: bool) {
        *self.0.lock().unwrap() = Some(Selection {
            session: session.clone(),
            operation: Identity::AdmissionBegin(run.clone()),
            refuse,
            lose_ack: false,
            hits: Vec::new(),
        });
    }
    pub(crate) fn arm_admission_recovery(&self, session: &SessionId, run: &RunId, refuse: bool) {
        *self.0.lock().unwrap() = Some(Selection {
            session: session.clone(),
            operation: Identity::AdmissionRecovery(run.clone()),
            refuse,
            lose_ack: false,
            hits: Vec::new(),
        });
    }
    pub(crate) fn after_admission_recovery(&self, run: &RunId) -> bool {
        self.after_commit(&Identity::AdmissionRecovery(run.clone()))
    }
    pub(crate) fn lose_ack(&self) {
        self.0.lock().unwrap().as_mut().unwrap().lose_ack = true;
    }
    pub(crate) fn after_legacy(&self, run: &RunId, call: &ToolCallId) -> bool {
        self.after_commit(&Identity::LegacyTool(run.clone(), call.clone()))
    }
    pub(crate) fn after_begin(&self, run: &RunId) -> bool {
        self.after_commit(&Identity::AdmissionBegin(run.clone()))
    }
    fn after_commit(&self, identity: &Identity) -> bool {
        self.0.lock().unwrap().as_ref().is_some_and(|selected| {
            selected.lose_ack && &selected.operation == identity && !selected.hits.is_empty()
        })
    }
    pub(crate) fn hits(&self) -> Vec<InputFingerprint> {
        self.0
            .lock()
            .unwrap()
            .as_ref()
            .map_or_else(Vec::new, |s| s.hits.clone())
    }
    pub(crate) fn observe(&self, scope: &Scope<'_>, fresh: &Fresh<'_>) -> Result<(), Failure> {
        let mut state = self.0.lock().unwrap();
        let Some(selected) = state
            .as_mut()
            .filter(|s| &s.session == scope.session && s.operation.matches(fresh))
        else {
            return Ok(());
        };
        let digest = match fresh {
            Fresh::Initial(i) => crate::native_enrollment::caller::contract::snapshot_digest(&(
                "crabber.native.raw-initial.test.v1",
                i.input.test_digest(),
                &i.first.issuer,
                &i.first.source,
                &i.result.session,
                &i.result.run,
                &i.result.fence,
                &i.result.assistant_placeholder,
                &i.result.epoch,
                &i.result.prior_history,
            )),

            Fresh::AdmissionRecovery(a) => {
                crate::native_enrollment::caller::contract::snapshot_digest(&(
                    "crabber.native.raw-admission-recovery.test.v1",
                    a.previous,
                    &a.result.record,
                    &a.result.run,
                    &a.result.fence,
                    a.completion,
                    &a.publication.issuer,
                    &a.publication.source,
                ))
            }
            Fresh::Phase(p) => crate::native_enrollment::caller::contract::snapshot_digest(&(
                "crabber.native.raw-phase.test.v1",
                p.run,
                p.request,
                &p.parent.snapshot,
                &p.parent.token,
                &p.parent.run_token,
                &p.record.snapshot,
                &p.record.token,
                &p.record.output,
                p.call,
                p.receipt,
                p.event,
                p.running_event,
                p.issued.is_some(),
            )),
            Fresh::AdmissionBegin(a) => {
                crate::native_enrollment::caller::contract::snapshot_digest(&(
                    "crabber.native.raw-admission-begin.test.v1",
                    a.fence,
                    a.run,
                    a.record,
                    a.sources,
                ))
            }
            Fresh::Attempt(a) => crate::native_enrollment::caller::contract::snapshot_digest(&(
                "crabber.native.raw-attempt.test.v1",
                a.run,
                a.request,
                &a.record.snapshot,
                &a.record.owner,
                &a.record.token,
                &a.record.run_token,
                a.record.lease_until,
                a.receipt,
                a.event,
                a.issued.is_some(),
            )),
            Fresh::LegacyTool(a) => crate::native_enrollment::caller::contract::snapshot_digest(&(
                "crabber.native.raw-legacy-tool.test.v1",
                a.run,
                a.fence,
                a.id,
                a.requested,
                a.call,
                a.cursor,
            )),
        };
        // A matched fixture can observe only a small fixed number of calls.
        if selected.hits.len() == 8 {
            return Err(unknown("test probe capacity"));
        }
        selected.hits.push(digest);
        if selected.refuse {
            return Err(unknown("scoped raw-call test fault"));
        }
        Ok(())
    }
}
