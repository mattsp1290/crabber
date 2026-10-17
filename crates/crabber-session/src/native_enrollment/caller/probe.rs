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
    hits: Vec<InputFingerprint>,
}
enum Identity {
    Control(ControlId),
    LegacyTool(RunId, ToolCallId),
    AdmissionBegin(RunId),
}
impl Identity {
    fn matches(&self, fresh: &Fresh<'_>) -> bool {
        match (self, fresh) {
            (Self::Control(id), Fresh::Attempt(a)) => *id == a.request.id,
            (Self::Control(id), Fresh::Phase(p)) => *id == p.request.id,
            (Self::AdmissionBegin(run), Fresh::AdmissionBegin(a)) => *run == a.run.id,
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
            hits: Vec::new(),
        });
    }
    pub(crate) fn arm_begin(&self, session: &SessionId, run: &RunId, refuse: bool) {
        *self.0.lock().unwrap() = Some(Selection {
            session: session.clone(),
            operation: Identity::AdmissionBegin(run.clone()),
            refuse,
            hits: Vec::new(),
        });
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
            Fresh::Phase(p) => &p.request.id,
        };
        let mut state = self.0.lock().unwrap();
        let Some(selected) = state
            .as_mut()
            .filter(|s| &s.session == scope.session && &s.operation == operation)
        else {
            return Ok(());
        };
        let digest = match fresh {
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
