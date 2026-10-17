//! Actual unprepared sources: observation occurs before any fresh preparation.
use super::*;
use crate::session_owner::preparation::{InspectPendingToolPreparation, PendingToolSource};
pub(crate) async fn unprepared(
    store: &impl Store,
    clock: &ManualClock,
    kind: u8,
    recovery: bool,
    unkeyed: bool,
) -> InspectPendingToolPreparation {
    let kind = match kind {
        0 => BatchKind::Tool,
        1 => BatchKind::Frontend,
        2 => BatchKind::Mixed,
        _ => unreachable!(),
    };
    let (context, batch, operation) = Box::pin(staged(store, kind, unkeyed)).await;
    if !recovery {
        return InspectPendingToolPreparation {
            context,
            source: match operation {
                PreparedOperation::ToolClaim(c) => PendingToolSource::Claim(Box::new(c)),
                PreparedOperation::FrontendClaim(c) => {
                    PendingToolSource::FrontendClaim(Box::new(c))
                }
                _ => unreachable!(),
            },
        };
    }
    let (source, key, work, intent) = match &operation {
        PreparedOperation::ToolClaim(c) => (
            PreparationSource::ToolClaim(c.clone()),
            PreparationKey::ToolContinuation(c.id.clone()),
            ExistingWorkTarget::ToolClaim(c.continuation.clone()),
            c.fingerprint().unwrap(),
        ),
        PreparedOperation::FrontendClaim(c) => (
            PreparationSource::FrontendClaim(c.clone()),
            PreparationKey::ToolContinuation(c.claim.id.clone()),
            ExistingWorkTarget::FrontendClaim(c.claim.continuation.clone()),
            c.fingerprint().unwrap(),
        ),
        _ => unreachable!(),
    };
    let run = store
        .get_run(&context_run(&operation))
        .await
        .unwrap()
        .unwrap();
    let joined = Box::pin(prepare(
        store,
        &run,
        source,
        operation.clone(),
        key,
        work,
        intent,
    ))
    .await;
    Box::pin(complete(store, joined)).await;
    clock.set(clock.now() + time::Duration::seconds(31));
    let expected_revision = store.session_revision(context.session_id()).await.unwrap();
    let source = match operation {
        PreparedOperation::ToolClaim(mut original) => {
            original.owner = "fresh-original-owner".into();
            original.lease_ms = 1;
            PendingToolSource::Recovery(Box::new(crate::ToolContinuationRecovery {
                id: ControlId::new(),
                expected_revision,
                original,
                attempt: batch.next_attempt,
                owner: "pending-tool-preflight".into(),
                lease_ms: 30000,
            }))
        }
        PreparedOperation::FrontendClaim(mut original) => {
            original.claim.owner = "fresh-original-owner".into();
            original.claim.lease_ms = 1;
            PendingToolSource::FrontendRecovery(Box::new(crate::FrontendContinuationRecovery {
                id: ControlId::new(),
                expected_revision,
                original,
                attempt: batch.next_attempt,
                owner: "pending-tool-preflight".into(),
                lease_ms: 30000,
            }))
        }
        _ => unreachable!(),
    };
    InspectPendingToolPreparation { context, source }
}

pub(crate) const CASES: &[(u8, bool, bool)] = &[
    (0, false, false),
    (0, true, false),
    (0, false, true),
    (0, true, true),
    (1, false, false),
    (1, true, false),
    (1, false, true),
    (1, true, true),
    (2, false, false),
    (2, true, false),
    (2, false, true),
    (2, true, true),
];

pub(crate) async fn after_recovery(
    store: &impl Store,
    clock: &ManualClock,
    kind: u8,
    unkeyed: bool,
) -> InspectPendingToolPreparation {
    let kind = match kind {
        0 => BatchKind::Tool,
        1 => BatchKind::Frontend,
        2 => BatchKind::Mixed,
        _ => unreachable!(),
    };
    let f = Box::pin(fixture_kind(store, clock, kind, true, unkeyed)).await;
    clock.set(clock.now() + time::Duration::seconds(31));
    let revision = store
        .session_revision(f.context.session_id())
        .await
        .unwrap();
    let source = match f.joined.observed_operation() {
        PreparedOperation::ToolRecovery(saved) => {
            let mut request = saved.clone();
            request.id = ControlId::new();
            request.expected_revision = revision;
            request.original.owner = "new-original-owner".into();
            request.original.lease_ms = 1;
            PendingToolSource::Recovery(Box::new(request))
        }
        PreparedOperation::FrontendRecovery(saved) => {
            let mut request = saved.clone();
            request.id = ControlId::new();
            request.expected_revision = revision;
            request.original.claim.owner = "new-original-owner".into();
            request.original.claim.lease_ms = 1;
            PendingToolSource::FrontendRecovery(Box::new(request))
        }
        _ => unreachable!(),
    };
    InspectPendingToolPreparation {
        context: f.context,
        source,
    }
}
