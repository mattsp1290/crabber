//! Explicit host outcomes settle logical results without claiming cleanup.
#[path = "resolution/barrier.rs"]
mod barrier;
#[path = "resolution/faults.rs"]
mod faults;
#[path = "resolution/guards.rs"]
mod guards;
#[path = "resolution/lifecycle.rs"]
mod lifecycle;
use super::{applied, fixture, mutation, resolver};
use crate::{
    PhaseAuthority, PhaseCommand, PhaseMutation, PhaseQuiescence, PhaseQuiescenceResult,
    PhaseReceipt, PhaseResolution, PhaseResolutionOutcome, PhaseWriteAuthority, Store,
    UncertainPhaseChoice,
};
use crabber_core::{
    ManualClock, MediaKind, MediaSource, MessageContent, MessageId, PartId, RunId, SessionId,
    ToolOutput,
};
use time::OffsetDateTime;
pub(super) async fn fault_contract(store: &dyn Store, set_fault: &impl Fn(super::FaultPoint)) {
    faults::run(store, set_fault).await;
}

pub(super) struct Prepared {
    pub admitted: crate::AdmitOutcome,
    pub execution: Box<dyn crate::ExecutionStore>,
    pub worker: PhaseAuthority,
    pub request: PhaseResolution,
}
pub(super) fn descriptor(
    session: SessionId,
    run: RunId,
    request: PhaseMutation,
) -> PhaseResolution {
    PhaseResolution {
        session_id: session,
        run_id: run,
        message_id: MessageId(format!("resolution-result-{}", request.id)),
        part_id: PartId(format!("resolution-part-{}", request.id)),
        resolution: request,
        turn_id: None,
        created_at: OffsetDateTime::UNIX_EPOCH,
        duration_ms: 7,
    }
}
pub(super) fn mixed_output() -> ToolOutput {
    ToolOutput::new(
        vec![
            MessageContent::Text {
                text: "protected reconciliation".into(),
            },
            MessageContent::Media {
                kind: MediaKind::Image,
                mime_type: "image/png".into(),
                source: MediaSource::File {
                    file_id: "opaque-resolution-artifact".into(),
                },
            },
            MessageContent::Text {
                text: "trailing text".into(),
            },
        ],
        false,
    )
    .unwrap()
}
pub(super) async fn prepare(
    store: &dyn Store,
    choice: UncertainPhaseChoice,
    quiet: PhaseQuiescence,
    started: bool,
) -> Prepared {
    let (admitted, execution, owner, claim) = fixture(store).await;
    let (mut receipt, worker) = applied(
        execution
            .mutate_phase(claim, Some(PhaseWriteAuthority::Attempt(owner)))
            .await
            .unwrap(),
    );
    let worker = worker.unwrap();
    if started {
        receipt = applied(
            execution
                .mutate_phase(
                    mutation(
                        receipt.revision,
                        PhaseCommand::Begin {
                            target: receipt.phase.target.clone(),
                        },
                    ),
                    Some(PhaseWriteAuthority::Phase(worker.clone())),
                )
                .await
                .unwrap(),
        )
        .0;
    }
    receipt = applied(
        execution
            .mutate_phase(
                mutation(
                    receipt.revision,
                    PhaseCommand::MarkStoppedUncertain {
                        target: receipt.phase.target.clone(),
                        claim: resolver(),
                    },
                ),
                None,
            )
            .await
            .unwrap(),
    )
    .0;
    if quiet != PhaseQuiescence::Pending {
        receipt = applied(
            execution
                .mutate_phase(
                    mutation(
                        receipt.revision,
                        PhaseCommand::RecordQuiescence {
                            target: receipt.phase.target.clone(),
                            result: if quiet == PhaseQuiescence::Complete {
                                PhaseQuiescenceResult::Complete
                            } else {
                                PhaseQuiescenceResult::Incomplete
                            },
                            host_claim: Some(resolver()),
                        },
                    ),
                    None,
                )
                .await
                .unwrap(),
        )
        .0;
    }
    let request = descriptor(
        admitted.session.id.clone(),
        admitted.run.id.clone(),
        mutation(
            receipt.revision,
            PhaseCommand::ResolveUncertain {
                target: receipt.phase.target,
                claim: resolver(),
                choice,
            },
        ),
    );
    Prepared {
        admitted,
        execution,
        worker,
        request,
    }
}
pub(super) fn fresh(outcome: PhaseResolutionOutcome) -> PhaseReceipt {
    let PhaseResolutionOutcome::Applied(receipt) = outcome else {
        panic!("fresh host resolution")
    };
    receipt
}
pub(super) async fn evidence(store: &dyn Store, request: &PhaseResolution) -> serde_json::Value {
    serde_json::json!({
        "run":store.get_run(&request.run_id).await.unwrap(),
        "attempt":store.latest_execution_attempt(&request.run_id).await.unwrap().map(|versioned|versioned.attempt),
        "phase":store.tool_phase(&request.run_id, &request.resolution.command.target().id).await.unwrap(),
        "calls":super::settlement::read_calls(store, &request.session_id).await.unwrap(),
        "messages":store.list_messages(&request.session_id,None).await.unwrap(),
        "events":store.list_events(&request.session_id,None,1000).await.unwrap(),
        "revision":store.session_revision(&request.session_id).await.unwrap(),
        "approval":store.approval_snapshot(&request.session_id,&request.run_id).await.unwrap(),
    })
}
pub(super) async fn run(store: &dyn Store, clock: &ManualClock) {
    for quiet in [
        PhaseQuiescence::Pending,
        PhaseQuiescence::Incomplete,
        PhaseQuiescence::Complete,
    ] {
        for choice in [
            UncertainPhaseChoice::Success {
                output: mixed_output(),
            },
            UncertainPhaseChoice::Failure { output: None },
            UncertainPhaseChoice::Failure {
                output: Some(mixed_output()),
            },
            UncertainPhaseChoice::Refuse,
        ] {
            clock.set(OffsetDateTime::UNIX_EPOCH);
            lifecycle::run(store, clock, choice, quiet).await;
        }
    }
    clock.set(OffsetDateTime::UNIX_EPOCH);
    let pending = prepare(
        store,
        UncertainPhaseChoice::Success {
            output: mixed_output(),
        },
        PhaseQuiescence::Pending,
        false,
    )
    .await;
    fresh(
        store
            .resolve_uncertain_phase(pending.request)
            .await
            .unwrap(),
    );
    guards::run(store).await;
    barrier::run(store, clock).await;
    clock.set(OffsetDateTime::UNIX_EPOCH);
}

pub(super) fn result_revision(request: &PhaseResolution) -> crabber_core::SessionRevision {
    let steps = if request.output().unwrap().is_some() {
        3
    } else {
        1
    };
    (0..steps).fold(request.resolution.expected_revision, |revision, _| {
        revision.next().unwrap()
    })
}
