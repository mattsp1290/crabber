//! Real original Tool/Frontend preparations, without inferred issuer cursors.
use super::*;
use crate::{ToolContinuationClaimOutcome, ToolContinuationReceipt, ToolContinuationSuspension};
use crabber_core::{Clock, ControlId, SessionContext, SessionId, SessionRevision};

pub(crate) struct Fixture {
    pub context: SessionContext,
    pub joined: JoinedPreparation,
    pub native: Box<crate::ToolContinuationGrant>,
    pub receipt: ToolContinuationReceipt,
    pub batch: ToolContinuationSuspension,
}

pub(crate) async fn fixture(
    store: &impl Store,
    clock: &ManualClock,
    frontend: bool,
    recovery: bool,
    unkeyed: bool,
) -> Fixture {
    let (context, batch, original) = staged(store, frontend, unkeyed).await;
    let (source, operation, key, work, intent) = match &original {
        PreparedOperation::ToolClaim(claim) => (
            PreparationSource::ToolClaim(claim.clone()),
            original.clone(),
            PreparationKey::ToolContinuation(claim.id.clone()),
            ExistingWorkTarget::ToolClaim(claim.continuation.clone()),
            claim.fingerprint().unwrap(),
        ),
        PreparedOperation::FrontendClaim(claim) => (
            PreparationSource::FrontendClaim(claim.clone()),
            original.clone(),
            PreparationKey::ToolContinuation(claim.claim.id.clone()),
            ExistingWorkTarget::FrontendClaim(claim.claim.continuation.clone()),
            claim.fingerprint().unwrap(),
        ),
        _ => unreachable!(),
    };
    let run = store
        .get_run(&context_run(&original))
        .await
        .unwrap()
        .unwrap();
    let mut joined = prepare(store, &run, source, operation, key, work, intent).await;
    let (mut receipt, mut native) = complete(store, joined.clone()).await;
    if recovery {
        clock.set(clock.now() + time::Duration::seconds(31));
        let revision = store.session_revision(context.session_id()).await.unwrap();
        let (source, operation, work, intent, id) = match original {
            PreparedOperation::ToolClaim(original) => {
                let request = crate::ToolContinuationRecovery {
                    id: ControlId::new(),
                    expected_revision: revision,
                    original,
                    attempt: batch.next_attempt.clone(),
                    owner: "tool-source-recovery".into(),
                    lease_ms: 30000,
                };
                (
                    PreparationSource::ToolRecovery(request.clone()),
                    PreparedOperation::ToolRecovery(request.clone()),
                    ExistingWorkTarget::ToolRecovery {
                        continuation: batch.continuation.clone(),
                        attempt: batch.next_attempt.clone(),
                    },
                    request.fingerprint().unwrap(),
                    request.id,
                )
            }
            PreparedOperation::FrontendClaim(original) => {
                let request = crate::FrontendContinuationRecovery {
                    id: ControlId::new(),
                    expected_revision: revision,
                    original,
                    attempt: batch.next_attempt.clone(),
                    owner: "frontend-source-recovery".into(),
                    lease_ms: 30000,
                };
                (
                    PreparationSource::FrontendRecovery(request.clone()),
                    PreparedOperation::FrontendRecovery(request.clone()),
                    ExistingWorkTarget::FrontendRecovery {
                        continuation: batch.continuation.clone(),
                        attempt: batch.next_attempt.clone(),
                    },
                    request.fingerprint().unwrap(),
                    request.id,
                )
            }
            _ => unreachable!(),
        };
        joined = prepare(
            store,
            &run,
            source,
            operation,
            PreparationKey::ToolContinuation(id),
            work,
            intent,
        )
        .await;
        (receipt, native) = complete(store, joined.clone()).await;
    }
    Fixture {
        context,
        joined,
        native,
        receipt,
        batch,
    }
}

fn context_run(operation: &PreparedOperation) -> crabber_core::RunId {
    match operation {
        PreparedOperation::ToolClaim(v) => v.run_id.clone(),
        PreparedOperation::FrontendClaim(v) => v.claim.run_id.clone(),
        _ => unreachable!(),
    }
}

async fn complete(
    store: &impl Store,
    joined: JoinedPreparation,
) -> (ToolContinuationReceipt, Box<crate::ToolContinuationGrant>) {
    let PreparationCompletion::ToolContinuation(ToolContinuationClaimOutcome::Applied {
        receipt,
        grant,
    }) = store.complete_preparation(joined).await.unwrap()
    else {
        panic!("actual original Tool completion");
    };
    (receipt, grant)
}

async fn prepare(
    store: &impl Store,
    run: &crabber_core::Run,
    source: PreparationSource,
    operation: PreparedOperation,
    key: PreparationKey,
    work: ExistingWorkTarget,
    intent: crabber_core::InputFingerprint,
) -> JoinedPreparation {
    let mut request = crate::session_owner::preparation_contract::continuations::request(
        store, run, source, key, work, intent,
    )
    .await;
    let binding = store
        .execution_binding(&run.session_id, &run.id)
        .await
        .unwrap()
        .unwrap();
    request.resource_contract =
        crate::session_owner::preparation::resource_contract(&binding.capsule.runtime_semantics)
            .unwrap();
    request.owner.binding.component_id = OWNED_PREPARATION_COMPONENT.into();
    request.owner.lease_ms = 30000;
    let root = crate::session_owner::preparation_contract::begin(store, &request).await;
    crate::run_control_contract::expired::owned::observe(store, root, operation).await
}

async fn staged(
    store: &impl Store,
    frontend: bool,
    unkeyed: bool,
) -> (
    SessionContext,
    ToolContinuationSuspension,
    PreparedOperation,
) {
    let session = SessionId::new();
    let mut bound =
        crate::session_owner::preparation_contract::fixture(&session, SessionRevision::INITIAL);
    let created = store
        .manage_session(crate::ManageSession {
            id: ControlId::new(),
            session_id: session.clone(),
            expected_revision: SessionRevision::INITIAL,
            change: crate::SessionChange::Create {
                workspace_id: bound.binding.request.workspace_id.clone(),
                directory: bound.binding.request.directory.clone(),
                title: "original Tool source".into(),
                defaults: crate::SessionDefaults::default(),
            },
        })
        .await
        .unwrap();
    bound = crate::session_owner::preparation_contract::fixture(&session, created.revision);
    let descriptor = crate::frontend_contract::fixture::descriptor();
    if frontend {
        let mut input =
            serde_json::to_value(bound.binding.input_envelope.as_ref().unwrap()).unwrap();
        input["frontend_tools"] = serde_json::json!([descriptor]);
        bound = crate::typed_admission_contract::prepare(
            bound,
            serde_json::from_value(input).unwrap(),
            time::OffsetDateTime::UNIX_EPOCH,
        );
    }
    let admitted =
        crate::session_owner::preparation_contract::resources::initial_driver_with_input(
            store, bound, unkeyed,
        )
        .await;
    let context = SessionContext::new(
        admitted.session.id.clone(),
        admitted.session.workspace_id.clone(),
        admitted.session.directory.clone(),
    );
    if frontend {
        let f = crate::frontend_contract::fixture::fixture_on(store, admitted, descriptor).await;
        f.execution
            .stage_frontend_call(f.request.clone(), f.authority.clone())
            .await
            .unwrap();
        let batch = crate::frontend_contract::fixture::suspension(store, &f.request).await;
        let receipt = f
            .execution
            .suspend_tool_continuation(
                batch.clone(),
                crate::ToolSuspensionAuthority {
                    attempt: f.authority,
                    phases: Vec::new(),
                },
            )
            .await
            .unwrap();
        let claim =
            crate::frontend_contract::fixture::external_claim(&f.request, &batch, receipt.revision);
        (context, batch, PreparedOperation::FrontendClaim(claim))
    } else {
        let f = crate::tool_continuation_contract::fixture::fixture_on(store, admitted).await;
        f.execution
            .suspend_tool_continuation(f.suspension.clone(), f.authority.clone())
            .await
            .unwrap();
        for index in 0..2 {
            store
                .answer_tool_interaction(
                    crate::tool_continuation_contract::fixture::answer(store, &f, index).await,
                )
                .await
                .unwrap();
        }
        let claim = crate::tool_continuation_contract::fixture::claim(store, &f).await;
        (context, f.suspension, PreparedOperation::ToolClaim(claim))
    }
}

/// Substitute valid full external content while preserving every locator and
/// original protected receipt/root. Original-source hashing must detect this.
pub(crate) fn replace_frontend_result(
    record: &mut crate::tool_continuation::ToolContinuationRecord,
) {
    let claim = record.frontend_claim.as_mut().unwrap();
    let original = &claim.results[0];
    claim.results[0] = crabber_core::FrontendToolResult::new(
        original.call_id().clone(),
        crabber_core::FrontendResultStatus::Completed,
        crabber_core::ToolOutput::from_json(&serde_json::json!("substituted external response"))
            .unwrap(),
    )
    .unwrap();
    claim.validate_for_batch(&record.batch).unwrap();
}
