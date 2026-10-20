use super::provider::Gate;
use crabber::core::{
    AuthorityRequirement, ControlId, InputContent, InputEnvelope, InputMessage, InputPart,
    InputState, MessageContent, Role, RunStatus,
};
use crabber::session::{
    ApprovalResolverClaim, RunControlAction, RunControlOutcome, RunControlRequest,
    RunControlStatus, RunControlTarget, Store,
};
use crabber::{Agent, AgentConfig, InputFingerprint, RuntimeError, Selection};
use std::sync::{Arc, atomic::Ordering};
pub(super) fn agent(store: Arc<dyn Store>, gate: &Gate) -> Agent {
    Agent::builder()
        .store(store)
        .provider(Arc::new(gate.clone()))
        .config(AgentConfig::new(Selection {
            provider_id: "fake".into(),
            model_id: "scripted".into(),
        }))
        .behavior_fingerprint(InputFingerprint::new("b".repeat(64)).unwrap())
        .build()
        .unwrap()
}
pub(super) fn cancel_action() -> RunControlAction {
    RunControlAction::Cancel {
        resolver: ApprovalResolverClaim {
            resolver_id: "host-controller".into(),
            authority: AuthorityRequirement::new("host-cancel", "1").unwrap(),
            authority_claim: "authorized host cancellation".into(),
        },
    }
}
pub(super) async fn request(
    store: &dyn Store,
    target: &RunControlTarget,
    action: RunControlAction,
) -> RunControlRequest {
    RunControlRequest {
        id: ControlId::new(),
        target: target.clone(),
        expected_revision: store.session_revision(&target.session_id).await.unwrap(),
        action,
    }
}
pub(super) async fn run(store: Arc<dyn Store>) {
    cancel(store.clone()).await;
    steer(store.clone()).await;
    super::faults::run(store.clone()).await;
    super::pause::run(store.clone()).await;
    super::tools::run(store).await;
}
async fn cancel(store: Arc<dyn Store>) {
    let gate = Gate::new(1);
    let worker = agent(store.clone(), &gate);
    let controller = agent(store.clone(), &gate);
    let handle = worker.prompt(None, "original").await.unwrap();
    gate.entered.notified().await;
    let target = controller
        .run_control_binding(handle.session_id(), handle.run_id())
        .await
        .unwrap()
        .target;
    let cancel = request(store.as_ref(), &target, cancel_action()).await;
    let RunControlOutcome::Applied(receipt) = controller.control_run(cancel.clone()).await.unwrap()
    else {
        panic!("fresh receipt")
    };
    assert!(matches!(
        controller.run_controls(&target).await.unwrap().controls[0].status,
        RunControlStatus::Accepted
            | RunControlStatus::Observed
            | RunControlStatus::TerminalCommitted { .. }
    ));
    let result = tokio::time::timeout(std::time::Duration::from_secs(5), handle.done())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result.status, RunStatus::Interrupted);
    assert_eq!(
        controller.run_controls(&target).await.unwrap().controls[0].status,
        RunControlStatus::TerminalCommitted {
            status: RunStatus::Interrupted
        }
    );
    let resolutions = gate.resolutions.load(Ordering::SeqCst);
    controller.close_extensions().await.unwrap();
    assert_eq!(
        controller.control_run(cancel).await.unwrap(),
        RunControlOutcome::Replayed(receipt)
    );
    assert_eq!(gate.resolutions.load(Ordering::SeqCst), resolutions);
    assert_eq!(gate.fake.requests().len(), 1);
}
async fn steer(store: Arc<dyn Store>) {
    let gate = Gate::new(2);
    let worker = agent(store.clone(), &gate);
    let controller = agent(store.clone(), &gate);
    let handle = worker.prompt(None, "original").await.unwrap();
    gate.entered.notified().await;
    let target = controller
        .run_control_binding(handle.session_id(), handle.run_id())
        .await
        .unwrap()
        .target;
    let revision = store.session_revision(&target.session_id).await.unwrap();
    let input = InputEnvelope::new(
        vec![InputMessage {
            id: Some(format!("steer-{}", target.run_id)),
            parent_id: Some("host-request-parent".into()),
            role: Role::User,
            parts: vec![InputPart {
                id: Some(format!("part-{}", target.run_id)),
                content: InputContent::Content {
                    content: MessageContent::Text {
                        text: "PRIVATE_STEER_CONTEXT".into(),
                    },
                },
            }],
        }],
        vec![],
        vec![],
        InputState::Absent,
        revision,
        crabber::core::AdmissionKind::Ordinary,
    )
    .unwrap();
    let steer = request(
        store.as_ref(),
        &target,
        RunControlAction::Steer {
            input: input.clone(),
        },
    )
    .await;
    let RunControlOutcome::Applied(receipt) = controller.control_run(steer.clone()).await.unwrap()
    else {
        panic!("fresh steer")
    };
    gate.release.notify_one();
    assert_eq!(handle.done().await.unwrap().status, RunStatus::Completed);
    assert_eq!(gate.fake.requests().len(), 2);
    let history = store.list_all_messages(&target.session_id).await.unwrap();
    let message = history
        .iter()
        .find(|m| m.id.0 == input.messages()[0].id.clone().unwrap())
        .unwrap();
    input
        .validate_message_projection(&target.session_id, Some(&target.run_id), [message])
        .unwrap();
    let page = controller.run_controls(&target).await.unwrap();
    assert!(matches!(
        page.controls[0].status,
        RunControlStatus::Incorporated { .. }
    ));
    assert!(
        !serde_json::to_string(&page.controls)
            .unwrap()
            .contains("PRIVATE_STEER_CONTEXT")
    );
    let requests = gate.fake.requests();
    assert!(
        serde_json::to_string(&requests[1].messages)
            .unwrap()
            .contains("PRIVATE_STEER_CONTEXT")
    );
    controller.close_extensions().await.unwrap();
    assert_eq!(
        controller.control_run(steer).await.unwrap(),
        RunControlOutcome::Replayed(receipt)
    );
    let revision = store.session_revision(&target.session_id).await.unwrap();
    let fresh = request(
        store.as_ref(),
        &target,
        RunControlAction::Steer {
            input: InputEnvelope::text("closed", revision).unwrap(),
        },
    )
    .await;
    assert!(matches!(
        controller.control_run(fresh).await,
        Err(RuntimeError::HostClosed)
    ));
    assert_eq!(gate.fake.requests().len(), 2);
}
