use super::*;
use crabber::{AgentConfig, ExecutionContext, ModelControls, ReasoningEffort};

fn host(native: Arc<dyn Store>, probe: &Probe, effort: ReasoningEffort) -> Agent {
    let mut config = AgentConfig::new(selection("fallback", "fallback"))
        .model_controls(ModelControls::default().reasoning_effort(effort));
    config.workspace_id = "defaults-workspace".into();
    config.directory = "/defaults".into();
    Agent::builder()
        .behavior_fingerprint(options().behavior_fingerprint)
        .store(native)
        .provider(Arc::new(probe.clone()))
        .config(config)
        .build()
        .unwrap()
}

pub(super) async fn retained(native: Arc<dyn Store>) {
    for session_defaults in [false, true] {
        let probe = Probe::new(1);
        let enqueue_host = host(native.clone(), &probe, ReasoningEffort::High);
        let session = create(native.as_ref(), defaults(Some("stored"), Some("stored"))).await;
        let scope = context(&session);
        let mut execution = ExecutionContext::new(scope.clone(), selection("explicit", "explicit"))
            .with_model_controls(ModelControls::default().reasoning_effort(ReasoningEffort::Low));
        if session_defaults {
            execution = execution.with_session_defaults();
        }
        let queue = QueueId::new();
        let input = InputEnvelope::text(
            "retained controls",
            native.session_revision(&session).await.unwrap(),
        )
        .unwrap();
        enqueue_host
            .enqueue_input_in(
                &execution,
                input,
                queue.clone(),
                ControlId::new(),
                options(),
            )
            .await
            .unwrap();
        let preparing = host(native.clone(), &probe, ReasoningEffort::Medium);
        let authority = claim(&preparing, native.as_ref(), &session).await;
        let prepare = QueueControl {
            id: ControlId::new(),
            expected_revision: native.session_revision(&session).await.unwrap(),
            authority,
            admission_lease: crabber::QueueAdmissionLease::default(),
        };
        let prepared = preparing
            .prepare_queue_in(&scope, prepare.clone())
            .await
            .unwrap();
        let frozen = native
            .retained_queue(&session, &queue)
            .await
            .unwrap()
            .prepared
            .unwrap();
        assert_eq!(
            frozen.binding.request.model_controls,
            *execution.model_controls()
        );
        dispatch_and_replay(
            &enqueue_host,
            &preparing,
            &scope,
            prepare,
            prepared.revision,
            &queue,
        )
        .await;
        let requests = probe.fake.requests();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].reasoning_effort, Some(ReasoningEffort::Low));
        assert_eq!(
            requests[0].selection,
            if session_defaults {
                selection("stored", "stored")
            } else {
                selection("explicit", "explicit")
            }
        );
        preparing.close_extensions().await.unwrap();
        enqueue_host.close_extensions().await.unwrap();
    }
}

async fn dispatch_and_replay(
    first: &Agent,
    next: &Agent,
    scope: &SessionContext,
    prepare: QueueControl,
    revision: SessionRevision,
    queue: &QueueId,
) {
    let admission_id = ControlId::new();
    let Admission::Started { handle, .. } = first
        .dispatch_queue_in(scope, prepare.clone(), admission_id.clone(), None)
        .await
        .unwrap()
    else {
        panic!("fresh queue must execute once")
    };
    handle.done().await.unwrap();
    assert!(matches!(
        next.admit_queue_in(
            scope,
            QueueControl {
                id: admission_id,
                expected_revision: revision,
                authority: prepare.authority,
                admission_lease: crabber::QueueAdmissionLease::default(),
            },
            None,
        )
        .await
        .unwrap(),
        Admission::Replayed(_)
    ));
    assert!(matches!(
        next.recover_queue_in(scope, queue, None).await.unwrap(),
        Admission::Replayed(_)
    ));
}
