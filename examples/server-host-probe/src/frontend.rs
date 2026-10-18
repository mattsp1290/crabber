//! Actual external results across two pauses, reconstruction and pure replay.
use super::{Error, call, done, selection};
use crabber::{
    Admission, Agent, AgentConfig, ContinueFrontend, ContinueTool, ExecutionContext, FakeProvider,
    SessionContext, ToolContinuation,
    core::{
        AdmissionKey, AdmissionOptions, ContentBlock, ControlId, FrontendResultStatus,
        FrontendTool, FrontendToolResult, InputEnvelope, InputFingerprint, RunId, RunStatus,
        SessionId, SessionRevision, ToolCallId, ToolOutput,
    },
    session::{FrontendResolverClaim, Store, ToolContinuationError, ToolContinuationSnapshot},
};
use serde_json::json;
use std::sync::Arc;

fn agent(store: Arc<dyn Store>, provider: FakeProvider) -> Result<Agent, Error> {
    Ok(Agent::builder()
        .behavior_version("frontend-probe.v1")
        .store(store)
        .provider(Arc::new(provider))
        .config(AgentConfig::new(selection()))
        .build()?)
}
fn input() -> Result<InputEnvelope, Error> {
    let mut value = serde_json::to_value(InputEnvelope::text(
        "external work",
        SessionRevision::INITIAL,
    )?)?;
    value["frontend_tools"] = json!([FrontendTool {
        name: "probe_external".into(),
        description: "A host result, with no backend executor".into(),
        input_parameters: json!({"type":"object"}),
        // The output contract validates ToolOutput, independently of the
        // input argument schema and of text produced by its JSON adapter.
        result_contract: Some(json!({
            "type":"object", "required":["content","is_error"], "additionalProperties":false,
            "properties":{
                "content":{"type":"array","minItems":1,"maxItems":1,"items":{
                    "type":"object","required":["type","text"],"additionalProperties":false,
                    "properties":{"type":{"const":"text"},"text":{"type":"string"}}
                }},
                "is_error":{"const":false}
            }
        })),
    }]);
    Ok(serde_json::from_value(value)?)
}
fn response(snapshot: &ToolContinuationSnapshot, text: &str) -> Result<ContinueFrontend, Error> {
    Ok(ContinueFrontend {
        continuation: ContinueTool {
            id: ControlId::new(),
            session_id: snapshot.session_id.clone(),
            run_id: snapshot.run_id.clone(),
            continuation: snapshot.continuation.clone(),
            expected_revision: snapshot.revision,
        },
        resolver: FrontendResolverClaim {
            resolver_id: "authenticated-probe-host".into(),
            authority_claim: "host authenticated the external responder".into(),
        },
        results: snapshot
            .frontend_calls
            .iter()
            .map(|call| {
                FrontendToolResult::new(
                    call.call_id.clone(),
                    FrontendResultStatus::Completed,
                    ToolOutput::from_json(&json!(text))?,
                )
            })
            .collect::<Result<Vec<_>, _>>()?,
    })
}
pub(crate) async fn run(store: Arc<dyn Store>) -> Result<usize, Error> {
    let provider =
        FakeProvider::scripted(vec![call("probe_external"), call("probe_external"), done()]);
    let mut host = agent(store.clone(), provider.clone())?;
    let session = SessionId::new();
    let execution = ExecutionContext::new(
        SessionContext::new(session.clone(), "frontend-probe", "/frontend-probe"),
        selection(),
    );
    let options = AdmissionOptions {
        key: AdmissionKey::new("frontend-probe-input")?,
        fingerprint: InputFingerprint::new("d".repeat(64))?,
        behavior_fingerprint: host.behavior_fingerprint().clone(),
    };
    let original_input = input()?;
    let Admission::Started { receipt, handle } = host
        .prompt_input_keyed_in(&execution, original_input.clone(), options.clone(), None)
        .await?
    else {
        panic!("fresh frontend admission must start")
    };
    assert_eq!(handle.done().await?.status, RunStatus::Paused);
    let mut results = Vec::new();
    for step in 1..=2 {
        let (next, result) = resume_one(
            store.clone(),
            &provider,
            host,
            &session,
            &receipt.run_id,
            step,
        )
        .await?;
        host = next;
        results.push(result);
    }
    assert!(matches!(host.prompt_input_keyed_in(
        &execution, original_input.clone(), options.clone(), None).await?,
        Admission::Replayed(saved) if saved == receipt));
    assert!(matches!(host.recover_input_admission_in(
        &execution, original_input, options, None).await?,
        Admission::Replayed(saved) if saved == receipt));
    assert_eq!(provider.requests().len(), 3);
    let messages = store.list_all_messages(&session).await?;
    for (id, text) in results {
        let expected = ToolOutput::from_json(&json!(text))?.to_content_blocks();
        assert!(messages.iter().flat_map(|message| &message.parts).any(|part|
            matches!(&part.content, ContentBlock::ToolResult { call_id, content, is_error: false }
                if call_id == &id && content == &expected)));
    }
    Ok(2)
}

async fn resume_one(
    store: Arc<dyn Store>,
    provider: &FakeProvider,
    host: Agent,
    session: &SessionId,
    run: &RunId,
    step: u64,
) -> Result<(Agent, (ToolCallId, String)), Error> {
    let snapshot = host
        .tool_continuation_snapshot(session, run)
        .await?
        .expect("actual frozen frontend wait");
    assert_eq!(snapshot.continuation.generation.get(), step);
    assert!(snapshot.interactions.is_empty());
    assert_eq!(snapshot.frontend_calls.len(), 1);
    let text = format!("external result {step}");
    let request = response(&snapshot, &text)?;
    let id = request.results[0].call_id().clone();
    host.close_extensions().await?;
    drop(host);
    let host = agent(store.clone(), provider.clone())?;
    reject_empty_output(&host, &*store, &request).await?;
    let ToolContinuation::Started { receipt, handle } =
        host.continue_frontend(request.clone()).await?
    else {
        panic!("fresh external result phase must start")
    };
    assert_eq!(
        handle.done().await?.status,
        if step == 1 {
            RunStatus::Paused
        } else {
            RunStatus::Completed
        }
    );
    let before = store.list_events(session, None, 1000).await?;
    let requests = provider.requests().len();
    host.close_extensions().await?;
    assert!(
        matches!(host.continue_frontend(request).await?, ToolContinuation::Replayed(saved) if saved == receipt)
    );
    assert_eq!(provider.requests().len(), requests);
    assert_eq!(store.list_events(session, None, 1000).await?, before);
    Ok((host, (id, text)))
}

async fn reject_empty_output(
    host: &Agent,
    store: &dyn Store,
    request: &ContinueFrontend,
) -> Result<(), Error> {
    let session = &request.continuation.session_id;
    let before = store.list_events(session, None, 1000).await?;
    let revision = store.session_revision(session).await?;
    let mut invalid = request.clone();
    invalid.results = vec![FrontendToolResult::new(
        request.results[0].call_id().clone(),
        FrontendResultStatus::Completed,
        ToolOutput::new(Vec::new(), false)?,
    )?];
    assert!(matches!(
        host.continue_frontend(invalid).await,
        Err(crabber::RuntimeError::ToolContinuation(
            ToolContinuationError::InvalidRequest
        ))
    ));
    assert_eq!(store.session_revision(session).await?, revision);
    assert_eq!(store.list_events(session, None, 1000).await?, before);
    Ok(())
}
