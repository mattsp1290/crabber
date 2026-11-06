use super::*;
use std::sync::atomic::AtomicBool;

#[derive(Default)]
struct SequentialEcho {
    active: AtomicBool,
    seen: Mutex<Vec<Value>>,
}
#[async_trait]
impl JsonToolExecutor for SequentialEcho {
    async fn execute(&self, arguments: Value) -> Result<Value, ExtensionError> {
        assert!(!self.active.swap(true, Ordering::SeqCst));
        tokio::task::yield_now().await;
        self.seen.lock().unwrap().push(arguments.clone());
        self.active.store(false, Ordering::SeqCst);
        Ok(arguments)
    }
}

#[tokio::test]
async fn interleaved_calls_execute_sequentially_and_replay_exact_ids_arguments_and_outputs() {
    let first = ToolCallId::from("first-read");
    let second = ToolCallId::from("second-read");
    let fake = FakeProvider::scripted(vec![
        vec![
            StreamDelta::ToolCallStart {
                call_id: first.clone(),
                name: "echo".into(),
            },
            StreamDelta::ToolCallStart {
                call_id: second.clone(),
                name: "echo".into(),
            },
            StreamDelta::ToolCallArgsDelta {
                call_id: second.clone(),
                text: r#"{"text":"nonce-"#.into(),
            },
            StreamDelta::ToolCallArgsDelta {
                call_id: first.clone(),
                text: r#"{"text":"nonce-a"}"#.into(),
            },
            StreamDelta::ToolCallArgsDelta {
                call_id: second.clone(),
                text: "b\"}".into(),
            },
            StreamDelta::ToolCallDone {
                call_id: second.clone(),
            },
            StreamDelta::ToolCallDone {
                call_id: first.clone(),
            },
            StreamDelta::Completed,
        ],
        text_script("done"),
    ]);
    let executor = Arc::new(SequentialEcho::default());
    let runtime = Orchestrator::builder()
        .behavior_version("batched-tools-test.v1")
        .store(Arc::new(MemoryStore::new()))
        .resolver(Arc::new(fake.clone()))
        .plan_provider(Arc::new(StaticPlanProvider::new(
            vec![tool(executor.clone())],
            vec![],
        )))
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .build()
        .unwrap();
    assert_eq!(
        runtime
            .start(request())
            .await
            .unwrap()
            .done()
            .await
            .unwrap()
            .status,
        RunStatus::Completed
    );
    assert_eq!(
        *executor.seen.lock().unwrap(),
        vec![json!({"text":"nonce-a"}), json!({"text":"nonce-b"})]
    );
    let requests = fake.requests();
    assert_eq!(requests.len(), 2);
    let replayed: Vec<_> = requests[1]
        .messages
        .iter()
        .flat_map(|message| &message.parts)
        .filter_map(|part| match &part.content {
            ContentBlock::ToolCall {
                call_id,
                name,
                arguments,
            } => Some(json!({"call_id":call_id,"name":name,"arguments":arguments})),
            ContentBlock::ToolResult {
                call_id,
                content,
                is_error,
            } => Some(json!({"call_id":call_id,"content":content,"is_error":is_error})),
            _ => None,
        })
        .collect();
    assert_eq!(
        replayed,
        vec![
            json!({"call_id":first,"name":"echo","arguments":{"text":"nonce-a"}}),
            json!({"call_id":second,"name":"echo","arguments":{"text":"nonce-b"}}),
            json!({"call_id":first,"content":[{"type":"text","text":"{\"text\":\"nonce-a\"}"}],"is_error":false}),
            json!({"call_id":second,"content":[{"type":"text","text":"{\"text\":\"nonce-b\"}"}],"is_error":false}),
        ]
    );
}
