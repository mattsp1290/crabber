use super::*;

#[test]
fn interleaved_responses_calls_preserve_ids_arguments_and_result_association() {
    let events = [
        json!({"type":"response.output_item.added","output_index":0,"item":{"type":"function_call","call_id":"read_a","name":"file_read"}}),
        json!({"type":"response.output_item.added","output_index":1,"item":{"type":"function_call","call_id":"read_b","name":"file_read"}}),
        json!({"type":"response.function_call_arguments.delta","output_index":1,"delta":"{\"path\":"}),
        json!({"type":"response.function_call_arguments.delta","output_index":0,"delta":"{\"path\":\"a"}),
        json!({"type":"response.function_call_arguments.delta","output_index":1,"delta":"\"b.txt\"}"}),
        json!({"type":"response.function_call_arguments.delta","output_index":0,"delta":".txt\"}"}),
        json!({"type":"response.output_item.done","output_index":1,"item":{"type":"function_call","arguments":"{}"}}),
        json!({"type":"response.output_item.done","output_index":0,"item":{"type":"function_call","arguments":"{}"}}),
        json!({"type":"response.completed","response":{"usage":{"input_tokens":10,"output_tokens":20}}}),
    ];
    let mut codec = responses::Codec::new(true);
    let deltas: Vec<_> = events
        .iter()
        .flat_map(|value| codec.event(&event("", value)).unwrap())
        .collect();
    let mut calls = std::collections::BTreeMap::<String, (String, String, usize)>::new();
    for delta in deltas {
        match delta {
            StreamDelta::ToolCallStart { call_id, name } => {
                calls.insert(call_id.0, (name, String::new(), 0));
            }
            StreamDelta::ToolCallArgsDelta { call_id, text } => {
                calls.get_mut(&call_id.0).unwrap().1.push_str(&text);
            }
            StreamDelta::ToolCallDone { call_id } => calls.get_mut(&call_id.0).unwrap().2 += 1,
            _ => {}
        }
    }
    assert_eq!(
        calls,
        std::collections::BTreeMap::from([
            (
                "read_a".into(),
                ("file_read".into(), r#"{"path":"a.txt"}"#.into(), 1)
            ),
            (
                "read_b".into(),
                ("file_read".into(), r#"{"path":"b.txt"}"#.into(), 1)
            ),
        ])
    );
    let assistant = message(
        Role::Assistant,
        &PartKind::FunctionToolCall,
        calls
            .iter()
            .map(|(id, (name, args, _))| ContentBlock::ToolCall {
                call_id: ToolCallId::from(id.as_str()),
                name: name.clone(),
                arguments: serde_json::from_str(args).unwrap(),
            })
            .collect(),
    );
    let outputs = message(
        Role::Tool,
        &PartKind::FunctionToolResult,
        [
            ContentBlock::ToolResult {
                call_id: ToolCallId::from("read_b"),
                content: vec![ContentBlock::Text {
                    text: "nonce-b\n".into(),
                }],
                is_error: false,
            },
            ContentBlock::ToolResult {
                call_id: ToolCallId::from("read_a"),
                content: vec![ContentBlock::Text {
                    text: "nonce-a\n".into(),
                }],
                is_error: false,
            },
        ]
        .into(),
    );
    let body = responses::body(&request(vec![assistant, outputs], vec![], None), true);
    assert_eq!(
        body["input"],
        json!([
            {"type":"function_call","call_id":"read_a","name":"file_read","arguments":"{\"path\":\"a.txt\"}"},
            {"type":"function_call","call_id":"read_b","name":"file_read","arguments":"{\"path\":\"b.txt\"}"},
            {"type":"function_call_output","call_id":"read_b","output":"nonce-b\n"},
            {"type":"function_call_output","call_id":"read_a","output":"nonce-a\n"},
        ])
    );
}
