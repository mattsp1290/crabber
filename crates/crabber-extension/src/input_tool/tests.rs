use super::*;
fn fixture() -> (InputTool, Vec<Question>, ToolCheckpoint) {
    let authority = AuthorityRequirement::new("operator", "1").unwrap();
    let definition = ToolDefinition::input("request_input", authority.clone()).unwrap();
    let tool = InputTool {
        identity: definition.executor.resumable_identity().unwrap(),
        authority,
    };
    let questions =
        questions(json!({"questions":[{"name":"color","prompt":"Choose","choices":["blue"]}]}))
            .unwrap();
    let checkpoint = tool.proposal(&questions).unwrap().checkpoint().clone();
    (tool, questions, checkpoint)
}
#[test]
fn input_resume_rejects_changed_checkpoint_and_misrouted_answers() {
    let (tool, mut questions, checkpoint) = fixture();
    let answer = AcceptedToolResponse {
        name: "color".into(),
        state: InteractionState::Answered,
        value: Some(json!("blue")),
    };
    assert!(
        !tool
            .finish(&questions, &checkpoint, vec![answer.clone()])
            .unwrap()
            .is_error()
    );
    questions[0].prompt = "Changed question".into();
    assert!(
        tool.finish(&questions, &checkpoint, vec![answer.clone()])
            .is_err()
    );
    questions[0].prompt = "Choose".into();
    for (name, state, value) in [
        ("other", InteractionState::Answered, Some(json!("blue"))),
        ("color", InteractionState::Answered, Some(json!("red"))),
        ("color", InteractionState::Pending, None),
        ("color", InteractionState::Denied, Some(json!("blue"))),
    ] {
        assert!(
            tool.finish(
                &questions,
                &checkpoint,
                vec![AcceptedToolResponse {
                    name: name.into(),
                    state,
                    value
                }]
            )
            .is_err()
        );
    }
    assert!(
        tool.finish(&questions, &checkpoint, vec![answer.clone(), answer])
            .is_err()
    );
}
#[test]
fn input_refusal_completes_with_error_without_fabricated_answer() {
    let (tool, questions, checkpoint) = fixture();
    for state in [
        InteractionState::Denied,
        InteractionState::Cancelled,
        InteractionState::Expired,
    ] {
        let output = tool
            .finish(
                &questions,
                &checkpoint,
                vec![AcceptedToolResponse {
                    name: "color".into(),
                    state,
                    value: None,
                }],
            )
            .unwrap();
        assert!(output.is_error());
        assert!(!serde_json::to_string(&output).unwrap().contains("blue"));
    }
}
#[test]
fn input_declarations_are_bounded_and_question_names_unique() {
    let (tool, _, _) = fixture();
    for arguments in [
        json!({"questions": []}),
        json!({"questions": [{"name":"a","prompt":"q","choices":["x","x"]}]}),
        json!({"questions": [{"name":"a","prompt":"q"},{"name":"a","prompt":"q"}]}),
        json!({"questions": [{"name":"a","prompt":"q","choices":["x".repeat(1_000_000)]}]}),
        json!({"questions": [{"name":"a","prompt":"q","response_schema":{"$ref":"https://example.invalid"}}]}),
    ] {
        assert!(
            questions(arguments)
                .and_then(|q| tool.proposal(&q))
                .is_err()
        );
    }
    assert!(ToolDefinition::input("", tool.authority.clone()).is_err());
    assert_ne!(
        tool.identity,
        ToolDefinition::input(
            "request_input",
            AuthorityRequirement::new("another-policy", "1").unwrap()
        )
        .unwrap()
        .executor
        .resumable_identity()
        .unwrap()
    );
}
