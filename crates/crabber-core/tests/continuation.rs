use crabber_core::{
    AuthorityRequirement, InteractionDescription, InteractionKind, ToolCheckpoint, ToolOutput,
    ToolPhaseOutcome, ToolSuspension,
};
use serde_json::json;

fn authority() -> AuthorityRequirement {
    AuthorityRequirement::new("host-user", "1").unwrap()
}
fn description(name: &str) -> InteractionDescription {
    InteractionDescription::new(
        name,
        InteractionKind::UserInput,
        "PRIVATE_PROMPT_SENTINEL",
        json!({"type":"string","enum":["a","b"]}),
        authority(),
        Some(1000),
    )
    .unwrap()
}
fn checkpoint() -> ToolCheckpoint {
    ToolCheckpoint::new(
        "fixture.choice",
        "1",
        json!({"phase":"choice","private":"PRIVATE_CHECKPOINT_SENTINEL","nul":"\u{0}"}),
    )
    .unwrap()
}

#[test]
fn suspension_keeps_private_checkpoint_and_named_pending_set_separate_from_completion() {
    let checkpoint = checkpoint();
    let restored: ToolCheckpoint =
        serde_json::from_value(serde_json::to_value(&checkpoint).unwrap()).unwrap();
    assert_eq!(restored, checkpoint);
    assert_eq!(restored.schema_id(), "fixture.choice");
    assert_eq!(restored.schema_version(), "1");
    assert_eq!(restored.value()["nul"], "\u{0}");
    let suspension =
        ToolSuspension::new(restored, vec![description("first"), description("second")]).unwrap();
    assert_eq!(
        suspension
            .interactions()
            .iter()
            .map(InteractionDescription::name)
            .collect::<Vec<_>>(),
        ["first", "second"]
    );
    assert_eq!(suspension.checkpoint(), &checkpoint);
    let outcome = ToolPhaseOutcome::Suspended(suspension);
    let debug = format!("{outcome:?}");
    assert!(!debug.contains("PRIVATE_CHECKPOINT_SENTINEL"));
    assert!(!debug.contains("PRIVATE_PROMPT_SENTINEL"));
    let literal =
        ToolOutput::from_json(&json!({"type":"suspended","checkpoint":{"private":"data"}}))
            .unwrap();
    assert!(matches!(
        ToolPhaseOutcome::Completed(literal),
        ToolPhaseOutcome::Completed(_)
    ));
}

#[test]
fn checkpoint_schema_and_format_versions_fail_closed_and_diagnostics_are_redacted() {
    let mut wire = serde_json::to_value(checkpoint()).unwrap();
    for invalid in [json!(0), json!(2), json!(null)] {
        wire["version"] = invalid;
        let error = serde_json::from_value::<ToolCheckpoint>(wire.clone()).unwrap_err();
        assert!(!error.to_string().contains("PRIVATE_CHECKPOINT_SENTINEL"));
    }
    assert!(ToolCheckpoint::new("fixture", "", json!({})).is_err());
    assert!(
        ToolCheckpoint::new(
            "fixture",
            "1",
            json!("x".repeat(crabber_core::MAX_TOOL_CHECKPOINT_BYTES))
        )
        .is_err()
    );
    let mut nested = json!(null);
    for _ in 0..=crabber_core::input::MAX_INPUT_JSON_DEPTH {
        nested = json!([nested]);
    }
    assert!(ToolCheckpoint::new("fixture", "1", nested).is_err());
}

#[test]
fn incomplete_or_duplicate_local_pending_sets_are_rejected_without_inventing_consent() {
    assert!(ToolSuspension::new(checkpoint(), vec![]).is_err());
    assert!(
        ToolSuspension::new(checkpoint(), vec![description("same"), description("same")]).is_err()
    );
    assert!(
        ToolSuspension::new(
            checkpoint(),
            (0..65).map(|i| description(&format!("name-{i}"))).collect()
        )
        .is_err()
    );
    let immediate = InteractionDescription::new(
        "approve",
        InteractionKind::Approval,
        "approve action",
        json!({"const":true}),
        authority(),
        Some(0),
    )
    .unwrap();
    assert_eq!(immediate.expires_after_ms(), Some(0));
    assert_eq!(immediate.kind(), InteractionKind::Approval);
    assert_eq!(immediate.authority().policy_id(), "host-user");
    assert_eq!(immediate.authority().policy_version(), "1");
    let restored: InteractionDescription =
        serde_json::from_value(serde_json::to_value(&immediate).unwrap()).unwrap();
    assert_eq!(restored, immediate);
}

#[test]
fn prompts_schemas_authority_and_expiry_have_bounded_validated_representation() {
    assert!(AuthorityRequirement::new("", "1").is_err());
    assert!(AuthorityRequirement::new("policy", "v\n1").is_err());
    for (prompt, schema, expiry) in [
        ("x".repeat(8193), json!({}), None),
        (
            "question".into(),
            json!({"description":"x".repeat(16384)}),
            None,
        ),
        ("question".into(), json!([]), None),
        ("question".into(), json!({}), Some(u64::MAX)),
    ] {
        assert!(
            InteractionDescription::new(
                "question",
                InteractionKind::UserInput,
                prompt,
                schema,
                authority(),
                expiry
            )
            .is_err()
        );
    }
    let mut wire = serde_json::to_value(description("name")).unwrap();
    wire["kind"] = json!("PRIVATE_ENUM_SENTINEL");
    let error = serde_json::from_value::<InteractionDescription>(wire).unwrap_err();
    assert!(!error.to_string().contains("PRIVATE_ENUM_SENTINEL"));
}
