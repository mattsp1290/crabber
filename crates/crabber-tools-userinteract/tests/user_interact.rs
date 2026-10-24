//! Pending relay contracts; durable continuation contracts live in durable.rs.
use crabber::ToolDefinition;
use crabber_tools_core::{WorkspaceRoot, config_hash};
use crabber_tools_testkit as support;
use crabber_tools_userinteract::*;
use serde_json::{Value, json};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
fn tool(policy: UserInteractPolicy) -> Arc<ToolDefinition> {
    definition(Arc::new(Options { policy })).unwrap()
}
async fn invoke(
    t: &ToolDefinition,
    args: Value,
    cancel: CancellationToken,
) -> Result<Value, crabber::ExtensionError> {
    let dir = tempfile::tempdir().unwrap();
    let root = WorkspaceRoot::open(dir.path()).unwrap();
    t.executor
        .execute_owned_context(support::context(&root, cancel), args)
        .await
        .map(|out| out.json_value().unwrap())
}
fn category(v: &Value, c: &str) {
    assert_eq!(v["outcome"], "failed");
    assert_eq!(v["error"]["category"], c);
}

#[test]
fn metadata() {
    let i = info();
    let mut upstream: Value = serde_json::from_str(include_str!(
        "../../../fixtures/eino-tools/user_interact.json"
    ))
    .unwrap();
    // Two intentional description edits; schemas otherwise remain identical.
    upstream["parameters"]["properties"]["answer"]["description"] =
        i.parameters["properties"]["answer"]["description"].clone();
    assert_eq!(i.parameters, upstream["parameters"]);
    assert_eq!(i.name, "user_interact");
    assert!(!i.retry_safe);
    assert_eq!(i.required_permissions, ["interaction.ask"]);
}

#[tokio::test]
async fn pending_agent_run_is_completed() {
    use crabber::{
        Agent, FakeProvider, PermissionDecision, StaticPolicy, core::ToolCallStatus,
        session::MemoryStore,
    };
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(MemoryStore::new());
    let agent = Agent::builder()
        .behavior_version(concat!("crabber-tools/", env!("CARGO_PKG_VERSION")))
        .store(store.clone())
        .provider(Arc::new(FakeProvider::scripted(vec![
            support::turn("user_interact", json!({"question":"Q?"})),
            support::done(),
        ])))
        .config(support::config(dir.path()))
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .shared_tool(tool(UserInteractPolicy::pending()))
        .build()
        .unwrap();
    let run = agent.prompt(None, "test").await.unwrap();
    let id = run.session_id().clone();
    run.done().await.unwrap();
    let calls = support::calls(&store, &id).await;
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].status, ToolCallStatus::Completed);
    assert_eq!(
        support::value(&calls[0]),
        json!({"outcome":"pending","question":"Q?"})
    );
    agent.close_extensions().await.unwrap();
}

#[tokio::test]
async fn arguments_are_validated_before_dispatch() {
    let mut p = UserInteractPolicy::pending();
    p.max_question_bytes = 3;
    p.max_answer_bytes = 3;
    let t = tool(p);
    for (args, c) in [
        (json!({"question":""}), "validation"),
        (json!({"question":"  "}), "validation"),
        (json!({"question":"Q","extra":1}), "validation"),
        (json!({"question":"\0"}), "validation"),
        (json!({"question":"\u{1b}"}), "validation"),
        (json!({"question":"abcd"}), "too_large"),
        (json!({"question":"Q","answer":"abcd"}), "too_large"),
        (json!({"question":"Q","answer":"\0"}), "validation"),
        (json!({"question":null}), "validation"),
        (json!({}), "validation"),
    ] {
        category(
            &invoke(&t, args, CancellationToken::new()).await.unwrap(),
            c,
        );
    }
    category(
        &t.executor
            .execute(json!({"question":"Q"}))
            .await
            .unwrap()
            .json_value()
            .unwrap(),
        "validation",
    );
    assert_eq!(
        invoke(&t, json!({"question":"Q\n\t"}), CancellationToken::new())
            .await
            .unwrap()["outcome"],
        "pending"
    );
}

#[test]
fn policy_validation_and_identity() {
    let valid = UserInteractPolicy::pending();
    valid.validate().unwrap();
    for (q, a) in [
        (0, 1),
        (MAX_QUESTION_BYTES + 1, 1),
        (1, 0),
        (1, MAX_ANSWER_BYTES + 1),
    ] {
        let mut p = valid.clone();
        p.max_question_bytes = q;
        p.max_answer_bytes = a;
        assert!(p.validate().is_err());
        assert_ne!(config_hash(&valid), config_hash(&p));
    }
}
#[test]
fn no_process_io() {
    for source in [
        include_str!("../src/lib.rs"),
        include_str!("../src/info.rs"),
        include_str!("../src/policy.rs"),
    ] {
        for token in ["stdin(", "stderr("] {
            assert!(!source.contains(token));
        }
    }
}

#[tokio::test]
async fn upstream_pending_cases_are_relays_without_consent() {
    use sha2::{Digest, Sha256};
    let cases: Vec<Value> = serde_json::from_str(include_str!("fixtures/go-cases.json")).unwrap();
    assert_eq!(
        cases[0]["upstream_sha256"],
        format!(
            "{:x}",
            Sha256::digest(include_bytes!("fixtures/upstream_userinteract_test.go"))
        )
    );
    for case in cases.iter().skip(1).filter(|c| c["surface"] == "mcp") {
        let result = invoke(
            &tool(UserInteractPolicy::pending()),
            case["args"].clone(),
            CancellationToken::new(),
        )
        .await
        .unwrap();
        let expected = &case["expected"];
        assert_eq!(result["outcome"], expected["outcome"], "{}", case["name"]);
        assert_eq!(
            result.get("answer").is_some(),
            expected["answer_key_present"].as_bool().unwrap()
        );
        for key in ["answer", "question"] {
            if let Some(value) = expected.get(key) {
                assert_eq!(&result[key], value);
            }
        }
    }
}
#[test]
fn durable_bounds_and_identity() {
    use crabber::core::AuthorityRequirement;
    use std::time::Duration;
    let policy = UserInteractPolicy::durable(AuthorityRequirement::new("operator", "1").unwrap());
    policy.validate().unwrap();
    let hash = config_hash(&policy);
    for change in 0..5 {
        let mut p = policy.clone();
        match change {
            0 => p.max_answer_bytes = MAX_DURABLE_ANSWER_BYTES + 1,
            1 => {
                p.surface = Surface::Durable {
                    authority: AuthorityRequirement::new("operator", "1").unwrap(),
                    expires_after: Some(Duration::ZERO),
                }
            }
            2 => {
                p.surface = Surface::Durable {
                    authority: AuthorityRequirement::new("operator", "1").unwrap(),
                    expires_after: Some(Duration::from_nanos(1)),
                }
            }
            3 => p.max_question_bytes = 0,
            _ => p.max_answer_bytes = 0,
        }
        assert!(p.validate().is_err());
        assert_ne!(hash, config_hash(&p));
    }
    assert_ne!(
        hash,
        config_hash(&UserInteractPolicy::durable(
            AuthorityRequirement::new("other", "1").unwrap()
        ))
    );
}

use support::ExecuteOwned as _;

#[test]
fn unvalidated_large_expiry_serializes_without_panicking() {
    let mut policy = UserInteractPolicy::durable(
        crabber::core::AuthorityRequirement::new("operator", "1").unwrap(),
    );
    policy.surface = Surface::Durable {
        authority: crabber::core::AuthorityRequirement::new("operator", "1").unwrap(),
        expires_after: Some(std::time::Duration::MAX),
    };
    assert!(serde_json::to_value(&policy).is_ok());
    assert!(policy.validate().is_err());
}
