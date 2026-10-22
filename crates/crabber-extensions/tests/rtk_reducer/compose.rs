use super::*;
use crabber_extensions::tool_result_redactor::{
    Limits as RedactorLimits, Options as RedactorOptions, ToolResultRedactor,
};
#[tokio::test(flavor = "multi_thread")]
async fn final_redactor_protects_reduced_text_regardless_of_order() {
    if !shell_available() {
        return;
    }
    let secret = "ghp_abcdefghijklmnopqrstuvwxyz0123456789";
    let body = format!("/bin/cat >/dev/null; printf 'R:{secret}'");
    let mut j = Journey::new(
        &body,
        json!(format!("{secret}{}", "x".repeat(600))),
        "fixture_text",
        json!({}),
        |_| {},
    );
    let redactor = ToolResultRedactor::new(RedactorOptions {
        order: -100,
        excluded_tools: vec![],
        additional_patterns: vec![],
        limits: RedactorLimits {
            max_field_bytes: 4096,
            max_total_bytes: 16384,
            max_depth: 16,
            max_nodes: 100,
            max_matches_per_field: 16,
            max_patterns: 4,
            max_pattern_bytes: 256,
            max_in_flight: 4,
        },
    })
    .unwrap();
    j.agent = Agent::builder()
        .behavior_version("crabber-extensions-tests.v1")
        .store(j.store.clone())
        .provider(j.provider.clone())
        .config(agent_config(j.dir.path()))
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .shared_tool(tool("fixture_text", j.executor.clone()))
        .shared_extension(j.ext.clone(), Scope::Global)
        .shared_extension(Arc::new(redactor), Scope::Global)
        .shared_extension(
            Arc::new(observe::ProbeMount(j.probe.clone())),
            Scope::Global,
        )
        .build()
        .unwrap();
    let session = j.run().await;
    let records = snapshot_tools(&j.store, &session).await;
    let expected = json!(marker(Filter::Log, secret.len() + 600) + "R:[REDACTED]");
    observe::assert_three_surfaces(&j.store, &session, &j.provider, &records[0].id, &expected)
        .await;
    assert!(!format!("{:?}", j.provider.requests()).contains(secret));
    j.close().await;
}
