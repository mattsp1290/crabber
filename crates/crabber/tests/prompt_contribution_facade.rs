use async_trait::async_trait;
use crabber::extension::{
    Extension, ExtensionError, MAX_PROMPT_CONTRIBUTION_BYTES, MAX_PROMPT_CONTRIBUTIONS_TOTAL_BYTES,
    MAX_PROMPT_CONTRIBUTOR_NAME_BYTES, MountedPromptContributor,
    PROMPT_CONTRIBUTION_CONTRACT_VERSION, PROMPT_CONTRIBUTION_DEADLINE,
    PROMPT_CONTRIBUTIONS_TOTAL_DEADLINE, PromptAttemptContext, PromptContribution,
    PromptContributionOutcome, PromptContributor, Registrar, Registry, Scope, WorkspaceContext,
    collect_prompt_contributions, prompt_contribution_failed_message,
};
use std::sync::Arc;
struct Named;
#[async_trait]
impl Extension for Named {
    fn id(&self) -> &'static str {
        "facade"
    }
    fn version(&self) -> &'static str {
        "1"
    }
    fn config_hash(&self) -> String {
        String::new()
    }
    async fn install(&self, r: &mut Registrar) -> Result<(), ExtensionError> {
        let callback: PromptContributor = Arc::new(|_| Box::pin(async { Ok(Some("text".into())) }));
        r.prompt_contributor(0, "name", callback);
        Ok(())
    }
}
#[tokio::test]
async fn all_prompt_contribution_items_are_reachable_through_facade() {
    let registry = Registry::new();
    registry
        .mount(Arc::new(Named), Scope::Global)
        .await
        .unwrap();
    let plan = registry.acquire(&"session".into());
    let contributors: &[MountedPromptContributor] = &plan.prompt_contributors;
    assert_eq!(contributors[0].name, "name");
    let context = PromptAttemptContext::new(
        "session".into(),
        "run".into(),
        "turn".into(),
        WorkspaceContext::from_persisted("", ""),
        "provider".into(),
        "model".into(),
        1,
        false,
    );
    assert_eq!(
        collect_prompt_contributions(contributors, context).await,
        PromptContributionOutcome::Completed {
            sections: vec![PromptContribution {
                name: "name".into(),
                text: "text".into()
            }]
        }
    );
    assert_eq!(
        prompt_contribution_failed_message("name"),
        "prompt contribution failed: name"
    );
    assert_eq!(PROMPT_CONTRIBUTION_CONTRACT_VERSION, 1);
    assert!(PROMPT_CONTRIBUTION_DEADLINE <= PROMPT_CONTRIBUTIONS_TOTAL_DEADLINE);
    assert!(MAX_PROMPT_CONTRIBUTION_BYTES <= MAX_PROMPT_CONTRIBUTIONS_TOTAL_BYTES);
    assert_eq!(MAX_PROMPT_CONTRIBUTOR_NAME_BYTES, 128);
}
