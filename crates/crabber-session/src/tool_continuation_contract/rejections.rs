use super::{ManualClock, Store, ToolContinuationError as Error, fixture};
use crate::ToolInteractionReply;
use crabber_core::{
    AuthorityRequirement, Clock, ControlId, InteractionDescription, InteractionKind,
};

#[allow(clippy::too_many_lines)] // A single rejection and expiration lifecycle.
pub(super) async fn run(store: &impl Store, clock: &ManualClock) {
    let mut f = fixture::fixture(store).await;
    let mut wrong = f.suspension.clone();
    wrong.phases[0].identity.checkpoint_schema_version = "other".into();
    assert_eq!(
        f.execution
            .suspend_tool_continuation(wrong, f.authority.clone())
            .await
            .unwrap_err(),
        Error::SchemaMismatch
    );
    assert_eq!(
        store
            .session_revision(&f.admitted.session.id)
            .await
            .unwrap(),
        f.suspension.expected_revision
    );
    f.suspension.phases[0].interactions[0].description = InteractionDescription::new(
        "first",
        InteractionKind::UserInput,
        "Choose",
        serde_json::json!({"type":"string"}),
        AuthorityRequirement::new("operator", "1").unwrap(),
        Some(1),
    )
    .unwrap();
    f.execution
        .suspend_tool_continuation(f.suspension.clone(), f.authority.clone())
        .await
        .unwrap();
    let original = fixture::answer(store, &f, 0).await;
    let mut wrong = original.clone();
    wrong.resolver.authority = AuthorityRequirement::new("untrusted", "1").unwrap();
    assert_eq!(
        store.answer_tool_interaction(wrong).await.unwrap_err(),
        Error::AuthorityMismatch
    );
    let mut wrong = original.clone();
    wrong.reply = ToolInteractionReply::Answer {
        value: serde_json::json!(12),
    };
    assert_eq!(
        store.answer_tool_interaction(wrong).await.unwrap_err(),
        Error::SchemaMismatch
    );
    let mut deep = serde_json::Value::Null;
    for _ in 0..34 {
        deep = serde_json::json!([deep]);
    }
    for value in [
        deep,
        serde_json::json!("x".repeat(65537)),
        serde_json::json!("nul\u{0000}value"),
    ] {
        let mut invalid = original.clone();
        invalid.reply = ToolInteractionReply::Answer { value };
        assert_eq!(
            store.answer_tool_interaction(invalid).await.unwrap_err(),
            Error::InvalidRequest
        );
    }
    let mut early = original.clone();
    early.reply = ToolInteractionReply::Expire;
    assert_eq!(
        store
            .answer_tool_interaction(early.clone())
            .await
            .unwrap_err(),
        Error::InvalidRequest
    );
    assert_eq!(
        store
            .session_revision(&f.admitted.session.id)
            .await
            .unwrap(),
        original.expected_revision
    );
    clock.set(clock.now() + time::Duration::milliseconds(1));
    assert_eq!(
        store
            .answer_tool_interaction(original.clone())
            .await
            .unwrap_err(),
        Error::Expired
    );
    early.id = ControlId::new();
    let expired = store.answer_tool_interaction(early.clone()).await.unwrap();
    assert_eq!(store.answer_tool_interaction(early).await.unwrap(), expired);
    let mut wrong = original;
    wrong.id = ControlId::new();
    wrong.expected_revision = expired.revision;
    wrong.reply = ToolInteractionReply::Cancel;
    assert_eq!(
        store.answer_tool_interaction(wrong).await.unwrap_err(),
        Error::SemanticConflict
    );
    let mut deny = fixture::answer(store, &f, 1).await;
    deny.reply = ToolInteractionReply::Deny;
    store.answer_tool_interaction(deny).await.unwrap();
    let crate::ToolContinuationClaimOutcome::Applied { grant, .. } = store
        .claim_tool_continuation(fixture::claim(store, &f).await)
        .await
        .unwrap()
    else {
        panic!("settled non-answer choices are explicit resume input")
    };
    assert_eq!(
        grant.phases[0].responses[0].state,
        crabber_core::InteractionState::Expired
    );
    assert_eq!(
        grant.phases[0].responses[1].state,
        crabber_core::InteractionState::Denied
    );
    assert!(
        grant.phases[0]
            .responses
            .iter()
            .all(|response| response.value.is_none())
    );
}
