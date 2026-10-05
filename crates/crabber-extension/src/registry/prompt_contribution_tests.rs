use super::*;
use crate::{PromptAttemptContext, PromptContributionOutcome, collect_prompt_contributions};

struct Named {
    entries: Vec<(i32, String, String)>,
    rolled_back: Arc<AtomicUsize>,
}
#[async_trait]
impl Extension for Named {
    fn id(&self) -> &'static str {
        "named"
    }
    fn version(&self) -> &'static str {
        "1"
    }
    fn config_hash(&self) -> String {
        String::new()
    }
    async fn install(&self, r: &mut Registrar) -> Result<(), ExtensionError> {
        let counter = self.rolled_back.clone();
        r.defer(move || {
            counter.fetch_add(1, Ordering::SeqCst);
        });
        for (order, name, value) in &self.entries {
            let value = value.clone();
            r.prompt_contributor(
                *order,
                name.clone(),
                Arc::new(move |_| {
                    let value = value.clone();
                    Box::pin(async move { Ok(Some(value)) })
                }),
            );
        }
        Ok(())
    }
}
fn extension(entries: &[(i32, &str, &str)]) -> Arc<Named> {
    Arc::new(Named {
        entries: entries
            .iter()
            .map(|(o, n, v)| (*o, (*n).into(), (*v).into()))
            .collect(),
        rolled_back: Arc::new(AtomicUsize::new(0)),
    })
}
async fn texts(plan: &RunPlan) -> Vec<String> {
    let context = PromptAttemptContext::new(
        "s".into(),
        "r".into(),
        "t".into(),
        crate::WorkspaceContext::from_persisted("", ""),
        "p".into(),
        "m".into(),
        1,
        false,
    );
    let PromptContributionOutcome::Completed { sections } =
        collect_prompt_contributions(&plan.prompt_contributors, context).await
    else {
        panic!("collection failed")
    };
    sections.into_iter().map(|s| s.text).collect()
}
#[tokio::test]
async fn session_shadows_global_in_both_mount_orders() {
    for session_first in [true, false] {
        let registry = Registry::new();
        let a = SessionId::from("a");
        let entries = if session_first {
            vec![
                (Scope::Session(a.clone()), "session"),
                (Scope::Global, "global"),
            ]
        } else {
            vec![
                (Scope::Global, "global"),
                (Scope::Session(a.clone()), "session"),
            ]
        };
        for (scope, value) in entries {
            registry
                .mount(extension(&[(0, "same", value)]), scope)
                .await
                .unwrap();
        }
        assert_eq!(texts(&registry.acquire(&a)).await, vec!["session"]);
        assert_eq!(texts(&registry.acquire(&"b".into())).await, vec!["global"]);
        let plan = registry.acquire(&a);
        assert_eq!(
            plan.components
                .iter()
                .filter(|c| c.id == "prompt-contributor:same")
                .count(),
            2
        );
    }
}
#[tokio::test]
async fn ordering_is_independent_of_mount_order_and_plan_is_frozen() {
    for reverse in [true, false] {
        let registry = Registry::new();
        let first = extension(&[(10, "z", "z"), (0, "b", "b")]);
        let second = extension(&[(0, "a", "a")]);
        let entries = if reverse {
            vec![second, first]
        } else {
            vec![first, second]
        };
        for e in entries {
            registry.mount(e, Scope::Global).await.unwrap();
        }
        let plan = registry.acquire(&"s".into());
        registry
            .mount(extension(&[(-1, "new", "new")]), Scope::Global)
            .await
            .unwrap();
        assert_eq!(texts(&plan).await, vec!["a", "b", "z"]);
        assert_eq!(
            texts(&registry.acquire(&"s".into())).await,
            vec!["new", "a", "b", "z"]
        );
    }
}
#[tokio::test]
async fn collisions_roll_back_without_mounting_and_closed_mount_does_not_collide() {
    let registry = Registry::new();
    let duplicate = extension(&[(0, "name", "a"), (1, "name", "b")]);
    assert!(
        matches!(registry.mount(duplicate.clone(), Scope::Global).await, Err(ExtensionError::PromptContributorCollision(n)) if n == "name")
    );
    assert_eq!(duplicate.rolled_back.load(Ordering::SeqCst), 1);
    assert!(registry.acquire(&"s".into()).prompt_contributors.is_empty());
    let handle = registry
        .mount(extension(&[(0, "name", "a")]), Scope::Global)
        .await
        .unwrap();
    let other = extension(&[(0, "name", "b")]);
    assert!(matches!(
        registry.mount(other.clone(), Scope::Global).await,
        Err(ExtensionError::PromptContributorCollision(_))
    ));
    assert_eq!(other.rolled_back.load(Ordering::SeqCst), 1);
    assert_eq!(texts(&registry.acquire(&"s".into())).await, vec!["a"]);
    handle.close().await.unwrap();
    registry.mount(other, Scope::Global).await.unwrap();
    registry
        .mount(
            extension(&[(0, "name", "session")]),
            Scope::Session("s".into()),
        )
        .await
        .unwrap();
    registry
        .mount(
            extension(&[(0, "name", "other")]),
            Scope::Session("t".into()),
        )
        .await
        .unwrap();
    assert!(matches!(
        registry
            .mount(
                extension(&[(0, "name", "duplicate")]),
                Scope::Session("s".into())
            )
            .await,
        Err(ExtensionError::PromptContributorCollision(_))
    ));
}
#[tokio::test]
async fn invalid_names_are_rejected_and_fingerprints_cover_name_order_and_contract() {
    for name in [
        String::new(),
        "x".repeat(MAX_PROMPT_CONTRIBUTOR_NAME_BYTES + 1),
        "bad\nname".into(),
        "bad\u{7f}name".into(),
    ] {
        let registry = Registry::new();
        let ext = extension(&[(0, &name, "text")]);
        assert!(
            matches!(registry.mount(ext.clone(), Scope::Global).await, Err(ExtensionError::Plan(message)) if message == "invalid prompt contributor name")
        );
        assert_eq!(ext.rolled_back.load(Ordering::SeqCst), 1);
        assert!(registry.acquire(&"s".into()).prompt_contributors.is_empty());
    }
    let mut fingerprints = Vec::new();
    for entries in [
        vec![],
        vec![(0, "a", "text")],
        vec![(1, "a", "text")],
        vec![(0, "b", "text")],
    ] {
        let registry = Registry::new();
        registry
            .mount(extension(&entries), Scope::Global)
            .await
            .unwrap();
        let plan = registry.acquire(&"s".into());
        assert_eq!(
            plan.components
                .iter()
                .filter(|c| c.id == "contract:crabber/prompt/contribution")
                .count(),
            usize::from(!entries.is_empty())
        );
        fingerprints.push(plan.fingerprint.clone());
    }
    for (i, a) in fingerprints.iter().enumerate() {
        for b in &fingerprints[i + 1..] {
            assert_ne!(a, b);
        }
    }
    let registry = Registry::new();
    assert_eq!(
        registry.acquire(&"s".into()).fingerprint,
        compute_fingerprint(&[])
    );
}
