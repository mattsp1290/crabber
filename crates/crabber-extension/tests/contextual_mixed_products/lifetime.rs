use super::*;

#[tokio::test]
async fn mixed_public_plan_uses_scope_local_members_and_real_sibling_references() {
    let tool = Factory::new("tool", FactoryScope::Session);
    let fs = Factory::new("fs", FactoryScope::Workspace);
    let guard = Factory::new("guard", FactoryScope::Session);
    let host = Arc::new(Host::default());
    let plans = ContextualPlanProvider::new_with_owned_products(
        Registry::new(),
        vec![
            tool.entry(Family::Tool),
            fs.entry(Family::Fs),
            guard.entry(Family::Guard),
        ],
        BOUND,
        host.clone(),
    )
    .unwrap();
    let a = context();
    let b = context();
    let first = plans.acquire_plan_in(&a).await.unwrap();
    let second = plans.acquire_plan_in(&b).await.unwrap();
    let retained = second.host_services.workspace_fs.as_ref().unwrap().clone();
    let resources = ToolResourceOwner::new();
    let owner = CallbackInvocationOwner::attach(resources.resources()).unwrap();
    with_callback_invocations(owner.capability(), async {
        assert_eq!(retained.read("actual").await.unwrap(), b"actual");
        let info = &first.tools[0].info;
        assert_eq!(
            first.guards[0].check_with_context(GuardContext {
                tool: info,
                arguments: &Value::Null,
                session_id: a.session_id(),
                run_id: &crabber_core::RunId::new(),
                call_id: &crabber_core::ToolCallId::new()
            }),
            GuardDecision::Allow
        );
    })
    .await;
    drop(owner);
    resources.close();
    assert_eq!(resources.drain(BOUND).await.pending, 0);
    drop(first);
    plans.close_session(&a, BOUND).await.unwrap();
    assert_eq!(fs.seen.made.load(Ordering::SeqCst), 1);
    assert_eq!(fs.seen.drops.load(Ordering::SeqCst), 0);
    drop(second);
    plans.close_session(&b, BOUND).await.unwrap();
    assert!(plans.close_all(Duration::from_millis(20)).await.is_err());
    assert_eq!(fs.seen.drops.load(Ordering::SeqCst), 0);
    drop(retained);
    plans.close_all(BOUND).await.unwrap();
    assert_eq!(fs.seen.drops.load(Ordering::SeqCst), 1);
    assert_eq!(tool.seen.drops.load(Ordering::SeqCst), 2);
    assert_eq!(guard.seen.drops.load(Ordering::SeqCst), 2);
    let graphs = host.0.lock().unwrap().clone();
    for graph in graphs {
        match graph.generation().scope() {
            ServiceGenerationScope::Workspace { .. } => disposed(&graph, Family::Fs, 0).await,
            ServiceGenerationScope::Session { .. } => {
                disposed(&graph, Family::Tool, 0).await;
                disposed(&graph, Family::Guard, 1).await;
            }
            ServiceGenerationScope::Shared => panic!("no shared product"),
        }
    }
}
#[tokio::test]
async fn later_mixed_constructor_failure_retains_and_disposes_original_earlier_node() {
    let tool = Factory::new("first-tool", FactoryScope::Session);
    let mut guard = Factory::new("second-guard", FactoryScope::Session);
    Arc::get_mut(&mut guard).unwrap().fail = true;
    let host = Arc::new(Host::default());
    let plans = ContextualPlanProvider::new_with_owned_products(
        Registry::new(),
        vec![tool.entry(Family::Tool), guard.entry(Family::Guard)],
        BOUND,
        host.clone(),
    )
    .unwrap();
    assert!(plans.acquire_plan_in(&context()).await.is_err());
    let _closed = plans.close_all(BOUND).await;
    assert_eq!(tool.seen.made.load(Ordering::SeqCst), 1);
    assert_eq!(tool.seen.drops.load(Ordering::SeqCst), 1);
    assert_eq!(guard.seen.made.load(Ordering::SeqCst), 1);
    assert_eq!(guard.seen.drops.load(Ordering::SeqCst), 0);
}
