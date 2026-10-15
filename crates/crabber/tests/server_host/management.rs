use super::*;
use crabber::{
    ExtensionFactory, FactoryContext, FactoryScope, ListSessions, ManageSession, SessionChange,
    SessionDefaults,
    core::{ControlId, SessionRevision},
    extension::ComponentIdentity,
};
pub(super) struct ForbiddenFactory(pub(super) Arc<AtomicUsize>);
#[async_trait]
impl ExtensionFactory for ForbiddenFactory {
    fn identity(&self) -> ComponentIdentity {
        ComponentIdentity {
            id: "management-factory".into(),
            version: "1".into(),
        }
    }
    fn config_hash(&self) -> String {
        "v1".into()
    }
    fn scope(&self) -> FactoryScope {
        FactoryScope::Session
    }
    async fn create(&self, _: FactoryContext) -> Result<Arc<dyn Extension>, ExtensionError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        panic!("metadata control must never initialize a factory");
    }
}
#[tokio::test]
async fn management_controls_work_without_factories_and_after_close() {
    let store = Arc::new(MemoryStore::new());
    let factories = Arc::new(AtomicUsize::new(0));
    let provider = FakeProvider::scripted(vec![]);
    let agent = Agent::builder()
        .behavior_version("management.v1")
        .store(store.clone())
        .provider(Arc::new(provider.clone()))
        .config(AgentConfig::new(selection("scripted")))
        .extension_factory(Arc::new(ForbiddenFactory(factories.clone())))
        .build()
        .unwrap();
    let request = ManageSession {
        id: ControlId::new(),
        session_id: SessionId::new(),
        expected_revision: SessionRevision::INITIAL,
        change: SessionChange::Create {
            workspace_id: "management-workspace".into(),
            directory: "/management".into(),
            title: "managed".into(),
            defaults: SessionDefaults::default(),
        },
    };
    let created = agent.manage_session(request.clone()).await.unwrap();
    assert_eq!(
        agent.session_summary(&request.session_id).await.unwrap(),
        created.summary
    );
    let page = agent
        .list_sessions(ListSessions {
            workspace_id: "management-workspace".into(),
            include_archived: false,
            limit: 1,
            cursor: None,
        })
        .await
        .unwrap();
    assert_eq!(page.sessions, vec![created.summary.clone()]);
    agent.close_extensions().await.unwrap();
    assert_eq!(
        agent.manage_session(request.clone()).await.unwrap(),
        created
    );
    let renamed = agent
        .manage_session(ManageSession {
            id: ControlId::new(),
            session_id: request.session_id.clone(),
            expected_revision: created.revision,
            change: SessionChange::Rename {
                title: "closed-control".into(),
            },
        })
        .await
        .unwrap();
    assert_eq!(renamed.summary.title, "closed-control");
    assert_eq!(
        agent
            .lookup_session_management(&request.session_id, &request.id)
            .await
            .unwrap(),
        Some(created)
    );
    assert_eq!(factories.load(Ordering::SeqCst), 0);
    assert_eq!(provider.requests().len(), 0);
    assert_eq!(store.list_unfinished_runs().await.unwrap().len(), 0);
}
