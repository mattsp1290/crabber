use crabber::{core::*, session::*, *};
use sqlx::postgres::PgPoolOptions;
use std::sync::Arc;

pub(super) struct Fixture {
    pub admin: sqlx::PgPool,
    pub host: Agent,
    pub worker: Agent,
    pub native: Arc<PostgresStore>,
    pub worker_native: Arc<PostgresStore>,
    pub worker_provider: Arc<FakeProvider>,
    pub workspace_a: String,
    pub workspace_b: String,
    pub roles: Vec<String>,
}
impl Fixture {
    pub async fn new() -> Option<Self> {
        let Ok(url) = std::env::var("CRABBER_TEST_POSTGRES_URL") else {
            assert!(
                std::env::var_os("CRABBER_REQUIRE_POSTGRES").is_none(),
                "required PostgreSQL URL missing"
            );
            return None;
        };
        PostgresStore::migrate(&url).await.unwrap();
        let admin = PgPoolOptions::new()
            .max_connections(2)
            .connect(&url)
            .await
            .unwrap();
        let workspace_a = format!("ws-a-{}", SessionId::new());
        let workspace_b = format!("ws-b-{}", SessionId::new());
        let mut roles = Vec::new();
        let mut urls = Vec::new();
        for group in ["crabber_host", "crabber_worker"] {
            let name = format!("crabber_api_{}", uuid::Uuid::new_v4().simple());
            let password = uuid::Uuid::new_v4().simple().to_string();
            sqlx::query(&format!(
                "CREATE ROLE {name} LOGIN PASSWORD '{password}' IN ROLE {group}"
            ))
            .execute(&admin)
            .await
            .unwrap();
            if group == "crabber_worker" {
                sqlx::query(
                    "INSERT INTO crabber_workspace_grants(role_name,workspace_id) VALUES($1,$2)",
                )
                .bind(&name)
                .bind(&workspace_a)
                .execute(&admin)
                .await
                .unwrap();
            }
            let mut parsed = url::Url::parse(&url).unwrap();
            parsed.set_username(&name).unwrap();
            parsed.set_password(Some(&password)).unwrap();
            roles.push(name);
            urls.push(parsed.to_string());
        }
        let native = Arc::new(PostgresStore::connect(&urls[0]).await.unwrap());
        let worker_native = Arc::new(PostgresStore::connect(&urls[1]).await.unwrap());
        let host_provider = provider(64);
        let worker_provider = provider(64);
        Some(Self {
            admin,
            host: agent(native.clone(), host_provider, &workspace_b),
            worker: agent(worker_native.clone(), worker_provider.clone(), &workspace_a),
            native,
            worker_native,
            worker_provider,
            workspace_a,
            workspace_b,
            roles,
        })
    }
    pub fn context(&self, own: bool) -> ExecutionContext {
        ExecutionContext::new(
            SessionContext::new(
                SessionId::new(),
                if own {
                    &self.workspace_a
                } else {
                    &self.workspace_b
                },
                "/roles",
            ),
            selection(),
        )
    }
    pub async fn create(&self, own: bool) -> ExecutionContext {
        let context = self.context(own);
        let agent = if own { &self.worker } else { &self.host };
        agent
            .manage_session(ManageSession {
                id: ControlId::new(),
                session_id: context.session().session_id().clone(),
                expected_revision: SessionRevision::INITIAL,
                change: SessionChange::Create {
                    workspace_id: context.session().workspace_id().into(),
                    directory: "/roles".into(),
                    title: "roles".into(),
                    defaults: SessionDefaults::default(),
                },
            })
            .await
            .unwrap();
        context
    }
    pub async fn snapshot(&self, session: &SessionId) -> Vec<Vec<String>> {
        let mut result = Vec::new();
        for (table, key) in [
            ("sessions", "id"),
            ("runs", "session_id"),
            ("future_queue", "session_id"),
            ("run_controls", "session_id"),
        ] {
            result.push(
                sqlx::query_scalar(&format!(
                    "SELECT row_to_json(t)::text FROM {table} t WHERE {key}=$1 ORDER BY 1"
                ))
                .bind(&session.0)
                .fetch_all(&self.admin)
                .await
                .unwrap(),
            );
        }
        result
    }
    pub async fn unchanged(&self, session: &SessionId, before: Vec<Vec<String>>) {
        assert_eq!(self.snapshot(session).await, before, "foreign rows changed");
        assert!(
            self.worker_provider.requests().is_empty(),
            "denied API called provider"
        );
    }
    pub async fn cleanup(self) {
        self.host.close_extensions().await.unwrap();
        self.worker.close_extensions().await.unwrap();
        for role in &self.roles {
            sqlx::query("DELETE FROM crabber_workspace_grants WHERE role_name=$1")
                .bind(role)
                .execute(&self.admin)
                .await
                .unwrap();
            sqlx::query(&format!("DROP OWNED BY {role}"))
                .execute(&self.admin)
                .await
                .unwrap();
            sqlx::query(&format!("DROP ROLE {role}"))
                .execute(&self.admin)
                .await
                .unwrap();
        }
    }
}
pub(super) fn selection() -> Selection {
    Selection {
        provider_id: "fake".into(),
        model_id: "scripted".into(),
    }
}
pub(super) fn provider(count: usize) -> Arc<FakeProvider> {
    Arc::new(FakeProvider::scripted(
        (0..count)
            .map(|_| {
                vec![
                    StreamDelta::TextDelta("done".into()),
                    StreamDelta::Completed,
                ]
            })
            .collect(),
    ))
}
pub(super) fn agent(store: Arc<dyn Store>, provider: Arc<FakeProvider>, workspace: &str) -> Agent {
    let mut config = AgentConfig::new(selection());
    config.workspace_id = workspace.into();
    config.directory = "/roles".into();
    Agent::builder()
        .behavior_fingerprint(options().behavior_fingerprint)
        .store(store)
        .provider(provider)
        .config(config)
        .build()
        .unwrap()
}
pub(super) fn options() -> AdmissionOptions {
    AdmissionOptions {
        key: AdmissionKey::new("api-key").unwrap(),
        fingerprint: InputFingerprint::new("a".repeat(64)).unwrap(),
        behavior_fingerprint: InputFingerprint::new("b".repeat(64)).unwrap(),
    }
}
pub(super) fn denied<T>(result: Result<T, RuntimeError>) {
    let error = result.err().expect("expected hidden-session denial");
    assert!(
        matches!(
            error,
            RuntimeError::Store(StoreError::NotFound | StoreError::SessionIdentityMismatch)
        ),
        "unexpected hidden-session denial: {error:?}"
    );
}
pub(super) fn public_request(session: &SessionId) -> PublicSessionRequestV2 {
    PublicSessionRequestV2 {
        session_id: session.clone(),
        continuation: None,
        limits: PublicSessionLimitsV2 {
            header_bytes: 1 << 20,
            proof_bytes: 32 << 20,
            approval_interactions: 64,
            tool_requirements: 64,
            queue_entries: 64,
            registered_owners: 64,
            run_controls: 64,
            history: SnapshotLimits {
                messages: 64,
                tool_calls: 64,
                parts: 256,
                text_bytes: 1 << 20,
                encoded_bytes: 2 << 20,
            },
        },
    }
}
pub(super) async fn legacy(store: &PostgresStore, workspace: &str) -> AdmitOutcome {
    let session = SessionId::new();
    let message = MessageId::new();
    let admitted = store
        .admit_run(AdmitRequest {
            expected_revision: None,
            additional_messages: vec![],
            input_envelope: None,
            session_id: None,
            workspace_id: workspace.into(),
            directory: "/roles".into(),
            title: "legacy".into(),
            user_message: Message {
                id: message.clone(),
                session_id: session,
                run_id: None,
                role: Role::User,
                parent_id: None,
                created_at: time::OffsetDateTime::now_utc(),
                parts: vec![Part {
                    id: PartId::new(),
                    message_id: message,
                    ordinal: 0,
                    kind: PartKind::UserInputText,
                    content: ContentBlock::Text {
                        text: "legacy".into(),
                    },
                }],
            },
            config_hash: "legacy-config".into(),
            plan_fingerprint: "legacy-plan".into(),
            owner: "stopped-legacy-worker".into(),
            lease: std::time::Duration::from_millis(1),
        })
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    admitted
}
