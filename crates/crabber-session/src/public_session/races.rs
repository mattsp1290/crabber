//! Faults and mutations between independently bounded constituent reads.
use super::*;
use crate::*;
use async_trait::async_trait;
use crabber_core::*;
use std::{
    collections::BTreeMap,
    sync::atomic::{AtomicBool, Ordering},
};
struct Interposed<'a> {
    inner: &'a dyn Store,
    boundary: u8,
    fired: AtomicBool,
}
impl Interposed<'_> {
    async fn mutate(&self, boundary: u8, session: &SessionId) {
        if self.boundary == boundary && !self.fired.swap(true, Ordering::SeqCst) {
            self.inner
                .manage_session(ManageSession {
                    id: ControlId::new(),
                    session_id: session.clone(),
                    expected_revision: self.inner.session_revision(session).await.unwrap(),
                    change: SessionChange::Rename {
                        title: format!("interposed-{boundary}"),
                    },
                })
                .await
                .unwrap();
        }
    }
}
#[async_trait]
impl Store for Interposed<'_> {
    async fn public_control_header_v2(
        &self,
        request: PublicHeaderRequestV2,
    ) -> Result<PublicHeaderOutcomeV2, PublicSessionError> {
        if self.boundary == 3 {
            return Err(PublicSessionError::UnknownStoreOutcome);
        }
        let session = request.session_id.clone();
        let result = self.inner.public_control_header_v2(request).await;
        self.mutate(1, &session).await;
        result
    }
    async fn public_newest_history_v2(
        &self,
        request: SnapshotRequest,
    ) -> Result<PublicNewestHistoryOutcome, StoreError> {
        if self.boundary == 4 {
            return Err(StoreError::HostUnsupported);
        }
        let session = request.session_id.clone();
        let result = self.inner.public_newest_history_v2(request).await;
        self.mutate(2, &session).await;
        result
    }
    async fn public_history_v2(
        &self,
        request: SnapshotRequest,
    ) -> Result<PublicHistoryOutcome, StoreError> {
        if self.boundary == 4 {
            return Err(StoreError::HostUnsupported);
        }
        let session = request.session_id.clone();
        let result = self.inner.public_history_v2(request).await;
        self.mutate(2, &session).await;
        result
    }
    async fn public_run_state(
        &self,
        session: &SessionId,
        cap: usize,
    ) -> Result<PublicRunOutcome, StoreError> {
        let result = self.inner.public_run_state(session, cap).await;
        self.mutate(0, session).await;
        result
    }
    async fn public_control_header(
        &self,
        request: PublicHeaderRequest,
    ) -> Result<PublicHeaderOutcome, PublicSessionError> {
        if self.boundary == 3 {
            return Err(PublicSessionError::UnknownStoreOutcome);
        }
        let session = request.session_id.clone();
        let result = self.inner.public_control_header(request).await;
        self.mutate(1, &session).await;
        result
    }
    async fn public_history(
        &self,
        request: SnapshotRequest,
    ) -> Result<PublicHistoryOutcome, StoreError> {
        if self.boundary == 4 {
            return Err(StoreError::HostUnsupported);
        }
        let session = request.session_id.clone();
        let result = self.inner.public_history(request).await;
        self.mutate(2, &session).await;
        result
    }
    async fn session_revision(&self, session: &SessionId) -> Result<SessionRevision, StoreError> {
        if self.boundary == 5 {
            return Err(StoreError::HostUnsupported);
        }
        self.inner.session_revision(session).await
    }
    async fn admit_run(&self, r: AdmitRequest) -> Result<AdmitOutcome, StoreError> {
        self.inner.admit_run(r).await
    }
    async fn execution(&self, f: RunFence) -> Result<Box<dyn ExecutionStore>, StoreError> {
        self.inner.execution(f).await
    }
    async fn get_session(&self, s: &SessionId) -> Result<Option<Session>, StoreError> {
        self.inner.get_session(s).await
    }
    async fn get_run(&self, r: &RunId) -> Result<Option<Run>, StoreError> {
        self.inner.get_run(r).await
    }
    async fn list_messages(
        &self,
        s: &SessionId,
        e: Option<EpochId>,
    ) -> Result<Vec<Message>, StoreError> {
        self.inner.list_messages(s, e).await
    }
    async fn list_all_messages(&self, s: &SessionId) -> Result<Vec<Message>, StoreError> {
        self.inner.list_all_messages(s).await
    }
    async fn list_events(
        &self,
        s: &SessionId,
        c: Option<EventCursor>,
        n: usize,
    ) -> Result<Vec<EventRecord>, StoreError> {
        self.inner.list_events(s, c, n).await
    }
    async fn list_unfinished_runs(&self) -> Result<Vec<Run>, StoreError> {
        self.inner.list_unfinished_runs().await
    }
    async fn list_unfinished_tool_calls(
        &self,
        r: &RunId,
    ) -> Result<Vec<ToolCallRecord>, StoreError> {
        self.inner.list_unfinished_tool_calls(r).await
    }
    async fn claim_expired_run(&self, r: &RunId, o: &str) -> Result<RunFence, StoreError> {
        self.inner.claim_expired_run(r, o).await
    }
    async fn get_extension_state(
        &self,
        e: &str,
        s: &SessionId,
    ) -> Result<BTreeMap<String, String>, StoreError> {
        self.inner.get_extension_state(e, s).await
    }
    async fn enqueue_inbox(
        &self,
        s: &SessionId,
        k: InboxKind,
        m: Message,
    ) -> Result<(), StoreError> {
        self.inner.enqueue_inbox(s, k, m).await
    }
}
pub(crate) async fn run(store: &impl Store) {
    let session = SessionId::new();
    store
        .manage_session(ManageSession {
            id: ControlId::new(),
            session_id: session.clone(),
            expected_revision: SessionRevision::INITIAL,
            change: SessionChange::Create {
                workspace_id: "race".into(),
                directory: "/race".into(),
                title: "before".into(),
                defaults: SessionDefaults::default(),
            },
        })
        .await
        .unwrap();
    for boundary in 0..6 {
        let revision = store.session_revision(&session).await.unwrap();
        let events = store.list_events(&session, None, 1000).await.unwrap();
        let reader = Interposed {
            inner: store,
            boundary,
            fired: AtomicBool::new(false),
        };
        let result = reader
            .public_session_snapshot(super::tests::request(session.clone()))
            .await;
        if boundary < 3 {
            assert!(
                matches!(result, Ok(PublicSessionOutcome::Invalidated)),
                "{boundary}: {result:?}"
            );
        } else {
            assert!(result.is_err(), "{boundary}: {result:?}");
            assert_eq!(store.session_revision(&session).await.unwrap(), revision);
            assert_eq!(
                store.list_events(&session, None, 1000).await.unwrap(),
                events
            );
        }
    }
}

pub(crate) async fn run_v2(store: &impl Store) {
    let (f, _, _) = crate::frontend_contract::fixture::paused(store).await;
    for (boundary, newest) in (0..6).flat_map(|boundary| [(boundary, false), (boundary, true)]) {
        let session = &f.request.session_id;
        let revision = store.session_revision(session).await.unwrap();
        let events = store.list_events(session, None, 1000).await.unwrap();
        let reader = Interposed {
            inner: store,
            boundary,
            fired: AtomicBool::new(false),
        };
        let request = PublicSessionRequestV2 {
            session_id: session.clone(),
            limits: crate::public_session::v2::tests::limits(),
            continuation: None,
        };
        let result = if newest {
            reader.public_session_newest_snapshot_v2(request).await
        } else {
            reader.public_session_snapshot_v2(request).await
        };
        if boundary < 3 {
            assert!(
                matches!(result, Ok(PublicSessionOutcomeV2::Invalidated)),
                "{boundary}: {result:?}"
            );
        } else {
            assert!(result.is_err(), "{boundary}: {result:?}");
            assert_eq!(store.session_revision(session).await.unwrap(), revision);
            assert_eq!(
                store.list_events(session, None, 1000).await.unwrap(),
                events
            );
        }
    }
}
