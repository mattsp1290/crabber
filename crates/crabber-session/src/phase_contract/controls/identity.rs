//! Public opaque scope identifiers follow the phase-control identity contract.
use crate::{AdmitRequest, PhaseError, PhasePageRequest, Store};
use crabber_core::{MessageId, PartId, SessionId};
use time::OffsetDateTime;
pub(super) async fn run(store: &impl Store) {
    let original = crate::public_state_contract::admit(store, OffsetDateTime::UNIX_EPOCH).await;
    let mut message = store
        .list_all_messages(&original.session.id)
        .await
        .unwrap()
        .remove(0);
    let session = SessionId(format!(
        "opaque session: 原本/{}:{}",
        uuid::Uuid::new_v4(),
        "é".repeat(160)
    ));
    assert!(session.0.len() > 128 && session.0.len() <= 512);
    message.id = MessageId::new();
    message.session_id = session.clone();
    message.run_id = None;
    for part in &mut message.parts {
        part.id = PartId::new();
        part.message_id = message.id.clone();
    }
    let admitted = store
        .admit_run(AdmitRequest {
            input_envelope: None,
            additional_messages: Vec::new(),
            expected_revision: None,
            session_id: None,
            workspace_id: "workspace".into(),
            directory: "/workspace".into(),
            title: "opaque session".into(),
            user_message: message,
            config_hash: "config".into(),
            plan_fingerprint: "plan".into(),
            owner: "owner".into(),
            lease: std::time::Duration::from_secs(30),
        })
        .await
        .unwrap();
    assert_eq!(admitted.session.id, session);
    let request = PhasePageRequest {
        session_id: session.clone(),
        run_id: admitted.run.id,
        limit: 1,
        after: None,
        expected_revision: None,
    };
    let page = store.list_tool_phases(request.clone()).await.unwrap();
    assert_eq!(page.phases, []);
    assert!(page.next_after.is_none());
    assert_eq!(
        page.revision,
        store.session_revision(&session).await.unwrap()
    );
    for invalid in [String::new(), "x".repeat(513), "bad\nscope".into()] {
        let mut bad = request.clone();
        bad.session_id = SessionId(invalid);
        assert_eq!(
            store.list_tool_phases(bad).await.unwrap_err(),
            PhaseError::InvalidRequest
        );
    }
    let mut opaque_run = request;
    opaque_run.run_id = crabber_core::RunId("opaque run: 原本/unknown".into());
    opaque_run.validate().unwrap();
    assert_eq!(
        store.list_tool_phases(opaque_run).await.unwrap_err(),
        PhaseError::NotFound
    );
}
