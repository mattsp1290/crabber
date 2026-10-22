use super::{event, message};
use crate::{
    AdmitRequest, ManageSession, QueueAction, QueueHeadsRequest, QueueInput, QueueMutation,
    QueueOutcome, QueueState, SessionChange, SessionDefaults, Store, StoreError,
    WorkspaceRunsRequest,
};
use crabber_core::{
    AdmissionKey, ControlId, EventKind, InputEnvelope, ManualClock, PartKind, QueueId, Role,
    RunStatus, SessionId, SessionRevision, Usage,
};
use std::{sync::Arc, time::Duration};
use time::OffsetDateTime;

const WORKSPACE_A: &str = "bound-workspace";
const WORKSPACE_B: &str = "discovery-other";

async fn create(store: &dyn Store, workspace: &str) -> SessionId {
    let session = SessionId::new();
    create_id(store, workspace, &session).await;
    session
}

async fn create_id(store: &dyn Store, workspace: &str, session: &SessionId) {
    store
        .manage_session(ManageSession {
            session_id: session.clone(),
            id: ControlId::new(),
            expected_revision: SessionRevision::INITIAL,
            change: SessionChange::Create {
                workspace_id: workspace.into(),
                directory: "/bound/workspace".into(),
                title: "discovery".into(),
                defaults: SessionDefaults::default(),
            },
        })
        .await
        .unwrap();
}

async fn enqueue(store: &dyn Store, session: &SessionId, workspace: &str) -> crate::QueueSummary {
    let revision = store.session_revision(session).await.unwrap();
    let input = InputEnvelope::text("future", revision).unwrap();
    let prepared = crate::typed_admission_contract::prepare(
        crate::execution_binding_contract::fixture(
            session,
            OffsetDateTime::UNIX_EPOCH,
            "discovery-backend",
            'b',
        ),
        input.clone(),
        OffsetDateTime::UNIX_EPOCH,
    );
    let mut request = prepared.binding.request;
    request.workspace_id = workspace.into();
    let mut admission = prepared.keyed.options;
    admission.key = AdmissionKey::new(ControlId::new().0).unwrap();
    let result = store
        .mutate_queue(QueueMutation {
            session_id: session.clone(),
            id: ControlId::new(),
            expected_revision: revision,
            action: QueueAction::Enqueue {
                queue_id: QueueId::new(),
                input: Box::new(QueueInput {
                    request,
                    input,
                    admission,
                }),
            },
        })
        .await
        .unwrap();
    result.receipt().summary.clone()
}

async fn unfinished(
    store: &dyn Store,
    session: &SessionId,
    workspace: &str,
    now: OffsetDateTime,
) -> crabber_core::Run {
    store
        .admit_run(AdmitRequest {
            expected_revision: None,
            additional_messages: vec![],
            input_envelope: None,
            session_id: Some(session.clone()),
            workspace_id: workspace.into(),
            directory: "/bound/workspace".into(),
            title: "discovery".into(),
            user_message: message(
                session,
                None,
                Role::User,
                PartKind::UserInputText,
                "run",
                now,
            ),
            config_hash: "config".into(),
            plan_fingerprint: "plan".into(),
            owner: "discovery".into(),
            lease: Duration::from_secs(1),
        })
        .await
        .unwrap()
        .run
}

async fn admitted_queue(store: &dyn Store, now: OffsetDateTime) {
    let session = create(store, WORKSPACE_A).await;
    let summary = enqueue(store, &session, WORKSPACE_A).await;
    let mutation = |action, revision| QueueMutation {
        session_id: session.clone(),
        id: ControlId::new(),
        expected_revision: revision,
        action,
    };
    let claimed = store
        .mutate_queue(mutation(
            QueueAction::Claim {
                owner: "discovery".into(),
                lease: Duration::from_secs(30),
            },
            store.session_revision(&session).await.unwrap(),
        ))
        .await
        .unwrap();
    let QueueOutcome::Applied {
        authority: Some(authority),
        ..
    } = claimed
    else {
        panic!("claim grants authority")
    };
    let retained = store
        .retained_queue(&session, &summary.queue_id)
        .await
        .unwrap();
    let mut prepared = crate::typed_admission_contract::prepare(
        crate::execution_binding_contract::fixture(&session, now, "discovery-backend", 'b'),
        retained.input.input,
        now,
    );
    prepared.keyed.options = retained.input.admission;
    store
        .mutate_queue(mutation(
            QueueAction::Prepare {
                authority: authority.clone(),
                prepared: Box::new(prepared),
            },
            store.session_revision(&session).await.unwrap(),
        ))
        .await
        .unwrap();
    let admitted = store
        .mutate_queue(mutation(
            QueueAction::Admit {
                authority,
                owner: "discovery".into(),
                lease: Duration::from_secs(30),
            },
            store.session_revision(&session).await.unwrap(),
        ))
        .await
        .unwrap();
    let QueueOutcome::Applied {
        admitted: Some(admitted),
        receipt,
        ..
    } = admitted
    else {
        panic!("queue admits")
    };
    assert_eq!(receipt.summary.state, QueueState::Admitted);
    let execution = store.execution(admitted.fence).await.unwrap();
    execution.begin_admission_execution().await.unwrap();
    execution
        .settle_run(
            RunStatus::Completed,
            None,
            Usage::default(),
            event(&session, &admitted.run.id, EventKind::RunSettled, now),
        )
        .await
        .unwrap();
}

/// Check scoped unfinished-run selection and FIFO-head discovery on a fresh Store.
/// # Panics
/// Panics if workspace selection, head state, ordering, bounds or pagination drift.
pub async fn run_workspace_discovery_contract<S, F>(factory: F)
where
    S: Store,
    F: Fn(Arc<ManualClock>) -> S,
{
    let now = OffsetDateTime::UNIX_EPOCH;
    let store = factory(Arc::new(ManualClock::new(now)));
    let a = create(&store, WORKSPACE_A).await;
    let b = create(&store, WORKSPACE_B).await;
    let a_run = unfinished(&store, &a, WORKSPACE_A, now).await;
    let b_run = unfinished(&store, &b, WORKSPACE_B, now).await;
    let a_head = enqueue(&store, &a, WORKSPACE_A).await;
    let b_head = enqueue(&store, &b, WORKSPACE_B).await;
    // A second pending entry must never appear as a second head for this session.
    enqueue(&store, &a, WORKSPACE_A).await;
    let a2 = create(&store, WORKSPACE_A).await;
    enqueue(&store, &a2, WORKSPACE_A).await;
    let claimed = store
        .mutate_queue(QueueMutation {
            session_id: a2.clone(),
            id: ControlId::new(),
            expected_revision: store.session_revision(&a2).await.unwrap(),
            action: QueueAction::Claim {
                owner: "discovery-claim".into(),
                lease: Duration::from_secs(30),
            },
        })
        .await
        .unwrap();
    let claimed_head = claimed.receipt().summary.clone();
    assert_eq!(claimed_head.state, QueueState::Claimed);
    assert_eq!(claimed_head.claim_generation, 1);
    admitted_queue(&store, now).await;
    for (workspace, expected) in [
        (WORKSPACE_A, vec![a_run]),
        (WORKSPACE_B, vec![b_run]),
        ("unknown", vec![]),
    ] {
        assert_eq!(
            store
                .list_workspace_unfinished_runs(WorkspaceRunsRequest {
                    workspace_id: workspace.into()
                })
                .await
                .unwrap(),
            expected
        );
    }
    let request = QueueHeadsRequest {
        workspace_id: WORKSPACE_A.into(),
        limit: 256,
        cursor: None,
    };
    let mut expected = vec![a_head, claimed_head];
    expected.sort_by(|left, right| left.session_id.cmp(&right.session_id));
    let page = store.list_queue_heads(request.clone()).await.unwrap();
    assert_eq!(page.heads, expected);
    assert!(page.next.is_none());
    let first = store
        .list_queue_heads(QueueHeadsRequest {
            limit: 1,
            ..request.clone()
        })
        .await
        .unwrap();
    assert_eq!(first.heads, expected[..1]);
    assert_eq!(first.next, Some(expected[0].session_id.clone()));
    let last = store
        .list_queue_heads(QueueHeadsRequest {
            limit: 1,
            cursor: first.next,
            ..request.clone()
        })
        .await
        .unwrap();
    assert_eq!(last.heads, expected[1..]);
    assert!(last.next.is_none());
    for (workspace, heads) in [(WORKSPACE_B, vec![b_head]), ("unknown", vec![])] {
        let page = store
            .list_queue_heads(QueueHeadsRequest {
                workspace_id: workspace.into(),
                ..request.clone()
            })
            .await
            .unwrap();
        assert_eq!(page.heads, heads);
        assert!(page.next.is_none());
    }
    for limit in [0, 257] {
        assert!(matches!(
            store
                .list_queue_heads(QueueHeadsRequest {
                    limit,
                    ..request.clone()
                })
                .await,
            Err(StoreError::Validation(_))
        ));
    }
    opaque_heads(&store).await;
}

async fn opaque_heads(store: &dyn Store) {
    let unique = SessionId::new().0;
    let workspace = format!("discovery-opaque-{unique}");
    let sessions = [
        SessionId(format!("a/{unique}")),
        SessionId(format!("b/所有者-{unique}")),
        SessionId(format!("c{unique}{}", "x".repeat(512 - unique.len() - 1))),
    ];
    let mut expected = Vec::new();
    for session in &sessions {
        create_id(store, &workspace, session).await;
        expected.push(enqueue(store, session, &workspace).await);
    }
    expected.sort_by(|left, right| left.session_id.cmp(&right.session_id));
    let request = QueueHeadsRequest {
        workspace_id: workspace,
        limit: 1,
        cursor: None,
    };
    let mut cursor = None;
    let mut actual = Vec::new();
    // A bounded walk prevents a broken cursor from turning the contract into a hang.
    for (index, head) in expected.iter().enumerate() {
        let page = store
            .list_queue_heads(QueueHeadsRequest {
                cursor,
                ..request.clone()
            })
            .await
            .unwrap();
        assert_eq!(page.heads, vec![head.clone()]);
        if index + 1 == expected.len() {
            assert!(page.next.is_none());
        } else {
            assert_eq!(page.next, Some(head.session_id.clone()));
        }
        cursor = page.next;
        actual.extend(page.heads);
    }
    assert_eq!(actual, expected);
    // A final long cursor is valid even when there are no later heads.
    let empty = store
        .list_queue_heads(QueueHeadsRequest {
            cursor: Some(sessions[2].clone()),
            ..request.clone()
        })
        .await
        .unwrap();
    assert_eq!(empty.heads, vec![]);
    assert!(empty.next.is_none());
    for invalid in [String::new(), "bad\nid".into(), "x".repeat(513)] {
        assert!(matches!(
            store
                .list_queue_heads(QueueHeadsRequest {
                    cursor: Some(SessionId(invalid)),
                    ..request.clone()
                })
                .await,
            Err(StoreError::Validation(_))
        ));
    }
}
