//! Shared backend acceptance, including privately persisted domain states.
use crate::*;
use crabber_core::*;
use serde_json::json;
use std::{sync::Arc, time::Duration};

#[async_trait::async_trait]
pub(crate) trait FixtureStore: Store {
    async fn seed_run(&self, run: Run);
    async fn calls(&self, run: &RunId) -> Vec<ToolCallRecord>;
    async fn unconsumed_inbox(&self, session: &SessionId) -> usize;
}

pub(crate) fn request() -> AdmitRequest {
    let session = SessionId::new();
    AdmitRequest {
        session_id: None,
        workspace_id: "test".into(),
        directory: ".".into(),
        title: "test".into(),
        config_hash: "unavailable".into(),
        plan_fingerprint: "unavailable".into(),
        owner: "owner".into(),
        lease: Duration::from_secs(30),
        user_message: Message {
            id: MessageId::new(),
            session_id: session,
            run_id: None,
            role: Role::User,
            parent_id: None,
            parts: vec![],
            created_at: time::OffsetDateTime::UNIX_EPOCH,
        },
    }
}
pub(crate) fn event(run: &Run, kind: EventKind) -> EventRecord {
    EventRecord {
        cursor: None,
        session_id: run.session_id.clone(),
        run_id: run.id.clone(),
        turn_id: None,
        kind,
        payload: json!({}),
        correlation: None,
        live_only: false,
        created_at: run.created_at,
    }
}
pub(crate) fn abandon_request(
    admitted: &AdmitOutcome,
    authority: AbandonAuthority,
) -> AbandonRequest {
    AbandonRequest {
        expected: admitted.fence.clone(),
        expected_owner: admitted.run.owner.clone(),
        authority,
    }
}

#[allow(clippy::too_many_lines)]
pub(crate) async fn assert_stale(execution: &dyn ExecutionStore, run: &Run, call: &ToolCallId) {
    let mut message = request().user_message;
    message.session_id = run.session_id.clone();
    message.run_id = Some(run.id.clone());
    let part = Part {
        id: PartId::new(),
        message_id: message.id.clone(),
        ordinal: 0,
        kind: PartKind::UserInputText,
        content: ContentBlock::Text {
            text: "stale".into(),
        },
    };
    let new_call = ToolCallRecord {
        id: ToolCallId::new(),
        run_id: run.id.clone(),
        name: "missing".into(),
        arguments: json!({}),
        status: ToolCallStatus::Pending,
        retry_safe: false,
        result: None,
    };
    let epoch = ContextEpoch {
        id: EpochId::new(),
        session_id: run.session_id.clone(),
        run_id: run.id.clone(),
        parent: Some(run.epoch_id.clone()),
        summarized_range: None,
        summary_message_id: None,
        tail_start_message_id: None,
        provider_id: "missing".into(),
        model_id: "missing".into(),
        reason: "stale".into(),
        next_policy: None,
    };
    let errors = [
        execution
            .renew_lease(run.lease_until + time::Duration::hours(1))
            .await,
        execution.append_message(message.clone()).await,
        execution.append_part(part).await,
        execution
            .append_event(event(run, EventKind::MessageCommitted))
            .await,
        execution
            .create_tool_call(new_call, event(run, EventKind::ToolCallPending))
            .await,
        execution
            .claim_tool_call(call, event(run, EventKind::ToolCallRunning))
            .await,
        execution
            .settle_tool_call(
                call,
                ToolResult {
                    status: ToolResultStatus::Completed,
                    content: vec![],
                },
                message.clone(),
                event(run, EventKind::ToolCallSettled),
            )
            .await,
        execution
            .settle_run(
                RunStatus::Completed,
                None,
                Usage::default(),
                event(run, EventKind::RunSettled),
            )
            .await,
        execution.start_epoch(epoch).await,
        execution.finish_epoch(&run.epoch_id, message).await,
        execution
            .pause_run(json!({}), event(run, EventKind::RunPaused))
            .await,
        execution
            .put_extension_state("stale", vec![("key".into(), Some("value".into()))])
            .await,
    ];
    for error in errors {
        assert_eq!(error.unwrap_err(), StoreError::Conflict);
    }
    assert_eq!(
        execution.claim_inbox(InboxKind::Steer).await.unwrap_err(),
        StoreError::Conflict
    );
    assert_eq!(
        execution
            .claim_inbox_into_history(InboxKind::FollowUp)
            .await
            .unwrap_err(),
        StoreError::Conflict
    );
}

#[allow(clippy::too_many_lines)]
pub(crate) async fn run_contract<S: FixtureStore>(store: S, clock: Arc<ManualClock>) {
    for status in [RunStatus::Pending, RunStatus::Running, RunStatus::Paused] {
        for authority in [
            AbandonAuthority::ExpiredLease,
            AbandonAuthority::HostStoppedOwner,
        ] {
            let now = time::OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();
            clock.set(now);
            let options = AdmissionOptions {
                key: AdmissionKey::new("preserved").unwrap(),
                fingerprint: InputFingerprint::new("a".repeat(64)).unwrap(),
                behavior_fingerprint: InputFingerprint::new("b".repeat(64)).unwrap(),
            };
            let mut keyed_request = request();
            keyed_request.session_id = Some(keyed_request.user_message.session_id.clone());
            let KeyedAdmitOutcome::Started { admitted, receipt } = store
                .admit_keyed_run(KeyedAdmitRequest {
                    execution: None,
                    request: keyed_request,
                    options: options.clone(),
                })
                .await
                .unwrap()
            else {
                panic!("new receipt")
            };
            let execution = store.execution(admitted.fence.clone()).await.unwrap();
            for (index, call_status) in [
                ToolCallStatus::Pending,
                ToolCallStatus::Running,
                ToolCallStatus::Completed,
                ToolCallStatus::Failed,
            ]
            .into_iter()
            .enumerate()
            {
                let call = ToolCallRecord {
                    id: ToolCallId::new(),
                    run_id: admitted.run.id.clone(),
                    name: format!("missing-{index}"),
                    arguments: json!({}),
                    status: ToolCallStatus::Pending,
                    retry_safe: false,
                    result: None,
                };
                execution
                    .create_tool_call(
                        call.clone(),
                        event(&admitted.run, EventKind::ToolCallPending),
                    )
                    .await
                    .unwrap();
                if call_status != ToolCallStatus::Pending {
                    execution
                        .claim_tool_call(&call.id, event(&admitted.run, EventKind::ToolCallRunning))
                        .await
                        .unwrap();
                }
                if matches!(
                    call_status,
                    ToolCallStatus::Completed | ToolCallStatus::Failed
                ) {
                    let mut message = request().user_message;
                    message.session_id = admitted.run.session_id.clone();
                    message.run_id = Some(admitted.run.id.clone());
                    message.role = Role::Tool;
                    let result = ToolResult {
                        status: if call_status == ToolCallStatus::Completed {
                            ToolResultStatus::Completed
                        } else {
                            ToolResultStatus::Failed
                        },
                        content: vec![ContentBlock::Text {
                            text: "retained".into(),
                        }],
                    };
                    message.parts.push(Part {
                        id: PartId::new(),
                        message_id: message.id.clone(),
                        ordinal: 0,
                        kind: PartKind::FunctionToolResult,
                        content: ContentBlock::ToolResult {
                            call_id: call.id.clone(),
                            content: result.content.clone(),
                            is_error: call_status == ToolCallStatus::Failed,
                        },
                    });
                    execution
                        .settle_tool_call(
                            &call.id,
                            result,
                            message,
                            event(&admitted.run, EventKind::ToolCallSettled),
                        )
                        .await
                        .unwrap();
                }
            }
            let mut run = admitted.run.clone();
            run.status = status;
            run.error = Some("retained diagnostic".into());
            run.usage = Usage {
                input_tokens: 123,
                output_tokens: 45,
            };
            // Checkpoint must be retained regardless of whether the host has the
            // executable configuration needed to interpret its continuation.
            run.checkpoint = Some(json!({"unavailable": "continuation"}));
            store.seed_run(run.clone()).await;
            for kind in [InboxKind::Steer, InboxKind::FollowUp] {
                let mut message = request().user_message;
                message.session_id = run.session_id.clone();
                store
                    .enqueue_inbox(&run.session_id, kind, message)
                    .await
                    .unwrap();
            }
            // Retain unrelated markers, including an old-owner-authored terminal
            // marker, but never let them shadow the newly rotated store evidence.
            for kind in [
                EventKind::Custom {
                    name: "history".into(),
                },
                EventKind::RunSettled,
            ] {
                let mut marker = event(&run, kind);
                marker.payload = json!({"abandonment_v1": {"request": abandon_request(&admitted, authority), "run": run, "interrupted_tools": []}});
                execution.append_event(marker).await.unwrap();
            }
            let original_calls = store.calls(&run.id).await;
            let original_messages = store.list_all_messages(&run.session_id).await.unwrap();
            let original_events = store.list_events(&run.session_id, None, 100).await.unwrap();
            let request = abandon_request(&admitted, authority);
            let mut wrong = request.clone();
            wrong.expected_owner = "replacement".into();
            assert_eq!(
                store.abandon_run(wrong).await.unwrap_err(),
                AbandonError::StaleOwner
            );
            let mut wrong = request.clone();
            wrong.expected.claim_token = "stale".into();
            assert_eq!(
                store.abandon_run(wrong).await.unwrap_err(),
                AbandonError::StaleOwner
            );
            let ordinary = abandon_request(&admitted, AbandonAuthority::ExpiredLease);
            assert_eq!(
                store.abandon_run(ordinary.clone()).await.unwrap_err(),
                AbandonError::LiveLease
            );
            clock.set(run.lease_until - time::Duration::nanoseconds(1));
            assert_eq!(
                store.abandon_run(ordinary).await.unwrap_err(),
                AbandonError::LiveLease
            );
            assert_eq!(store.get_run(&run.id).await.unwrap().unwrap(), run);
            assert_eq!(
                store.list_all_messages(&run.session_id).await.unwrap(),
                original_messages
            );
            assert_eq!(
                store.list_events(&run.session_id, None, 100).await.unwrap(),
                original_events
            );
            if authority == AbandonAuthority::ExpiredLease {
                clock.set(run.lease_until);
            }
            let mut snapshot_request = SnapshotRequest {
                session_id: run.session_id.clone(),
                limits: SnapshotLimits {
                    messages: 0,
                    tool_calls: 100,
                    parts: 100,
                    text_bytes: 100_000,
                    encoded_bytes: 1_000_000,
                },
                continuation: None,
            };
            let SnapshotOutcome::Limited { continuation, .. } =
                store.snapshot(snapshot_request.clone()).await.unwrap()
            else {
                panic!("capture snapshot continuation")
            };
            snapshot_request.continuation = Some(continuation);
            let outcome = store.abandon_run(request.clone()).await.unwrap();
            assert!(matches!(
                store.snapshot(snapshot_request.clone()).await.unwrap(),
                SnapshotOutcome::Invalidated { .. }
            ));
            assert_eq!(outcome.run.status, RunStatus::Interrupted);
            assert_ne!(outcome.run.claim_token, run.claim_token);
            assert_eq!(outcome.run.usage, run.usage);
            assert_eq!(outcome.run.error, run.error);
            assert_eq!(outcome.run.checkpoint, run.checkpoint);
            assert_eq!(outcome.interrupted_tools.len(), 2);
            let calls = store.calls(&run.id).await;
            let messages = store.list_all_messages(&run.session_id).await.unwrap();
            let events = store.list_events(&run.session_id, None, 100).await.unwrap();
            snapshot_request.continuation = None;
            snapshot_request.limits.messages = 100;
            let SnapshotOutcome::Page(page) = store.snapshot(snapshot_request).await.unwrap()
            else {
                panic!("fresh settled snapshot")
            };
            assert!(page.continuation.is_none());
            assert_eq!(page.messages, messages);
            let mut snapshot_calls = page.tool_calls;
            snapshot_calls.sort_by(|a, b| a.id.cmp(&b.id));
            assert_eq!(snapshot_calls, calls);
            assert_eq!(
                page.usage.encoded_bytes,
                messages
                    .iter()
                    .map(|m| serde_json::to_vec(m).unwrap().len())
                    .sum::<usize>()
                    + calls
                        .iter()
                        .map(|c| serde_json::to_vec(c).unwrap().len())
                        .sum::<usize>()
            );
            assert_eq!(&messages[..original_messages.len()], original_messages);
            assert_eq!(&events[..original_events.len()], original_events);
            assert_eq!(
                events
                    .iter()
                    .filter(|e| e.kind == EventKind::RunSettled)
                    .count(),
                2
            );
            assert_eq!(events.last().unwrap(), &outcome.terminal_event);
            for original in &original_calls {
                let call = calls.iter().find(|call| call.id == original.id).unwrap();
                if matches!(
                    original.status,
                    ToolCallStatus::Completed | ToolCallStatus::Failed
                ) {
                    assert_eq!(call, original);
                    continue;
                }
                assert_eq!(call.status, ToolCallStatus::Interrupted);
                let result = call.result.as_ref().unwrap();
                assert_eq!(result.status, ToolResultStatus::Interrupted);
                assert_eq!(messages.iter().flat_map(|m| &m.parts).filter(|p| matches!(&p.content, ContentBlock::ToolResult {call_id, content, is_error: true} if call_id == &call.id && content == &result.content)).count(), 1);
                assert_eq!(
                    events
                        .iter()
                        .filter(|e| e.kind == EventKind::ToolCallSettled
                            && e.correlation.as_deref() == Some(call.id.0.as_str())
                            && e.payload["status"] == "interrupted")
                        .count(),
                    1
                );
            }
            assert_stale(execution.as_ref(), &run, &outcome.interrupted_tools[0]).await;
            assert_eq!(
                store.execution(admitted.fence.clone()).await.err().unwrap(),
                StoreError::Conflict
            );
            assert_eq!(
                store.list_unfinished_tool_calls(&run.id).await.unwrap(),
                [] as [crabber_core::ToolCallRecord; 0]
            );
            assert_eq!(store.unconsumed_inbox(&run.session_id).await, 2);
            assert_eq!(
                store
                    .lookup_admission(&run.session_id, &options.key)
                    .await
                    .unwrap(),
                Some(receipt)
            );
            assert_eq!(store.abandon_run(request.clone()).await.unwrap(), outcome);
            for alteration in 0..3 {
                let mut wrong = request.clone();
                match alteration {
                    0 => wrong.expected_owner = "changed".into(),
                    1 => wrong.expected.claim_token = "changed".into(),
                    _ => {
                        wrong.authority = if authority == AbandonAuthority::ExpiredLease {
                            AbandonAuthority::HostStoppedOwner
                        } else {
                            AbandonAuthority::ExpiredLease
                        }
                    }
                }
                assert_eq!(
                    store.abandon_run(wrong).await.unwrap_err(),
                    AbandonError::StaleOwner
                );
            }
            assert_eq!(
                store.list_all_messages(&run.session_id).await.unwrap(),
                messages
            );
            assert_eq!(
                store.list_events(&run.session_id, None, 100).await.unwrap(),
                events
            );
            let mut next_request = self::request();
            next_request.session_id = Some(run.session_id.clone());
            next_request.user_message.session_id = run.session_id.clone();
            let next = store.admit_run(next_request).await.unwrap();
            assert_ne!(next.run.id, run.id);
            assert_eq!(store.abandon_run(request).await.unwrap(), outcome);
        }
    }
    let missing = AbandonRequest {
        expected: RunFence {
            run_id: RunId::new(),
            claim_token: "missing".into(),
        },
        expected_owner: "missing".into(),
        authority: AbandonAuthority::ExpiredLease,
    };
    assert_eq!(
        store.abandon_run(missing).await.unwrap_err(),
        AbandonError::NotFound
    );
    let admitted = store.admit_run(request()).await.unwrap();
    let execution = store.execution(admitted.fence.clone()).await.unwrap();
    execution
        .settle_run(
            RunStatus::Completed,
            None,
            Usage::default(),
            event(&admitted.run, EventKind::RunSettled),
        )
        .await
        .unwrap();
    assert_eq!(
        store
            .abandon_run(abandon_request(
                &admitted,
                AbandonAuthority::HostStoppedOwner
            ))
            .await
            .unwrap_err(),
        AbandonError::AlreadyTerminal
    );
}

#[allow(clippy::too_many_lines)]
pub(crate) async fn untrusted_terminal_contract<S: FixtureStore>(store: &S, clock: &ManualClock) {
    for terminal in [RunStatus::Interrupted, RunStatus::Completed] {
        for earlier_assertion in [false, true] {
            clock.set(time::OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap());
            let admitted = store.admit_run(request()).await.unwrap();
            let execution = store.execution(admitted.fence.clone()).await.unwrap();
            for running in [false, true] {
                let call = ToolCallRecord {
                    id: ToolCallId::new(),
                    run_id: admitted.run.id.clone(),
                    name: "unavailable".into(),
                    arguments: json!({}),
                    status: ToolCallStatus::Pending,
                    retry_safe: false,
                    result: None,
                };
                execution
                    .create_tool_call(
                        call.clone(),
                        event(&admitted.run, EventKind::ToolCallPending),
                    )
                    .await
                    .unwrap();
                if running {
                    execution
                        .claim_tool_call(&call.id, event(&admitted.run, EventKind::ToolCallRunning))
                        .await
                        .unwrap();
                }
            }
            let real_request = abandon_request(&admitted, AbandonAuthority::HostStoppedOwner);
            let mut earlier_request = real_request.clone();
            earlier_request.expected.claim_token = "arbitrary-earlier-fence".into();
            let fake_request = if earlier_assertion {
                earlier_request.clone()
            } else {
                real_request.clone()
            };
            let mut predicted = admitted.run.clone();
            predicted.status = terminal;
            predicted.error = None;
            predicted.usage = Usage::default();
            predicted.updated_at = clock.now();
            let mut marker = event(&admitted.run, EventKind::RunSettled);
            marker.payload = json!({"abandonment_v1": {"request": fake_request, "run": predicted, "interrupted_tools": []}});
            execution
                .settle_run(terminal, None, Usage::default(), marker)
                .await
                .unwrap();
            // This reproduces the public exploit exactly: the ordinary event's
            // snapshot is correct and its assertion may even name another token.
            assert_eq!(
                store.get_run(&admitted.run.id).await.unwrap().unwrap(),
                predicted
            );
            let calls = store.calls(&admitted.run.id).await;
            let messages = store
                .list_all_messages(&admitted.run.session_id)
                .await
                .unwrap();
            let events = store
                .list_events(&admitted.run.session_id, None, 100)
                .await
                .unwrap();
            for attempted in [real_request, earlier_request] {
                assert_eq!(
                    store.abandon_run(attempted).await.unwrap_err(),
                    AbandonError::AlreadyTerminal
                );
            }
            assert_eq!(
                store.get_run(&admitted.run.id).await.unwrap().unwrap(),
                predicted
            );
            assert_eq!(store.calls(&admitted.run.id).await, calls);
            assert_eq!(
                store
                    .list_unfinished_tool_calls(&admitted.run.id)
                    .await
                    .unwrap()
                    .len(),
                2
            );
            assert_eq!(
                store
                    .list_all_messages(&admitted.run.session_id)
                    .await
                    .unwrap(),
                messages
            );
            assert_eq!(
                store
                    .list_events(&admitted.run.session_id, None, 100)
                    .await
                    .unwrap(),
                events
            );
            assert_eq!(predicted.claim_token, admitted.fence.claim_token);
        }
    }
}
