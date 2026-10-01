//! Shared executable acceptance journey; imports only the public Crabber facade.
use crabber::{
    SnapshotContinuation, SnapshotLimit, SnapshotLimits, SnapshotOutcome, SnapshotPage,
    SnapshotRequest, SnapshotUsage,
    core::{
        ContentBlock, CoreError, EventCursor, EventKind, EventRecord, ManualClock, Message,
        MessageId, Part, PartId, PartKind, Role, RunId, SessionId, ToolCallId, ToolCallRecord,
        ToolCallStatus, ToolResult, ToolResultStatus,
    },
    session::{AdmitOutcome, AdmitRequest, MemoryStore, Store},
};
use std::{process::Command, sync::Arc, time::Duration};
use time::OffsetDateTime;

pub type Stores = (Arc<dyn Store>, Arc<dyn Store>);

pub fn memory() -> Stores {
    let store = MemoryStore::with_clock(Arc::new(ManualClock::new(OffsetDateTime::UNIX_EPOCH)));
    (Arc::new(store.clone()), Arc::new(store))
}

#[cfg(feature = "postgres")]
pub async fn postgres(migrate: bool) -> Stores {
    let url = std::env::var("CRABBER_TEST_POSTGRES_URL")
        .expect("CRABBER_TEST_POSTGRES_URL is required for PostgreSQL");
    if migrate {
        crabber::session::PostgresStore::migrate(&url)
            .await
            .unwrap();
    }
    let clock = Arc::new(ManualClock::new(OffsetDateTime::UNIX_EPOCH));
    // Separate connect calls create independent pools, not Store clones.
    let reader = crabber::session::PostgresStore::connect(&url)
        .await
        .unwrap()
        .with_clock(clock.clone());
    let writer = crabber::session::PostgresStore::connect(&url)
        .await
        .unwrap()
        .with_clock(clock);
    (Arc::new(reader), Arc::new(writer))
}

pub fn request(session: &SessionId) -> SnapshotRequest {
    SnapshotRequest {
        session_id: session.clone(),
        limits: SnapshotLimits {
            messages: 7,
            tool_calls: 1,
            parts: 7,
            text_bytes: 4096,
            encoded_bytes: 8192,
        },
        continuation: None,
    }
}

fn message(
    session: &SessionId,
    run: Option<RunId>,
    id: &str,
    role: Role,
    content: ContentBlock,
) -> Message {
    let id = MessageId::from(format!("{session}:{id}"));
    Message {
        id: id.clone(),
        session_id: session.clone(),
        run_id: run,
        role,
        parent_id: None,
        parts: vec![Part {
            id: PartId::from(format!("{id}-part")),
            message_id: id,
            ordinal: 0,
            kind: PartKind::AssistantText,
            content,
        }],
        created_at: OffsetDateTime::UNIX_EPOCH,
    }
}

fn text(session: &SessionId, run: Option<RunId>, id: &str, value: &str) -> Message {
    message(
        session,
        run,
        id,
        Role::Assistant,
        ContentBlock::Text { text: value.into() },
    )
}

fn event(session: &SessionId, run: &RunId, index: usize) -> EventRecord {
    EventRecord {
        cursor: None,
        session_id: session.clone(),
        run_id: run.clone(),
        turn_id: None,
        kind: EventKind::ToolCallSettled,
        payload: serde_json::json!({"fixture_index":index}),
        correlation: None,
        live_only: false,
        created_at: OffsetDateTime::UNIX_EPOCH,
    }
}

async fn admit(store: &dyn Store, session: &SessionId, first: &str) -> AdmitOutcome {
    store
        .admit_run(AdmitRequest {
            session_id: None,
            workspace_id: "fixture".into(),
            directory: "fixture".into(),
            title: "bounded fixture".into(),
            user_message: message(
                session,
                None,
                "user",
                Role::User,
                ContentBlock::Text { text: first.into() },
            ),
            config_hash: "fixture".into(),
            plan_fingerprint: "fixture".into(),
            owner: "fixture".into(),
            lease: Duration::from_secs(600),
        })
        .await
        .unwrap()
}

fn page(outcome: SnapshotOutcome) -> SnapshotPage {
    let SnapshotOutcome::Page(page) = outcome else {
        panic!("expected bounded page")
    };
    page
}

fn text_bytes(block: &ContentBlock) -> usize {
    match block {
        ContentBlock::Text { text } | ContentBlock::Reasoning { text, .. } => text.len(),
        ContentBlock::ToolResult { content, .. } => content.iter().map(text_bytes).sum(),
        _ => 0,
    }
}

pub fn check_page(page: &SnapshotPage, query: &SnapshotRequest) {
    let mut usage = SnapshotUsage::default();
    for message in &page.messages {
        usage.messages += 1;
        usage.parts += message.parts.len();
        usage.text_bytes += message
            .parts
            .iter()
            .map(|part| text_bytes(&part.content))
            .sum::<usize>();
        usage.encoded_bytes += serde_json::to_vec(message).unwrap().len();
    }
    for call in &page.tool_calls {
        usage.tool_calls += 1;
        usage.text_bytes += call
            .result
            .as_ref()
            .map_or(0, |result| result.content.iter().map(text_bytes).sum());
        usage.encoded_bytes += serde_json::to_vec(call).unwrap().len();
    }
    assert_eq!(page.usage, usage);
    assert!(usage.messages <= query.limits.messages);
    assert!(usage.tool_calls <= query.limits.tool_calls);
    assert!(usage.parts <= query.limits.parts);
    assert!(usage.text_bytes <= query.limits.text_bytes);
    assert!(usage.encoded_bytes <= query.limits.encoded_bytes);
    if let Some(token) = &page.continuation {
        assert!(token.0.len() <= 2048);
    }
}

pub fn print_page(
    source: &Source,
    backend: &str,
    session: &SessionId,
    number: usize,
    page: &SnapshotPage,
) {
    println!(
        "source={source} backend={backend} session={session} page={number} H={} messages={} tool_calls={} parts={} text_bytes={} encoded_bytes={}",
        page.high_water.0,
        page.usage.messages,
        page.usage.tool_calls,
        page.usage.parts,
        page.usage.text_bytes,
        page.usage.encoded_bytes
    );
}

/// Resolve the invocation checkout at runtime. A reused binary must not silently
/// label itself using an embedded `CARGO_MANIFEST_DIR` from a different worktree.
pub struct Source {
    sha: String,
    clean: bool,
}

impl std::fmt::Display for Source {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} source_clean={}", self.sha, self.clean)
    }
}

pub fn source(require_clean: bool) -> Source {
    let output = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .unwrap();
    assert!(output.status.success(), "invoke inside the source checkout");
    let sha = String::from_utf8(output.stdout).unwrap().trim().to_owned();
    assert_eq!(sha.len(), 40);
    let status = Command::new("git")
        .args(["status", "--porcelain", "--untracked-files=normal"])
        .output()
        .unwrap();
    assert!(status.status.success());
    let clean = status.stdout.is_empty();
    assert!(
        !require_clean || clean,
        "commit the invocation checkout before producing source evidence"
    );
    // The invocation must be the workspace containing this public example.
    let root = Command::new("git")
        .args(["rev-parse", "--show-toplevel"])
        .output()
        .unwrap();
    let root = String::from_utf8(root.stdout).unwrap();
    assert!(
        std::path::Path::new(root.trim())
            .join("examples/bounded-snapshot/src/journey.rs")
            .is_file()
    );
    Source { sha, clean }
}

/// Test helper and demo differ only in their child entrypoint.
#[derive(Clone, Copy)]
pub enum ChildMode {
    None,
    Demo(bool),
    Test,
}

fn verify_child(
    query: &SnapshotRequest,
    h: EventCursor,
    expected: &[MessageId],
    expected_events: &[EventCursor],
    mode: ChildMode,
) {
    let mut command = Command::new(std::env::current_exe().unwrap());
    match mode {
        ChildMode::None => return,
        ChildMode::Demo(check) => {
            command.arg("--postgres-child");
            if check {
                command.env("CRABBER_BOUNDED_CHECK", "1");
            }
        }
        ChildMode::Test => {
            command.env("CRABBER_BOUNDED_CHECK", "1");
            command.args(["--exact", "postgres_snapshot_child_process", "--nocapture"]);
        }
    }
    let output = command
        .env(
            "CRABBER_BOUNDED_CHILD",
            serde_json::to_string(query).unwrap(),
        )
        .output()
        .unwrap();
    assert!(output.status.success(), "fresh-process snapshot failed");
    let stdout = String::from_utf8(output.stdout).unwrap();
    let report = stdout
        .lines()
        .find_map(|line| line.strip_prefix("child_report="))
        .expect("child report");
    let report: serde_json::Value = serde_json::from_str(report).unwrap();
    assert_eq!(report["H"], h.0);
    assert_eq!(report["messages"], serde_json::to_value(expected).unwrap());
    assert_eq!(
        report["postH"],
        serde_json::to_value(expected_events.iter().map(|id| id.0).collect::<Vec<_>>()).unwrap()
    );
    println!(
        "fresh_process=true H={} returned_messages={} postH_ids={:?}",
        h.0,
        expected.len(),
        expected_events.iter().map(|id| id.0).collect::<Vec<_>>()
    );
}

#[cfg(feature = "postgres")]
pub async fn child() {
    let input = std::env::var("CRABBER_BOUNDED_CHILD").expect("child request required");
    let mut query: SnapshotRequest = serde_json::from_str(&input).unwrap();
    assert!(query.continuation.is_some());
    let (reader, _) = postgres(false).await; // connect only: no migration in child
    let mut ids = Vec::new();
    let mut h = None;
    let mut number = 1;
    loop {
        let page = page(reader.snapshot(query.clone()).await.unwrap());
        check_page(&page, &query);
        assert_eq!(*h.get_or_insert(page.high_water), page.high_water);
        print_page(
            &source(std::env::var("CRABBER_BOUNDED_CHECK").as_deref() != Ok("1")),
            "postgres-child",
            &query.session_id,
            number,
            &page,
        );
        ids.extend(page.messages.into_iter().map(|message| message.id));
        let Some(token) = page.continuation else {
            break;
        };
        query.continuation = Some(token);
        number += 1;
    }
    let events = tail(&*reader, &query.session_id, h.unwrap()).await;
    println!(
        "child_report={}",
        serde_json::json!({"H":h.unwrap().0,"messages":ids,"postH":events.iter().map(|id| id.0).collect::<Vec<_>>()})
    );
}

async fn tail(reader: &dyn Store, session: &SessionId, h: EventCursor) -> Vec<EventCursor> {
    let mut after = h;
    let mut ids = Vec::new();
    loop {
        let events = reader.list_events(session, Some(after), 2).await.unwrap();
        if events.is_empty() {
            break;
        }
        for event in events {
            let id = event.cursor.unwrap();
            assert!(id > after && id > h);
            after = id;
            ids.push(id);
        }
    }
    ids
}

#[allow(clippy::too_many_lines)]
pub async fn journey(stores: Stores, backend: &str, source: &Source, mode: ChildMode) {
    let (reader, writer) = stores;
    let session = SessionId::from(format!("bounded-{backend}-{}", std::process::id()));
    let admitted = admit(&*writer, &session, "fake").await;
    let run = admitted.run.id.clone();
    let execution = writer.execution(admitted.fence.clone()).await.unwrap();
    // Large fixture; expectation derives solely from construction, never from an
    // unbounded Store read. The host retains only IDs while walking pages.
    let mut expected = vec![MessageId::from(format!("{session}:user"))];
    for index in 0..1024 {
        let id = format!("fake-{index}");
        execution
            .append_message(text(&session, Some(run.clone()), &id, "é fake reasoning"))
            .await
            .unwrap();
        expected.push(MessageId::from(format!("{session}:{id}")));
    }
    let call = ToolCallId::from(format!("{session}:fake-call"));
    let call_message = message(
        &session,
        Some(run.clone()),
        "call-message",
        Role::Assistant,
        ContentBlock::ToolCall {
            call_id: call.clone(),
            name: "fake".into(),
            arguments: serde_json::json!({"opaque":[1,2]}),
        },
    );
    execution
        .append_message(call_message.clone())
        .await
        .unwrap();
    execution
        .create_tool_call(
            ToolCallRecord {
                id: call.clone(),
                run_id: run.clone(),
                name: "fake".into(),
                arguments: serde_json::json!({"opaque":[1,2]}),
                status: ToolCallStatus::Pending,
                retry_safe: true,
                result: None,
            },
            event(&session, &run, 0),
        )
        .await
        .unwrap();
    execution
        .claim_tool_call(&call, event(&session, &run, 1))
        .await
        .unwrap();
    let result = ToolResult {
        status: ToolResultStatus::Completed,
        content: vec![
            ContentBlock::Reasoning {
                text: "é nested result".into(),
                provider_state: None,
            },
            ContentBlock::ToolResult {
                call_id: call.clone(),
                content: vec![ContentBlock::Text {
                    text: "recursive".into(),
                }],
                is_error: false,
            },
        ],
    };
    let mut result_message = message(
        &session,
        Some(run.clone()),
        "result-message",
        Role::Tool,
        ContentBlock::ToolResult {
            call_id: call.clone(),
            content: result.content.clone(),
            is_error: false,
        },
    );
    result_message.parent_id = Some(call_message.id.clone());
    execution
        .settle_tool_call(
            &call,
            result.clone(),
            result_message.clone(),
            event(&session, &run, 2),
        )
        .await
        .unwrap();
    expected.extend([call_message.id.clone(), result_message.id.clone()]);
    let mut query = request(&session);
    query.limits.messages = 0;
    let SnapshotOutcome::Limited {
        limit,
        continuation,
        high_water: h,
    } = reader.snapshot(query).await.unwrap()
    else {
        panic!("explicit limit")
    };
    assert_eq!(limit, SnapshotLimit::Messages);
    assert!(continuation.0.len() <= 2048);
    println!(
        "source={source} backend={backend} session={session} outcome=Limited limit=Messages H={} retry=same_position",
        h.0
    );
    let mut query = request(&session);
    query.continuation = Some(continuation);
    let mut observed = Vec::new();
    let mut observed_calls = Vec::new();
    let mut saw_call = false;
    let mut saw_result = false;
    let mut post_events = Vec::new();
    let mut number = 0;
    loop {
        let current = page(reader.snapshot(query.clone()).await.unwrap());
        assert_eq!(current.high_water, h);
        check_page(&current, &query);
        print_page(source, backend, &session, number, &current);
        saw_call |= current.messages.contains(&call_message);
        saw_result |= current.messages.contains(&result_message);
        observed.extend(current.messages.into_iter().map(|message| message.id));
        observed_calls.extend(current.tool_calls);
        let Some(token) = current.continuation else {
            break;
        };
        query.continuation = Some(token);
        if number == 0 {
            let writer = writer.clone();
            let fence = admitted.fence.clone();
            let session = session.clone();
            let run = run.clone();
            // Concurrent reader and independent writer: appends may land while a
            // page is being read. Cutoffs and H must survive either scheduling.
            let append = tokio::spawn(async move {
                let execution = writer.execution(fence).await.unwrap();
                let mut expected_events = Vec::new();
                for index in 0..5 {
                    execution
                        .append_message(text(
                            &session,
                            Some(run.clone()),
                            &format!("post-{index}"),
                            "fake append",
                        ))
                        .await
                        .unwrap();
                    execution
                        .append_event(event(&session, &run, 100 + index))
                        .await
                        .unwrap();
                    let mut metadata = request(&session);
                    metadata.limits.messages = 0;
                    let SnapshotOutcome::Limited { high_water, .. } =
                        writer.snapshot(metadata).await.unwrap()
                    else {
                        panic!("writer boundary")
                    };
                    expected_events.push(high_water);
                }
                expected_events
            });
            let (probe, joined) = tokio::join!(reader.snapshot(query.clone()), append);
            post_events = joined.unwrap();
            let probe = page(probe.unwrap());
            assert_eq!(probe.high_water, h);
            check_page(&probe, &query);
            // The writer captured each committed cursor using bounded metadata
            // outcomes. The host has not consumed any tail events yet.
            verify_child(&query, h, &expected[observed.len()..], &post_events, mode);
        }
        number += 1;
    }
    assert_eq!(observed, expected);
    assert_eq!(observed_calls.len(), 1);
    assert_eq!(observed_calls[0].id, call);
    assert_eq!(observed_calls[0].status, ToolCallStatus::Completed);
    assert_eq!(observed_calls[0].result, Some(result));
    assert!(saw_call && saw_result);
    // The host starts consuming only once the final page has no continuation.
    assert_eq!(tail(&*reader, &session, h).await, post_events);
    let mut after = h;
    for index in 0..5 {
        let events = reader.list_events(&session, Some(after), 1).await.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(
            events[0].payload,
            serde_json::json!({"fixture_index":100 + index})
        );
        after = events[0].cursor.unwrap();
    }
    println!(
        "source={source} backend={backend} session={session} complete=true H={} pages={} messages={} calls=1 postH_ids={:?}",
        h.0,
        number + 1,
        observed.len(),
        post_events.iter().map(|id| id.0).collect::<Vec<_>>()
    );
    // Mutation invalidates a continuation; discard/restart instead of combining.
    let before = page(reader.snapshot(request(&session)).await.unwrap());
    execution
        .append_part(Part {
            id: PartId::from(format!("{session}:changed-part")),
            message_id: MessageId::from(format!("{session}:fake-0")),
            ordinal: 1,
            kind: PartKind::AssistantText,
            content: ContentBlock::Text {
                text: "changed".into(),
            },
        })
        .await
        .unwrap();
    let mut next = request(&session);
    next.continuation = before.continuation;
    assert!(
        matches!(reader.snapshot(next).await.unwrap(), SnapshotOutcome::Invalidated { high_water } if high_water == before.high_water)
    );
    assert!(matches!(
        reader.snapshot(request(&session)).await.unwrap(),
        SnapshotOutcome::Page(_)
    ));
    assert!(matches!(
        reader
            .snapshot(request(&SessionId::from("missing-bounded-session")))
            .await,
        Err(CoreError::NotFound)
    ));
    let mut bad = request(&session);
    bad.continuation = Some(SnapshotContinuation("invalid".into()));
    assert!(matches!(
        reader.snapshot(bad).await,
        Err(CoreError::Validation(_))
    ));
    println!(
        "source={source} backend={backend} session={session} invalidated=restart errors=NotFound,Validation"
    );
}

/// Measure only reads on a single runtime thread, after fixture writes. Covers
/// 512 KiB and 32 MiB histories and oversized first/next records for both byte
/// budgets. No whole-history oracle or allocator instrumentation in server SQL.
#[allow(clippy::too_many_lines)]
pub fn allocation_proof(
    runtime: &tokio::runtime::Runtime,
    stores: &Stores,
    backend: &str,
    source: &Source,
) {
    let (reader, writer) = stores;
    let mut peaks = Vec::new();
    for count in [2, 128] {
        let session = SessionId::from(format!(
            "bounded-alloc-{backend}-{count}-{}",
            std::process::id()
        ));
        runtime.block_on(async {
            let admitted = admit(&**writer, &session, "small").await;
            let execution = writer.execution(admitted.fence).await.unwrap();
            let value = "x".repeat(262_144);
            for index in 0..count {
                execution
                    .append_message(text(
                        &session,
                        Some(admitted.run.id.clone()),
                        &format!("large-{index}"),
                        &value,
                    ))
                    .await
                    .unwrap();
            }
        });
        let mut q = request(&session);
        q.limits.encoded_bytes = 4096;
        // Warm the SQL driver/runtime independently of the measured read.
        let _ = runtime.block_on(reader.snapshot(q.clone())).unwrap();
        let mut next = None;
        let measured = allocation_counter::measure(|| {
            let first = page(runtime.block_on(reader.snapshot(q.clone())).unwrap());
            assert_eq!(first.messages.len(), 1);
            next = first.continuation;
        });
        assert!(
            measured.bytes_max < 131_072,
            "bounded read allocated history: {measured:?}"
        );
        peaks.push(measured.bytes_max);
        for limit in [SnapshotLimit::EncodedBytes, SnapshotLimit::TextBytes] {
            q.continuation.clone_from(&next);
            q.limits.text_bytes = if limit == SnapshotLimit::TextBytes {
                4096
            } else {
                1_000_000
            };
            q.limits.encoded_bytes = if limit == SnapshotLimit::EncodedBytes {
                4096
            } else {
                1_000_000
            };
            let measured = allocation_counter::measure(|| {
                assert!(
                    matches!(runtime.block_on(reader.snapshot(q.clone())).unwrap(),
                    SnapshotOutcome::Limited { limit: actual, .. } if actual == limit)
                );
            });
            assert!(
                measured.bytes_max < 131_072,
                "oversized next payload fetched/cloned: {measured:?}"
            );
            println!(
                "source={source} backend={backend} session={session} allocation=next limit={limit:?} history_bytes={} peak_client_bytes={}",
                count * 262_144,
                measured.bytes_max
            );
        }
    }
    assert!(
        peaks[1] <= peaks[0] + 65_536,
        "read allocation grew with history: {peaks:?}"
    );
    let session = SessionId::from(format!(
        "bounded-alloc-first-{backend}-{}",
        std::process::id()
    ));
    runtime.block_on(admit(&**writer, &session, &"x".repeat(262_144)));
    for limit in [SnapshotLimit::EncodedBytes, SnapshotLimit::TextBytes] {
        let mut q = request(&session);
        q.limits.text_bytes = if limit == SnapshotLimit::TextBytes {
            4096
        } else {
            1_000_000
        };
        q.limits.encoded_bytes = if limit == SnapshotLimit::EncodedBytes {
            4096
        } else {
            1_000_000
        };
        let measured = allocation_counter::measure(|| {
            assert!(
                matches!(runtime.block_on(reader.snapshot(q)).unwrap(), SnapshotOutcome::Limited { limit: actual, .. } if actual == limit)
            );
        });
        assert!(
            measured.bytes_max < 131_072,
            "oversized first payload fetched/cloned: {measured:?}"
        );
        println!(
            "source={source} backend={backend} session={session} allocation=first limit={limit:?} peak_client_bytes={}",
            measured.bytes_max
        );
    }
    println!(
        "source={source} backend={backend} history_sizes=524288,33554432 first_page_peak_client_bytes={peaks:?}"
    );
}
